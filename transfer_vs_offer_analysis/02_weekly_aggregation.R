# ============================================================================
# 1. CREATE WEEKLY SNAPSHOTS OF OFFERS
# ============================================================================

# Create a list of all Mondays from 2022 onwards
weekly_dates <- seq.Date(
  from = as.Date("2022-01-03"),  # First Monday in 2022
  to = Sys.Date(),               # Today
  by = "week"                    # Every 7 days
)

# Get list of all unique retailers
retailers <- unique(offers_res_elec$retailer)

# --- FUNCTION: Calculate what offers a retailer had on a specific date ---
calculate_offer_metrics <- function(offers_data, target_date, retailer_name) {
  
  # Step 1: Find all offers that were active on this date
  # An offer is "active" if:
  #   - It started on or before this date
  #   - It hasn't ended yet, OR it ended after this date
  active_offers <- offers_data %>%
    filter(
      retailer == retailer_name,
      start_date <= target_date,
      (is.na(end_date) | end_date >= target_date)
    )
  
  # Step 2: If this retailer had no active offers, return empty data
  if (nrow(active_offers) == 0) {
    return(data.frame(
      date = target_date,
      retailer = retailer_name,
      n_offers = 0,
      median_price = NA,
      min_price = NA,
      max_price = NA,
      pct_guaranteed_discount = NA,
      pct_conditional_discount = NA,
      pct_no_discount = NA,
      avg_contract_term = NA,
      pct_solar_available = NA,
      pct_with_incentive = NA,
      avg_annual_fee = NA,
      avg_late_fee = NA,
      cprg_retailer_size = NA
    ))
  }
  
  # Step 3: Calculate the Victorian market average for this date
  vic_benchmark <- offers_data %>%
    filter(
      start_date <= target_date,
      (is.na(end_date) | end_date >= target_date)
    ) %>%
    summarise(vic_median = median(total_bill_pre_transaction, na.rm = TRUE)) %>%
    pull(vic_median)
  
  # Step 4: Calculate metrics for this specific retailer
  metrics <- active_offers %>%
    summarise(
      date = target_date,
      retailer = retailer_name,
      
      n_offers = n(),
      
      # PRICING METRICS
      median_price = median(total_bill_pre_transaction, na.rm = TRUE),
      min_price = min(total_bill_pre_transaction, na.rm = TRUE),
      max_price = max(total_bill_pre_transaction, na.rm = TRUE),
      
      # DISCOUNT TYPE METRICS (as percentages)
      pct_guaranteed_discount = mean(offer_category == "Market offers with\nguaranteed discounts", na.rm = TRUE) * 100,
      pct_conditional_discount = mean(offer_category == "Market offers with\nconditional discounts", na.rm = TRUE) * 100,
      pct_no_discount = mean(offer_category == "Market offers without\ndiscounts", na.rm = TRUE) * 100,
      
      # CONTRACT TERM
      # Convert contract terms to numbers (blank/"none" = 0, "1 year" = 1, etc.)
      avg_contract_term = mean(case_when(
        is.na(contract_term) | contract_term %in% c("", "None", "Other") ~ 0,
        contract_term == "1 year" ~ 1,
        contract_term == "2 year" ~ 2,
        TRUE ~ 0
      ), na.rm = TRUE),
      
      # SOLAR
      # What % of offers are available for solar customers?
      pct_solar_available = mean(solar_offer %in% c("Yes", "Optional"), na.rm = TRUE) * 100,
      
      # INCENTIVES
      # What % of offers include an incentive (bonus, credit, etc.)?
      pct_with_incentive = mean(has_incentive == "Yes", na.rm = TRUE) * 100,
      
      # FEES (calculate averages)
      avg_annual_fee = mean(total_annual_fee, na.rm = TRUE),
      avg_late_fee = mean(total_late_payment_fee, na.rm = TRUE),
      
      # Add the Victorian market average
      vic_median = vic_benchmark,
      
      cprg_retailer_size = first(cprg_retailer_size)
    ) %>%
    mutate(
      # Calculate how much more/less expensive than market average
      price_vs_market = ((median_price - vic_median) / vic_median) * 100,
      
      # yes/no: are they more expensive than average?
      is_above_market = median_price > vic_median
    )
  
  return(metrics)
}

# --- NOW: Create snapshots for every retailer for every week ---
# This will take a few minutes because we're processing thousands of combinations

message("Creating weekly offer snapshots... this may take a few minutes!")

# Create a grid of all combinations: every week × every retailer
# Then calculate metrics for each combination
offer_snapshots <- expand.grid(
  date = weekly_dates,
  retailer = retailers,
  stringsAsFactors = FALSE
) %>%
  # For each row, calculate the metrics
  rowwise() %>%
  do({
    calculate_offer_metrics(offers_res_elec, .$date, .$retailer)
  }) %>%
  ungroup()

# Add a "competitiveness rank" for each week
# Rank 1 = cheapest retailer that week, higher rank = more expensive
offer_snapshots <- offer_snapshots %>%
  group_by(date) %>%
  mutate(
    price_competitiveness_rank = rank(median_price, ties.method = "average")
  ) %>%
  ungroup()

message("✓ Offer snapshots created!")

# ============================================================================
# 2. AGGREGATE TRANSFERS TO WEEKLY LEVEL
# ============================================================================

transfers_weekly <- transfers_daily %>%
  # Round each date down to the Monday of that week
  mutate(
    week_start = floor_date(transfer_date, "week", week_start = 1)
  ) %>%
  # Group by week and retailer
  group_by(week_start, Year, Month, from_retailer) %>%
  summarise(
    # Count total transfers for the week
    total_transfers_out = sum(transfers_count),
    # Calculate average per day
    avg_daily_transfers = mean(transfers_count),
    .groups = "drop"
  ) %>%
  # Rename to match offer_snapshots
  rename(
    date = week_start,
    retailer = from_retailer
  ) %>%
  # Join with customer base using Year and Month
  left_join(customer_base, by = c("Year", "Month", "retailer")) %>%
  mutate(
    #calculate normalised transfer rates
    transfer_rate = if_else(
      !is.na(customer_count) & customer_count > 0,
      (total_transfers_out / customer_count) * 100,
      NA
    ),
    # Also calculate average daily transfer rate
    avg_daily_transfer_rate = ifelse(
      !is.na(customer_count) & customer_count > 0,
      (avg_daily_transfers / customer_count) * 100,
      NA
    )
  )

# ============================================================================
# 3. CREATE TIME-LAGGED DATASETS
# ============================================================================
# When a retailer changes prices, customers might not switch immediately
# Test different lag periods (1 week, 2 weeks, 4 weeks, 8 weeks)

# --- FUNCTION: Create a dataset with a specific time lag ---
create_lagged_dataset <- function(offers, transfers, lag_weeks) {
  
  # Shift the offer dates forward by X weeks
  # Example: An offer from Jan 1 gets moved to Jan 8 (if lag = 1 week)
  # This lets us test if Jan 1 offers affected Jan 8 transfers
  offers_lagged <- offers %>%
    mutate(date = date + weeks(lag_weeks)) %>%

    rename_with(~paste0(., "_lag", lag_weeks, "w"), 
                -c(date, retailer))
  
  # Join lagged offers with transfers
  combined <- transfers %>%
    left_join(offers_lagged, by = c("date", "retailer"))
  
  return(combined)
}

# Create datasets with different time lags
# Lag 0 = same week (immediate effect)
# Lag 1 = 1 week later
# Lag 2 = 2 weeks later, etc.

message("Creating lagged datasets...")

combined_lag0 <- transfers_weekly %>%
  left_join(offer_snapshots, by = c("date", "retailer"))

combined_lag1 <- create_lagged_dataset(offer_snapshots, transfers_weekly, 1)
combined_lag2 <- create_lagged_dataset(offer_snapshots, transfers_weekly, 2)
combined_lag4 <- create_lagged_dataset(offer_snapshots, transfers_weekly, 4)
combined_lag8 <- create_lagged_dataset(offer_snapshots, transfers_weekly, 8)

# Combine all lags into ONE master dataset
# Each row has: transfers + current offers + lagged offers (1w, 2w, 4w, 8w ago)
analysis_data <- combined_lag0 %>%
  left_join(
    combined_lag1 %>% select(date, retailer, ends_with("_lag1w")),
    by = c("date", "retailer")
  ) %>%
  left_join(
    combined_lag2 %>% select(date, retailer, ends_with("_lag2w")),
    by = c("date", "retailer")
  ) %>%
  left_join(
    combined_lag4 %>% select(date, retailer, ends_with("_lag4w")),
    by = c("date", "retailer")
  ) %>%
  left_join(
    combined_lag8 %>% select(date, retailer, ends_with("_lag8w")),
    by = c("date", "retailer")
  )

# REMOVE RETAILERS WITH LESS THAN 100 WEEKS OF DATA

retailer_weeks <- analysis_data %>%
  group_by(retailer) %>%
  summarise(
    n_weeks = n(),
    .groups = "drop"
  )

retailers_to_keep <- retailer_weeks %>%
  filter(n_weeks >= 100) %>%
  pull(retailer)

analysis_data <- analysis_data %>%
  filter(retailer %in% retailers_to_keep,
         !is.na(customer_count))

message("✓ Master dataset created!")
message(paste("✓ Remaining retailers:", length(unique(analysis_data$retailer))))
