# ============================================================================
# OFFERS & TRANSFERS CORRELATION ANALYSIS - EXTENDED VERSION
# ============================================================================
#
# PURPOSE:
# Understand what makes customers switch energy retailers by analyzing the
# relationship between offer characteristics and actual transfer data.
#
# Eg. If a retailer has high prices or bad contract terms,
# do more customers leave? How long does it take for them to switch?
#
# ============================================================================

# ============================================================================
# ANALYSIS 1: SEGMENTED ANALYSIS BY PRICE DIFFERENCE
# ============================================================================

# Create grouping for retailer price vs market price

message("\n=== ANALYSIS 1: SEGMENTED BY PRICE DIFFERENCE (WITH LAGS) ===\n")

# --- FUNCTION: Create segmented analysis for a specific lag ---
# This function groups retailers by price position and calculates avg transfers
create_segmented_analysis <- function(data, price_column, lag_label) {
  
  # Check if the price column exists
  if (!price_column %in% names(data)) {
    return(NULL)
  }
  
  # Create price groups and calculate averages
  result <- data %>%
    filter(!is.na(.data[[price_column]])) %>%
    mutate(
      # Categorize each retailer-week into a price group
      price_group = case_when(
        .data[[price_column]] > 10 ~ "Much higher (>10%)",
        .data[[price_column]] > 5 ~ "Moderately higher (5-10%)",
        .data[[price_column]] > -5 ~ "Near average (±5%)",
        .data[[price_column]] > -10 ~ "Moderately lower (5-10%)",
        TRUE ~ "Much lower (<-10%)"
      ),
      # Convert to factor to control order in charts
      price_group = factor(price_group, levels = c(
        "Much lower (<-10%)",
        "Moderately lower (5-10%)",
        "Near average (±5%)",
        "Moderately higher (5-10%)",
        "Much higher (>10%)"
      ))
    ) %>%
    # Calculate average transfers for each price group
    group_by(price_group) %>%
    summarise(
      avg_weekly_transfer_rate = mean(transfer_rate, na.rm = TRUE),
      median_weekly_transfers = median(transfer_rate, na.rm = TRUE),
      n_retailer_weeks = n(),
      .groups = "drop"
    ) %>%
    # Add a column to identify which lag this is
    mutate(lag = lag_label)
  
  return(result)
}

# Create segmented analysis for each lag period

segmented_lag0 <- create_segmented_analysis(analysis_data, "price_vs_market", "Current Week (Lag 0)")
segmented_lag2w <- create_segmented_analysis(analysis_data, "price_vs_market_lag2w", "2 Weeks Ago (Lag 2w)")
segmented_lag4w <- create_segmented_analysis(analysis_data, "price_vs_market_lag4w", "4 Weeks Ago (Lag 4w)")
segmented_lag8w <- create_segmented_analysis(analysis_data, "price_vs_market_lag8w", "8 Weeks Ago (Lag 8w)")

# Combine all lag results into one data set
segmented_all_lags <- bind_rows(
  segmented_lag0,
  segmented_lag2w,
  segmented_lag4w,
  segmented_lag8w
)

segmented_all_lags <- segmented_all_lags %>%
  mutate(
    lag = factor(lag, levels = c(
      "Current Week (Lag 0)",
      "2 Weeks Ago (Lag 2w)",
      "4 Weeks Ago (Lag 4w)",
      "8 Weeks Ago (Lag 8w)"
    ))
  )

# Print results for each lag
cat("\n--- CURRENT WEEK PRICING (Lag 0) ---\n")
print(segmented_lag0 %>% select(price_group, avg_weekly_transfer_rate, n_retailer_weeks))

cat("\n--- 2 WEEKS AGO PRICING (Lag 2w) ---\n")
print(segmented_lag2w %>% select(price_group, avg_weekly_transfer_rate, n_retailer_weeks))

cat("\n--- 4 WEEKS AGO PRICING (Lag 4w) ---\n")
print(segmented_lag4w %>% select(price_group, avg_weekly_transfer_rate, n_retailer_weeks))

cat("\n--- 8 WEEKS AGO PRICING (Lag 8w) ---\n")
print(segmented_lag8w %>% select(price_group, avg_weekly_transfer_rate, n_retailer_weeks))

# --- Create comparison chart showing all lags side-by-side ---
plot_segmented_lags <- plot_ly(
  data = segmented_all_lags,
  x = ~price_group,
  y = ~avg_weekly_transfer_rate,
  color = ~lag,
  type = "bar",
  colors = c("#236192", "#4986a0", "#75787b", "#183028"),
  text = ~paste0(round(avg_weekly_transfer_rate, 2), "%"),
  textposition = "outside"
) %>%
  layout(
    title = "<b>Average Weekly Transfer Rate by Retailer Price Position</b>",
    xaxis = list(title = "Retailer vs Market Median Offer Price"),
    yaxis = list(title = "Average Weekly Transfer Rate (%)"),
    margin = list(t = 110),
    barmode = "group",
    legend = list(
      orientation = "h", 
      y = 1.15, 
      x = 0.5, 
      xanchor = "center",
      font = list(size = 10)
    )
  )

print(plot_segmented_lags)

# --- Create a "difference from average" chart ---
# This shows how much MORE (or LESS) each price group loses compared to average
# Useful for seeing the "penalty" of being expensive

segmented_differences <- segmented_all_lags %>%
  group_by(lag) %>%
  mutate(
    # Calculate the difference from the "Near average" group
    avg_baseline = avg_weekly_transfer_rate[price_group == "Near average (±5%)"],
    difference_from_baseline = avg_weekly_transfer_rate - avg_baseline,
    pct_difference = (difference_from_baseline / avg_baseline) * 100
  ) %>%
  ungroup()

plot_segmented_differences <- plot_ly(
  data = segmented_differences,
  x = ~price_group,
  y = ~difference_from_baseline,
  color = ~lag,
  type = "bar",
  colors = c("#236192", "#4986a0", "#75787b", "#183028")
) %>%
  layout(
    title = "<b>Difference in Transfer Rates Compared to Median (for each Price Group)</b><br>",
    xaxis = list(title = "Price vs Market Average"),
    yaxis = list(title = "Retailer vs Market Median Offer Price"),
    margin = list(t = 110),
    barmode = "group",
    legend = list(
      orientation = "h", 
      y = 1.15, 
      x = 0.5, 
      xanchor = "center",
      font = list(size = 10)
    ),
    shapes = list(
      # Add a horizontal line at 0 (the baseline)
      list(
        type = "line",
        x0 = -0.5, x1 = 5,
        y0 = 0, y1 = 0,
        line = list(color = "gray", dash = "dash", width = 2)
      )
    )
  )

print(plot_segmented_differences)

# --- Calculate which lag shows the strongest effect ---
# Look for the lag where "Much higher" has the biggest penalty

penalty_comparison <- segmented_differences %>%
  filter(price_group == "Much higher (>10%)") %>%
  select(lag, difference_from_baseline, pct_difference) %>%
  arrange(desc(difference_from_baseline))

cat("\n--- PENALTY FOR BEING >10% ABOVE MARKET ---\n")
cat("Which timeframe shows the biggest impact?\n\n")
print(penalty_comparison)

cat("\n✓ INSIGHT: The '", penalty_comparison$lag[1], 
    "' timeframe shows the strongest effect\n")
cat(sprintf("  Retailers >10%% above market have %.2f%% points higher transfer rate (%.1f%% more)\n",
            penalty_comparison$difference_from_baseline[1],
            penalty_comparison$pct_difference[1]))

# ============================================================================
# ANALYSIS 2: RELATIONSHIP BETWEEN OFFER PRICES AND TRANSFERS
# ============================================================================

message("\n=== ANALYSIS 2: RELATIONSHIP BETWEEN OFFER PRICES AND TRANSFERS ===\n")

# Create 20 price buckets from -20% to +20%
threshold_analysis <- analysis_data %>%
  filter(!is.na(price_vs_market), 
         !is.na(transfer_rate),
         price_vs_market >= -20, 
         price_vs_market <= 20) %>%
  mutate(
    price_bucket = round(price_vs_market / 2) * 2
  ) %>%
  group_by(price_bucket) %>%
  summarise(
    avg_transfer_rate = mean(transfer_rate, na.rm = TRUE),
    n_observations = n(),
    .groups = "drop"
  ) %>%
  # Only keep buckets with enough data
  filter(n_observations >= 10)

print(threshold_analysis)

# Create a line chart to see if there's a threshold
plot_threshold <- plot_ly(
  data = threshold_analysis,
  x = ~price_bucket,
  y = ~avg_transfer_rate,
  type = "scatter",
  mode = "lines+markers",
  line = list(color = "#236192", width = 3),
  marker = list(size = 8),
  text = ~paste0("Rate: ", round(avg_transfer_rate, 2), "%")
) %>%
  layout(
    title = "<b>Transfer Rate by Retailer Price Position</b>",
    xaxis = list(
      title = "Retailer vs Market Median Offer Price (%)",
      zeroline = TRUE,
      zerolinecolor = "gray",
      zerolinewidth = 2,
      tickformat = "%d%%"
    ),
    yaxis = list(title = "Average Weekly Transfer Rate (%)"),
    margin = list(t = 110),
    shapes = list(
      # Add a vertical line at 0% (market average)
      list(
        type = "line",
        x0 = 0, x1 = 0,
        y0 = 0, y1 = max(threshold_analysis$avg_transfer_rate),
        line = list(color = "red", dash = "dash", width = 2)
      )
    ),
    annotations = list(
      list(
        x = 0,
        y = max(threshold_analysis$avg_transfer_rate) * 0.95,
        text = "Market Average",
        showarrow = FALSE,
        xanchor = "left",
        xshift = 5,
        yshift = 20
      )
    )
  )

print(plot_threshold)

# ================================================================
# LINE PLOT: One line per retailer, coloured by CPRG retailer size
# ================================================================

# We must first prepare the data so each retailer has ordered price buckets
plot_lines_by_retailer <- analysis_data %>%
  filter(
    !is.na(price_vs_market),
    !is.na(transfer_rate),
    !is.na(cprg_retailer_size)
  ) %>%
  mutate(
    price_bucket = round(price_vs_market / 2) * 2   # same bucket logic as before
  ) %>%
  group_by(retailer, cprg_retailer_size, price_bucket) %>%
  summarise(
    avg_transfer_rate = mean(transfer_rate, na.rm = TRUE),
    n = n(),
    .groups = "drop"
  ) %>%
  filter(n >= 3)   # optional: avoid ultra-noisy single-week lines

plot_retailer_lines <- plot_ly(
  data = plot_lines_by_retailer,
  x = ~price_bucket,
  y = ~avg_transfer_rate,
  split = ~retailer,
  color = ~cprg_retailer_size,       # colour shows size group
  type = "scatter",
  mode = "lines",
  line = list(width = 1.5),
  hovertemplate = paste(
    "<b>%{split}</b><br>",
    "Size: %{color}<br>",
    "Price bucket: %{x}%<br>",
    "Avg transfer rate: %{y:.2f}%<extra></extra>"
  )
) %>%
  layout(
    title = "<b>Transfer Rate vs Price Position<br>One Line Per Retailer</b>",
    xaxis = list(title = "Price vs Market (%)"),
    yaxis = list(title = "Transfer Rate (%)"),
    legend = list(title = list(text = "<b>Retailer Size</b>"))
  )

print(plot_retailer_lines)
