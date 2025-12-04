# ============================================================================
# 1. PREPARE AND CLEAN OFFER DATA
# ============================================================================

# 1. Get VEC electricity data
elec_raw <- read_excel("VEC Elec Offers with CPRG VDO.xlsx",
                       guess_max = 50000) %>%
  mutate(Fuel = "electricity")

# 2. Filter for relevancy

df_clean <- elec_raw %>%
  filter(year(identifier_date) >= 2022,
         offer_type == "market offer",
         cust_type == "res")

# 3. Keep only the most recent identifier_date for each offer_id
df_clean <- df_clean %>%
  group_by(offer_id) %>%
  filter(identifier_date == max(identifier_date)) %>%
  ungroup()

# 4. Convert date columns to proper date format
df_clean <- df_clean %>%
  mutate(
    identifier_date = ymd(identifier_date),  # Format: YYYY-MM-DD
    start_date = ymd(start_date)
  )

# 5. Calculate end_date as last day of month for identifier_date
# If identifier_date is the maximum date in the dataset, leave end_date blank (NA)
# as the offer may still be available in the following month

max_identifier_date <- max(df_clean$identifier_date, na.rm = TRUE)

df_clean <- df_clean %>%
  mutate(
    end_date = case_when(
      identifier_date == max_identifier_date ~ as.Date(NA),
      TRUE ~ ceiling_date(identifier_date, "month") - days(1)
    )
  )

# 5. Create offer categories
offers_res_elec <- df_clean %>%
  mutate(
    offer_category = case_when(
      has_discount == 1 & has_disc_conditional == 1 ~ "Market offers with\nconditional discounts",
      has_discount == 1 & has_disc_conditional == 0 ~ "Market offers with\nguaranteed discounts",
      has_discount == 0 & has_disc_conditional == 0 ~ "Market offers without\ndiscounts",
      TRUE ~ "Other"
    )
  )

# ============================================================================
#  2. LOAD AND CLEAN TRANSFERS DATA
# ============================================================================

# Read the CSV file containing transfer information
transfers_raw <- readRDS("transfers_raw.rds")

# Separate transfers and customer base data
# M57A = Daily transfer counts
# M71 = Monthly customer base (captured on last day of each month)

# transfers data
transfers_daily <- transfers_raw %>%
  filter(stat_shortcut == "M57A") %>%
  mutate(stat_date = ymd(stat_date)) %>%
  filter(
    Year >= 2022,
    # Keep only Charge Retailer Codes (1000-1040)
    crcode %in% c(1000, 1010, 1020, 1030, 1040)
  ) %>%
  rename(
    transfer_date = stat_date,        
    transfers_count = stat_value,     
    from_retailer = FRMP_Retailer_name,
    to_retailer = NEWFRMP_Retailer_name, 
    reason_code = crcode
  ) %>%
  select(transfer_date, Year, Month, transfers_count, from_retailer, to_retailer, reason_code)

# customer base data
customer_base <- transfers_raw %>%
  filter(stat_shortcut == "M71") %>%
  mutate(stat_date = ymd(stat_date)) %>%
  filter(year(stat_date) >= 2022) %>%
  rename(
    retailer = FRMP_Retailer_name
  ) %>%
  group_by(retailer, Year, Month) %>%
  summarise(customer_count = sum(stat_value, na.rm = TRUE), .groups = "drop") %>%
  select(Year, Month, retailer, customer_count) %>%
  distinct(Year, Month, retailer, .keep_all = TRUE)

message("✓ Transfers and customer base data loaded successfully")
