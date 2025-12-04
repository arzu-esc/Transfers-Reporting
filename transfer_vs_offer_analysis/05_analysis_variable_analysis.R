# ============================================================================
# ANALYSIS 4: INDIVIDUAL VARIABLE EFFECTS (USING TRANSFER RATE)
# ============================================================================
# Purpose: Understand how each offer feature relates to customer switching,
# after normalising by customer base size.
# ----------------------------------------------------------------------------

message("\n=== ANALYSIS 4: INDIVIDUAL VARIABLE EFFECTS (TRANSFER RATE) ===\n")

# ============================================================================
# 4A. CONTRACT TERM EFFECTS
# ============================================================================

contract_analysis <- analysis_data %>%
  filter(!is.na(avg_contract_term),
         !is.na(transfer_rate)) %>%
  mutate(
    contract_category = case_when(
      avg_contract_term == 0 ~ "No fixed term",
      avg_contract_term < 0.75 ~ "Short-term / flexible",
      avg_contract_term <= 1.25 ~ "1-year typical",
      TRUE ~ "Longer term (1.5–2 yrs)"
    )
  ) %>%
  group_by(contract_category) %>%
  summarise(
    avg_transfer_rate = mean(transfer_rate, na.rm = TRUE),
    median_transfer_rate = median(transfer_rate, na.rm = TRUE),
    n_weeks = n(),
    .groups = "drop"
  )

cat("\n4A. CONTRACT TERM EFFECTS:\n")
print(contract_analysis)


# ============================================================================
# 4B. SOLAR AVAILABILITY EFFECTS
# ============================================================================

solar_analysis <- analysis_data %>%
  filter(!is.na(pct_solar_available),
         !is.na(transfer_rate)) %>%
  mutate(
    solar_category = case_when(
      pct_solar_available < 10 ~ "No solar options",
      pct_solar_available < 50 ~ "Some solar options",
      TRUE ~ "Mostly solar-friendly offers"
    )
  ) %>%
  group_by(solar_category) %>%
  summarise(
    avg_transfer_rate = mean(transfer_rate, na.rm = TRUE),
    n_weeks = n(),
    .groups = "drop"
  )

cat("\n4B. SOLAR AVAILABILITY EFFECTS:\n")
print(solar_analysis)


# ============================================================================
# 4C. INCENTIVES EFFECTS
# ============================================================================

incentive_analysis <- analysis_data %>%
  filter(!is.na(pct_with_incentive),
         !is.na(transfer_rate)) %>%
  mutate(
    incentive_category = case_when(
      pct_with_incentive < 10 ~ "No incentives",
      pct_with_incentive < 50 ~ "Some incentives",
      TRUE ~ "Most offers include incentives"
    )
  ) %>%
  group_by(incentive_category) %>%
  summarise(
    avg_transfer_rate = mean(transfer_rate, na.rm = TRUE),
    n_weeks = n(),
    .groups = "drop"
  )

cat("\n4C. INCENTIVES EFFECTS:\n")
print(incentive_analysis)


# ============================================================================
# ANALYSIS 4D: SCATTERPLOTS FOR FEES (TRANSFER RATE)
# ============================================================================

# ============================================================================
# SIMPLE SCATTER PLOTS: TRANSFER RATE vs FEES
# ============================================================================

# Filter to rows with both fee values + transfer rate
fees_scatter_data <- analysis_data %>%
  filter(
    !is.na(transfer_rate),
    !is.na(avg_annual_fee),
    !is.na(avg_late_fee)
  )

# ============================================================================
# ANNUAL FEE SCATTER PLOT
# ============================================================================

plot_annual_fee_scatter <- plot_ly(
  data = fees_scatter_data,
  x = ~avg_annual_fee,
  y = ~transfer_rate,
  type = "scatter",
  mode = "markers",
  marker = list(size = 6, color = "rgba(35,97,146,0.5)")
) %>%
  layout(
    title = "<b>Transfer Rate vs Annual Fee</b>",
    xaxis = list(title = "Annual Fee ($)"),
    yaxis = list(title = "Transfer Rate (%)")
  )

print(plot_annual_fee_scatter)


# ============================================================================
# LATE FEE SCATTER PLOT
# ============================================================================

plot_late_fee_scatter <- plot_ly(
  data = fees_scatter_data,
  x = ~avg_late_fee,
  y = ~transfer_rate,
  type = "scatter",
  mode = "markers",
  marker = list(size = 6, color = "rgba(73,134,160,0.5)")
) %>%
  layout(
    title = "<b>Transfer Rate vs Late Payment Fee</b>",
    xaxis = list(title = "Late Fee ($)"),
    yaxis = list(title = "Transfer Rate (%)")
  )

print(plot_late_fee_scatter)
