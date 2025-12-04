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
      avg_weekly_transfers = mean(total_transfers_out, na.rm = TRUE),
      median_weekly_transfers = median(total_transfers_out, na.rm = TRUE),
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

# Combine all lag results into one dataset
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
print(segmented_lag0 %>% select(price_group, avg_weekly_transfers, n_retailer_weeks))

cat("\n--- 2 WEEKS AGO PRICING (Lag 2w) ---\n")
print(segmented_lag2w %>% select(price_group, avg_weekly_transfers, n_retailer_weeks))

cat("\n--- 4 WEEKS AGO PRICING (Lag 4w) ---\n")
print(segmented_lag4w %>% select(price_group, avg_weekly_transfers, n_retailer_weeks))

cat("\n--- 8 WEEKS AGO PRICING (Lag 8w) ---\n")
print(segmented_lag8w %>% select(price_group, avg_weekly_transfers, n_retailer_weeks))

# --- Create comparison chart showing all lags side-by-side ---
plot_segmented_lags <- plot_ly(
  data = segmented_all_lags,
  x = ~price_group,
  y = ~avg_weekly_transfers,
  color = ~lag,
  type = "bar",
  colors = c("#236192", "#4986a0", "#75787b", "#183028"),
  text = ~paste0(round(avg_weekly_transfers, 0)),
  textposition = "outside"
) %>%
  layout(
    title = "<b>Average Weekly Transfers by Price Position</b>",
    xaxis = list(title = "Retaier vs Market Median Offer Price"),
    yaxis = list(title = "Average Weekly Transfers Out"),
    margin = list(t = 110),
    barmode = "group",
    legend = list(
      orientation = "h", 
      y = 1.12, 
      x = 0.5, 
      xanchor = "center",
      font = list(size = 10)
    )
  )

print(plot_segmented_lags)

# ============================================================================
# ANALYSIS 2: RELATIONSHIP BETWEEN OFFER PRICES AND TRANSFERS
# ============================================================================

message("\n=== ANALYSIS 2: RELATIONSHIP BETWEEN OFFER PRICES AND TRANSFERS ===\n")

# Create 20 price buckets from -20% to +20%
threshold_analysis <- analysis_data %>%
  filter(!is.na(price_vs_market), 
         price_vs_market >= -20, 
         price_vs_market <= 20) %>%
  mutate(
    # Round to nearest 2% (so -7.8% becomes -8%, -6.1% becomes -6%, etc.)
    price_bucket = round(price_vs_market / 2) * 2
  ) %>%
  group_by(price_bucket) %>%
  summarise(
    avg_transfers = mean(total_transfers_out, na.rm = TRUE),
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
  y = ~avg_transfers,
  type = "scatter",
  mode = "lines+markers",
  line = list(color = "#236192", width = 3),
  marker = list(size = 8)
) %>%
  layout(
    title = "<b>Transfer Rate by Price Position</b><br><sub>Looking for a 'tipping point' where customers leave in larger numbers</sub>",
    xaxis = list(
      title = "Price vs Market (%)",
      zeroline = TRUE,
      zerolinecolor = "gray",
      zerolinewidth = 2
    ),
    yaxis = list(title = "Average Weekly Transfers"),
    shapes = list(
      # Add a vertical line at 0% (market average)
      list(
        type = "line",
        x0 = 0, x1 = 0,
        y0 = 0, y1 = max(threshold_analysis$avg_transfers),
        line = list(color = "red", dash = "dash", width = 2)
      )
    ),
    annotations = list(
      list(
        x = 0,
        y = max(threshold_analysis$avg_transfers) * 0.95,
        text = "Market Average",
        showarrow = FALSE,
        xanchor = "left",
        xshift = 5
      )
    )
  )

print(plot_threshold)

# ============================================================================
# ANALYSIS 3: MULTIPLE REGRESSION MODEL
# ============================================================================
# Test ALL factors at once to see which matter most

message("\n=== ANALYSIS 3: MULTIPLE REGRESSION MODEL ===\n")

# Prepare data for regression
# Remove rows with missing values in key variables
regression_data <- analysis_data %>%
  filter(
    !is.na(price_vs_market),
    !is.na(avg_contract_term),
    !is.na(pct_solar_available),
    !is.na(pct_with_incentive),
    !is.na(pct_guaranteed_discount),
    !is.na(avg_annual_fee),
    !is.na(avg_late_fee)
  ) %>%
  # Convert total_transfers_out to regular numeric to avoid integer64 overflow
  mutate(
    total_transfers_out = as.numeric(total_transfers_out),
    # Also ensure all other variables are numeric
    price_vs_market = as.numeric(price_vs_market),
    price_competitiveness_rank = as.numeric(price_competitiveness_rank),
    pct_guaranteed_discount = as.numeric(pct_guaranteed_discount),
    pct_conditional_discount = as.numeric(pct_conditional_discount),
    avg_contract_term = as.numeric(avg_contract_term),
    pct_solar_available = as.numeric(pct_solar_available),
    pct_with_incentive = as.numeric(pct_with_incentive),
    avg_annual_fee = as.numeric(avg_annual_fee),
    avg_late_fee = as.numeric(avg_late_fee)
  )

# Build the regression model
# This tests: Does each factor independently predict transfers?
regression_model <- lm(
  total_transfers_out ~ 
    # PRICING FACTORS
    price_vs_market +                    # How far from market average?
    price_competitiveness_rank +         # Rank (1=cheapest, higher=pricier)
    
    # DISCOUNT FACTORS
    pct_guaranteed_discount +            # % offers with guaranteed discounts
    pct_conditional_discount +           # % offers with conditional discounts
    
    # CONTRACT & FEATURES
    avg_contract_term +                  # Average contract length
    pct_solar_available +                # % offers for solar customers
    pct_with_incentive +                 # % offers with sign-up bonuses
    
    # FEES
    avg_annual_fee +                     # Average annual fee
    avg_late_fee,                        # Average late payment fee
  
  data = regression_data
)

# Display results
cat("\nREGRESSION RESULTS:\n")
cat("===================\n\n")
summary(regression_model)

# Extract key statistics for easier interpretation
# Handle case where some variables might be dropped due to collinearity
model_summary <- summary(regression_model)
coefficients_table <- model_summary$coefficients

# Create results dataframe, excluding the intercept
regression_results <- data.frame(
  Variable = rownames(coefficients_table)[-1],  # Exclude intercept
  Coefficient = coefficients_table[-1, "Estimate"],
  Std_Error = coefficients_table[-1, "Std. Error"],
  T_Value = coefficients_table[-1, "t value"],
  P_Value = coefficients_table[-1, "Pr(>|t|)"],
  row.names = NULL
) %>%
  mutate(
    Significant = ifelse(P_Value < 0.05, "YES", "NO"),
    Direction = ifelse(Coefficient > 0, "Positive (+)", "Negative (-)"),
    Interpretation = case_when(
      Variable == "price_vs_market" & Significant == "YES" ~ 
        "Higher prices → More transfers",
      Variable == "price_competitiveness_rank" & Significant == "YES" ~ 
        "Higher rank (more expensive) → More transfers",
      Variable == "avg_contract_term" & Significant == "YES" & Coefficient > 0 ~ 
        "Longer contracts → More transfers",
      Variable == "avg_contract_term" & Significant == "YES" & Coefficient < 0 ~ 
        "Longer contracts → Fewer transfers (retention)",
      Variable == "pct_with_incentive" & Significant == "YES" & Coefficient < 0 ~ 
        "More incentives → Fewer transfers (retention)",
      Variable == "pct_with_incentive" & Significant == "YES" & Coefficient > 0 ~ 
        "More incentives → More transfers",
      Variable == "pct_guaranteed_discount" & Significant == "YES" & Coefficient < 0 ~ 
        "More guaranteed discounts → Fewer transfers",
      Variable == "pct_conditional_discount" & Significant == "YES" & Coefficient < 0 ~ 
        "More conditional discounts → Fewer transfers",
      Variable == "avg_annual_fee" & Significant == "YES" & Coefficient > 0 ~ 
        "Higher annual fees → More transfers",
      Variable == "avg_late_fee" & Significant == "YES" & Coefficient > 0 ~ 
        "Higher late fees → More transfers",
      TRUE ~ "See coefficient"
    )
  ) %>%
  arrange(P_Value)

cat("\nKEY FINDINGS (Significant Variables Only):\n")
print(regression_results %>% filter(Significant == "YES"))

# Visualize the significant factors
sig_vars <- regression_results %>%
  filter(Significant == "YES") %>%
  head(10)  # Top 10 most significant

if (nrow(sig_vars) > 0) {
  plot_regression <- plot_ly(
    data = sig_vars,
    x = ~reorder(Variable, -abs(Coefficient)),
    y = ~Coefficient,
    type = "bar",
    marker = list(color = ifelse(sig_vars$Coefficient > 0, "#dc2626", "#16a34a")),
    text = ~paste0(Direction, "<br>p=", round(P_Value, 4)),
    textposition = "outside"
  ) %>%
    layout(
      title = "<b>Which Factors Predict Customer Transfers?</b><br><sub>Statistically significant variables only (p < 0.05)</sub>",
      xaxis = list(title = "Variable"),
      yaxis = list(title = "Effect Size (Coefficient)"),
      showlegend = FALSE
    )
  
  print(plot_regression)
}

# Calculate R-squared (how much variance is explained)
r_squared <- summary(regression_model)$r.squared
adj_r_squared <- summary(regression_model)$adj.r.squared

cat(sprintf("\nModel Fit:\n"))
cat(sprintf("  R-squared: %.4f (%.2f%% of variance explained)\n", 
            r_squared, r_squared * 100))
cat(sprintf("  Adjusted R-squared: %.4f\n", adj_r_squared))

# ============================================================================
# ANALYSIS 3B: MULTICOLLINEARITY DIAGNOSTICS
# ============================================================================

message("\n=== MULTICOLLINEARITY CHECK ===\n")

# Install car package if needed for VIF (Variance Inflation Factor)
if (!require(car, quietly = TRUE)) {
  install.packages("car")
  library(car)
}

# Calculate VIF (Variance Inflation Factor)
# VIF > 10 indicates serious multicollinearity
# VIF > 5 suggests moderate multicollinearity
tryCatch({
  vif_values <- vif(regression_model)
  
  cat("\nVariance Inflation Factors (VIF):\n")
  cat("Rules: VIF < 5 = OK, VIF 5-10 = Moderate concern, VIF > 10 = High multicollinearity\n\n")
  print(vif_values)
  
  # Identify problematic variables
  high_vif <- vif_values[vif_values > 5]
  if (length(high_vif) > 0) {
    cat("\n⚠️  Variables with multicollinearity concerns:\n")
    print(high_vif)
    cat("\nConsider removing one of the correlated variables or using Ridge/Lasso regression.\n")
  } else {
    cat("\n✓ No serious multicollinearity detected.\n")
  }
}, error = function(e) {
  cat("\nNote: VIF calculation failed. This can happen with dropped variables.\n")
})

# ============================================================================
# ANALYSIS 3C: CONTRACT TERM INVESTIGATION
# ============================================================================

message("\n=== CONTRACT TERM INVESTIGATION ===\n")

# The huge positive coefficient for contract_term is suspicious
# Let's investigate whether this is a reverse causality issue

contract_investigation <- regression_data %>%
  mutate(
    contract_category = case_when(
      avg_contract_term < 0.1 ~ "No fixed term (0)",
      avg_contract_term < 0.5 ~ "Mostly flexible (<0.5)",
      avg_contract_term < 1.5 ~ "Mostly 1-year (0.5-1.5)",
      TRUE ~ "Long term (>1.5)"
    )
  ) %>%
  group_by(contract_category) %>%
  summarise(
    avg_transfers = mean(total_transfers_out, na.rm = TRUE),
    median_transfers = median(total_transfers_out, na.rm = TRUE),
    avg_price_vs_market = mean(price_vs_market, na.rm = TRUE),
    avg_discount_pct = mean(pct_guaranteed_discount + pct_conditional_discount, na.rm = TRUE),
    n_weeks = n(),
    .groups = "drop"
  )

cat("\n--- CONTRACT TERM vs TRANSFERS ---\n")
print(contract_investigation)

cat("\nInsight: Check if long-term contract retailers also have other characteristics\n")
cat("(e.g., higher prices, fewer discounts) that might explain high transfers.\n")

# ============================================================================
# ANALYSIS 3D: POLYNOMIAL REGRESSION (NON-LINEAR EFFECTS)
# ============================================================================

message("\n=== POLYNOMIAL REGRESSION (U-SHAPED RELATIONSHIPS) ===\n")

# Build polynomial regression with quadratic terms for key variables
# This captures U-shaped or inverted U-shaped relationships

poly_model <- lm(
  total_transfers_out ~ 
    # PRICING - Add squared term to capture U-shape
    price_vs_market + I(price_vs_market^2) +
    
    # DISCOUNT FACTORS - Test if effects are non-linear
    pct_guaranteed_discount + I(pct_guaranteed_discount^2) +
    pct_conditional_discount + I(pct_conditional_discount^2) +
    
    # CONTRACT & FEATURES
    avg_contract_term + I(avg_contract_term^2) +
    pct_solar_available +
    pct_with_incentive + I(pct_with_incentive^2) +
    
    # FEES
    avg_annual_fee + I(avg_annual_fee^2) +
    avg_late_fee,
  
  data = regression_data
)

cat("\nPOLYNOMIAL REGRESSION RESULTS:\n")
cat("==============================\n\n")
summary(poly_model)

# Compare R-squared with linear model
poly_r_squared <- summary(poly_model)$r.squared
cat(sprintf("\nModel Comparison:\n"))
cat(sprintf("  Linear R-squared:     %.4f (%.2f%%)\n", r_squared, r_squared * 100))
cat(sprintf("  Polynomial R-squared: %.4f (%.2f%%)\n", poly_r_squared, poly_r_squared * 100))
cat(sprintf("  Improvement:          %.4f (%.2f%% points)\n", 
            poly_r_squared - r_squared, 
            (poly_r_squared - r_squared) * 100))

# Test if the improvement is statistically significant
anova_result <- anova(regression_model, poly_model)
cat("\n--- Model Comparison Test (ANOVA) ---\n")
print(anova_result)

# ============================================================================
# ANALYSIS 3E: INTERACTION EFFECTS
# ============================================================================

message("\n=== INTERACTION EFFECTS ANALYSIS ===\n")

# Test if certain variables interact (e.g., does pricing matter more when there are no discounts?)

interaction_model <- lm(
  total_transfers_out ~ 
    # MAIN EFFECTS
    price_vs_market +
    pct_guaranteed_discount +
    pct_conditional_discount +
    avg_contract_term +
    pct_with_incentive +
    avg_annual_fee +
    avg_late_fee +
    
    # INTERACTION TERMS (test key hypotheses)
    price_vs_market * pct_conditional_discount +    # Does pricing matter more without discounts?
    price_vs_market * avg_contract_term +           # Does pricing matter more for flexible contracts?
    pct_with_incentive * avg_contract_term +        # Do incentives work differently for contract types?
    pct_guaranteed_discount * pct_with_incentive,   # Do discounts + incentives interact?
  
  data = regression_data
)

cat("\nINTERACTION MODEL RESULTS:\n")
cat("==========================\n\n")
summary(interaction_model)

# Compare R-squared
interaction_r_squared <- summary(interaction_model)$r.squared
cat(sprintf("\nModel Comparison:\n"))
cat(sprintf("  Linear R-squared:      %.4f (%.2f%%)\n", r_squared, r_squared * 100))
cat(sprintf("  Interaction R-squared: %.4f (%.2f%%)\n", interaction_r_squared, interaction_r_squared * 100))
cat(sprintf("  Improvement:           %.4f (%.2f%% points)\n", 
            interaction_r_squared - r_squared, 
            (interaction_r_squared - r_squared) * 100))

# ============================================================================
# ANALYSIS 3F: RANDOM FOREST (NON-LINEAR, MACHINE LEARNING)
# ============================================================================

message("\n=== RANDOM FOREST MODEL ===\n")

# Install randomForest if needed
if (!require(randomForest, quietly = TRUE)) {
  install.packages("randomForest")
  library(randomForest)
}

# Prepare data - remove any rows with NA in predictors
rf_data <- regression_data %>%
  select(
    total_transfers_out,
    price_vs_market,
    pct_guaranteed_discount,
    pct_conditional_discount,
    avg_contract_term,
    pct_solar_available,
    pct_with_incentive,
    avg_annual_fee,
    avg_late_fee
  ) %>%
  na.omit()

cat("Building Random Forest model... (this may take a minute)\n")

# Build Random Forest
# ntree = number of trees (more = better but slower)
# mtry = number of variables to try at each split
set.seed(123)  # For reproducibility

rf_model <- randomForest(
  total_transfers_out ~ .,
  data = rf_data,
  ntree = 500,           # Build 500 trees
  importance = TRUE,     # Calculate variable importance
  mtry = 3               # Try 3 variables at each split
)

cat("\nRANDOM FOREST RESULTS:\n")
cat("======================\n\n")
print(rf_model)

# Calculate R-squared for Random Forest
# RF doesn't give R-squared directly, so we calculate it
rf_predictions <- predict(rf_model, rf_data)
rf_ss_res <- sum((rf_data$total_transfers_out - rf_predictions)^2)
rf_ss_tot <- sum((rf_data$total_transfers_out - mean(rf_data$total_transfers_out))^2)
rf_r_squared <- 1 - (rf_ss_res / rf_ss_tot)

cat(sprintf("\nModel Performance Comparison:\n"))
cat(sprintf("  Linear Regression: %.4f (%.2f%%)\n", r_squared, r_squared * 100))
cat(sprintf("  Polynomial:        %.4f (%.2f%%)\n", poly_r_squared, poly_r_squared * 100))
cat(sprintf("  Random Forest:     %.4f (%.2f%%)\n", rf_r_squared, rf_r_squared * 100))
cat(sprintf("  RF Improvement:    %.4f (%.2f%% points)\n\n", 
            rf_r_squared - r_squared,
            (rf_r_squared - r_squared) * 100))

# Get variable importance
importance_df <- data.frame(
  Variable = rownames(importance(rf_model)),
  Importance = importance(rf_model)[, "%IncMSE"]  # % increase in MSE if variable removed
) %>%
  arrange(desc(Importance))

cat("\n--- VARIABLE IMPORTANCE (Random Forest) ---\n")
cat("Higher values = more important for predictions\n\n")
print(importance_df)

# Visualize variable importance
plot_rf_importance <- plot_ly(
  data = importance_df,
  x = ~reorder(Variable, Importance),
  y = ~Importance,
  type = "bar",
  marker = list(color = "#236192"),
  text = ~paste0(round(Importance, 2), "%"),
  textposition = "outside"
) %>%
  layout(
    title = "<b>Random Forest: Variable Importance</b><br><sub>Which factors matter most for predictions?</sub>",
    xaxis = list(title = ""),
    yaxis = list(title = "Importance (% Increase in MSE)"),
    margin = list(b = 150)
  ) %>%
  config(displayModeBar = FALSE)

print(plot_rf_importance)

# ============================================================================
# ANALYSIS 3G: GRADIENT BOOSTING (XGBOOST)
# ============================================================================

message("\n=== GRADIENT BOOSTING (XGBoost) ===\n")

# Install xgboost if needed
if (!require(xgboost, quietly = TRUE)) {
  install.packages("xgboost")
  library(xgboost)
}

# Prepare data for XGBoost (needs matrix format)
xgb_features <- rf_data %>% 
  select(-total_transfers_out) %>%
  as.matrix()

xgb_label <- rf_data$total_transfers_out

# Create DMatrix (XGBoost's data structure)
xgb_data <- xgb.DMatrix(data = xgb_features, label = xgb_label)

cat("Training XGBoost model...\n")

# Train XGBoost model
set.seed(123)
xgb_model <- xgboost(
  data = xgb_data,
  nrounds = 100,              # Number of boosting rounds
  max_depth = 6,              # Maximum tree depth
  eta = 0.1,                  # Learning rate
  objective = "reg:squarederror",  # Regression task
  verbose = 0                 # Suppress training output
)

# Make predictions
xgb_predictions <- predict(xgb_model, xgb_features)

# Calculate R-squared
xgb_ss_res <- sum((xgb_label - xgb_predictions)^2)
xgb_ss_tot <- sum((xgb_label - mean(xgb_label))^2)
xgb_r_squared <- 1 - (xgb_ss_res / xgb_ss_tot)

cat("\nXGBOOST RESULTS:\n")
cat("================\n\n")
cat(sprintf("R-squared: %.4f (%.2f%%)\n\n", xgb_r_squared, xgb_r_squared * 100))

# Get feature importance from XGBoost
xgb_importance <- xgb.importance(
  feature_names = colnames(xgb_features),
  model = xgb_model
)

cat("--- FEATURE IMPORTANCE (XGBoost) ---\n")
print(xgb_importance)

# Visualize XGBoost importance
plot_xgb_importance <- plot_ly(
  data = xgb_importance,
  x = ~reorder(Feature, Gain),
  y = ~Gain,
  type = "bar",
  marker = list(color = "#dc2626"),
  text = ~paste0(round(Gain, 4)),
  textposition = "outside"
) %>%
  layout(
    title = "<b>XGBoost: Feature Importance</b><br><sub>Contribution to model accuracy</sub>",
    xaxis = list(title = ""),
    yaxis = list(title = "Gain (Information Gain)"),
    margin = list(b = 150)
  ) %>%
  config(displayModeBar = FALSE)

print(plot_xgb_importance)




cat("4. INDIVIDUAL FACTORS:\n")
cat(sprintf("   Contract terms: See table above\n"))
cat(sprintf("   Solar availability: See table above\n"))
cat(sprintf("   Incentives: See table above\n"))
cat(sprintf("   Fees: See plot and tables above\n"))

cat("\n========================================\n")

# ============================================================================
# SAVE ALL RESULTS
# ============================================================================

message("\nSaving results...")

# Save all datasets
write.csv(analysis_data, "analysis_master_dataset.csv", row.names = FALSE)
write.csv(segmented_analysis, "analysis_price_segments.csv", row.names = FALSE)
write.csv(threshold_analysis, "analysis_threshold.csv", row.names = FALSE)
write.csv(regression_results, "analysis_regression_results.csv", row.names = FALSE
