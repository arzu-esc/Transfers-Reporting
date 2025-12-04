# ============================================================================
# ANALYSIS 3: MULTIPLE REGRESSION MODEL (ABSOLUTE TRANSFERS + LOG CUSTOMER SIZE)
# ============================================================================

message("\n=== ANALYSIS 3: MULTIPLE REGRESSION MODEL (ABSOLUTE TRANSFERS) ===\n")

# Prepare data for regression
regression_data <- analysis_data %>%
  filter(
    !is.na(price_vs_market),
    !is.na(avg_contract_term),
    !is.na(pct_solar_available),
    !is.na(pct_with_incentive),
    !is.na(pct_guaranteed_discount),
    !is.na(avg_annual_fee),
    !is.na(avg_late_fee),
    !is.na(customer_count)
  ) %>%
  mutate(
    total_transfers_out = as.numeric(total_transfers_out),
    price_vs_market = as.numeric(price_vs_market),
    price_competitiveness_rank = as.numeric(price_competitiveness_rank),
    pct_guaranteed_discount = as.numeric(pct_guaranteed_discount),
    pct_conditional_discount = as.numeric(pct_conditional_discount),
    avg_contract_term = as.numeric(avg_contract_term),
    pct_solar_available = as.numeric(pct_solar_available),
    pct_with_incentive = as.numeric(pct_with_incentive),
    avg_annual_fee = as.numeric(avg_annual_fee),
    avg_late_fee = as.numeric(avg_late_fee),
    
    # ⭐ NEW — log transform customer size
    log_customer_size = log(customer_count)
  )


# ----------------------------------------------------------------------------
# LINEAR REGRESSION MODEL
# ----------------------------------------------------------------------------

regression_model <- lm(
  total_transfers_out ~ 
    price_vs_market +
    price_competitiveness_rank +
    pct_guaranteed_discount +
    pct_conditional_discount +
    avg_contract_term +
    pct_solar_available +
    pct_with_incentive +
    avg_annual_fee +
    avg_late_fee +
    log_customer_size,
  data = regression_data
)

cat("\nREGRESSION RESULTS:\n")
summary(regression_model)


# Extract coefficients
model_summary <- summary(regression_model)
coefficients_table <- model_summary$coefficients

regression_results <- data.frame(
  Variable = rownames(coefficients_table)[-1],
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

# R-squared
r_squared <- summary(regression_model)$r.squared
adj_r_squared <- summary(regression_model)$adj.r.squared

cat(sprintf("\nModel Fit:\n  R-squared: %.4f (%.2f%%)\n  Adjusted R-squared: %.4f\n",
            r_squared, r_squared * 100, adj_r_squared))

# ============================================================================
# PLOT: LINEAR REGRESSION COEFFICIENT PLOT
# ============================================================================

coef_df <- regression_results %>%
  mutate(
    Variable = reorder(Variable, Coefficient),
    Colour = ifelse(Coefficient > 0, "#d7191c", "#1a9641")
  )

plot_coefficients <- plot_ly(
  data = coef_df,
  x = ~Variable,
  y = ~Coefficient,
  type = "bar",
  marker = list(color = ~Colour),
  text = ~paste0("p = ", round(P_Value, 4)),
  textposition = "outside"
) %>%
  layout(
    title = "<b>Linear Regression: Effect Sizes</b><br><sub>Positive = More Transfers, Negative = Fewer Transfers</sub>",
    xaxis = list(title = "", tickangle = -45),
    yaxis = list(title = "Coefficient Value"),
    showlegend = FALSE
  )

plot_coefficients

# ----------------------------------------------------------------------------
# MULTICOLLINEARITY CHECK (VIF)
# ----------------------------------------------------------------------------

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

# ----------------------------------------------------------------------------
# POLYNOMIAL REGRESSION (NON-LINEAR)
# ----------------------------------------------------------------------------

message("\n=== POLYNOMIAL REGRESSION ===\n")

poly_model <- lm(
  total_transfers_out ~ 
    price_vs_market + I(price_vs_market^2) +
    pct_guaranteed_discount + I(pct_guaranteed_discount^2) +
    pct_conditional_discount + I(pct_conditional_discount^2) +
    avg_contract_term + I(avg_contract_term^2) +
    pct_with_incentive + I(pct_with_incentive^2) +
    avg_annual_fee + I(avg_annual_fee^2) +
    avg_late_fee +
    log_customer_size,               
  data = regression_data
)

summary(poly_model)
poly_r_squared <- summary(poly_model)$r.squared


# ----------------------------------------------------------------------------
# INTERACTION EFFECTS
# ----------------------------------------------------------------------------

message("\n=== INTERACTION EFFECTS MODEL ===\n")

interaction_model <- lm(
  total_transfers_out ~ 
    price_vs_market * pct_conditional_discount +
    price_vs_market * avg_contract_term +
    pct_with_incentive * avg_contract_term +
    pct_guaranteed_discount * pct_with_incentive +
    log_customer_size * price_vs_market,               
  data = regression_data
)

summary(interaction_model)
interaction_r_squared <- summary(interaction_model)$r.squared


# ----------------------------------------------------------------------------
# RANDOM FOREST
# ----------------------------------------------------------------------------

message("\n=== RANDOM FOREST MODEL ===\n")

if (!require(randomForest, quietly = TRUE)) {
  install.packages("randomForest")
  library(randomForest)
}

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
    avg_late_fee,
    log_customer_size                   
  ) %>%
  na.omit()

set.seed(123)
rf_model <- randomForest(
  total_transfers_out ~ ., 
  data = rf_data,
  ntree = 500,
  importance = TRUE,
  mtry = 3
)

rf_predictions <- predict(rf_model, rf_data)
rf_ss_res <- sum((rf_data$total_transfers_out - rf_predictions)^2)
rf_ss_tot <- sum((rf_data$total_transfers_out - mean(rf_data$total_transfers_out))^2)
rf_r_squared <- 1 - (rf_ss_res / rf_ss_tot)

importance_df <- data.frame(
  Variable = rownames(importance(rf_model)),
  Importance = importance(rf_model)[, "%IncMSE"]
) %>% arrange(desc(Importance))

# ============================================================================
# PLOT: RANDOM FOREST VARIABLE IMPORTANCE
# ============================================================================

plot_importance_rf <- importance_df %>%
  mutate(
    Variable = reorder(Variable, Importance)
  ) %>%
  plot_ly(
    x = ~Variable,
    y = ~Importance,
    type = "bar",
    marker = list(color = "#236192"),
    text = ~round(Importance, 1),
    textposition = "outside"
  ) %>%
  layout(
    title = "<b>Random Forest: Variable Importance</b>",
    xaxis = list(title = "", tickangle = -45),
    yaxis = list(title = "% Increase in MSE<br><sub>(Higher = More Important)</sub>")
  )

plot_importance_rf

# ----------------------------------------------------------------------------
# XGBOOST
# ----------------------------------------------------------------------------

message("\n=== XGBOOST MODEL ===\n")

if (!require(xgboost, quietly = TRUE)) {
  install.packages("xgboost")
  library(xgboost)
}

xgb_features <- rf_data %>% select(-total_transfers_out) %>% as.matrix()
xgb_label <- rf_data$total_transfers_out
xgb_data <- xgb.DMatrix(data = xgb_features, label = xgb_label)

set.seed(123)
xgb_model <- xgboost(
  data = xgb_data,
  nrounds = 100,
  max_depth = 6,
  eta = 0.1,
  objective = "reg:squarederror",
  verbose = 0
)

xgb_predictions <- predict(xgb_model, xgb_features)
xgb_ss_res <- sum((xgb_label - xgb_predictions)^2)
xgb_ss_tot <- sum((xgb_label - mean(xgb_label))^2)
xgb_r_squared <- 1 - (xgb_ss_res / xgb_ss_tot)

xgb_importance <- xgb.importance(
  feature_names = colnames(xgb_features),
  model = xgb_model
)

# ============================================================================
# PLOT: XGBOOST VARIABLE IMPORTANCE
# ============================================================================

plot_importance_xgb <- xgb_importance %>%
  mutate(Feature = reorder(Feature, Gain)) %>%
  plot_ly(
    x = ~Feature,
    y = ~Gain,
    type = "bar",
    marker = list(color = "#c44536"),
    text = ~round(Gain, 4),
    textposition = "outside"
  ) %>%
  layout(
    title = "<b>XGBoost: Feature Importance</b>",
    xaxis = list(title = "", tickangle = -45),
    yaxis = list(title = "Gain (Information Delivered)")
  )

plot_importance_xgb

# ----------------------------------------------------------------------------
# FINAL MODEL COMPARISON
# ----------------------------------------------------------------------------

model_comparison <- data.frame(
  Model = c(
    "Linear Regression",
    "Polynomial Regression",
    "Interaction Model",
    "Random Forest",
    "XGBoost"
  ),
  R_Squared = c(
    r_squared,
    poly_r_squared,
    interaction_r_squared,
    rf_r_squared,
    xgb_r_squared
  )
) %>%
  arrange(desc(R_Squared))

print(model_comparison)
best_model <- model_comparison$Model[1]
best_r2 <- model_comparison$R_squared[1]