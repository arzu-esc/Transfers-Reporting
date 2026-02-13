# ====================================================
# Script: 00_run_all.R
# Purpose: Run all scripts in order
# Description: This script sequentially runs all the necessary scripts to 
#   generate the monthly transfer report.
# Author: Arzu Khanna
# Last updated: 2025-11-19
# ====================================================

source("01_config.R")
source("02_get_new_month_data.R")

# ==============================================================================
# Create database connection ONCE (interactive auth happens here)
# ==============================================================================

cli::cli_alert_info("Establishing database connection (may prompt for login)...")

con <- DBI::dbConnect(
  odbc::odbc(),
  Driver                 = driver,
  Server                 = server,
  Database               = db_name,
  uid                    = UID,
  Authentication         = auth,
  Encrypt                = "yes",
  TrustServerCertificate = "yes",
  Timeout                = 0
)

cli::cli_alert_success("Database connection established")

# Make connection available globally
assign("con", con, envir = .GlobalEnv)

# ==============================================================================
# Run remaining scripts (they'll use the existing 'con' object)
# ==============================================================================

source("03_check_retailer_ids.R")
source("04_load_data_sql.R")
source("05_read_updated_transfer_data.R")

# Clean up
dbDisconnect(con)

# Render report
rmarkdown::render("transfers_report.Rmd", output_format = "html_document")
message("Transfer report created")