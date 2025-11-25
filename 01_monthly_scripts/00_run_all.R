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
source("03_load_data_sql.R")
source("04_check_retailer_ids.R")
source("05_read_updated_transfer_data.R")

message("Transfer report created")
