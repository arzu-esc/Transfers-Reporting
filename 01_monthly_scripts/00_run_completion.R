# ====================================================
# Script: 00_run_completion.R
# Purpose: Run all scripts in order
# Description: This script sequentially runs all the necessary scripts to 
#   generate the monthly transfer report AFTER retailer look ups have been updated 
# Author: Arzu Khanna
# Last updated: 2025-11-25
# ====================================================

source("01_config.R")
source("03_load_data_sql.R")
source("05_read_updated_transfer_data.R")
source("transfers_report.Rmd")

message("Transfer report created")
