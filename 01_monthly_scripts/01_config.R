# ====================================================
# Script: 01_config.R
# Purpose: Define global configuration, load required packages, and set database connection details
# Description:
#   - Load all required R packages
#   - Specify database connection parameters
# Author: Arzu Khanna
# Last updated: 2025-11-19
# ====================================================

# ────────────────────────────────────────────────────
# Load Packages --------------------------------------
# ────────────────────────────────────────────────────

suppressPackageStartupMessages({
  library(DBI)
  library(odbc)
  library(dplyr)
  library(stringr)
  library(scales)
  library(writexl)
  library(openxlsx)
  library(purrr)
  library(glue)
  library(here)
  library(tidyverse)
  library(fs)
  library(lubridate)
  library(Microsoft365R)
  library(cli)
  library(readr)
  library(janitor)
})

# ────────────────────────────────────────────────────
# Set SQL DB connection details ----------------------
# ────────────────────────────────────────────────────

driver  <- "ODBC Driver 17 for SQL Server"
server  <- "esc-dev-ssdw-01.database.windows.net"
db_name <- "esc-dev-poc-01"
auth    <- "ActiveDirectoryInteractive"
UID     <- "arzu.khanna@esc.vic.gov.au"

# ────────────────────────────────────────────────────
# Set Sharepoint connection details ------------------
# ────────────────────────────────────────────────────

sharepoint_url         <- "https://escvic.sharepoint.com/teams/IntelligenceandAnalysisESC"
sharepoint_data_folder <- "3 - Services/AEMO MSATS Transfers Data/Data"
sharepoint_import_folder <- "5 - Data Repository/AEMO Transfers/Imported"


# ────────────────────────────────────────────────────
# Set years required and output location -------------
# ────────────────────────────────────────────────────

#config <- list(
#  year_from = 202007L,
#  year_to   = 202506L,
#  output_xlsx  = here::here("VEMD_data_downloads_v4.xlsx")
#)