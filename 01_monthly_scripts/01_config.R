# ====================================================
# Script: 01_config.R
# Purpose: Define global configuration, load required packages, and set database connection details
# Description:
#   - Load all required R packages
#   - Specify database connection parameters
# Author: Arzu Khanna
# Last updated: 2025-11-19
# ====================================================

# ==============================================================================
# 1. Load Packages
# ==============================================================================

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
  library(readxl)
  library(tidyr)
})

# ==============================================================================
# 2. Global Folder Paths
# ==============================================================================

local_monthly_data <- "02_data"
output_dir <- "04_outputs"
checkpoint_dir <- "05_checkpoints"

checkpoint_path <- file.path(checkpoint_dir, "transfers_completed_files.csv")
local_snapshot_dir <- file.path(local_monthly_data, "snapshots")

# Create required directories if missing
dir_create(local_monthly_data, recurse = TRUE)
dir_create(checkpoint_dir, recurse = TRUE)
dir_create(local_snapshot_dir, recurse = TRUE)

# ==============================================================================
# 3. Timezone
# ==============================================================================

local_tz <- "Australia/Melbourne"

# ==============================================================================
# 4. Set SQL DB connection details
# ==============================================================================

driver  <- "ODBC Driver 17 for SQL Server"
server  <- "esc-dev-ssdw-01.database.windows.net"
db_name <- "esc-dev-poc-01"
auth    <- "ActiveDirectoryInteractive"
UID     <- "arzu.khanna@esc.vic.gov.au"

# Staging destination table constants
schema <- "stg"
table  <- "aemo_transfers"
tbl_id <- DBI::Id(schema = schema, table = table)

# ==============================================================================
# 5. Set Sharepoint connection details
# ==============================================================================

sharepoint_url         <- "https://escvic.sharepoint.com/teams/IntelligenceandAnalysisESC"
sharepoint_data_folder <- "3 - Services/AEMO MSATS Transfers Data/Data"
sharepoint_import_folder <- "5 - Data Repository/AEMO Transfers/Imported"
sharepoint_aemo_folder <- "3 - Services/AEMO MSATS Transfers Data"

# Create persistent SP site + drive objects
SITE  <- get_sharepoint_site(site_url = sharepoint_url)
DRIVE <- SITE$get_drive("Documents")

# ==============================================================================
# 6. Reusable helper functions
# ==============================================================================

# Convert Sys.time() → Melbourne time → naive POSIXct (SQL DATETIME2)
melbourne_now_naive <- function() {
  tt <- with_tz(Sys.time(), tzone = local_tz)
  as.POSIXct(strftime(tt, "%Y-%m-%d %H:%M:%S"), tz = "UTC")
}

# Convert text to BIT value (1/0/NA)
as_bit <- function(x) {
  y <- trimws(tolower(as.character(x)))
  out <- ifelse(y %in% c("1","true","t","yes","y"), 1L,
                ifelse(y %in% c("0","false","f","no","n"), 0L, NA_integer_))
  as.integer(out)
}
