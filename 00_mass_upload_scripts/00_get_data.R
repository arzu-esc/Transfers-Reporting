# ==============================================================================
# Script: 00_get_data.R

# Purpose: Download ALL monthly transfer CSVs from SharePoint into:
#   00_mass_upload_scripts/data/raw

# Author: Arzu Khanna
# Last updated: 2025-11-25
# ==============================================================================

# ==============================================================================
# 1. LOAD PACKAGES
# ==============================================================================


library(fs)
library(lubridate)
library(Microsoft365R)
library(tidyverse)
library(cli)

# ==============================================================================
# 2. Configuration
# ==============================================================================

# Local file config ------------------------------------------------------

local_raw_dir <- "00_mass_upload_scripts/data/raw"

if (!dir_exists(local_raw_dir)) {
  dir_create(local_raw_dir, recurse = TRUE)
}

# SharePoint config ------------------------------------------------------

sharepoint_url <- Sys.getenv("SHAREPOINT_SITE_URL")
sharepoint_data_folder <- Sys.getenv("SHAREPOINT_DATA_FOLDER")

if (sharepoint_url == "" || sharepoint_data_folder == "") {
  stop("Please set SHAREPOINT_SITE_URL and SHAREPOINT_DATA_FOLDER in your .Renviron file.")
}

SITE  <- get_sharepoint_site(site_url = sharepoint_url)
DRIVE <- SITE$get_drive("Documents")

# ==============================================================================
# 3. Get Sharepoint items (specifically CSVs)
# ==============================================================================

cli::cli_alert_info("Listing files from SharePoint folder: {sharepoint_data_folder}")

items <- DRIVE$list_items(sharepoint_data_folder)

csv_items <- items %>%
  filter(!isdir, grepl("\\.csv$", name, ignore.case = TRUE))

stopifnot(nrow(csv_items) > 0)

cli::cli_alert_success("Found {nrow(csv_items)} CSV files on SharePoint.")

# ==============================================================================
# 4. Download all CSVs
# ==============================================================================

for (i in seq_len(nrow(csv_items))) {

  sp_name <- csv_items$name[i]
  sp_path <- file.path(sharepoint_data_folder, sp_name)
  local_path <- fs::path(local_raw_dir, sp_name)

  # Skip if local exists
  if (fs::file_exists(local_path)) {
    cli::cli_alert_info("Already exists locally → {local_path}")
    next
  }

  # Download
  cli::cli_alert_info("Downloading {sp_name} ...")

  DRIVE$get_item(sp_path)$download(
    dest      = local_path,
    overwrite = TRUE
  )

  cli::cli_alert_success("✓ Downloaded {sp_name}")
}

cli::cli_alert_success("All downloads complete. Files stored in {local_raw_dir}")
