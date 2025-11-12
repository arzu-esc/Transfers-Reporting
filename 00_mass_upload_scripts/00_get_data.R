# ==============================================================================

# Name: 00_get_data.R
# Author: Arzu Khanna
# Date: 2025-11-03
# Description:
#
#   Gets the newest monthly transfer CSV from SharePoint and save it to 02_data.
#   Before downloading, checks the local checkpoint to see if this file has already
#   been loaded to SQL, and if so returns a message.

# ==============================================================================

# Load packages  ----------------------------------------------------------

library(fs)
library(lubridate)
library(Microsoft365R)
library(tidyverse)
library(cli)   # For progress messages

# Set Local folders  -----------------------------------------------------------

monthly_dir     <- "02_data"
checkpoint_dir  <- file.path(monthly_dir, "checkpoints")
checkpoint_path <- file.path(checkpoint_dir, "transfers_completed_files.csv")

# SharePoint config ------------------------------------------------------------

sharepoint_url <- Sys.getenv("SHAREPOINT_SITE_URL")
sharepoint_data_folder<- Sys.getenv("SHAREPOINT_DATA_FOLDER")

if (sharepoint_url == "" || sharepoint_data_folder == "") {
  stop("Please set SHAREPOINT_SITE_URL and SHAREPOINT_DATA_FOLDER in your .Renviron file.")
}

SITE <- get_sharepoint_site(site_url = sharepoint_url)
DRIVE <- SITE$get_drive("Documents")

# Get new file from SharePoint -------------------------------------------------

items <- DRIVE$list_items(sharepoint_data_folder)

csv_items <- items %>%
  filter(!isdir, grepl("\\.csv$", name, ignore.case = TRUE))

stopifnot(nrow(csv_items) > 0)

# Pick the newest by name (works for JUR_DATA_CSV_VIC_YYYYMM.csv pattern)
csv_newest <- csv_items %>%
  arrange(desc(name)) %>%
  slice(1)

sp_name <- csv_newest$name
cli::cli_alert_info("Newest CSV on SharePoint: {sp_name}")

# Read checkpoint (to see what’s already in SQL) -------------------------------

already_loaded <- character(0)
if (file_exists(checkpoint_path)) {
  cp <- readr::read_csv(checkpoint_path, show_col_types = FALSE)
  if ("source_file" %in% names(cp)) {
    already_loaded <- unique(cp$source_file)
  }
}

if (sp_name %in% already_loaded) {
  stop(
    paste0(
      sp_name,
      " already exists in the database. ",
      "If ", sp_name, " is not the newest transfer dataset, remove all existing files from 02_data ",
      "and ensure only the newest dataset is in the folder."
    ),
    call. = FALSE
  )
}

# Download newest csv if not in checkpoint -------------------------------------

new_data_file <- fs::path(monthly_dir, sp_name)

if (fs::file_exists(new_data_file)) {
  cli::cli_alert_info("Local copy already exists → {new_data_file}")
} else {
  cli::cli_alert_info("Downloading newest monthly transfer data from SharePoint…")
  DRIVE$get_item(file.path(sharepoint_data_folder, sp_name))$download(
    new_data_file      = new_data_file,
    overwrite = TRUE
  )
  cli::cli_alert_success("✓ Downloaded {sp_name} → {new_data_file}")
}

cli::cli_alert_success("Done. Newest file available at: {new_data_file}")
