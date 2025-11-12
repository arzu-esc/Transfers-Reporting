# Title: 00_get_data.R ---------------------------------------------------------
# Author: Arzu Khanna
# Date: 2025-09-08 (updated: single newest file mode)
# Description:
#   For the monthly transfer update:
#   - connect to SharePoint
#   - find the newest CSV in SHAREPOINT_DATA_FOLDER
#   - clear local Mass Upload Scripts/02_data
#   - download that single newest CSV there

suppressPackageStartupMessages({
  library(fs)
  library(lubridate)
  library(Microsoft365R)
  library(tidyverse)
  library(cli)
})

# ---------------------------------------------------------------------
# 1. ensure local folder exists (we now use 02_data, not 01_data/raw)
# ---------------------------------------------------------------------
local_monthly_dir <- "Mass Upload Scripts/02_data"
if (!dir_exists(local_monthly_dir)) {
  dir_create(local_monthly_dir, recurse = TRUE)
} else {
  # wipe anything that was there before
  dir_delete(local_monthly_dir)
  dir_create(local_monthly_dir, recurse = TRUE)
}

# ---------------------------------------------------------------------
# 2. env vars
# ---------------------------------------------------------------------
sharepoint_url         <- Sys.getenv("SHAREPOINT_SITE_URL")
sharepoint_data_folder <- Sys.getenv("SHAREPOINT_DATA_FOLDER")

if (sharepoint_url == "" || sharepoint_data_folder == "") {
  stop("Please set SHAREPOINT_SITE_URL and SHAREPOINT_DATA_FOLDER in your .Renviron file.")
}

# ---------------------------------------------------------------------
# 3. connect to SharePoint
# ---------------------------------------------------------------------
SITE  <- get_sharepoint_site(site_url = sharepoint_url)
DRIVE <- SITE$get_drive("Documents")

# list items in the configured folder
items <- DRIVE$list_items(sharepoint_data_folder)

# keep only CSVs (non-folders)
csv_items <- items %>%
  filter(!isdir, grepl("\\.csv$", name, ignore.case = TRUE))

if (nrow(csv_items) == 0) {
  stop("No CSV files found in SharePoint folder: ", sharepoint_data_folder)
}

# ---------------------------------------------------------------------
# 4. pick the newest one
# ---------------------------------------------------------------------
# Microsoft365R usually returns lastModifiedDateTime in the data frame
if ("lastModifiedDateTime" %in% names(csv_items)) {
  csv_items <- csv_items %>%
    mutate(lastModifiedDateTime = ymd_hms(lastModifiedDateTime, quiet = TRUE)) %>%
    arrange(desc(lastModifiedDateTime))
} else {
  # fallback: sort by name descending
  csv_items <- csv_items %>%
    arrange(desc(name))
}

newest <- csv_items[1, ]
nm     <- newest$name

cli::cli_alert_info("Newest CSV in SharePoint is: {nm}")

# ---------------------------------------------------------------------
# 5. download that one file to 02_data
# ---------------------------------------------------------------------
dest <- fs::path(local_monthly_dir, nm)

cli::cli_progress_step(
  "Downloading newest CSV → {dest}",
  msg_done = "✓ Downloaded {nm} to {dest}"
)

DRIVE$get_item(file.path(sharepoint_data_folder, nm))$download(dest = dest, overwrite = TRUE)

# ---------------------------------------------------------------------
# 6. final message
# ---------------------------------------------------------------------
cli::cli_alert_success("Monthly data refresh complete.")
message("Saved: ", dest)
