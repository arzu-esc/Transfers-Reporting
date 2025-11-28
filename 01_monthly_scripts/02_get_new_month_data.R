# ====================================================
# Script: 02_get_new_month_data.R
# Purpose: Download the newest monthly transfer data CSV from SharePoint
# Description:
#   For the monthly transfer update:
#   - connect to SharePoint
#   - find the newest CSV in SHAREPOINT_DATA_FOLDER
#   - clear local 02_data folder
#   - download the newest CSV there
# Author: Arzu Khanna
# Last updated: 2025-11-19

# ====================================================

# ==============================================================================
# 1. Ensure local folder 02_data exists to store data in
# ==============================================================================

if (!dir_exists(local_monthly_data)) {
  dir_create(local_monthly_data, recurse = TRUE)
}

# ==============================================================================
# 2. Connect to SharePoint
# ==============================================================================

# list items in the configured folder
items <- DRIVE$list_items(sharepoint_data_folder)

# keep only CSVs
csv_items <- items %>%
  filter(!isdir, grepl("\\.csv$", name, ignore.case = TRUE))

if (nrow(csv_items) == 0) {
  stop("No CSV files found in SharePoint folder: ", sharepoint_data_folder)
}

# ==============================================================================
# 3. Pick the newest monthly transfer CSV file
# ==============================================================================

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

# ==============================================================================
# 4. Download the new file to 02_data and name it 'new_data_file'
# ==============================================================================

dest <- fs::path(local_monthly_data, nm)

cli::cli_progress_step(
  "Downloading newest CSV → {dest}",
  msg_done = "✓ Downloaded {nm} to {dest}"
)

DRIVE$get_item(file.path(sharepoint_data_folder, nm))$download(dest = dest, overwrite = TRUE)

assign("new_data_file", dest, envir = .GlobalEnv)

cli::cli_alert_success("Monthly data refresh complete.")
message("Saved: ", dest)
