# Title: 00_get_data.R ---------------------------------------------------------
# Author: Arzu Khanna
# Date: 2025-09-08
# Description:
#
#   This script sources the data required to run the transfer reporting update.
#   Users must configure their `.Renviron` file to point to correct locations.
# 
#   Using `.Renviron` ensures file paths are not hardcoded, making the script 
#   portable across users These data sources are used in subsequent scripts 
#   to build the analysis and generate the final HTML report.
#

# Load packages  ----------------------------------------------------------

# fs is needed to copy files into working directory
# Microsoft365R: used to connect to I&A teams Sharepoint

library(fs)
library(Microsoft365R)
library(tidyverse)
library(cli)   # For progress messages


# Create local directory locations for data -------------------------------

# Ensure consistent folder structure so downstream scripts can find input/output

if (!dir_exists("01_data/raw")) {
  dir_create("01_data/raw")
}

if (!dir_exists("01_data/processed")) {
  dir_create("01_data/processed")
}

## Get SharePoint site details from .Renviron ----------------------------------

## usethis::edit_r_environ() ## to edit .Renviron

sharepoint_url <- Sys.getenv("SHAREPOINT_SITE_URL")
sharepoint_data_folder<- Sys.getenv("SHAREPOINT_DATA_FOLDER")

if (sharepoint_url == "" || sharepoint_data_folder == "") {
  stop("Please set SHAREPOINT_SITE_URL and SHAREPOINT_DATA_FOLDER in your .Renviron file.")
}


## Connect to SharePoint and locate Access database ----------------------------

SITE <- get_sharepoint_site(site_url = sharepoint_url)
DRIVE <- SITE$get_drive("Documents")

## Download csv files locally  -------------------------------------------

# List and filter to CSVs (case-insensitive)
items <- DRIVE$list_items(sharepoint_data_folder)

csv_items <- items %>%
  filter(!isdir, grepl("\\.csv$", name, ignore.case = TRUE))

stopifnot(nrow(csv_items) > 0)


# Total number of files
total_files <- length(csv_items$name)

# Progress message before starting
cli::cli_alert_info("Starting download of {total_files} CSV files from SharePoint...")

local_paths <- purrr::map_chr(seq_along(csv_items$name), function(i) {
  nm <- csv_items$name[i]
  dest <- fs::path("01_data/raw", nm)
  
  # Skip download if file already exists locally
  if (!fs::file_exists(dest)) {
    cli::cli_progress_step(
      "Downloading {i}/{nrow(csv_items)}: {nm}",
      msg_done = "✓ Downloaded {nm}"
    )
    DRIVE$get_item(file.path(sharepoint_data_folder, nm))$download(dest = dest, overwrite = TRUE)
  } else {
    cli::cli_progress_step(
      "Skipping {i}/{nrow(csv_items)}: {nm} (already exists)",
      msg_done = "⏩ Skipped {nm}"
    )
  }
  
  dest
})

# Final summary
cli::cli_alert_success("All {total_files} CSV files downloaded successfully → 02_data/raw")


message("Downloaded ", length(local_paths), " CSV(s) to: 02_data/raw")

# ---- Combine all downloaded CSVs into one table ----

# Safe reader:
# - Reads everything as character first to avoid type clashes,
# - Adds source_file (the filename),
# - Then lets readr guess better types with type_convert().
# Always read everything as character

read_one <- function(p) {
  readr::read_csv(
    p,
    col_types = readr::cols(.default = readr::col_character()),  # force all character
    guess_max = 200000,
    show_col_types = FALSE
  ) |>
    mutate(source_file = fs::path_file(p))  # add source filename
}

# Read + bind all CSVs into one dataset
combined <- purrr::map_dfr(local_paths, read_one)

# Save combined dataset
write_csv(combined, "01_data/processed/aemo_msats_all.csv")
saveRDS(combined, "01_data/processed/aemo_msats_all.rds")

message(
  "Combined ", length(local_paths), " files into one dataset → ",
  normalizePath("01_data/processed/aemo_msats_all.csv"), "\n",
  "Rows: ", format(nrow(combined), big.mark=","), " | Cols: ", ncol(combined)
)
