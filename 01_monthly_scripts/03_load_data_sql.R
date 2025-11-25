# ==============================================================================
# Script: 03_load_data_sql.R
# Purpose: Load the newest monthly transfer CSV into SQL staging table
# Description: 
#   - Uses `new_data_file` created by 02_get_new_month_data.R
#   - Appends data to stg.aemo_transfers
#   - Saves snapshot locally + uploads to SharePoint
#   - Updates checkpoint of loaded files
#   - Runs incremental stored procedure to refresh dbo.aemo_transfers_data
# Author: Arzu Khanna
# Last updated: 2025-11-18
# ==============================================================================


# ==============================================================================
# 1. Validate that 02_get_new_month_data.R ran
# ==============================================================================

if (!exists("new_data_file", envir = .GlobalEnv)) {
  stop("new_data_file not found. Ensure 02_get_new_month_data.R was run first.")
}

file_path   <- get("new_data_file", envir = .GlobalEnv)
source_name <- basename(file_path)

cli::cli_alert_info("Loading file from 02_get_new_month_data.R → {source_name}")

# ==============================================================================
# 2. Connect to SQL Server
# ==============================================================================

con <- DBI::dbConnect(
  odbc::odbc(),
  Driver                  = driver,
  Server                  = server,
  Database                = db_name,
  Authentication          = auth,
  UID                     = UID,
  Encrypt                 = "yes",
  TrustServerCertificate  = "yes",
  Timeout                 = 0
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

sql_cols <- DBI::dbListFields(con, tbl_id)

# ==============================================================================
# 3. Transform function (specific to monthly CSV format)
# ==============================================================================

transform_monthly <- function(df, source_file) {
  if (nrow(df) == 0) return(df)
  
  df <- janitor::clean_names(df)
  df <- mutate(
    df,
    source_file   = source_file,
    date_imported = melbourne_now_naive()
  )

  # parse date-like columns
  date_cols <- intersect(c("stat_date", "processingdt", "maintcreatedt"), names(df))

  if (length(date_cols)) {
    df <- df %>%
      mutate(across(all_of(date_cols), ~ {
        x <- trimws(as.character(.x))
        x[x == "" | x == "NA" | is.na(x)] <- NA_character_
        
        parsed <- lubridate::dmy_hm(x, tz = local_tz, quiet = TRUE)
        still_na <- is.na(parsed)
        if (any(still_na)) {
          parsed[still_na] <- lubridate::ymd_hms(x[still_na], tz = local_tz, quiet = TRUE)
        }
        as.POSIXct(parsed, tz = local_tz)
      }))
  }
  
  # parse integer-like columns
  if ("id_mts2" %in% names(df))    df$id_mts2    <- suppressWarnings(as.integer(df$id_mts2))
  if ("stat_value" %in% names(df)) df$stat_value <- suppressWarnings(as.integer(df$stat_value))
  
  # parse bit-like columns
  for (bn in c("newlr","mdp","mpb","misc2","misc3")) {
    if (bn %in% names(df)) df[[bn]] <- as_bit(df[[bn]])
  }
  
  # fill missing SQL cols
  missing_cols <- setdiff(sql_cols, names(df))
  for (m in missing_cols) df[[m]] <- NA
  df <- df[, sql_cols]
  df
}

# ==============================================================================
# 4. Read → Transform → Append to SQL
# ==============================================================================

cli::cli_alert_info("Reading local CSV: {file_path}")
raw_df <- readr::read_csv(file_path, col_types = cols(.default = col_character()),
  show_col_types = FALSE
)

cli::cli_alert_info("Transforming data …")
monthly_df <- transform_monthly(raw_df, source_file = source_name)

rows_n <- nrow(monthly_df)
cli::cli_alert_info("Appending {rows_n} rows to {schema}.{table} …")

DBI::dbAppendTable(
  con,
  name       = tbl_id,
  value      = monthly_df,
  batch_rows = min(50000L, rows_n)
)

cli::cli_alert_success("✓ Appended {rows_n} rows.")

# ==============================================================================
# 5. Save snapshot (local + SharePoint)
# ==============================================================================

ts <- format(with_tz(now(tzone = local_tz), local_tz), "%Y%m%d_%H%M%S")
local_csv_path <- file.path(local_snapshot_dir, paste0(source_name, "_snapshot_", ts, ".csv"))

write_csv(monthly_df, local_csv_path, na = "")
cli::cli_alert_info("Saved local CSV snapshot: {local_csv_path}")

if (nzchar(sharepoint_site_url) && nzchar(sharepoint_import_folder)) {

  remote_csv_path <- paste(sharepoint_import_folder, basename(local_csv_path), sep = "/")
  DRIVE$upload_file(src = local_csv_path, dest = remote_csv_path)
  cli::cli_alert_success("✓ Uploaded CSV snapshot to SharePoint: {remote_csv_path}")

}

# ==============================================================================
# 6. Update checkpoint
# ==============================================================================

new_cp <- tibble(source_file = source_name)

if (file_exists(checkpoint_path)) {
  old_cp <- readr::read_csv(checkpoint_path, show_col_types = FALSE)
  combined <- dplyr::bind_rows(old_cp, new_cp) %>%
    dplyr::distinct(source_file, .keep_all = TRUE)
  readr::write_csv(combined, checkpoint_path)
} else {
  readr::write_csv(new_cp, checkpoint_path)
}

cli::cli_alert_success("Monthly load complete. Checkpoint updated → {checkpoint_path}")

# ==============================================================================
# 7. Run stored procedure to refresh dbo.aemo_transfers_data with new data
# ==============================================================================

DBI::dbExecute(
  con,
  "EXEC dbo.usp_upsert_aemo_transfers_data @SourceFile = ?",
  params = list(source_name)
)
cli::cli_alert_success("Incremental aemo_transfers_data refreshed for {source_name}.")