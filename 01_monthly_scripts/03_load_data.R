# ==============================================================================

# Name: 01_load_data.R
# Author: Arzu Khanna
# Date: 2025-11-03
# Description:
#
#   Reads the newest monthly transfer CSV (as identified by 00_get_data.R via
#   `new_data_file`) and appends it to stg.aemo_transfers, adding date_imported 
#   and source_file for traceability. After a successful load, saves a snapshot 
#   locally, uploads it to SharePoint, and appends the file name to the 
#   checkpoint at 02_data/checkpoints/transfers_completed_files.csv.

# ==============================================================================

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(janitor)
  library(DBI)
  library(odbc)
  library(fs)
  library(cli)
  library(lubridate)
  library(Microsoft365R)
})

# ── CONFIG --------------------------------------------------------------------
monthly_dir <- "02_data"   # where 00_get_data.R dropped the NEWEST file
local_tz    <- "Australia/Melbourne"

# Checkpoint lives alongside 02_data
checkpoint_dir  <- file.path(monthly_dir, "checkpoints")
checkpoint_path <- file.path(checkpoint_dir, "transfers_completed_files.csv")

# SharePoint snapshot config
sharepoint_url      <- Sys.getenv("SHAREPOINT_SITE_URL")
sharepoint_import_folder <- Sys.getenv("SHAREPOINT_IMPORT_FOLDER")

# SQL destination (already exists!)
schema <- "stg"
table  <- "aemo_transfers"
tbl_id <- DBI::Id(schema = schema, table = table)

# ── helper: "now" in Melbourne, but saved as naive DATETIME2 ------------------
melbourne_now_naive <- function() {
  tt <- lubridate::with_tz(Sys.time(), tzone = local_tz)
  as.POSIXct(strftime(tt, "%Y-%m-%d %H:%M:%S"), tz = "UTC")
}

# ── determine which file to load ----------------------------------------------
if (exists("new_data_file", envir = .GlobalEnv) &&
    is.character(get("new_data_file", envir = .GlobalEnv)) &&
    file_exists(get("new_data_file", envir = .GlobalEnv))) {
  
  file_path   <- get("new_data_file", envir = .GlobalEnv)
  source_name <- basename(file_path)
  cli::cli_alert_info("Using file provided by 00_get_data.R: {source_name}")
  
} else {
  # fallback: pick newest CSV in 02_data
  if (!dir_exists(monthly_dir)) {
    stop("02_data folder not found: ", monthly_dir)
  }
  
  monthly_files <- dir_ls(monthly_dir, glob = "*.csv", recurse = FALSE)
  if (!length(monthly_files)) {
    stop("No CSV found in 02_data. Run 00_get_data.R first.")
  }
  
  monthly_info  <- file_info(monthly_files) |> arrange(desc(modification_time))
  file_path     <- monthly_info$path[1]
  source_name   <- basename(file_path)
  
  cli::cli_alert_warning("new_data_file not found in environment — using newest file in 02_data: {source_name}")
}

# ── SQL connection (your working pattern) -------------------------------------
con <- DBI::dbConnect(
  odbc::odbc(),
  Driver                  = "ODBC Driver 17 for SQL Server",
  Server                  = Sys.getenv("SQL_SERVER"),
  Database                = Sys.getenv("SQL_DATABASE"),
  Authentication          = Sys.getenv("SQL_AUTH", "ActiveDirectoryInteractive"),
  Encrypt                 = "yes",
  TrustServerCertificate  = "yes",
  Timeout                 = 15
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

# ── get SQL columns ------------------------------------------------------------
sql_cols <- DBI::dbListFields(con, tbl_id)

# minimal type hints for filling missing cols
fields <- c(
  id_mts2           = "INT",
  stat_date         = "DATETIME2",
  stat_shortcut     = "VARCHAR(50)",
  stat_name         = "VARCHAR(100)",
  stat_value        = "INT",
  stat_vis          = "VARCHAR(50)",
  processingdt      = "DATETIME2",
  maintcreatedt     = "DATETIME2",
  jurisdictioncode  = "VARCHAR(50)",
  nmiclasscode      = "VARCHAR(50)",
  tierstatus        = "VARCHAR(50)",
  lnsp              = "VARCHAR(50)",
  lr                = "VARCHAR(50)",
  frmp              = "VARCHAR(50)",
  newlr             = "BIT",
  newfrmp           = "VARCHAR(50)",
  crstatuscode      = "VARCHAR(50)",
  crcode            = "VARCHAR(50)",
  mdp               = "BIT",
  mpb               = "BIT",
  objectioncode     = "VARCHAR(50)",
  misc1             = "VARCHAR(50)",
  misc2             = "BIT",
  misc3             = "BIT",
  date_imported     = "DATETIME2",
  source_file       = "VARCHAR(255)"
)

# ── transform helper -----------------------------------------------------------
as_bit <- function(x) {
  y <- trimws(tolower(as.character(x)))
  out <- ifelse(y %in% c("1", "true", "t", "yes", "y"), 1L,
                ifelse(y %in% c("0", "false", "f", "no", "n"), 0L, NA_integer_))
  as.integer(out)
}

transform_monthly <- function(df, source_file) {
  if (nrow(df) == 0) return(df)
  
  df <- janitor::clean_names(df)
  df <- mutate(
    df,
    source_file   = source_file,
    date_imported = melbourne_now_naive()
  )
  
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
  
  if ("id_mts2" %in% names(df))    df$id_mts2    <- suppressWarnings(as.integer(df$id_mts2))
  if ("stat_value" %in% names(df)) df$stat_value <- suppressWarnings(as.integer(df$stat_value))
  
  for (bn in c("newlr","mdp","mpb","misc2","misc3")) {
    if (bn %in% names(df)) df[[bn]] <- as_bit(df[[bn]])
  }
  
  # fill missing SQL cols
  missing_cols <- setdiff(sql_cols, names(df))
  if (length(missing_cols)) {
    for (m in missing_cols) {
      df[[m]] <- if (m %in% names(fields) && grepl("DATETIME", fields[m])) {
        as.POSIXct(NA, tz = local_tz)
      } else if (m %in% names(fields) && grepl("INT|BIT", fields[m])) {
        NA_integer_
      } else {
        NA_character_
      }
    }
  }
  
  df <- df[, sql_cols]
  df
}

# ── read, transform, append ----------------------------------------------------
cli::cli_alert_info("Reading local CSV: {file_path}")
raw_df <- readr::read_csv(
  file_path,
  col_types = cols(.default = col_character()),
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

# ── SNAPSHOT -------------------------------------------------------------------
local_snapshot_dir <- file.path(monthly_dir, "snapshots")
dir_create(local_snapshot_dir, recurse = TRUE)

ts <- format(with_tz(now(tzone = local_tz), local_tz), "%Y%m%d_%H%M%S")
local_csv_path <- file.path(local_snapshot_dir, paste0(source_name, "snapshot_", ts, ".csv"))

readr::write_csv(monthly_df, local_csv_path, na = "")
cli::cli_alert_info("Saved local CSV snapshot: {local_csv_path}")

if (nzchar(sharepoint_site_url) && nzchar(sharepoint_import_folder)) {
  SITE  <- get_sharepoint_site(site_url = sharepoint_site_url)
  DRIVE <- SITE$get_drive("Documents")
  remote_csv_path <- paste(sharepoint_import_folder, basename(local_csv_path), sep = "/")
  DRIVE$upload_file(src = local_csv_path, dest = remote_csv_path)
  cli::cli_alert_success("✓ Uploaded CSV snapshot to SharePoint: {remote_csv_path}")
} else {
  cli::cli_alert_warning("SharePoint upload skipped: SHAREPOINT_SITE_URL or SHAREPOINT_IMPORT_FOLDER not set.")
}

# ── UPDATE CHECKPOINT ---------------------------------------------------------
dir_create(checkpoint_dir, recurse = TRUE)
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

# ── UPDATE dbo.aemo_transfers_data --------------------------------------------

DBI::dbExecute(
  con,
  "EXEC dbo.usp_upsert_aemo_transfers_data @SourceFile = ?",
  params = list(source_name)
)
cli::cli_alert_success("Incremental aemo_transfers_data refreshed for {source_name}.")