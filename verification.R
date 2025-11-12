# verify_all_uploads.R ---------------------------------------------------------
suppressPackageStartupMessages({
  library(DBI)
  library(odbc)
  library(dplyr)
  library(cli)
  library(readr)
  library(fs)
})

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
local_tz        <- "Australia/Melbourne"
schema          <- "stg"
table           <- "aemo_transfers"

checkpoint_path <- "00_mass_upload_scripts/data/processed/checkpoints/transfers_completed_files.csv"
raw_dir         <- "00_mass_upload_scripts/data/raw"

# keep this small to avoid big IN (...) and long-lived connections
files_per_chunk <- 5L

# ---------------------------------------------------------------------
# 1. helper: fast CSV line counter (local)
# ---------------------------------------------------------------------
count_csv_rows_fast <- function(file_path, chunk_size = 100000L) {
  n <- 0L
  readr::read_lines_chunked(
    file = file_path,
    callback = readr::SideEffectChunkCallback$new(function(x, pos) {
      n <<- n + length(x)
      invisible(NULL)
    }),
    chunk_size   = chunk_size,
    progress     = FALSE
  )
  max(0L, n - 1L)  # minus header
}

# ---------------------------------------------------------------------
# 2. helper: make the SAME connection you just tested
# ---------------------------------------------------------------------
make_con <- function() {
  DBI::dbConnect(
    odbc::odbc(),
    Driver   = "ODBC Driver 17 for SQL Server",
    Server   = Sys.getenv("SQL_SERVER"),
    Database = Sys.getenv("SQL_DATABASE"),
    Authentication = Sys.getenv("SQL_AUTH", "ActiveDirectoryInteractive"),
    Encrypt  = "yes",
    TrustServerCertificate = "yes",
    Timeout  = 15
  )
}

# one-shot query: open → query → close
run_query_once <- function(sql) {
  con <- make_con()
  on.exit(try(DBI::dbDisconnect(con), silent = TRUE))
  DBI::dbGetQuery(con, sql)
}

# ---------------------------------------------------------------------
# 3. read checkpoint
# ---------------------------------------------------------------------
cli::cli_h1("AEMO Transfers – Full Load Verification")

if (!file_exists(checkpoint_path)) {
  stop("Checkpoint CSV not found: ", checkpoint_path)
}

ck <- readr::read_csv(checkpoint_path, show_col_types = FALSE)
all_files <- unique(ck$source_file)

cli::cli_alert_info("Found {length(all_files)} file(s) in checkpoint to verify.")

if (!dir_exists(raw_dir)) {
  cli::cli_alert_warning("Raw dir not found at {raw_dir}. Local row-count checks will be skipped.")
}

# split files into small groups
file_chunks <- split(all_files, ceiling(seq_along(all_files) / files_per_chunk))

missing_in_sql   <- character(0)
date_null_issues <- list()
row_mismatches   <- list()

chunk_idx <- 0L

for (chunk in file_chunks) {
  chunk_idx <- chunk_idx + 1L
  cli::cli_h2("Chunk {chunk_idx}/{length(file_chunks)} ({length(chunk)} files)")
  
  in_list <- paste(chunk, collapse = "','")
  
  # ---------------------------------------------------
  # CHECK 1: file is in SQL + row count
  # ---------------------------------------------------
  q_loaded <- sprintf("
    SELECT source_file, COUNT(*) AS row_count
    FROM %s.%s
    WHERE source_file IN ('%s')
    GROUP BY source_file
    ORDER BY source_file
  ", schema, table, in_list)
  
  loaded <- run_query_once(q_loaded)
  
  if (nrow(loaded) == 0) {
    cli::cli_alert_danger("✗ None of these files are in SQL.")
    missing_in_sql <- c(missing_in_sql, chunk)
    next
  } else {
    cli::cli_alert_success("✓ {nrow(loaded)} file(s) from this chunk found in SQL.")
  }
  
  # any missing from this chunk?
  missing_here <- setdiff(chunk, loaded$source_file)
  if (length(missing_here)) {
    cli::cli_alert_danger("✗ Missing in SQL: {paste(missing_here, collapse = ', ')}")
    missing_in_sql <- c(missing_in_sql, missing_here)
  }
  
  # ---------------------------------------------------
  # CHECK 2: date columns not null
  # ---------------------------------------------------
  q_dates <- sprintf("
    SELECT 
      source_file,
      COUNT(*) AS total_rows,
      SUM(CASE WHEN stat_date     IS NULL THEN 1 ELSE 0 END) AS stat_date_nulls,
      SUM(CASE WHEN processingdt  IS NULL THEN 1 ELSE 0 END) AS processingdt_nulls,
      SUM(CASE WHEN maintcreatedt IS NULL THEN 1 ELSE 0 END) AS maintcreatedt_nulls
    FROM %s.%s
    WHERE source_file IN ('%s')
    GROUP BY source_file
  ", schema, table, in_list)
  
  dates_df <- run_query_once(q_dates)
  
  for (i in seq_len(nrow(dates_df))) {
    r <- dates_df[i, ]
    if (r$stat_date_nulls > 0 || r$processingdt_nulls > 0 || r$maintcreatedt_nulls > 0) {
      cli::cli_alert_danger(
        "✗ {r$source_file}: stat_date NULL={r$stat_date_nulls}, processingdt NULL={r$processingdt_nulls}, maintcreatedt NULL={r$maintcreatedt_nulls}"
      )
      date_null_issues[[r$source_file]] <- r
    } else {
      cli::cli_alert_success("✓ {r$source_file}: all date columns populated ({r$total_rows} rows)")
    }
  }
  
  # ---------------------------------------------------
  # CHECK 3: local CSV row-count match (optional)
  # ---------------------------------------------------
  for (fname in chunk) {
    local_path <- file.path(raw_dir, fname)
    sql_count  <- loaded$row_count[loaded$source_file == fname]
    
    if (length(sql_count) == 0) {
      cli::cli_alert_warning("⚠️  {fname}: in checkpoint but not returned by SQL query.")
      next
    }
    
    if (file_exists(local_path)) {
      csv_count <- count_csv_rows_fast(local_path)
      if (identical(as.integer(sql_count), as.integer(csv_count))) {
        cli::cli_alert_success("✓ {fname}: SQL={sql_count}, CSV={csv_count}")
      } else {
        diff <- as.integer(sql_count) - as.integer(csv_count)
        cli::cli_alert_warning("⚠️  {fname}: SQL={sql_count}, CSV={csv_count} (diff {diff})")
        row_mismatches[[fname]] <- list(sql = sql_count, csv = csv_count, diff = diff)
      }
    } else {
      cli::cli_alert_warning("⚠️  {fname}: CSV not found in {raw_dir}, can't compare row counts.")
    }
  }
}

# ---------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------
cli::cli_rule("VERIFICATION SUMMARY")

if (!length(missing_in_sql) && !length(date_null_issues) && !length(row_mismatches)) {
  cli::cli_alert_success("✓ All files in checkpoint passed verification.")
} else {
  if (length(missing_in_sql)) {
    cli::cli_alert_danger("Files in checkpoint but NOT in SQL:")
    cat("  - ", paste(unique(missing_in_sql), collapse = "\n  - "), "\n")
  }
  if (length(date_null_issues)) {
    cli::cli_alert_danger("Files with NULL date columns:")
    cat("  - ", paste(names(date_null_issues), collapse = "\n  - "), "\n")
  }
  if (length(row_mismatches)) {
    cli::cli_alert_warning("Files where SQL row count != CSV line count:")
    cat("  - ", paste(names(row_mismatches), collapse = "\n  - "), "\n")
  }
}

cli::cli_alert_info("Done.")
