# ==============================================================================
# Script: 02_verification.R
#
# Purpose: Verify that every CSV in the checkpoint has been successfully
#   loaded into SQL with correct row counts and non-null date fields.
#
# Description:
#   - Reads the checkpoint list of completed files and verifies each file exists
#     in SQL Server (via source_file column).
#   - Checks that all date columns (stat_date, processingdt, maintcreatedt) were
#     correctly parsed and are non-null.
#   - Cross-checks SQL row counts against the original CSV row counts.
#   - Logs missing files, mismatches, and null-date issues for audit.
#   - Produces a clear success/failure summary at the end.

# Author: Arzu Khanna
# Last updated: 2025-11-25
# ==============================================================================


# ==============================================================================
# 1. LOAD PACKAGES
# ==============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(odbc)
  library(dplyr)
  library(cli)
  library(readr)
  library(fs)
})



# ==============================================================================
# 2. CONFIGURATION
#    - Paths, SQL schema/table, chunk size, local timezone.
# ==============================================================================

local_tz        <- "Australia/Melbourne"
schema          <- "stg"
table           <- "aemo_transfers"

checkpoint_path <- "00_mass_upload_scripts/data/processed/checkpoints/transfers_completed_files.csv"
raw_dir         <- "00_mass_upload_scripts/data/raw"

# Chunk size = 1 to avoid large IN (...) queries (safer for ODBC + big schemas)
files_per_chunk <- 1L



# ==============================================================================
# 3. FAST CSV LINE COUNTER
#    - Reads file in chunks to count rows without loading full CSV.
#    - Subtracts 1 for header row.
# ==============================================================================

count_csv_rows_fast <- function(file_path, chunk_size = 100000L) {
  n <- 0L
  readr::read_lines_chunked(
    file = file_path,
    callback = readr::SideEffectChunkCallback$new(function(x, pos) {
      n <<- n + length(x)
      invisible(NULL)
    }),
    chunk_size = chunk_size,
    progress   = FALSE
  )
  max(0L, n - 1L)  # subtract header
}



# ==============================================================================
# 4. LOAD CHECKPOINT FILE
#    - This defines the list of files we expect to find in SQL.
# ==============================================================================

if (!file_exists(checkpoint_path)) {
  stop("Checkpoint CSV not found: ", checkpoint_path)
}

checkpoint <- readr::read_csv(checkpoint_path, show_col_types = FALSE)
all_files_to_check <- unique(checkpoint$source_file)

cli::cli_h1("AEMO Transfers – Full Load Verification")
cli::cli_alert_info("Found {length(all_files_to_check)} file(s) in checkpoint to verify.")

if (!dir_exists(raw_dir)) {
  cli::cli_alert_warning("Raw dir not found at {raw_dir}. Row comparisons will be skipped.")
}



# ==============================================================================
# 5. SQL CONNECTION (single connection for entire verification run)
# ==============================================================================

driver            <- Sys.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
sql_server_name   <- Sys.getenv("SQL_SERVER")
sql_database_name <- Sys.getenv("SQL_DATABASE")
auth              <- Sys.getenv("SQL_AUTH", "ActiveDirectoryInteractive")
user_id           <- Sys.getenv("SQL_UID", "")

con <- tryCatch(
  dbConnect(
    odbc::odbc(),
    Driver         = driver,
    Server         = sql_server_name,
    Database       = sql_database_name,
    uid            = user_id,
    Authentication = auth,
    Mars_Connection = "yes",
    Packet_Size     = 32767,
    QueryTimeout    = 0
  ),
  error = function(e) {
    cli::cli_alert_danger("✗ Could not connect to SQL: {conditionMessage(e)}")
    stop(e)
  }
)
on.exit(try(dbDisconnect(con), silent = TRUE), add = TRUE)



# ==============================================================================
# 6. PREP CHUNKS OF FILES TO VERIFY
#    - Because files_per_chunk = 1, this simply iterates file-by-file.
# ==============================================================================

file_chunks <- split(
  all_files_to_check,
  ceiling(seq_along(all_files_to_check) / files_per_chunk)
)

missing_in_sql   <- character(0)  # files not found in SQL
date_null_issues <- list()        # files where some date columns were NULL
row_mismatches   <- list()        # SQL != CSV row count mismatches

chunk_idx <- 0L
total_chunks <- length(file_chunks)



# ==============================================================================
# 7. VERIFICATION LOOP (per file)
# ==============================================================================

for (chunk in file_chunks) {
  chunk_idx <- chunk_idx + 1L
  
  # Because chunk size = 1, it's always a single filename
  fname <- chunk[[1]]
  cli::cli_h2("File {chunk_idx}/{total_chunks}: {fname}")
  
  in_list <- paste(chunk, collapse = "','")
  
  # ---------------------------------------------------------------------------
  # CHECK 1: File exists in SQL (source_file match)
  # ---------------------------------------------------------------------------
  query_loaded <- sprintf("
    SELECT source_file, COUNT(*) AS row_count
    FROM %s.%s
    WHERE source_file IN ('%s')
    GROUP BY source_file
  ", schema, table, in_list)
  
  loaded <- tryCatch(
    dbGetQuery(con, query_loaded),
    error = function(e) {
      cli::cli_alert_danger("✗ SQL check failed for {fname}: {conditionMessage(e)}")
      missing_in_sql <<- c(missing_in_sql, fname)
      return(NULL)
    }
  )
  
  if (is.null(loaded) || nrow(loaded) == 0) {
    cli::cli_alert_danger("✗ {fname}: not found in SQL (or query failed).")
    missing_in_sql <- c(missing_in_sql, fname)
    next
  } else {
    cli::cli_alert_success("✓ {fname}: found in SQL with {loaded$row_count[1]} rows.")
  }
  
  # ---------------------------------------------------------------------------
  # CHECK 2: Date columns are non-null
  # ---------------------------------------------------------------------------
  query_dates <- sprintf("
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
  
  dates_df <- tryCatch(
    dbGetQuery(con, query_dates),
    error = function(e) {
      cli::cli_alert_danger("✗ Date check failed for {fname}: {conditionMessage(e)}")
      date_null_issues[[fname]] <<- "query_failed"
      return(NULL)
    }
  )
  
  if (!is.null(dates_df) && nrow(dates_df)) {
    row <- dates_df[1, ]
    
    if (row$stat_date_nulls > 0 || row$processingdt_nulls > 0 || row$maintcreatedt_nulls > 0) {
      cli::cli_alert_danger(
        "✗ {fname}: stat_date NULL={row$stat_date_nulls}, processingdt NULL={row$processingdt_nulls}, maintcreatedt NULL={row$maintcreatedt_nulls}"
      )
      date_null_issues[[fname]] <- row
    } else {
      cli::cli_alert_success("✓ {fname}: all date fields populated ({row$total_rows} rows).")
    }
  }
  
  # ---------------------------------------------------------------------------
  # CHECK 3: SQL row count matches CSV row count
  # ---------------------------------------------------------------------------
  local_path <- file.path(raw_dir, fname)
  sql_count  <- loaded$row_count[1]
  
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
    cli::cli_alert_warning("⚠️  {fname}: raw CSV not found; cannot compare row counts.")
  }
}



# ==============================================================================
# 8. FINAL SUMMARY REPORT
# ==============================================================================

cli::cli_rule("VERIFICATION SUMMARY")

if (length(missing_in_sql) == 0 &&
    length(date_null_issues) == 0 &&
    length(row_mismatches) == 0) {
  
  cli::cli_alert_success("✓ All files verified — no issues detected.")
  
} else {
  
  if (length(missing_in_sql)) {
    cli::cli_alert_danger("Files missing in SQL or failed SQL lookup:")
    cat("  - ", paste(unique(missing_in_sql), collapse = "\n  - "), "\n")
  }
  
  if (length(date_null_issues)) {
    cli::cli_alert_danger("Files with NULL date fields or date query failures:")
    cat("  - ", paste(names(date_null_issues), collapse = "\n  - "), "\n")
  }
  
  if (length(row_mismatches)) {
    cli::cli_alert_warning("Files where SQL row count != CSV row count:")
    cat("  - ", paste(names(row_mismatches), collapse = "\n  - "), "\n")
  }
}

cli::cli_alert_info("Done.")
