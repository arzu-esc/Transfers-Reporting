# 2_load_and_write_to_sql.R -------------------------------------
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(janitor)
  library(DBI)
  library(odbc)
  library(fs)
  library(cli)
  library(lubridate)
  library(openxlsx)
})

# ── Configuration -------------------------------------------------------------
raw_dir         <- "00_mass_upload_scripts/data/raw"
batch_size      <- 5L                                # 5 files per batch (93 files = 19 batches)
row_chunk_size  <- 200000L                          # Larger chunks for faster processing
checkpoint_dir  <- "00_mass_upload_scripts/data/processed/checkpoints"
checkpoint_path <- file.path(checkpoint_dir, "transfers_completed_files.csv")
local_tz        <- "Australia/Melbourne"
do_checkpoint   <- TRUE # ← don't mark files complete until you’re happy

melbourne_now_naive <- function() {
  # Convert "now" to Melbourne, keep seconds, then drop TZ (naive DATETIME2)
  tt <- lubridate::with_tz(Sys.time(), tzone = local_tz)
  as.POSIXct(strftime(tt, "%Y-%m-%d %H:%M:%S"), tz = "UTC")
}

# Snapshot configuration
sharepoint_site_url     <- Sys.getenv("SHAREPOINT_SITE_URL")
sharepoint_import_folder <- Sys.getenv("SHAREPOINT_IMPORT_FOLDER") |> 
  gsub('^["\']|["\']$', "", x = _)
save_format <- "csv"  # which formats to create locally before upload

# Destination (SQL Server)
schema <- "stg"
table  <- "aemo_transfers"
tbl_id <- DBI::Id(schema = schema, table = table)

# ── SQL connection from .Renviron --------------------------------------------
driver            <- Sys.getenv("SQL_DRIVER")
sql_server_name   <- Sys.getenv("SQL_SERVER")
sql_database_name <- Sys.getenv("SQL_DATABASE")
auth              <- Sys.getenv("SQL_AUTH", "ActiveDirectoryInteractive")
user_id           <- Sys.getenv("SQL_UID", "")   # optional with AAD interactive

if (!nzchar(sql_server_name) || !nzchar(sql_database_name)) {
  stop("Missing SQL_SERVER or SQL_DATABASE in .Renviron", call. = FALSE)
}

con <- dbConnect(
  odbc::odbc(),
  Driver         = driver,
  Server         = sql_server_name,
  Database       = sql_database_name,
  uid            = user_id,
  Authentication = auth,
  # Performance optimizations
  Mars_Connection = "yes",           # Multiple Active Result Sets
  Packet_Size = 32767,                # Larger packet size (max 32767)
  QueryTimeout = 0                    # No timeout for large batches
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

# ── Ensure schema/table exist (create once if missing) ------------------------
# Create schema if missing
DBI::dbExecute(con, sprintf("
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'%s')
    EXEC('CREATE SCHEMA [%s]');
", schema, schema))

# Expected table schema (matches your earlier definition, plus source_file)
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

if (!DBI::dbExistsTable(con, tbl_id)) {
  DBI::dbCreateTable(con, name = tbl_id, fields = fields)
  message("✓ Created table ", schema, ".", table)
} else {
  message("Table exists; will append.")
}

# Capture SQL column order to align chunks before append
sql_cols <- DBI::dbListFields(con, tbl_id)

# ── List input files & checkpoint --------------------------------------------
if (!dir_exists(raw_dir)) stop("Raw folder not found: ", raw_dir)
csv_files <- dir_ls(raw_dir, glob = "*.csv", recurse = FALSE)
if (!length(csv_files)) stop("No CSV files found in ", raw_dir)

dir_create(checkpoint_dir, recurse = TRUE)

completed <- character(0)
if (file_exists(checkpoint_path)) {
  completed <- readr::read_csv(checkpoint_path, show_col_types = FALSE)[["source_file"]]
  completed <- unique(na.omit(completed))
}

# Remaining files to process
all_files <- path_file(csv_files)
names(csv_files) <- all_files
remaining_files <- setdiff(all_files, completed)
if (!length(remaining_files)) {
  cli::cli_alert_success("Nothing to do. All {length(all_files)} files already loaded per checkpoint.")
  quit(save = "no")
}

# Batch into groups of up to 5 files
batches <- split(remaining_files, ceiling(seq_along(remaining_files) / batch_size))
cli::cli_alert_info("Loading {length(remaining_files)} files in {length(batches)} batches of up to {batch_size}.")

# ── Helpers -------------------------------------------------------------------

# Safe boolean coercion for BIT columns
as_bit <- function(x) {
  y <- trimws(tolower(as.character(x)))
  out <- ifelse(y %in% c("1", "true", "t", "yes", "y"), 1L,
                ifelse(y %in% c("0", "false", "f", "no", "n"), 0L, NA_integer_))
  as.integer(out)
}

# FIXED: Transform chunk with correct Australian date parsing
transform_chunk <- function(df_chunk, source_file) {
  if (nrow(df_chunk) == 0) return(df_chunk)
  
  df_chunk <- janitor::clean_names(df_chunk)
  df_chunk <- mutate(
    df_chunk,
    source_file  = source_file,
    date_imported = melbourne_now_naive()
  )
  
  # Parse Australian date format (d/m/Y H:M - no seconds)
  date_cols <- intersect(c("stat_date","processingdt","maintcreatedt"), names(df_chunk))
  if (length(date_cols)) {
    df_chunk <- df_chunk %>%
      mutate(across(all_of(date_cols), ~ {
        x <- trimws(as.character(.x))
        x[x == "" | x == "NA" | is.na(x)] <- NA_character_
        
        # Try format 1: d/m/Y H:M (e.g., "1/01/2018 0:00")
        parsed <- lubridate::dmy_hm(x, tz = local_tz, quiet = TRUE)
        
        # If that failed, try format 2: Y/m/d H:M:S (e.g., "2018/02/01 00:00:00")
        still_na <- is.na(parsed)
        if (any(still_na)) {
          parsed[still_na] <- lubridate::ymd_hms(x[still_na], tz = local_tz, quiet = TRUE)
        }
        
        as.POSIXct(parsed, tz = local_tz)
      }))
  }
  
  if ("id_mts2" %in% names(df_chunk))   df_chunk$id_mts2   <- suppressWarnings(as.integer(df_chunk$id_mts2))
  if ("stat_value" %in% names(df_chunk))df_chunk$stat_value<- suppressWarnings(as.integer(df_chunk$stat_value))
  
  for (bn in c("newlr","mdp","mpb","misc2","misc3")) {
    if (bn %in% names(df_chunk)) df_chunk[[bn]] <- as_bit(df_chunk[[bn]])
  }
  
  missing_cols <- setdiff(sql_cols, names(df_chunk))
  if (length(missing_cols)) {
    for (m in missing_cols) {
      df_chunk[[m]] <- if (grepl("DATETIME", fields[m])) {
        as.POSIXct(NA, tz = local_tz)
      } else if (grepl("INT|BIT", fields[m])) {
        NA_integer_
      } else {
        NA_character_
      }
    }
  }
  
  df_chunk <- df_chunk[, sql_cols]
  df_chunk
}

# OPTIMIZED: Streaming append with chunk accumulation and frequent logging
append_one_file_streamed <- function(con, tbl_id, file_path, chunk_rows = row_chunk_size, log_every = 1L) {
  sf <- fs::path_file(file_path)
  rows_in_file <- 0L
  chunks_done  <- 0L
  
  # Accumulate 2-3 chunks before writing to reduce round trips
  accumulated_chunks <- list()
  accumulation_limit <- 2  # Write every 2 chunks (balance between speed and feedback)
  
  cb <- SideEffectChunkCallback$new(function(x, pos) {
    x2 <- transform_chunk(x, source_file = sf)
    if (nrow(x2)) {
      accumulated_chunks <<- c(accumulated_chunks, list(x2))
      
      # Write when we hit the accumulation limit
      if (length(accumulated_chunks) >= accumulation_limit) {
        combined <- bind_rows(accumulated_chunks)
        
        # Use larger batch_rows for better performance
        odbc::dbAppendTable(con, name = tbl_id, value = combined, 
                            batch_rows = min(50000L, nrow(combined)))
        
        rows_in_file <<- rows_in_file + nrow(combined)
        chunks_done  <<- chunks_done  + length(accumulated_chunks)
        accumulated_chunks <<- list()
        
        # Frequent logging to track progress
        cat(sprintf("  [%s] chunk %d: +%s rows (total %s) [%s]\n",
                    sf, chunks_done, 
                    format(nrow(combined), big.mark=","), 
                    format(rows_in_file, big.mark=","),
                    format(Sys.time(), "%H:%M:%S")))
        flush.console()
      }
    }
    invisible(NULL)
  })
  
  readr::read_csv_chunked(
    file = file_path,
    callback = cb,
    chunk_size = chunk_rows,
    col_types = cols(.default = col_character()),
    progress = FALSE,
    show_col_types = FALSE,
    lazy = FALSE
  )
  
  # Write any remaining accumulated chunks
  if (length(accumulated_chunks) > 0) {
    combined <- bind_rows(accumulated_chunks)
    odbc::dbAppendTable(con, name = tbl_id, value = combined, 
                        batch_rows = min(50000L, nrow(combined)))
    rows_in_file <- rows_in_file + nrow(combined)
    chunks_done  <- chunks_done  + length(accumulated_chunks)
  }
  
  cat(sprintf("  [%s] ✓ COMPLETE: %s total rows [%s]\n", 
              sf, format(rows_in_file, big.mark=","), 
              format(Sys.time(), "%H:%M:%S")))
  flush.console()
  
  rows_in_file
}

# ── Main loop -----------------------------------------------------------------
use_cli <- interactive() && requireNamespace("cli", quietly = TRUE)
if (use_cli) options(cli.dynamic = TRUE)

for (bi in seq_along(batches)) {
  files_batch <- batches[[bi]]
  cli::cli_h1(sprintf("Batch %d/%d (%d files)", bi, length(batches), length(files_batch)))
  
  if (use_cli) {
    pb <- cli::cli_progress_bar("Loading files in batch", total = length(files_batch), clear = FALSE)
  } else {
    pb <- utils::txtProgressBar(min = 0, max = length(files_batch), style = 3)
    on.exit(close(pb), add = TRUE)
  }
  
  DBI::dbBegin(con)
  ok <- FALSE
  files_done <- character(0)
  total_rows <- 0L
  batch_data <- list()
  
  try({
    for (i in seq_along(files_batch)) {
      f  <- files_batch[[i]]
      fp <- csv_files[[f]]
      
      # 1) STREAMED append to SQL (big files) — your function
      rows_loaded <- append_one_file_streamed(
        con      = con,
        tbl_id   = tbl_id,
        file_path= fp,
        chunk_rows = row_chunk_size
      )
      total_rows <- total_rows + rows_loaded
      files_done <- c(files_done, f)
      
      # 2) MANDATORY SNAPSHOT: re-read full file once (non-streamed) for SharePoint/logging
      snap_df <- readr::read_csv(
        fp,
        col_types = cols(.default = col_character()),
        show_col_types = FALSE
      )
      snap_df <- transform_chunk(janitor::clean_names(snap_df), source_file = f)
      batch_data[[f]] <- snap_df
      
      if (use_cli) {
        cli::cli_progress_update(
          id = pb, set = i,
          status = sprintf("%s • +%s rows (streamed)", f, format(rows_loaded, big.mark = ","))
        )
      } else {
        utils::setTxtProgressBar(pb, i)
      }
    }
    ok <- TRUE
  }, silent = FALSE)
  
  if (ok) {
    DBI::dbCommit(con)
    
    # build batch snapshot
    to_load <- bind_rows(batch_data)
    ts <- format(with_tz(now(tzone = local_tz), local_tz), "%Y%m%d_%H%M%S")
    
    # local snapshot dir (we DO create this)
    local_snapshot_dir <- "00_mass_upload_scripts/data/processed/snapshots"
    dir_create(local_snapshot_dir, recurse = TRUE)
    
    # CSV only
    local_csv_path <- file.path(local_snapshot_dir, paste0("snapshot_batch", bi, "_", ts, ".csv"))
    readr::write_csv(to_load, local_csv_path, na = "")
    cli::cli_alert_info("Saved local CSV snapshot: {local_csv_path}")
    
    # upload CSV to SharePoint
    if (nzchar(sharepoint_site_url) && nzchar(sharepoint_import_folder)) {
      SITE  <- get_sharepoint_site(site_url = sharepoint_site_url)
      DRIVE <- SITE$get_drive("Documents")
      remote_csv_path <- paste(sharepoint_import_folder, basename(local_csv_path), sep = "/")
      DRIVE$upload_file(src = local_csv_path, dest = remote_csv_path)
      cli::cli_alert_info("Uploaded CSV snapshot to SharePoint: {remote_csv_path}")
    } else {
      cli::cli_alert_warning("SharePoint upload skipped: SHAREPOINT_SITE_URL or SHAREPOINT_IMPORT_FOLDER not set.")
    }
    
    # checkpoint update
    if (ok && isTRUE(do_checkpoint)) {
      newly_completed <- tibble(source_file = files_done)
      if (file_exists(checkpoint_path)) {
        readr::write_csv(
          dplyr::bind_rows(
            readr::read_csv(checkpoint_path, show_col_types = FALSE),
            newly_completed
          ) %>% distinct(source_file, .keep_all = TRUE),
          checkpoint_path
        )
      } else {
        dir_create(checkpoint_dir, recurse = TRUE)
        readr::write_csv(newly_completed, checkpoint_path)
      }
    }
    
    cli::cli_alert_success(
      "Committed batch {bi}: {length(files_done)} file(s), {format(total_rows, big.mark=',')} rows."
    )
  } else {
    DBI::dbRollback(con)
    cli::cli_alert_danger("Rolled back batch {bi}. Investigate and re-run.")
    stop("Stopping after rollback to keep state consistent.", call. = FALSE)
  }
}

cli::cli_alert_success("Done. Checkpoint at {checkpoint_path} records completed files.")