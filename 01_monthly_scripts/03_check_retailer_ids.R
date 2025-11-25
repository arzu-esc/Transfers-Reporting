# ==============================================================================
# Script: 03_check_retailer_ids.R
# Purpose: Validate FRMP and NEWFRMP IDs in the latest monthly AEMO CSV
#
# Description:
#   - Validates the newest monthly CSV against the retailer lookup table:
#       dbo.aemo_retailer_lookup
#   - Applies checks for SMALL NMI transfers:
#       - M71  → FRMP must exist in lookup
#       - M57A → FRMP and NEWFRMP must exist in lookup (if NEWFRMP column exists)
#   - Produces a CSV report of all missing IDs.
#   - Stops the run if any missing IDs are found.
#
# Author: Arzu Khanna
# Last updated: 2025-11-25
# ==============================================================================


# ==============================================================================
# 1. Validate that 02_get_new_month_data.R ran
# ==============================================================================

if (!exists("new_data_file", envir = .GlobalEnv)) {
  stop("new_data_file not found — run 02_get_new_month_data.R first.")
}

csv_path <- get("new_data_file", envir = .GlobalEnv)
cli::cli_alert_info("Validating retailer IDs in monthly file: {csv_path}")


# ==============================================================================
# 2. Read CSV
# ==============================================================================

df <- read_csv(
  csv_path,
  col_types = cols(.default = col_character()),
  show_col_types = FALSE
) %>% 
  clean_names()

required_cols <- c("stat_shortcut", "frmp")
if (!all(required_cols %in% names(df))) {
  stop("Incoming CSV is missing required columns: stat_shortcut, frmp")
}


# ==============================================================================
# 3. Load unified lookup table from SQL
# ==============================================================================

cli::cli_alert_info("Connecting to SQL and reading unified lookup table…")

con <- DBI::dbConnect(
  odbc::odbc(),
  Driver                 = driver,
  Server                 = server,
  Database               = db_name,
  uid                    = UID,
  Authentication         = auth,
  Encrypt                = "yes",
  TrustServerCertificate = "yes",
  Timeout                = 0
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

lookup_tbl <- DBI::dbReadTable(
  con, 
  DBI::Id(schema = "dbo", table = "aemo_retailer_lookup")
) %>%
  mutate(
    id = trimws(as.character(id)),
    licence_common_id = trimws(as.character(licence_common_id))
  )

cli::cli_alert_success("Unified lookup loaded → {nrow(lookup_tbl)} rows.")

# IMPORTANT:
# Include IDs even if licence_common_id is blank
lookup_ids <- lookup_tbl %>%
  filter(!is.na(id), id != "") %>%
  pull(id)

# ==============================================================================
# 4. Prepare for validation
# ==============================================================================

lookup_ids <- lookup_tbl$id
fail_rows <- list()

# ==============================================================================
# 5. Validate M71 (FRMP must exist in lookup)
# ==============================================================================

m71_rows <- df %>%
  filter(stat_shortcut == "M71", nmiclasscode == "SMALL") %>%
  mutate(frmp = trimws(frmp))

if (nrow(m71_rows)) {
  
  missing_m71 <- m71_rows %>%
    filter(is.na(frmp) | frmp == "" | !(frmp %in% lookup_ids))
  
  if (nrow(missing_m71)) {
    
    m71_summary <- missing_m71 %>%
      filter(!is.na(frmp), frmp != "") %>%
      distinct(frmp) %>%
      transmute(
        stat_shortcut = "M71",
        id_type       = "FRMP",
        missing_id    = frmp
      )
    
    fail_rows[[length(fail_rows) + 1]] <- m71_summary
  }
  
} else {
  cli::cli_alert_info("No SMALL M71 rows present.")
}

# ==============================================================================
# 6. Validate M57A (FRMP and NEWFRMP must exist)
# ==============================================================================

m57a_rows <- df %>%
  filter(stat_shortcut == "M57A", nmiclasscode == "SMALL")

has_newfrmp <- "newfrmp" %in% names(m57a_rows)

m57a_rows <- m57a_rows %>%
  mutate(frmp = trimws(frmp))

if (has_newfrmp) {
  m57a_rows <- m57a_rows %>% mutate(newfrmp = trimws(newfrmp))
}

if (nrow(m57a_rows)) {
  
  # --- FRMP missing ---
  missing_frmp <- m57a_rows %>%
    filter(is.na(frmp) | frmp == "" | !(frmp %in% lookup_ids))
  
  if (nrow(missing_frmp)) {
    bad_frmp <- missing_frmp %>%
      filter(!is.na(frmp), frmp != "") %>%
      distinct(frmp) %>%
      transmute(
        stat_shortcut = "M57A",
        id_type       = "FRMP",
        missing_id    = frmp
      )
    fail_rows[[length(fail_rows) + 1]] <- bad_frmp
  }
  
  # --- NEWFRMP missing ---
  if (has_newfrmp) {
    missing_newfrmp <- m57a_rows %>%
      filter(!is.na(newfrmp), newfrmp != "") %>%
      filter(!(newfrmp %in% lookup_ids))
    
    if (nrow(missing_newfrmp)) {
      bad_newfrmp <- missing_newfrmp %>%
        distinct(newfrmp) %>%
        transmute(
          stat_shortcut = "M57A",
          id_type       = "NEWFRMP",
          missing_id    = newfrmp
        )
      fail_rows[[length(fail_rows) + 1]] <- bad_newfrmp
    }
  }
  
} else {
  cli::cli_alert_info("No SMALL M57A rows present.")
}

# ==============================================================================
# 7. Final outcome → error or success
# ==============================================================================

if (length(fail_rows)) {
  
  summary_fail <- bind_rows(fail_rows) %>%
    arrange(stat_shortcut, id_type, missing_id)
  
  out_dir  <- file.path(output_dir, "missing_ids")
  dir_create(out_dir, recurse = TRUE)
  out_path <- file.path(out_dir, "missing_ids_summary.csv")
  
  write_csv(summary_fail, out_path, na = "")
  
  cli::cli_alert_info("Summary of missing IDs:")
  print(summary_fail, n = nrow(summary_fail))
  cli::cli_alert_info("Saved to: {out_path}")
  
  stop(
    paste(
      "Validation failed. FRMP or NEWFRMP values are missing from the unified Retailer Lookup.",
      "",
      "To fix:",
      "  1. Update RetailersLookup.xlsx with correct Participant/CorporationIDs.",
      "  2. Run update_retailers_lookup.R to refresh dbo.aemo_retailer_lookup.",
      "  3. Re-run this validation script.",
      sep = "\n"
    ),
    call. = FALSE
  )
  
} else {
  cli::cli_alert_success("Validation passed. All retailer IDs map correctly.")
}
