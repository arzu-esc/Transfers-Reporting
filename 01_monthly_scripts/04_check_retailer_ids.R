# ==============================================================================

# Script: 04_check_retailer_ids.R
# Purpose: Validate FRMP and NEWFRMP IDs in the latest monthly AEMO CSV
# Description:
#   Validate NEW monthly AEMO CSV in 02_data/ against SQL lookup tables:
#   - M71: FRMP must exist in dbo.aemo_participantid_lookup.participant_id
#   - M57A: FRMP and NEWFRMP must exist in dbo.aemo_corpid_lookup.corp_id
# Author: Arzu Khanna
# Last updated: 2025-11-18

# ==============================================================================

# ---------------------------------------------------------------------
# 1. Config settings
# ---------------------------------------------------------------------

output_dir <- "04_outputs"

if (!exists("new_data_file", envir = .GlobalEnv)) {
  stop("new_data_file not found — run 02_get_new_month_data.R first.")
}

csv_path <- get("new_data_file", envir = .GlobalEnv)
cli::cli_alert_info("Validating monthly file: {csv_path}")

# ---------------------------------------------------------------------
# 2. Read CSV
# ---------------------------------------------------------------------

df <- read_csv(
  csv_path,
  col_types = cols(.default = col_character()),
  show_col_types = FALSE
) %>%
  clean_names()

if (!"stat_shortcut" %in% names(df) || !"frmp" %in% names(df)) {
  stop("Incoming CSV is missing required columns (stat_shortcut, frmp).")
}

# ---------------------------------------------------------------------
# 3. Lookups from SQL
# ---------------------------------------------------------------------

cli::cli_alert_info("Connecting to SQL and reading lookup tables…")

con <- DBI::dbConnect(
  odbc::odbc(),
  Driver                 = driver,
  Server                 = sql_server_name,
  Database               = sql_database_name,
  uid                    = user_id,
  Authentication         = auth,
  Encrypt                = "yes",
  TrustServerCertificate = "yes",
  Timeout                = 0
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

part_lu <- DBI::dbReadTable(con, DBI::Id(schema = "dbo", table = "aemo_participantid_lookup")) |>
  mutate(participant_id = trimws(as.character(participant_id)))

corp_lu <- DBI::dbReadTable(con, DBI::Id(schema = "dbo", table = "aemo_corpid_lookup")) |>
  mutate(corp_id = trimws(as.character(corp_id)))

cli::cli_alert_success("Got participant ({nrow(part_lu)}) and corp ({nrow(corp_lu)}) lookups.")

# Collect failures
fail_rows <- list()

# ---------------------------------------------------------------------
# 4. Validate M71: FRMP must exist in participant lookup
# ---------------------------------------------------------------------
m71_rows <- df |>
  filter(stat_shortcut == "M71", nmiclasscode == "SMALL") |>
  mutate(frmp = trimws(frmp))

if (nrow(m71_rows)) {
  missing_m71 <- m71_rows |>
    filter(is.na(frmp) | frmp == "" | !(frmp %in% part_lu$participant_id))
  
  if (nrow(missing_m71)) {
    # build compact summary
    m71_summary <- missing_m71 |>
      filter(!is.na(frmp), frmp != "") |>
      distinct(frmp) |>
      transmute(
        stat_shortcut = "M71",
        frmp_newfrmp  = "FRMP",
        missing_id    = frmp,
        lookup_field  = "participant_id"
      )
    fail_rows[[length(fail_rows) + 1]] <- m71_summary
  }
} else {
  cli::cli_alert_info("No M71 rows present in this monthly file.")
}

# ---------------------------------------------------------------------
# . Validate M57A: FRMP + NEWFRMP must exist in corp lookup
# ---------------------------------------------------------------------

m57a_rows <- df |>
  filter(stat_shortcut == "M57A", nmiclasscode == "SMALL")

has_newfrmp <- "newfrmp" %in% names(m57a_rows)

# normalise FRMP always
m57a_rows <- m57a_rows |>
  mutate(frmp = trimws(frmp))

# normalise NEWFRMP only if present
if (has_newfrmp) {
  m57a_rows <- m57a_rows |>
    mutate(newfrmp = trimws(newfrmp))
}

if (nrow(m57a_rows)) {
  # FRMP missing
  m57a_missing_frmp <- m57a_rows |>
    filter(is.na(frmp) | frmp == "" | !(frmp %in% corp_lu$corp_id))
  
  if (nrow(m57a_missing_frmp)) {
    m57a_frmp_summary <- m57a_missing_frmp |>
      filter(!is.na(frmp), frmp != "") |>
      distinct(frmp) |>
      transmute(
        stat_shortcut = "M57A",
        frmp_newfrmp  = "FRMP",
        missing_id    = frmp,
        lookup_field  = "corp_id"
      )
    fail_rows[[length(fail_rows) + 1]] <- m57a_frmp_summary
  }
  
  # NEWFRMP missing (only if column exists)
  if (has_newfrmp) {
    m57a_missing_newfrmp <- m57a_rows |>
      filter(!is.na(newfrmp), newfrmp != "") |>
      filter(!(newfrmp %in% corp_lu$corp_id))
    
    if (nrow(m57a_missing_newfrmp)) {
      m57a_newfrmp_summary <- m57a_missing_newfrmp |>
        filter(!is.na(newfrmp), newfrmp != "") |>
        distinct(newfrmp) |>
        transmute(
          stat_shortcut = "M57A",
          frmp_newfrmp  = "NEWFRMP",
          missing_id    = newfrmp,
          lookup_field  = "corp_id"
        )
      fail_rows[[length(fail_rows) + 1]] <- m57a_newfrmp_summary
    }
  }
} else {
  cli::cli_alert_info("No M57A rows present in this monthly file.")
}

# ---------------------------------------------------------------------
# 6. Final result: output + error if failures exist
# ---------------------------------------------------------------------
if (length(fail_rows)) {
  # bind all problems
  summary_fail <- dplyr::bind_rows(fail_rows) |>
    arrange(stat_shortcut, frmp_newfrmp, missing_id)
  
  out_dir <- file.path(output_dir, "missing_ids")
  dir_create(out_dir, recurse = TRUE)
  out_path <- file.path(out_dir, "missing_ids_summary.csv")
  readr::write_csv(summary_fail, out_path, na = "")
  
  cli::cli_alert_info("Summary of missing IDs:")
  print(summary_fail, n = nrow(summary_fail))
  cli::cli_alert_info("Saved to: {out_path}")
  
  stop(
    paste(
      "Validation failed as there are FRMP and/or NEWFRMP values that cannot be mapped in RetailersLookup.",
      "Steps to take:",
      "1. Contact AEMO and ask for:",
      "   - Updated participant ID list",
      "   - Updated participant and companyID mapping.",
      "2. Update RetailersLookup with the new mapping.",
      "3. Run 03_lookup_changes/update_retailers_lookup.R.",
      "4. Run 01_monthly_scripts/check_retailer_ids.R again to make sure the mapping is working.",
      "5. Run the transfer report.",
      sep = "\n"
    ),
    call. = FALSE
  )
  
  
} else {
  cli::cli_alert_success("Validation passed. You can now run the SQL load for 02_data/")
}