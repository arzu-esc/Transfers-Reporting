# ==============================================================================
# Script: update_retailers_lookup.R
# Purpose: Refresh an AEMO → ESC retailer lookup table in SQL Server.
#
# Description:
#   - Downloads RetailersLookup.xlsx from SharePoint.
#   - Expects three source columns: PARTICIPANTID, CORPORATIONID, ESC RetailerCommonID.
#   - Normalises all IDs and expands rows:
#       - If PARTICIPANTID == CORPORATIONID → keep one row.
#       - If they differ → produce two rows.
#       - Remove blanks, NAs, whitespace and perfect duplicates.
#   - Builds ONE unified lookup table with columns:
#         id, licence_common_id
#   - Truncates + reloads dbo.aemo_retailer_lookup in SQL Server.
#
# Author: Arzu Khanna
# Last updated: 2025-11-25
# ==============================================================================

# ==============================================================================
# 1. Download the Excel file from SharePoint
# ==============================================================================

xlsx_filename          <- "RetailersLookup.xlsx"
sheet_name             <- "CorporationID Lookup"  

cli::cli_h1("Updating Retailers Lookup Table")

temp_file <- tempfile(fileext = ".xlsx")
dl_path   <- paste0(sharepoint_aemo_folder, "/", xlsx_filename)

cli_alert_info("Downloading: {dl_path}")
DRIVE$get_item(dl_path)$download(dest = temp_file)
on.exit({ if (file_exists(temp_file)) file_delete(temp_file) }, add = TRUE)

cli_alert_success("Downloaded to temporary file: {temp_file}")


# ==============================================================================
# 2. Load Excel and validate columns
# ==============================================================================

cli_alert_info("Reading Excel sheet: {sheet_name}")

df <- read_excel(temp_file, sheet = sheet_name)

needed_cols <- c("PARTICIPANTID", "CORPORATIONID", "ESC RetailerCommonID")
missing_cols <- setdiff(needed_cols, names(df))
if (length(missing_cols)) {
  stop("❌ Missing required columns in Excel: ", paste(missing_cols, collapse = ", "))
}

# ==============================================================================
# 3. Build unified lookup table (DROP rows with missing licence_common_id)
# ==============================================================================

cli_alert_info("Transforming lookup data…")

lookup_tbl <- df %>%
  transmute(
    participant_id     = as.character(`PARTICIPANTID`),
    corporation_id     = as.character(`CORPORATIONID`),
    licence_common_id  = as.character(`ESC RetailerCommonID`)
  ) %>%
  mutate(across(everything(), ~ trimws(.))) %>%
  
  # Drop rows where participant + corp IDs are BOTH empty
  filter(
    !(
      (is.na(participant_id) | participant_id == "") &
        (is.na(corporation_id) | corporation_id == "")
    )
  ) %>%
  
  # Split into long format → one ID per row
  pivot_longer(
    cols = c(participant_id, corporation_id),
    names_to = "id_type",
    values_to = "id"
  ) %>%
  
  # Remove empty ID rows
  filter(!is.na(id), id != "") %>%
  
  # Final unified structure
  transmute(
    id = id,
    licence_common_id = licence_common_id
  ) %>%
  
  distinct()

cli_alert_success("Unified lookup created: {nrow(lookup_tbl)} unique ID rows.")

# ==============================================================================
# 4. Connect to SQL Server
# ==============================================================================

cli_alert_info("Connecting to SQL Server…")

con <- dbConnect(
  odbc::odbc(),
  Driver         = driver,
  Server         = server,
  Database       = db_name,
  Authentication = auth,
  uid            = UID,
  Mars_Connection = "yes",
  Packet_Size     = 32767,
  QueryTimeout    = 0
)
on.exit(try(dbDisconnect(con), silent = TRUE), add = TRUE)

cli_alert_success("Connected.")


# ==============================================================================
# 5. Truncate + Reload unified lookup table
# ==============================================================================

# SQL unified lookup table
lookup_table <- DBI::Id(schema = "dbo", table = "aemo_retailer_lookup")

cli_alert_info("Truncating existing table dbo.aemo_retailer_lookup…")
dbExecute(con, "TRUNCATE TABLE dbo.aemo_retailer_lookup;")

cli_alert_info("Reloading new lookup table…")
dbAppendTable(con, lookup_table, lookup_tbl)

n_rows <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM dbo.aemo_retailer_lookup;")$n

cli_alert_success("Lookup refresh complete.")
cli_alert_success("→ aemo_retailer_lookup now contains {format(n_rows, big.mark=',')} rows.")

cli_alert("Retailer Lookup Update Successful ✔")
