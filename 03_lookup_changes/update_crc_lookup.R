# ==============================================================================
# Name: 03_update_crc_lookup.R
# Author: Arzu Khanna
# Date: 2025-11-03
# Description:
#
#   Downloads the latest CRC_lookup.xlsx from SharePoint (AEMO folder), reads the
#   "Lookup" sheet, and refreshes the existing SQL table:
#       dbo.aemo_crc_lookup
#
#   Assumes dbo.aemo_crc_lookup already exists in SQL Server.
# ==============================================================================

suppressPackageStartupMessages({
  library(Microsoft365R)
  library(readxl)
  library(DBI)
  library(odbc)
  library(dplyr)
  library(cli)
  library(fs)
})

# ====== CONFIG ======
sharepoint_url         <- Sys.getenv("SHAREPOINT_SITE_URL")
sharepoint_aemo_folder <- Sys.getenv("SHAREPOINT_AEMO_FOLDER")  # e.g. "AEMO"
xlsx_filename          <- "CRC_lookup.xlsx"
sheet_name             <- "Lookup"

driver            <- Sys.getenv("SQL_DRIVER", unset = "ODBC Driver 17 for SQL Server")
sql_server_name   <- Sys.getenv("SQL_SERVER")
sql_database_name <- Sys.getenv("SQL_DATABASE")
user_id           <- Sys.getenv("SQL_UID")
auth              <- Sys.getenv("SQL_AUTHENTICATION", unset = "ActiveDirectoryInteractive")

# ====== DOWNLOAD FROM SHAREPOINT ======
cli_alert_info("Connecting to SharePoint and downloading {xlsx_filename} ...")
SITE  <- get_sharepoint_site(site_url = sharepoint_url)
DRIVE <- SITE$get_drive("Documents")

temp_file <- tempfile(fileext = ".xlsx")
DRIVE$get_item(paste0(sharepoint_aemo_folder, "/", xlsx_filename))$download(dest = temp_file)
on.exit({ if (file_exists(temp_file)) file_delete(temp_file) }, add = TRUE)
cli_alert_success("Downloaded temporary file: {temp_file}")

# ====== READ EXCEL (minimal checks) ======
cli_alert_info("Reading sheet: {sheet_name}")
df <- read_excel(temp_file, sheet = sheet_name)

needed  <- c(
  "CRC_Code",
  "Event",
  "Sub_event",
  "Description",
  "Further_details",
  "Version",
  "Reporting_group"
)
missing <- setdiff(needed, names(df))
if (length(missing)) {
  stop("Missing columns in Excel: ", paste(missing, collapse = ", "))
}

# Build upload frame
crc_tbl <- df %>%
  transmute(
    crc_code        = as.character(`CRC_Code`),
    event           = as.character(`Event`),
    sub_event       = as.character(`Sub_event`),
    description     = as.character(`Description`),
    further_details = as.character(`Further_details`),
    version         = as.character(`Version`),
    reporting_group = as.character(`Reporting_group`)
  ) %>%
  mutate(across(everything(), ~ trimws(.))) %>%
  filter(!is.na(crc_code), crc_code != "") %>%
  distinct(crc_code, .keep_all = TRUE)

cli_alert_success("Prepared CRC lookup rows: {nrow(crc_tbl)}")

# ====== SQL CONNECTION ======
cli_alert_info("Connecting to SQL Server…")
con <- dbConnect(
  odbc::odbc(),
  Driver         = driver,
  Server         = sql_server_name,
  Database       = sql_database_name,
  uid            = user_id,
  Authentication = auth,
  Mars_Connection = "yes",
  Packet_Size     = 32767,
  QueryTimeout    = 0
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
cli_alert_success("Connected to {sql_database_name}")

# ====== TRUNCATE + RELOAD (table assumed to exist) ======
cli_alert_info("Truncating dbo.aemo_crc_lookup …")
dbExecute(con, "TRUNCATE TABLE dbo.aemo_crc_lookup;")

cli_alert_info("Loading CRC lookup …")
dbAppendTable(con, Id(schema = "dbo", table = "aemo_crc_lookup"), crc_tbl)

n_crc <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM dbo.aemo_crc_lookup;")$n
cli_alert_success("Reload complete → dbo.aemo_crc_lookup: {format(n_crc, big.mark = ',')} rows")

cli_alert_success("CRC lookup refresh successful.")