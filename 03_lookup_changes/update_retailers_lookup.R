# ──────────────────────────────────────────────────────────────────────────────
# SharePoint → SQL Server
#   - Loads:
#       dbo.aemo_corpid_lookup(corp_id, licence_common_id)
#       dbo.aemo_participantid_lookup(participant_id, licence_common_id)
#   - Source Excel columns expected: PARTICIPANTID, CORPORATIONID, ESC RetailerCommonID
# ──────────────────────────────────────────────────────────────────────────────

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
xlsx_filename          <- "RetailersLookup.xlsx"
sheet_name             <- "CorporationID Lookup"  # <-- your tab name (adjust if needed)

driver            <- Sys.getenv("SQL_DRIVER", unset = "SQL Server")
sql_server_name   <- Sys.getenv("SQL_SERVER")
sql_database_name <- Sys.getenv("SQL_DATABASE")
user_id           <- Sys.getenv("SQL_UID")
auth              <- Sys.getenv("SQL_AUTHENTICATION", unset = "ActiveDirectoryIntegrated")

# ====== DOWNLOAD FROM SHAREPOINT ======
cli_alert_info("Connecting to SharePoint and downloading RetailersLookup.xlssx...")
SITE  <- get_sharepoint_site(site_url = sharepoint_url)
DRIVE <- SITE$get_drive("Documents")

temp_file <- tempfile(fileext = ".xlsx")
DRIVE$get_item(paste0(sharepoint_aemo_folder, "/", xlsx_filename))$download(dest = temp_file)
on.exit({ if (file_exists(temp_file)) file_delete(temp_file) }, add = TRUE)
cli_alert_success("Downloaded: {temp_file}")

# ====== READ EXCEL (minimal checks) ======
cli_alert_info("Reading sheet: {sheet_name}")
df <- read_excel(temp_file, sheet = sheet_name)

# Expect: PARTICIPANTID, CORPORATIONID, ESC RetailerCommonID
needed <- c("PARTICIPANTID", "CORPORATIONID", "ESC RetailerCommonID")
missing <- setdiff(needed, names(df))
if (length(missing)) stop("Missing columns in Excel: ", paste(missing, collapse = ", "))

# Build two upload frames
corpid_tbl <- df %>%
  transmute(
    corp_id            = as.character(`CORPORATIONID`),
    licence_common_id  = as.character(`ESC RetailerCommonID`)
  ) %>%
  mutate(across(everything(), ~ trimws(.))) %>%
  filter(!is.na(corp_id), corp_id != "") %>%
  distinct(corp_id, .keep_all = TRUE)

participant_tbl <- df %>%
  transmute(
    participant_id     = as.character(`PARTICIPANTID`),
    licence_common_id  = as.character(`ESC RetailerCommonID`)
  ) %>%
  mutate(across(everything(), ~ trimws(.))) %>%
  filter(!is.na(participant_id), participant_id != "") %>%
  distinct(participant_id, .keep_all = TRUE)

cli_alert_success("Prepared: corp {nrow(corpid_tbl)} rows; participant {nrow(participant_tbl)} rows")

# ====== SQL CONNECTION ======
cli_alert_info("Connecting to SQL Server…")
con <- dbConnect(
  odbc::odbc(),
  Driver         = driver,
  Server         = sql_server_name,
  Database       = sql_database_name,
  uid            = user_id,
  Authentication = "ActiveDirectoryInteractive",
  # Performance optimizations
  Mars_Connection = "yes",           # Multiple Active Result Sets
  Packet_Size = 32767,                # Larger packet size (max 32767)
  QueryTimeout = 0                    # No timeout for large batches
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)
cli_alert_success("Connected.")

# ====== TRUNCATE AND RELOAD ======
cli_alert_info("Truncating and reloading lookup tables...")

dbExecute(con, "TRUNCATE TABLE dbo.aemo_corpid_lookup;")
dbExecute(con, "TRUNCATE TABLE dbo.aemo_participantid_lookup;")

dbAppendTable(con, Id(schema="dbo", table="aemo_corpid_lookup"), corpid_tbl)
dbAppendTable(con, Id(schema="dbo", table="aemo_participantid_lookup"), participant_tbl)

n_corp <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM dbo.aemo_corpid_lookup;")$n
n_part <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM dbo.aemo_participantid_lookup;")$n

cli_alert_success("Reload complete.")
cli_alert_success("→ aemo_corpid_lookup: {format(n_corp, big.mark=',')} rows")
cli_alert_success("→ aemo_participantid_lookup: {format(n_part, big.mark=',')} rows")

cli_alert_success("Retailer lookup refresh successful.")