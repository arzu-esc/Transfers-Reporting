con <- DBI::dbConnect(
  odbc::odbc(),
  Driver         = "ODBC Driver 17 for SQL Server",
  Server         = "esc-dev-ssdw-01.database.windows.net",
  Database       = "esc-dev-poc-01",
  Authentication = "ActiveDirectoryInteractive"
)

transfers_raw <- dbGetQuery(con, "
  SELECT 
    id_mts2,
    stat_date,
    Year,
    Month,
    FinYear,
    FinYearQuarter,
    stat_shortcut,
    stat_value,
    lnsp,
    lr,
    frmp,
    newfrmp,
    crcode,
    ESC_FRMP,
    ESC_NEWFRMP,
    FRMP_Retailer_name,
    NEWFRMP_Retailer_name,
    FRMP_Retailer_size,
    NEWFRMP_Retailer_size,
    transfer_type
  FROM dbo.vw_aemo_transfers
")

saveRDS(transfers_raw, "02_data/transfers_raw.rds")
