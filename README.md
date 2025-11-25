# AEMO Transfers Reporting

## Overview

Project: AEMO MSATS Transfers reporting pipeline.

Purpose: Collect monthly AEMO transfer CSVs from SharePoint, validate and load them into an Azure SQL staging table (`stg.aemo_transfers`), run verification checks and generate monthly outputs and charts.

Author: Arzu Khanna

Last updated: 2025-11-19

## Prerequisites

### Software Requirements
- **R**
- **RStudio**
- **ODBC Driver** for SQL Server (version 17 or 18)

**Repository layout**
- **`00_mass_upload_scripts/`**: Scripts and raw data used for bulk loading many historical CSVs to SQL. Key files:
  - `00_get_data.R` — download **all** CSVs from SharePoint to `data/raw/` (used for mass/historical loads).
  - `01_load_and_write_to_sql.R` — batch loader for raw CSV files into SQL with transformations and checkpoints (5 files per batch, 200k rows per chunk).
  - `03_verification.R` — verification checks to compare loaded rows and detect null-date issues.
  - `data/raw/` — source CSVs for mass loads (populated by `00_get_data.R`).
  - `data/processed/checkpoints/` — historical checkpoint storage for mass loads.

- **`01_monthly_scripts/`**: Scripts used for the monthly pipeline (preferred for regular monthly refreshes). All scripts source `01_config.R` first for shared config.
  - `01_config.R` — global configuration: folder paths, SQL/SharePoint connection details, timezone, helper functions (`melbourne_now_naive()`, `as_bit()`).
  - `00_run_all.R` — orchestrator that sources scripts 01–05 in sequence to run the full monthly pipeline.
  - `02_get_new_month_data.R` — download the newest monthly CSV from SharePoint into `02_data/`, replacing any previous file.
  - `03_load_data_sql.R` — clean, transform, and append the monthly CSV to `stg.aemo_transfers`; save snapshots locally and to SharePoint; update checkpoint.
  - `04_check_retailer_ids.R` — validate FRMP/NEWFRMP IDs in the monthly CSV against SQL lookup tables; output missing IDs summary.
  - `05_read_updated_transfer_data.R` — read the final view `dbo.vw_aemo_transfers` and save as RDS snapshot (`02_data/transfers_raw.rds`).
  - `transfer_charts.R` — chart generation helpers for Sankey and other visualizations (used by reporting Rmd).

- **`02_data/`**: Working folder for the monthly pipeline. Contains the most recent downloaded CSV and snapshot RDS outputs.

- **`03_lookup_changes/`**: Helpers to update lookup tables (CRC and retailers) used by the validation scripts.

- **`04_outputs/`**: Outputs and validation reports (missing IDs, summaries).

- **`azure-sql-database-dev/`**: SQL Server scripts and stored procedures used for data preparation and incremental refresh on the DB side.

**High-level workflow**
- For the monthly refresh use the monthly scripts (recommended):

  - Download newest file from SharePoint and clear `02_data/`:

```powershell
# from repository root
Rscript 01_monthly_scripts/02_get_new_month_data.R
```

  - Load the newest file to SQL, save snapshots and update checkpoints (this step assumes environment variables and SQL access are configured):

```powershell
Rscript 01_monthly_scripts/03_load_data_sql.R
```

  - Run validations and post-processing (retailer ID checks, and reading final view):

```powershell
Rscript 01_monthly_scripts/04_check_retailer_ids.R
Rscript 01_monthly_scripts/05_read_updated_transfer_data.R
```

  - Or run the entire monthly pipeline from `00_run_all.R` which sources the above scripts in order:

```powershell
Rscript 01_monthly_scripts/00_run_all.R
```

**Environment / prerequisites**
- R (tested with modern R versions); key R packages used across scripts:
  - `DBI`, `odbc`, `dplyr`, `tidyverse`, `readr`, `fs`, `lubridate`, `Microsoft365R`, `cli`, `janitor`, `openxlsx`, `writexl`, `purrr`, `glue`, `here`.
  - Additional packages may be required for plotting (e.g. `plotly`) used by `transfer_charts.R`.

- Database: Azure SQL with network access from the machine running the scripts. The pipeline appends into `stg.aemo_transfers` and expects stored procedures in `azure-sql-database-dev/` for downstream refreshes.

- Authentication: The scripts use Active Directory interactive sign-in by default. Make sure your account has access to the target SQL database and SharePoint site.

**Required environment variables (.Renviron)**
Create a `.Renviron` in your user/home directory or add these variables to your environment before running scripts. The following are referenced in the repository:

- SharePoint
  - `SHAREPOINT_SITE_URL` — full SharePoint site URL (e.g. `https://.../teams/IntelligenceandAnalysisESC`).
  - `SHAREPOINT_DATA_FOLDER` — path within the drive where monthly CSVs live (used by `00_get_data.R`/`02_get_new_month_data.R`).
  - `SHAREPOINT_IMPORT_FOLDER` — (used by `01_load_and_write_to_sql.R` when uploading snapshots).

- SQL
  - `SQL_DRIVER` — e.g. `ODBC Driver 17 for SQL Server` (or `ODBC Driver 18 for SQL Server`).
  - `SQL_SERVER` — server host (e.g. `esc-dev-ssdw-01.database.windows.net`).
  - `SQL_DATABASE` — database name (e.g. `esc-dev-poc-01`).
  - `SQL_AUTH` — authentication method (default `ActiveDirectoryInteractive`).
  - `SQL_UID` — optional user id (email) for some auth methods.

Notes:
- If using `ActiveDirectoryInteractive`, you will be prompted to sign in interactively when the script connects to SQL or SharePoint.
- Store secrets securely and avoid committing `.Renviron` to source control.

**Checkpoints & snapshots**
- **Mass load checkpoints**: `05_checkpoints/transfers_completed_files.csv` tracks which files have been loaded via the mass-load pipeline.
- **Monthly snapshots**: Saved to `02_data/snapshots/` when `03_load_data_sql.R` runs (both local CSV and upload to SharePoint).
- Checkpoints are read before processing to skip already-loaded files and avoid duplicates.

**Troubleshooting**
- If a SharePoint connection fails, confirm `SHAREPOINT_SITE_URL` and `SHAREPOINT_DATA_FOLDER` values and that `Microsoft365R` can access your SharePoint via AAD.
- If SQL connection fails, confirm `SQL_SERVER`, `SQL_DATABASE`, and your account permissions. Try connecting with `odbc::odbc()` interactively first.
- If a script stops because a file is already in the checkpoint, check `transfers_completed_files.csv` to verify if the file was already loaded.

**Extending / Development notes**
- Lookups and retailer validation logic live in `03_lookup_changes/` and the SQL lookup tables in the database (see `azure-sql-database-dev/` for table/stored-proc definitions).
- The `azure-sql-database-dev/` folder contains SQL scripts used to create views and stored procedures for downstream consumption and QA.

**Contact / Author**
- For questions about the pipeline, contact Arzu Khanna (author of the scripts).

---
This README is a summary to help run and maintain the Transfers-Reporting pipeline. For details, inspect the scripts referenced above.
