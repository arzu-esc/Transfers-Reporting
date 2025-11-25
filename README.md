# AEMO MSATS Transfers reporting pipeline.

## Overview

Purpose: Collect monthly AEMO (Australian Energy Market Operator) retail transfer CSVs from SharePoint, validate and load them into an Azure SQL staging table (`stg.aemo_transfers`), run verification checks and generate monthly outputs and charts.

### What the Pipeline Does

**Data Processing:**
- Downloads monthly transfer data files from SharePoint (CSV format)
- Validates retailer identifiers against authoritative lookup tables
- Loads validated data into a SQL Server staging environment
- Enriches raw transfer records with retailer names, sizes, and classifications
- Maintains a complete historical dataset from 2018 onwards

**Analysis & Reporting:**
- Generates interactive HTML reports with drill-down capabilities
- Tracks transfer trends over time (daily, monthly, quarterly, yearly views)
- Analyzes market movements between retailers and retailer size categories
- Calculates net customer gains/losses for each retailer
- Visualizes transfer flows using Sankey diagrams
- Identifies top gaining and losing retailers

**Data Quality:**
- Validates all retailer codes before loading into production
- Maintains checkpoint system to prevent duplicate data loads
- Creates audit snapshots of all imported data
- Flags unmapped retailer IDs for resolution with AEMO

### Key Metrics Tracked

- **M57A** - Transfer statistics (customer switches between retailers)
- **M71** - Active NMI (National Meter Identifier) counts per retailer

## Prerequisites

### Software Requirements
- **R**
- **RStudio**
- **ODBC Driver** for SQL Server (version 17 or 18)

### R Packages
**Core data processing:** `DBI`, `odbc`, `dplyr`, `lubridate`, `readr`, `janitor`, `tidyr`  
**SharePoint integration:** `Microsoft365R`  
**Utilities:** `cli`, `fs`, `here`, `stringr`, `glue`, `purrr`  
**Reporting:** `rmarkdown`, `plotly`, `crosstalk`, `kableExtra`, `htmltools`  
**Excel handling:** `readxl`, `writexl`, `openxlsx`

### Access Requirements
- **Azure SQL Database** read/write permissions to:
  - `stg.aemo_transfers` (staging table for incoming data)
  - `dbo.aemo_transfers_data` (production table with enriched records)
  - `dbo.vw_aemo_transfers` (analytical view with all lookups applied)
  - `dbo.aemo_retailer_lookup` (AEMO IDs to ESC retailer ID mapping)
  - `dbo.aemo_crc_lookup` (Change Request Code definitions)
  - `dbo.usp_upsert_aemo_transfers_data` (stored procedure for incremental refresh)
  
- **SharePoint** access to:
  - AEMO monthly transfer data folder (source CSVs)
  - Import snapshots folder (audit trail)
  - Lookup files: `RetailersLookup.xlsx`, `CRC_lookup.xlsx`

## Project Structure

### Historical Data Load (00_mass_upload_scripts/)
**Purpose:** One-time bulk import of historical data (2018-2025)

| Script Name | Description |
|------------|-------------|
| `00_get_data.R` | Download ALL monthly transfer CSVs from SharePoint into local data folder |
| `01_load_and_write_to_sql.R` | Batch processing script for historical data:<br>- Processes 93 CSV files in configurable batches (default: 5 files)<br>- Uses streaming reads with 200k row chunks to manage memory<br>- Accumulates 2-3 chunks before writing to reduce SQL round trips<br>- Wraps each batch in SQL transaction for atomicity<br>- Creates timestamped snapshots after each successful batch<br>- Uploads snapshots to SharePoint for audit trail<br>- Updates checkpoint after each commit<br>- Logs detailed progress (chunk counts, row counts, timestamps)<br>- Rolls back and stops on any error to maintain consistency<br>- **Run once during initial setup** |
| `02_verification.R` | Post-load validation suite:<br>- Reads checkpoint file to get list of loaded files<br>- Processes files in tiny chunks (1 at a time) to avoid SQL timeout<br>- **Check 1:** Verifies each file exists in SQL<br>- **Check 2:** Validates date columns (`stat_date`, `processingdt`, `maintcreatedt`) have no NULLs<br>- **Check 3:** Compares SQL row counts against source CSV line counts<br>- Uses fast line counting (streaming, no parsing)<br>- Generates comprehensive summary report<br>- Flags any discrepancies for investigation |

---

### Monthly Update Pipeline (01_monthly_scripts/)
**Purpose:** Routine monthly data ingestion and reporting

| Script Name | Description |
|------------|-------------|
| `00_run_all.R` | **Primary entry point** - Executes all scripts below |
| `00_run_completion.R` | **Completion pipeline** - Loads validated data to SQL, prepares reporting data, and generates HTML report. Run manually after fixing validation failures |
| `01_config.R` | Configuration and environment setup:<br>- Loads all required R packages<br>- Defines database connection parameters **⚠️ Update `UID` with your email**<br>- Specifies SharePoint paths for data sources and snapshots<br>- Sets local timezone to Australia/Melbourne |
| `02_get_new_month_data.R` | SharePoint data retrieval:<br>- Connects to ESC SharePoint site using Microsoft365R<br>- Lists all CSV files in configured data folder<br>- Identifies newest file by `lastModifiedDateTime`<br>- Clears local `02_data/` folder to prevent confusion<br>- Downloads latest monthly transfer file<br>- Sets global variable `new_data_file` for downstream scripts |
| `04_check_retailer_ids.R` | **Validation checkpoint** - Data quality gate (runs BEFORE SQL load):<br>- Reads the newly downloaded CSV from `02_data/`<br>- Retrieves current lookup tables from SQL<br>- **M71 validation:** Checks FRMP exists in `participant_id` lookup<br>- **M57A validation:** Checks FRMP and NEWFRMP exist in `corp_id` lookup<br>- Generates summary report of missing IDs<br>- **⚠️ Stops pipeline execution if validation fails**<br>- Outputs detailed error report to `04_outputs/missing_ids/`<br>- Provides remediation instructions for contacting AEMO<br>- **Only proceeds to SQL load if validation passes** |
| `03_load_data_sql.R` | Data loading and transformation (runs AFTER successful validation):<br>- Reads validated CSV using streaming for memory efficiency<br>- Transforms column names to snake_case<br>- Parses Australian date formats (d/m/Y H:M)<br>- Converts data types (integers, bits, dates)<br>- Adds metadata columns (`source_file`, `date_imported`)<br>- Appends to `stg.aemo_transfers` in batches<br>- Creates timestamped snapshot (local + SharePoint)<br>- Updates checkpoint file to track loaded files<br>- Executes `dbo.usp_upsert_aemo_transfers_data` to refresh production table |
| `05_read_updated_transfer_data.R` | Report data preparation:<br>- Connects to SQL Server database<br>- Queries `dbo.vw_aemo_transfers` (enriched view with lookups)<br>- Retrieves all required columns for reporting<br>- Saves result as `02_data/transfers_raw.rds` for R Markdown report<br>- Provides fast, serialized data access for report rendering |

---

### Lookup Updates (03_lookup_changes/)
**Purpose:** Maintain reference data for retailer identification

| Script Name | Description |
|------------|-------------|
| `update_retailers_lookup.R` | Retailer ID mapping refresh:<br>- Downloads `RetailersLookup.xlsx` from SharePoint AEMO folder<br>- Reads "CorporationID Lookup" sheet<br>- Expects columns: `PARTICIPANTID`, `CORPORATIONID`, `ESC RetailerCommonID`<br>- Creates two lookup tables:<br>&nbsp;&nbsp;• `dbo.aemo_corpid_lookup` (corp_id → licence_common_id)<br>&nbsp;&nbsp;• `dbo.aemo_participantid_lookup` (participant_id → licence_common_id)<br>- Truncates and reloads tables (full refresh strategy)<br>- Logs row counts for verification<br>- **Run when AEMO provides updated retailer mappings** |
| `update_crc_lookup.R` | Customer Role Code definitions refresh:<br>- Downloads `CRC_lookup.xlsx` from SharePoint AEMO folder<br>- Reads "Lookup" sheet<br>- Parses CRC metadata (code, event, description, reporting group)<br>- Truncates and reloads `dbo.aemo_crc_lookup`<br>- Used for transfer event classification and reporting |

---

### Checkpoints & snapshots (05_checkpoints/ and 02_data/snapshots/)
**Purpose:** Maintain a record of what has been loaded to SQL

- **Mass load checkpoints**: `05_checkpoints/transfers_completed_files.csv` tracks which files have been loaded via the mass-load pipeline.
- **Monthly snapshots**: Saved to `02_data/snapshots/` when `03_load_data_sql.R` runs (both local CSV and upload to SharePoint).
- Checkpoints are read before processing to skip already-loaded files and avoid duplicates.

---

### Reporting

| File Name | Description |
|-----------|-------------|
| `transfers_report.Rmd` | R Markdown analytical report:<br>**Data Visualizations:**<br>- Interactive Plotly charts with drill-down (monthly/quarterly/yearly)<br>- Time series of transfer trends by retailer size<br>- Breakdown by transfer type (Change Retailer, Move-in, Other)<br>- Sankey diagrams showing retailer-to-retailer flows<br>- Heatmap matrices of transfer movements<br>- Net change analysis (absolute and normalized)<br>- Top 5 move-in retailers over 12 months<br>- Top 10 retailers gaining/losing customers<br>**Features:**<br>- Tabbed interface for exploring different time periods<br>- Toggle buttons for absolute vs. percentage views<br>- Color-coded by retailer size (Small/Medium/Large)<br>- Collapsible summary tables<br>- ESC brand styling (colors, fonts)<br>- Responsive design for viewing on different devices |

---

## Set-Up Steps

### Initial Setup (One-Time)

**1. Clone Repository**
   - Open RStudio
   - Select File → New Project → Version Control → Git
   - Paste repository URL:
```bash
     https://github.com/arzu-esc/aemo-retail-transfers.git
```
   > ⚠️ **Important:** Ensure project directory is **NOT** in OneDrive or SharePoint (prevents file locking issues)

**2. Configure Project**
   - Install required packages:
   ```r
    install.packages(c("dplyr", "ggplot2", "janitor", "readxl", "scales"))
   ``` 
   - Open `01_monthly_scripts/01_config.R`
   - Update database connection:
```r
     UID <- "your.email@esc.vic.gov.au"  # Replace with your ESC email
```
   - Verify SharePoint paths match your site structure:
```r
     sharepoint_url <- "https://escvic.sharepoint.com/teams/..."
     sharepoint_data_folder <- "3 - Services/AEMO MSATS Transfers Data/Data"
     sharepoint_import_folder <- "5 - Data Repository/AEMO Transfers/Imported"
```

**3. Prepare for Monthly Update**
  - Ensure new monthly CSV has been published to SharePoint by AEMO
  - Check that previous month's update completed successfully
  - Verify you have database write permissions

**4. Run Primary Pipeline**
```r
setwd("01_monthly_scripts")
source("00_run_all.R")
```

This executes in sequence:
1. **Configuration** (`01_config.R`)
2. **Data Download** (`02_get_new_month_data.R`)
3. **Validation** (`03_check_retailer_ids.R`)

**Two possible outcomes:**

1. **All retailer IDs map correctly** and the rest of the scripts execute automatically:
  - `04_load_data_sql.R`
  - `05_read_updated_transfer_data.R`
  - `transfers_report.Rmd` --> Generates `transfers_report.html`

2. **AEMO retailer IDs missing from retailer look up**

  **Steps to take:**
    1. Review the missing_ids in `04_outputs/missing_ids`
    
    ```r
    missing_ids <- read.csv("04_outputs/missing_ids/missing_ids_summary.csv")
    View(missing_ids)
    ```
    This shows:
      - Which stat type has unmapped IDs (M71 or M57A)
      - Which field is affected (FRMP or NEWFRMP)
      - The actual unmapped ID values
      
    2. Contact AEMO and ask for:
      - Updated participant ID list
      - Updated participant and company ID mapping.
      
    3. Update **RetailersLookup.xlsx** with the new mapping.
      - Navigate to SharePoint AEMO folder
      - Open `RetailersLookup.xlsx`
      - Add new rows for unmapped IDs
      - Ensure all columns are complete:
        - `PARTICIPANTID` - AEMO participant identifier
        - `CORPORATIONID` - AEMO corporation identifier  
        - `ESC RetailerCommonID` - ESC internal retailer ID
      - Save and close file
    
    4. Run script to Refresh SQL Lookup Table:
    
      ```r 
      source("03_lookup_changes/update_retailers_lookup.R")
      ```
      
    5. Run retailer id validation script again to make sure mapping is working: 
    
      ```r 
      source("01_monthly_scripts/04_check_retailer_ids.R")
      ```
      
    6. Run 00_run_completion.R to complete the pipeline if validation passes:
    
      ```r 
      source("00_run_completion.R")
      ```
---

### 7. Manually Regenerate Report (Optional)

If you need to regenerate just the report (without re-loading data):

```r
# First ensure data is current
source("01_monthly_scripts/05_read_updated_transfer_data.R")

# Then render report
rmarkdown::render("transfers_report.Rmd")
```

---

### 8. Verify Completion

**Checklist:**
- [ ] Console shows: `"Transfer report created"`
- [ ] File exists: `02_data/transfers_raw.rds`
- [ ] File exists: `transfers_report.html`
- [ ] Report opens correctly in browser
- [ ] Charts are interactive (hover tooltips work)
- [ ] Drill-down buttons function properly
- [ ] Data matches expected date range
- [ ] Latest month appears in visualisations
- [ ] Snapshot uploaded to SharePoint: `5 - Data Repository/AEMO Transfers/Imported/`
- [ ] Checkpoint updated with new file

**Troubleshooting**
- If a SharePoint connection fails, confirm `SHAREPOINT_SITE_URL` and `SHAREPOINT_DATA_FOLDER` values and that `Microsoft365R` can access your SharePoint via AAD.
- If SQL connection fails, confirm `SQL_SERVER`, `SQL_DATABASE`, and your account permissions. Try connecting with `odbc::odbc()` interactively first.
- If a script stops because a file is already in the checkpoint, check `transfers_completed_files.csv` to verify if the file was already loaded.

**Contact / Author**
- For questions about the pipeline, contact Arzu Khanna (author of the scripts).

---
This README is a summary to help run and maintain the Transfers-Reporting pipeline. For details, inspect the scripts referenced above.
