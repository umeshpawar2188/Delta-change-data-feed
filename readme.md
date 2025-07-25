# 📊 Delta Lake CDF ETL Pipeline for Stock Exchange Data

This repository demonstrates how to implement a Change Data Feed (CDF)-based ETL pipeline using PySpark and Delta Lake in Microsoft Fabric or Databricks. The sample use case involves tracking changes to stock trade data across days, and promoting those changes from a Silver Lakehouse to a business-ready Gold Lakehouse.

---

## 📁 Project Structure

| File Name                  | Description                                                                 |
|---------------------------|-----------------------------------------------------------------------------|
| `cdf_utils.py`            | Common utility functions to read metadata, get latest version, and filter CDF changes. |
| `cdf_driver.py`           | Main executable script to process Day N changes and apply them to the Gold layer using `MERGE`. |
| `sample_data_load.py`     | Loads initial sample stock trade data into the Silver and Gold layers (Day 0). |
| `day0_day1_etl.py`        | Optional consolidated version of the day 0 + day 1 logic in a single script. |
| `create_etl_metadata.sql` | SQL script to create the `etl_metadata` tracking table used to store processing version and timestamp. |
| `CDF_PyScripts.zip`       | Zip archive of all the code artifacts for easy download and use.            |

---

## 📈 Sample Data

The Silver layer table (`Silver_StockTrades`) tracks stock exchange activity:

| Exchange         | NumTradesExecuted | TotalVolume |
|------------------|-------------------|-------------|
| LSE_London       | 100000            | 200000      |
| NASDAQ_NewYork   | 150000            | 250000      |
| NYSE_NewYork     | 130000            | 240000      |

We simulate updates, deletions, and insertions for **Day 1**:

- ✅ **Update**: `LSE_London` trades increased to `105000`
- ❌ **Delete**: Remove `NASDAQ_NewYork`
- ➕ **Insert**: Add `TSX_Toronto` with `NumTradesExecuted = 40000`, `TotalVolume = 48000`

---

## 🔧 Prerequisites

- Spark environment (Databricks or Microsoft Fabric)
- Delta Lake support
- Tables created in `lseg_lh_silver` and `lseg_lh_gold` Lakehouses
- Permissions to run `MERGE`, `UPDATE`, and `INSERT` queries

---

## 🧪 Setup Instructions

### 1. Create Metadata Table

```sql
-- Run in Spark SQL environment
CREATE TABLE lseg_lh_silver.dbo.etl_metadata (
    table_name STRING,
    last_processed_version INT,
    last_processed_timestamp TIMESTAMP
);

INSERT INTO lseg_lh_silver.dbo.etl_metadata VALUES (
    'Silver_StockTrades', 0, current_timestamp()
);
