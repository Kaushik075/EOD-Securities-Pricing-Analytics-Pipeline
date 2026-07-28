
-- copy_to_raw.sql
-- Purpose : Load EOD CSVs from S3 external stage into RAW.RAW_EOD_PRICES
-- Expected : Airflow passes params={"trading_ds_task_id": "<resolver_task_id>"}
-- Notes   : COPY INTO is idempotent per file name (Snowflake load history).


-- Compute & context
USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;
USE SCHEMA RAW;



COPY INTO RAW.RAW_EOD_PRICES
  (TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME, _SRC_FILE, _INGEST_TS)
FROM (
  SELECT
    TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}') AS TRADE_DATE,
    $2::STRING                                    AS SYMBOL,            -- keep source symbol intact (normalize later in CORE)
    TO_DECIMAL($3,18,6)                           AS OPEN,
    TO_DECIMAL($4,18,6)                           AS HIGH,
    TO_DECIMAL($5,18,6)                           AS LOW,
    TO_DECIMAL($6,18,6)                           AS CLOSE,
    TO_NUMBER($7,38,0)                            AS VOLUME,
    METADATA$FILENAME                             AS _SRC_FILE,         -- lineage: staged file name
    CURRENT_TIMESTAMP()                           AS _INGEST_TS         -- lineage: per-row ingest timestamp
  FROM '@RAW.EXT_BRONZE/eod/eod_prices_{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}.csv'
)
FILE_FORMAT = (
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL')
)
PATTERN = '.*\\.(csv|csv\\.gz)$'                   -- only load CSV/CSV.GZ
ON_ERROR = 'CONTINUE'
FORCE = TRUE;
