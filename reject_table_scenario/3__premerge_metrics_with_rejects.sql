------------------------------------------------------------
-- PURPOSE:
-- Pre-merge check: Estimate insert, update, and reject counts for a specific trading date.
--
-- This is the reject-aware variant of 3_transform/3__premerge_metrics.sql — it also
-- counts rows that will be diverted to CORE.EOD_PRICES_REJECT for having negative
-- volume. Pair with 4__merge_core_with_rejects.sql and reject_table_creation.sql.
------------------------------------------------------------
USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;
------------------------------------------------------------

WITH td AS (
  -- Pull trading date from Airflow XCom
  SELECT TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}') AS d
),
raw_cnt AS (
  -- Total records in RAW
  SELECT COUNT(*) AS c
  FROM RAW.RAW_EOD_PRICES
  WHERE TRADE_DATE = (SELECT d FROM td)
),
reject_cnt AS (
  -- Count records with negative volume (will be rejected)
  SELECT COUNT(*) AS c
  FROM RAW.RAW_EOD_PRICES
  WHERE TRADE_DATE = (SELECT d FROM td)
    AND VOLUME < 0
),
valid_keys AS (
  -- Get distinct keys from RAW excluding negative volume records
  SELECT DISTINCT UPPER(TRIM(SYMBOL)) AS SYMBOL, TRADE_DATE
  FROM RAW.RAW_EOD_PRICES
  WHERE TRADE_DATE = (SELECT d FROM td)
    AND VOLUME >= 0
),
core_existing AS (
  -- Count how many valid RAW keys already exist in CORE (these will be UPDATES)
  SELECT COUNT(*) AS c
  FROM valid_keys r
  JOIN CORE.EOD_PRICES c
    ON UPPER(TRIM(c.SYMBOL)) = r.SYMBOL
    AND c.TRADE_DATE = r.TRADE_DATE
),
total_valid_keys AS (
  -- Total unique valid keys in RAW
  SELECT COUNT(*) AS c FROM valid_keys
)
SELECT
  r.c AS raw_cnt,
  rej.c AS reject_cnt,
  (t.c - e.c) AS est_inserts,  -- Valid keys not in CORE
  e.c AS est_updates           -- Valid keys already in CORE
FROM raw_cnt r
CROSS JOIN reject_cnt rej
CROSS JOIN total_valid_keys t
CROSS JOIN core_existing e;
