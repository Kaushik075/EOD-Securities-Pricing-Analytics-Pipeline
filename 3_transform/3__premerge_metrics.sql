------------------------------------------------------------
-- PURPOSE:
-- Estimate insert vs update counts when loading RAW → CORE for a specific trading date (passed from Airflow).
-- This is a pre-merge audit step to validate data volume, ensure consistency, and prevent duplicate loads.
--
-- Note: this is the baseline version used by the main daily pipeline. See
-- /reject_table_scenario for the variant that also tracks negative-volume rejects.
------------------------------------------------------------


-- Set the active compute and database context
USE WAREHOUSE WH_INGEST;    -- small warehouse for ingestion/ETL
USE DATABASE SEC_PRICING;   -- working within securities pricing database


------------------------------------------------------------
-- Compute record statistics for the target trading date
------------------------------------------------------------
WITH td AS (
  -- Pull trading date dynamically from Airflow XCom context
  SELECT TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}') AS d
),
raw_cnt AS (
  -- Count all records in RAW for that trading date
  SELECT COUNT(*) AS c
  FROM RAW.RAW_EOD_PRICES
  WHERE TRADE_DATE = (SELECT d FROM td)
),
today_keys AS (
  -- Extract distinct SYMBOL + TRADE_DATE keys for the trading day
  -- Normalize symbol casing and trim spaces to avoid false mismatches
  SELECT DISTINCT UPPER(TRIM(SYMBOL)) AS SYMBOL, TRADE_DATE
  FROM RAW.RAW_EOD_PRICES
  WHERE TRADE_DATE = (SELECT d FROM td)
),
key_cnt AS (
  -- Total unique keys (distinct securities traded that day)
  SELECT COUNT(*) AS c FROM today_keys
),
core_existing AS (
  -- Identify how many of today's keys already exist in CORE
  -- This represents potential *updates* during the MERGE.
  SELECT COUNT(*) AS c
  FROM today_keys k
  JOIN CORE.EOD_PRICES t
    ON UPPER(TRIM(t.SYMBOL)) = k.SYMBOL AND t.TRADE_DATE = k.TRADE_DATE
)
SELECT
  r.c                         AS raw_cnt,            -- total rows in RAW
  e.c                         AS core_existing_cnt,  -- keys already in CORE
  (k.c - e.c)                 AS core_inserts_est,   -- new rows expected
  e.c                         AS core_updates_est    -- updates expected
FROM raw_cnt r
CROSS JOIN key_cnt k
CROSS JOIN core_existing e;
