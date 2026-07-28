------------------------------------------------------------
-- PURPOSE
-- Populate the DM_FACT.FACT_DAILY_PRICE table by merging
-- daily EOD prices (from CORE) with corresponding dimension keys.

-- Goals:
--   1️. Deduplicate CORE records for the target trading date.
--   2️. Join CORE data to DIM_SECURITY and DIM_DATE to fetch keys.
--   3️. Upsert (update or insert) fact rows keyed by
--      (SECURITY_ID, DATE_SK).
------------------------------------------------------------

-- Compute & context
USE WAREHOUSE WH_INGEST;   -- lightweight ETL warehouse
USE DATABASE SEC_PRICING;  -- pricing database
USE SCHEMA DM_FACT;        -- fact schema for analytical models


------------------------------------------------------------
-- SECTION : Merge logic
-- Deduplicate → add foreign keys → merge into FACT.
------------------------------------------------------------
MERGE INTO DM_FACT.FACT_DAILY_PRICE f
USING (SELECT
    ds.SECURITY_ID,
    TO_NUMBER(TO_CHAR(e.TRADE_DATE, 'YYYYMMDD')) AS DATE_SK,
    e.TRADE_DATE,
    e.OPEN,
    e.HIGH,
    e.LOW,
    e.CLOSE,
    e.VOLUME
FROM CORE.EOD_PRICES e
JOIN DM_DIM.DIM_SECURITY ds ON ds.SYMBOL = e.SYMBOL
JOIN DM_DIM.DIM_DATE dd ON dd.DATE_SK = TO_NUMBER(TO_CHAR(e.TRADE_DATE, 'YYYYMMDD'))
WHERE e.TRADE_DATE = TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}')
) src
ON f.SECURITY_ID = src.SECURITY_ID AND f.DATE_SK = src.DATE_SK
WHEN MATCHED THEN UPDATE SET
  TRADE_DATE = src.TRADE_DATE,
  OPEN = src.OPEN,
  HIGH = src.HIGH,
  LOW = src.LOW,
  CLOSE = src.CLOSE,
  VOLUME = src.VOLUME,
  LOAD_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  SECURITY_ID, DATE_SK, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, LOAD_TS
) VALUES (
  src.SECURITY_ID, src.DATE_SK, src.TRADE_DATE, src.OPEN, src.HIGH, src.LOW,
  src.CLOSE, src.VOLUME, CURRENT_TIMESTAMP()
);