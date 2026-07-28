
------------------------------------------------------------
-- PURPOSE
-- Validate row counts after the RAW → CORE → FACT pipeline completes.

-- Helps detect any data loss or mismatch between layers after merge.
-- Typically used as a final audit step in the Airflow DAG.
------------------------------------------------------------

-- Set the active compute and database context
USE WAREHOUSE WH_INGEST;    -- small warehouse for ingestion/ETL
USE DATABASE SEC_PRICING;   -- working within securities pricing database


-- Post-merge metrics for the trading date (rows now present in CORE and DM_FACT)
WITH ld AS (
  SELECT TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}') AS d
)
SELECT
  /* rows in CORE for the trade date */
  (SELECT COUNT(*) FROM CORE.EOD_PRICES
    WHERE TRADE_DATE = (SELECT d FROM ld)) AS core_rows_for_dt,
  /* rows in FACT for the same trade date (FACT is in DM_FACT) */
  (SELECT COUNT(*) FROM DM_FACT.FACT_DAILY_PRICE
    WHERE TRADE_DATE = (SELECT d FROM ld)) AS fact_rows_for_dt;
