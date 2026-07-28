------------------------------------------------------------
-- PURPOSE
-- Keep the DIM_SECURITY table (in DM_DIM schema) up to date
-- by inserting any new SYMBOLs found in CORE.EOD_PRICES.
--
-- This ensures the dimension stays synchronized with facts,
-- allowing consistent joins (FACT ↔ DIM) on SYMBOL/SECURITY_ID.
-----------------------------------------------------------


-- Compute & context
USE WAREHOUSE WH_INGEST;      -- lightweight ETL/ingestion warehouse
USE DATABASE SEC_PRICING;     -- main pricing database
USE SCHEMA DM_DIM;            -- schema containing dimension tables

SET next_id = (SELECT COALESCE(MAX(SECURITY_ID), 0) + 1 FROM DM_DIM.DIM_SECURITY);

-- 11882
-- AKRE(1), KOL(2), GG(3), KL(4)
-- 11882, 11883, 11884, 11885

MERGE INTO DM_DIM.DIM_SECURITY d
USING (
  -- SOURCE: Get distinct symbols from today's CORE data with generated IDs
  SELECT
    UPPER(TRIM(SYMBOL)) AS SYMBOL,
    ROW_NUMBER() OVER (ORDER BY UPPER(TRIM(SYMBOL))) + $next_id - 1 AS SECURITY_ID
  FROM (
    SELECT DISTINCT UPPER(TRIM(SYMBOL)) AS SYMBOL
    FROM CORE.EOD_PRICES
    WHERE TRADE_DATE = TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}')
  )
) s
ON d.SYMBOL = s.SYMBOL
WHEN NOT MATCHED THEN
  INSERT (SECURITY_ID, SYMBOL)
  VALUES (s.SECURITY_ID, s.SYMBOL)
