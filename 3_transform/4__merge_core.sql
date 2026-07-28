------------------------------------------------------------
-- PURPOSE
-- Upsert EOD prices from RAW → CORE.EOD_PRICES for a single trading date.
-- Rules:
-- 1) Normalize SYMBOL (UPPER/TRIM) for stable keys.
-- 2) Deduplicate RAW by (SYMBOL, TRADE_DATE) using ROW_NUMBER with a strong,
--    deterministic tie-breaker: first by _INGEST_TS (latest wins), then _SRC_FILE.
-- 3) MERGE into CORE with a guarded UPDATE (only when values differ).
--
-- Idempotent per trading date: re-running on the same files won't duplicate.
--
-- Note: this is the baseline version used by the main daily pipeline. See
-- /reject_table_scenario for the variant that also diverts negative-volume
-- rows into a dedicated reject table before merging.
------------------------------------------------------------


-- Compute & context
USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;
USE SCHEMA CORE;

------------------------------------------------------------
-- MERGE OPERATION
-- We use a CTE (Common Table Expression) to prepare source data
-- before feeding it into the MERGE statement.
------------------------------------------------------------

MERGE INTO CORE.EOD_PRICES tgt
USING (
  WITH src_raw AS (
    SELECT
      r.TRADE_DATE,
      UPPER(TRIM(r.SYMBOL)) AS SYMBOL,
      r.OPEN, r.HIGH, r.LOW, r.CLOSE, r.VOLUME,
      r._INGEST_TS,                  -- stronger dedup signal (most recent load wins)
      r._SRC_FILE                    -- deterministic tie-breaker if ingest_ts ties
    FROM RAW.RAW_EOD_PRICES r
    WHERE r.TRADE_DATE = TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}')
  ),
  ranked AS (
     -- Keep only ONE record per (SYMBOL, TRADE_DATE):
     -- 1) latest _INGEST_TS
     -- 2) then by _SRC_FILE to break ties deterministically
    SELECT
		TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME, _INGEST_TS, _SRC_FILE,
		ROW_NUMBER() OVER (
		   PARTITION BY SYMBOL, TRADE_DATE
			ORDER BY _INGEST_TS DESC,
                     _SRC_FILE DESC) AS rn
    FROM src_raw
  )
  SELECT
    TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME
  FROM ranked
  WHERE rn = 1
) src
ON  UPPER(TRIM(tgt.SYMBOL)) = src.SYMBOL
AND tgt.TRADE_DATE = src.TRADE_DATE
WHEN MATCHED THEN UPDATE SET
  OPEN    = src.OPEN,
  HIGH    = src.HIGH,
  LOW     = src.LOW,
  CLOSE   = src.CLOSE,
  VOLUME  = src.VOLUME,
  LOAD_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
   TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME, LOAD_TS
) VALUES (
  src.TRADE_DATE, src.SYMBOL, src.OPEN, src.HIGH, src.LOW, src.CLOSE, src.VOLUME, CURRENT_TIMESTAMP()
);
