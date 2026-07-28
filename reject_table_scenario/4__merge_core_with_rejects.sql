------------------------------------------------------------
-- PURPOSE
-- Upsert EOD prices from RAW → CORE.EOD_PRICES for a single trading date, with
-- explicit reject handling for negative-volume rows.
--
-- This is the reject-aware variant of 3_transform/4__merge_core.sql — pair with
-- 3__premerge_metrics_with_rejects.sql and reject_table_creation.sql.
--
-- Rules:
-- 1) Normalize SYMBOL (UPPER/TRIM) for stable keys.
-- 2) Deduplicate RAW by (SYMBOL, TRADE_DATE) using ROW_NUMBER with a strong,
--    deterministic tie-breaker: first by _INGEST_TS (latest wins), then _SRC_FILE.
-- 3) Divert negative-volume rows into CORE.EOD_PRICES_REJECT instead of dropping
--    or loading them silently.
-- 4) MERGE the remaining valid rows into CORE with a guarded UPDATE.
--
-- Idempotent per trading date: re-running on the same files won't duplicate.
------------------------------------------------------------


-- Compute & context
USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;
USE SCHEMA CORE;


-- 1) Capture rejects (negative volume) idempotently
MERGE INTO CORE.EOD_PRICES_REJECT rej
USING (
  SELECT
	r.TRADE_DATE,
    UPPER(TRIM(r.SYMBOL)) AS SYMBOL,
    r.OPEN, r.HIGH, r.LOW, r.CLOSE, r.VOLUME,
    'NEGATIVE_VOLUME'     AS REJECT_REASON,
    r._SRC_FILE,
    r._INGEST_TS
  FROM SEC_PRICING.RAW.RAW_EOD_PRICES r
  WHERE r.TRADE_DATE = TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}')
    AND r.VOLUME < 0
) src
ON rej.SYMBOL     = src.SYMBOL
AND rej.TRADE_DATE = src.TRADE_DATE
WHEN NOT MATCHED THEN INSERT (
  TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME,
  REJECT_REASON, _SRC_FILE, _INGEST_TS
) VALUES (
   src.TRADE_DATE, src.SYMBOL, src.OPEN, src.HIGH, src.LOW, src.CLOSE, src.VOLUME,
  src.REJECT_REASON, src._SRC_FILE, src._INGEST_TS
);


-- 2) Upsert only valid (non-negative) rows into CORE
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
    AND r.VOLUME >= 0                          -- exclude rejects here
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
