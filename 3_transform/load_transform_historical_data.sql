
/*===============================================================
  Context & session setup
  - Choose a small, cheap warehouse for ingestion/transforms
  - Scope all operations to the target database
================================================================*/
USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;


/*================================================================
  High-level model flow
  RAW  →  CORE  →  DIM (SECURITY, DATE)  →  FACT
  - RAW: landing, file metadata (_INGEST_TS, _SRC_FILE)
  - CORE: de-duplicated, type/shape aligned records
  - DIM : conformed dimensions for keys & reporting attributes
  - FACT: grain = (security_id, date_sk)
================================================================*/


/*===============================================================
  1) Merge RAW → CORE.EOD_PRICES
     Purpose:
       - Keep exactly one row per (SYMBOL, TRADE_DATE)
       - Ties broken by most recent _INGEST_TS, then by _SRC_FILE
===============================================================*/

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


-- Quick sanity checks
Select Count(*) From SEC_PRICING.CORE.EOD_PRICES;
SELECT * FROM SEC_PRICING.CORE.EOD_PRICES;



/*===============================================================
  2) Build/maintain DM_DIM.DIM_SECURITY
     Purpose:
       - One row per SYMBOL
       - Only inserts new symbols observed in CORE
===============================================================*/
-- DROP TABLE DM_FACT.FACT_DAILY_PRICE;
-- DROP TABLE DM_DIM.DIM_SECURITY;

MERGE INTO DM_DIM.DIM_SECURITY d
USING (
    SELECT
        SYMBOL,
        ROW_NUMBER() OVER (ORDER BY SYMBOL) as EXPECTED_ID
    FROM (
        SELECT DISTINCT SYMBOL
        FROM CORE.EOD_PRICES
    )
) s
ON d.SYMBOL = s.SYMBOL
WHEN NOT MATCHED THEN
    INSERT (SECURITY_ID, SYMBOL)
    VALUES (s.EXPECTED_ID, s.SYMBOL);

-- Dimension sanity checks
Select Count(*) From SEC_PRICING.DM_DIM.DIM_SECURITY;
SELECT * FROM SEC_PRICING.DM_DIM.DIM_SECURITY;



/*===============================================================
  3) Build/maintain DM_DIM.DIM_DATE
     Purpose:
       - Populate date rows that appear in CORE
       - Derive common calendar attributes for reporting
===============================================================*/

MERGE INTO DM_DIM.DIM_DATE d
USING (
  SELECT DISTINCT
    TO_NUMBER(TO_CHAR(e.TRADE_DATE, 'YYYYMMDD')) AS DATE_SK,
    e.TRADE_DATE                                 AS CAL_DATE,
    EXTRACT(YEAR    FROM e.TRADE_DATE)           AS YEAR_NUM,
    EXTRACT(QUARTER FROM e.TRADE_DATE)           AS QUARTER_NUM,
    EXTRACT(MONTH   FROM e.TRADE_DATE)           AS MONTH_NUM,
    MONTHNAME(e.TRADE_DATE)                     AS MONTH_NAME,
    EXTRACT(DAY     FROM e.TRADE_DATE)           AS DAY_NUM,
    DAYNAME(e.TRADE_DATE)                       AS DAY_NAME,
    EXTRACT(DAYOFWEEK FROM e.TRADE_DATE)         AS DAY_OF_WEEK,   -- 0=Sun in some DBs; Snowflake: 0=Sunday. Adjust if you prefer 1=Mon.
    EXTRACT(WEEK    FROM e.TRADE_DATE)           AS WEEK_OF_YEAR,
    IFF(EXTRACT(DAYOFWEEK FROM e.TRADE_DATE) IN (0,6), TRUE, FALSE) AS IS_WEEKEND
  FROM CORE.EOD_PRICES e
  ORDER BY DATE_SK
) s
ON d.DATE_SK = s.DATE_SK
WHEN NOT MATCHED THEN
  INSERT (DATE_SK, CAL_DATE, YEAR_NUM, QUARTER_NUM, MONTH_NUM, MONTH_NAME, DAY_NUM, DAY_NAME, DAY_OF_WEEK, WEEK_OF_YEAR, IS_WEEKEND)
  VALUES (s.DATE_SK, s.CAL_DATE, s.YEAR_NUM, s.QUARTER_NUM, s.MONTH_NUM, s.MONTH_NAME, s.DAY_NUM, s.DAY_NAME ,s.DAY_OF_WEEK, s.WEEK_OF_YEAR, s.IS_WEEKEND);


-- Dimension sanity checks
Select Count(*) From SEC_PRICING.DM_DIM.DIM_DATE;
SELECT * FROM SEC_PRICING.DM_DIM.DIM_DATE;



/*===============================================================
  4) Build/maintain DM_FACT.FACT_DAILY_PRICE
     Grain:
       - One row per (SECURITY_ID, DATE_SK)
     Inputs:
       - CORE.EOD_PRICES (de-duplicated per SYMBOL, TRADE_DATE by latest LOAD_TS)
       - DIM_SECURITY for SECURITY_ID
       - DIM_DATE for DATE_SK
===============================================================*/
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


-- Fact sanity checks
Select Count(*) From SEC_PRICING.DM_FACT.FACT_DAILY_PRICE;
SELECT * FROM SEC_PRICING.DM_FACT.FACT_DAILY_PRICE;