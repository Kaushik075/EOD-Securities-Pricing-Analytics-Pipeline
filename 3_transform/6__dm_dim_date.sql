------------------------------------------------------------
-- PURPOSE
-- Ensure the DM_DIM.DIM_DATE table includes all trading dates
-- present in the current CORE.EOD_PRICES load.
--
-- This guarantees that every fact record (by TRADE_DATE)
-- has a matching surrogate key (DATE_SK) in the DIM_DATE table.
---------------------------------


-- Compute context
USE WAREHOUSE WH_INGEST;    -- light compute for ETL
USE DATABASE SEC_PRICING;   -- main pricing database
USE SCHEMA DM_DIM;          -- schema containing dimension tables


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
  WHERE e.TRADE_DATE = TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}')
) s
ON d.DATE_SK = s.DATE_SK
WHEN NOT MATCHED THEN
  INSERT (DATE_SK, CAL_DATE, YEAR_NUM, QUARTER_NUM, MONTH_NUM, MONTH_NAME, DAY_NUM, DAY_NAME, DAY_OF_WEEK, WEEK_OF_YEAR, IS_WEEKEND)
  VALUES (s.DATE_SK, s.CAL_DATE, s.YEAR_NUM, s.QUARTER_NUM, s.MONTH_NUM, s.MONTH_NAME, s.DAY_NUM, s.DAY_NAME ,s.DAY_OF_WEEK, s.WEEK_OF_YEAR, s.IS_WEEKEND);
