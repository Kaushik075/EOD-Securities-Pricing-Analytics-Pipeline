------------------------------------------------------------
-- SECTION 1: WAREHOUSE SETUP
-- Purpose: Lightweight, cost-friendly compute for ingestion/ELT.
------------------------------------------------------------


-- Auto-suspend after 60s to minimize cost; auto-resume on demand.
-- INITIALLY_SUSPENDED prevents the cluster from starting on create.
CREATE WAREHOUSE IF NOT EXISTS WH_INGEST
  WAREHOUSE_SIZE                = 'XSMALL'          -- keep tiny; scale later if needed
  AUTO_SUSPEND                  = 60                -- seconds of inactivity before suspend
  AUTO_RESUME                   = TRUE              -- wake up on first query
  INITIALLY_SUSPENDED           = TRUE              -- do not start at create time
  STATEMENT_TIMEOUT_IN_SECONDS  = 3600              -- guardrail for runaway queries
  COMMENT                       = 'ETL ingest warehouse';



  ------------------------------------------------------------
-- SECTION 2: DATABASE AND SCHEMA STRUCTURE
-- Purpose: Create logical containers for ELT layers.
------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS SEC_PRICING
  COMMENT = 'Securities pricing data';

-- Use the ingest warehouse + target database for this session
USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;


-- Layered approach:
-- RAW   : landed as-is from source (immutable; add only)
-- CORE  : cleansed/standardized canonical models
-- DM_DIM: conformed dimensions for analytics
-- DM_FACT: fact tables for analytics
CREATE SCHEMA IF NOT EXISTS SEC_PRICING.RAW;
CREATE SCHEMA IF NOT EXISTS SEC_PRICING.CORE;
CREATE SCHEMA IF NOT EXISTS SEC_PRICING.DM_DIM;
CREATE SCHEMA IF NOT EXISTS SEC_PRICING.DM_FACT;



------------------------------------------------------------
-- SECTION 3: TABLE DEFINITIONS
-- Note: Using UPPERCASE, unquoted identifiers (Snowflake default).
------------------------------------------------------------

-- RAW: landed end-of-day prices (append-only)
-- _SRC_FILE  : provenance (S3 key or filename)
-- _INGEST_TS : load audit timestamp
CREATE TABLE IF NOT EXISTS RAW.RAW_EOD_PRICES (
  TRADE_DATE   DATE,
  SYMBOL       STRING,
  OPEN         NUMBER(18,6),
  HIGH         NUMBER(18,6),
  LOW          NUMBER(18,6),
  CLOSE        NUMBER(18,6),
  VOLUME       NUMBER(38,0),
  _SRC_FILE    STRING,
 _INGEST_TS    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);


-- CORE: cleaned/pruned version of RAW for downstream joins
CREATE TABLE IF NOT EXISTS CORE.EOD_PRICES (
  TRADE_DATE   DATE,
  SYMBOL       STRING,
  OPEN         NUMBER(18,6),
  HIGH         NUMBER(18,6),
  LOW          NUMBER(18,6),
  CLOSE        NUMBER(18,6),
  VOLUME       NUMBER(38,0),
  LOAD_TS      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);


-- DM_DIM: conformed dimensions
-- Surrogate key via IDENTITY; SYMBOL kept unique to avoid dup members.
CREATE TABLE IF NOT EXISTS DM_DIM.DIM_SECURITY (
  SECURITY_ID  NUMBER IDENTITY START 1 INCREMENT 1,
  SYMBOL       STRING UNIQUE,
  IS_ACTIVE    BOOLEAN DEFAULT TRUE,
  LOAD_TS      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_DIM_SECURITY PRIMARY KEY (SECURITY_ID)
);


-- date dimension
CREATE TABLE IF NOT EXISTS DM_DIM.DIM_DATE (
  DATE_SK       NUMBER(8,0),          -- yyyymmdd integer surrogate
  CAL_DATE      DATE UNIQUE,
  YEAR_NUM      NUMBER(4,0),
  QUARTER_NUM   NUMBER(1,0),
  MONTH_NUM     NUMBER(2,0),
  MONTH_NAME    VARCHAR(20),
  DAY_NUM       NUMBER(2,0),
  DAY_NAME      VARCHAR(20),
  DAY_OF_WEEK   NUMBER(1,0),
  WEEK_OF_YEAR  NUMBER(2,0),
  IS_WEEKEND    BOOLEAN,
  CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_SK)
);



-- DM_FACT: grain = (SECURITY_ID, DATE_SK)
CREATE TABLE IF NOT EXISTS DM_FACT.FACT_DAILY_PRICE (
  SECURITY_ID  NUMBER,                -- FK → DM_DIM.DIM_SECURITY.SECURITY_ID
  DATE_SK      NUMBER(8,0),           -- FK → DM_DIM.DIM_DATE.DATE_SK
  TRADE_DATE   DATE,                  -- denormalized copy
  OPEN         NUMBER(18,6),
  HIGH         NUMBER(18,6),
  LOW          NUMBER(18,6),
  CLOSE        NUMBER(18,6),
  VOLUME       NUMBER(38,0),
  LOAD_TS      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_FACT_DAILY PRIMARY KEY (SECURITY_ID, DATE_SK)
);


------------------------------------------------------------
-- SECTION 4: Check Important Information
------------------------------------------------------------

select
  current_organization_name()  as org,
  current_account_name()       as account_name,
  current_account()            as account_locator,
  current_region()             as region;

SHOW USERS;



------------------------------------------------------------
-- SECTION 5: HANDY DEBUG/MAINTENANCE SNIPPETS (Commented)
------------------------------------------------------------

-- SELECT * FROM SEC_PRICING.RAW.RAW_EOD_PRICES;
-- SELECT * FROM SEC_PRICING.CORE.EOD_PRICES;
-- SELECT * FROM SEC_PRICING.DM_DIM.DIM_DATE
-- SELECT * FROM SEC_PRICING.DM_DIM.DIM_SECURITY
-- SELECT * FROM SEC_PRICING.DM_FACT.FACT_DAILY_PRICE



-- Select Count(*) From SEC_PRICING.RAW.RAW_EOD_PRICES;
-- Select Count(*) From SEC_PRICING.CORE.EOD_PRICES;
-- SELECT  Count(*) FROM SEC_PRICING.DM_DIM.DIM_DATE;
-- SELECT  Count(*) FROM SEC_PRICING.DM_DIM.DIM_SECURITY;
-- SELECT  Count(*) FROM SEC_PRICING.DM_FACT.FACT_DAILY_PRICE;



-- TRUNCATE TABLE SEC_PRICING.RAW.RAW_EOD_PRICES;
-- TRUNCATE TABLE SEC_PRICING.CORE.EOD_PRICES;
-- TRUNCATE TABLE SEC_PRICING.DM_DIM.DIM_DATE;
-- TRUNCATE TABLE SEC_PRICING.DM_DIM.DIM_SECURITY;
-- TRUNCATE TABLE SEC_PRICING.DM_FACT.FACT_DAILY_PRICE;


-- DROP TABLE SEC_PRICING.RAW.RAW_EOD_PRICES;
-- DROP TABLE SEC_PRICING.CORE.EOD_PRICES;
-- DROP TABLE SEC_PRICING.DM_DIM.DIM_DATE;
-- DROP TABLE SEC_PRICING.DM_DIM.DIM_SECURITY;
-- DROP TABLE SEC_PRICING.DM_FACT.FACT_DAILY_PRICE;