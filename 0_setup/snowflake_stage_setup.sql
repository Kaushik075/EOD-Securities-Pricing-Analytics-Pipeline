/*===============================================================
  Context & session setup
  - Choose a small, cheap warehouse for ingestion/transforms
  - Scope all operations to the target database
================================================================*/
USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;



------------------------------------------------------------
-- SECTION 1: FILE FORMAT CONFIGURATION
-- Purpose: Define a reusable CSV file format to interpret staged files.
------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT SEC_PRICING.RAW.CSV_EOD
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL');



------------------------------------------------------------
-- SECTION 2: STORAGE INTEGRATION (S3)
-- Step 1: In AWS, create an IAM role named 'kaushik-eodsecurities-s3-role'
--         and configure a trust relationship with Snowflake's external ID.
-- Step 2: Use that IAM role ARN in the storage integration below.
-- This integration securely manages credentials for S3 access — Snowflake
-- assumes the role via trust policy rather than holding long-lived AWS keys.
--
-- Replace <AWS_ACCOUNT_ID> with your own account ID once the role exists.
------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION INT_KAUSHIK_EODSECURITIES_S3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/kaushik-eodsecurities-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://kaushik-eodsecurities-data/market/bronze/');

-- Verify integration details (Snowflake provides an external ID and S3 policy template).
DESC INTEGRATION INT_KAUSHIK_EODSECURITIES_S3;


------------------------------------------------------------
-- SECTION 3: EXTERNAL STAGE CREATION
-- Create a named external stage pointing to the S3 bucket.
-- The stage uses the above integration to access files securely.
------------------------------------------------------------

CREATE OR REPLACE STAGE SEC_PRICING.RAW.EXT_BRONZE
  URL = 's3://kaushik-eodsecurities-data/market/bronze/'
  STORAGE_INTEGRATION = INT_KAUSHIK_EODSECURITIES_S3;

-- Verify the stage configuration and permissions.
DESC STAGE SEC_PRICING.RAW.EXT_BRONZE;

-- List all files currently available in the S3 stage.
LIST @SEC_PRICING.RAW.EXT_BRONZE;
