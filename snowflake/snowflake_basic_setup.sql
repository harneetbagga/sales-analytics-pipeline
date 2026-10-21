-- ALTER USER <YOUR_SNOWFLAKE_USER> SET MINS_TO_UNLOCK = 0;   -- only if the account is locked

SELECT CURRENT_ACCOUNT();

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS sales_wh
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Database and schemas
CREATE DATABASE IF NOT EXISTS sales_analytics;
CREATE SCHEMA IF NOT EXISTS sales_analytics.bronze;
CREATE SCHEMA IF NOT EXISTS sales_analytics.silver;
CREATE SCHEMA IF NOT EXISTS sales_analytics.gold;

-- Storage integration (same IAM role as before)
CREATE OR REPLACE STORAGE INTEGRATION SALES_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/sales-analytical-snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://sales-analyical-pipeline/');

-- After creating, run this and update the IAM trust policy in AWS with new values
DESC INTEGRATION SALES_S3_INTEGRATION;

-- Bronze ingestion stage
CREATE OR REPLACE STAGE sales_analytics.bronze.s3_raw_stage
    URL = 's3://sales-analyical-pipeline/raw/'
    STORAGE_INTEGRATION = SALES_S3_INTEGRATION
    FILE_FORMAT = (
        TYPE = 'JSON'
        STRIP_OUTER_ARRAY = FALSE
    );

-- Gold export stage
CREATE OR REPLACE STAGE sales_analytics.gold.s3_export_stage
    URL = 's3://sales-analyical-pipeline/'
    STORAGE_INTEGRATION = SALES_S3_INTEGRATION
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('')
        EMPTY_FIELD_AS_NULL = TRUE
    );

    -- Check if Silver tables exist
SELECT TABLE_NAME, TABLE_TYPE
FROM sales_analytics.information_schema.tables
WHERE TABLE_SCHEMA IN ( 'SILVER', 'GOLD' )
ORDER BY TABLE_NAME;

-- Check if storage integration exists and get new IAM values
DESC INTEGRATION SALES_S3_INTEGRATION;

LIST @sales_analytics.bronze.s3_raw_stage;

SELECT CURRENT_USER();

-- Check Bronze table counts in Snowflake
SELECT 'close_crm_users_raw' AS tbl, COUNT(*) AS cnt FROM sales_analytics.bronze.close_crm_users_raw
UNION ALL SELECT 'custom_activites_raw', COUNT(*) FROM sales_analytics.bronze.custom_activites_raw
UNION ALL SELECT 'leads_raw', COUNT(*) FROM sales_analytics.bronze.leads_raw
UNION ALL SELECT 'lead_activites_raw', COUNT(*) FROM sales_analytics.bronze.lead_activites_raw;

-- Force reload by copying with FORCE = TRUE
COPY INTO sales_analytics.bronze.close_crm_users_raw
FROM @sales_analytics.bronze.s3_raw_stage/close_crm_users_raw/
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
FORCE = TRUE;

-- Check what files exist in each stage path
LIST @sales_analytics.bronze.s3_raw_stage/close_crm_users_raw/;
LIST @sales_analytics.bronze.s3_raw_stage/lead_activites_raw/;


-- Truncate all Bronze tables
TRUNCATE TABLE sales_analytics.bronze.close_crm_users_raw;
TRUNCATE TABLE sales_analytics.bronze.custom_activites_raw;
TRUNCATE TABLE sales_analytics.bronze.leads_raw;
TRUNCATE TABLE sales_analytics.bronze.lead_activites_raw;

-- Reload close_crm_users_raw — extract inner JSON_OBJECT value
COPY INTO sales_analytics.bronze.close_crm_users_raw (JSON_OBJECT, LOAD_DATE)
FROM (
    SELECT
        $1:JSON_OBJECT::VARCHAR AS JSON_OBJECT,
        TO_DATE(
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'load_date=', 2), '/', 1)
        ) AS LOAD_DATE
    FROM @sales_analytics.bronze.s3_raw_stage/close_crm_users_raw/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
FORCE = TRUE;

-- Reload custom_activites_raw
COPY INTO sales_analytics.bronze.custom_activites_raw (JSON_OBJECT, LOAD_DATE)
FROM (
    SELECT
        $1:JSON_OBJECT::VARCHAR AS JSON_OBJECT,
        TO_DATE(
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'load_date=', 2), '/', 1)
        ) AS LOAD_DATE
    FROM @sales_analytics.bronze.s3_raw_stage/custom_activites_raw/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
FORCE = TRUE;

-- Reload leads_raw
COPY INTO sales_analytics.bronze.leads_raw (JSON_OBJECT, LOAD_DATE)
FROM (
    SELECT
        $1:JSON_OBJECT::VARCHAR AS JSON_OBJECT,
        TO_DATE(
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'load_date=', 2), '/', 1)
        ) AS LOAD_DATE
    FROM @sales_analytics.bronze.s3_raw_stage/leads_raw/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
FORCE = TRUE;

-- Reload lead_activites_raw
COPY INTO sales_analytics.bronze.lead_activites_raw (JSON_OBJECT, LOAD_DATE)
FROM (
    SELECT
        $1:JSON_OBJECT::VARCHAR AS JSON_OBJECT,
        TO_DATE(
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'load_date=', 2), '/', 1)
        ) AS LOAD_DATE
    FROM @sales_analytics.bronze.s3_raw_stage/lead_activites_raw/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
FORCE = TRUE;

-- Check the raw value is now correct (should start with { 'data': )
SELECT LEFT(JSON_OBJECT, 100) AS raw_value
FROM sales_analytics.bronze.close_crm_users_raw
LIMIT 1;


LIST @sales_analytics.gold.s3_export_stage/dea_setter_report/;
LIST @sales_analytics.gold.s3_export_stage/dea_closer_report/;