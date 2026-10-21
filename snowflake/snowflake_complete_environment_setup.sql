-- ============================================================
-- Sales Analytics Pipeline — Full Snowflake Environment Setup
-- Run top-to-bottom on any fresh/new Snowflake trial account.
-- Assumes role ACCOUNTADMIN (typical default on a new trial).
-- ============================================================

-- ============================================================
-- STEP 1 — WAREHOUSE
-- ============================================================
CREATE WAREHOUSE IF NOT EXISTS sales_wh
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE sales_wh;

-- ============================================================
-- STEP 2 — DATABASE & SCHEMAS
-- ============================================================
CREATE DATABASE IF NOT EXISTS sales_analytics;
USE DATABASE sales_analytics;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- STEP 3 — FILE FORMAT (used by the external stage's COPY INTO)
-- ============================================================
USE SCHEMA bronze;

CREATE FILE FORMAT IF NOT EXISTS bronze.json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE;

-- ============================================================
-- STEP 4 — EXTERNAL STAGE (points at the S3 staging bucket)
-- ============================================================
-- Option A (recommended): Storage Integration — no long-lived AWS
-- keys stored in Snowflake. Requires a one-time AWS IAM trust setup;
-- see Snowflake docs "Configuring an Amazon S3 bucket using a
-- storage integration". Uncomment and fill in once the integration
-- exists:
--
-- CREATE STORAGE INTEGRATION IF NOT EXISTS s3_sales_analytics_int
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = 'S3'
--     ENABLED = TRUE
--     STORAGE_AWS_ROLE_ARN = '<arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME>'
--     STORAGE_ALLOWED_LOCATIONS = ('s3://sales-analyical-pipeline/');
--
-- -- After creating, run DESC INTEGRATION s3_sales_analytics_int;
-- -- and update the AWS IAM role's trust policy with the returned
-- -- STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID before using it.
--
-- CREATE STAGE IF NOT EXISTS bronze.s3_raw_stage
--     URL = 's3://sales-analyical-pipeline/raw/'
--     STORAGE_INTEGRATION = s3_sales_analytics_int
--     FILE_FORMAT = bronze.json_format;

-- Option B (faster for a throwaway trial rebuild): direct AWS keys.
-- Replace credentials before running; rotate/delete after use.
CREATE STAGE IF NOT EXISTS bronze.s3_raw_stage
    URL = 's3://sales-analyical-pipeline/raw/'
    CREDENTIALS = (AWS_KEY_ID = '<your_access_key_id>' AWS_SECRET_KEY = '<your_secret_access_key>')
    FILE_FORMAT = bronze.json_format;

-- ============================================================
-- STEP 5 — BRONZE TABLES (4)
-- Column structure confirmed from actual COPY INTO target lists
-- in s3_to_snowflake_bronze.py — not the original 2-column doc.
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.leads_raw (
    JSON_OBJECT         VARCHAR,
    SOURCE_INSERT_DATE  VARCHAR,
    PG_INSERT_DATE      VARCHAR,
    LOAD_DATE           DATE
);

CREATE TABLE IF NOT EXISTS bronze.lead_activites_raw (
    JSON_OBJECT         VARCHAR,
    SOURCE_INSERT_DATE  VARCHAR,
    PG_INSERT_DATE      VARCHAR,
    LOAD_DATE           DATE
);

CREATE TABLE IF NOT EXISTS bronze.close_crm_users_raw (
    JSON_OBJECT         VARCHAR,
    SOURCE_INSERT_DATE  VARCHAR,
    PG_INSERT_DATE      VARCHAR,
    LOAD_DATE           DATE
);

CREATE TABLE IF NOT EXISTS bronze.custom_activites_raw (
    JSON_OBJECT         VARCHAR,
    SOURCE_INSERT_DATE  VARCHAR,
    PG_INSERT_DATE      VARCHAR,
    LOAD_DATE           DATE
);

-- ============================================================
-- STEP 6 — REPAIR UDFs
-- Full bodies from 00_udfs.sql — regex-based single-quote-to-JSON
-- repair, NOT ast.literal_eval (see decisions-and-learnings.md for
-- why that distinction matters if this comes up in conversation).
-- ============================================================
USE SCHEMA silver;

CREATE OR REPLACE FUNCTION silver.repair_json_users(input_str VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'repair_json_users'
AS $$
import re

def repair_json_users(input_str):
    if input_str is None:
        return None
    result = input_str
    result = re.sub(r"([a-zA-Z])''([a-zA-Z])", r"\1__APOSTROPHE__\2", result)
    result = re.sub(r"([a-zA-Z])'([a-zA-Z])", r"\1__APOSTROPHE__\2", result)
    result = re.sub(
        r"'([^']*)'(\s*[:},\]])",
        lambda m: '"' + m.group(1).replace('"', '\\"') + '"' + m.group(2),
        result
    )
    result = result.replace("'", '"')
    result = result.replace("__APOSTROPHE__", "'")
    result = result.replace(': None', ': null')
    result = result.replace(':None', ':null')
    result = result.replace(': True', ': true')
    result = result.replace(': False', ': false')
    result = result.replace(':True', ':true')
    result = result.replace(':False', ':false')
    return result
$$;

CREATE OR REPLACE FUNCTION silver.repair_json_activities(input_str VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'repair_json_activities'
AS $$
import re

def neutralize_descriptions(s):
    result = []
    i = 0
    desc_key = "'description': '"
    while i < len(s):
        idx = s.find(desc_key, i)
        if idx == -1:
            result.append(s[i:])
            break
        result.append(s[i:idx])
        result.append("'description': null")
        i = idx + len(desc_key)
        while i < len(s):
            if s[i] == "'":
                j = i + 1
                while j < len(s) and s[j] == ' ':
                    j += 1
                if j < len(s) and s[j] in (',', '}', ']'):
                    i = i + 1
                    break
                else:
                    i += 1
            else:
                i += 1
    return ''.join(result)

def repair_json_activities(input_str):
    if input_str is None:
        return None
    result = input_str
    result = neutralize_descriptions(result)
    result = re.sub(r"([a-zA-Z])''([a-zA-Z])", r"\1__APOSTROPHE__\2", result)
    result = re.sub(r"([a-zA-Z])'([a-zA-Z])", r"\1__APOSTROPHE__\2", result)
    result = re.sub(
        r"'([^']*)'(\s*[:},\]])",
        lambda m: '"' + m.group(1).replace('"', '\\"') + '"' + m.group(2),
        result
    )
    result = result.replace("'", '"')
    result = result.replace("__APOSTROPHE__", "'")
    result = result.replace(': None', ': null')
    result = result.replace(':None', ':null')
    result = result.replace(': True', ': true')
    result = result.replace(': False', ': false')
    result = result.replace(':True', ':true')
    result = result.replace(':False', ':false')
    return result
$$;

-- ============================================================
-- STEP 7 — SILVER TABLES (6 persisted + 2 transient staging)
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.close_crm_users_processed (
    USER_ID       VARCHAR,
    EMAIL         VARCHAR,
    FIRST_NAME    VARCHAR,
    LAST_NAME     VARCHAR,
    MD5_HASH      VARCHAR,
    DATE_CREATED  TIMESTAMP_NTZ,
    DATE_UPDATED  TIMESTAMP_NTZ,
    INSERT_DATE   TIMESTAMP_NTZ,
    UPDATE_DATE   TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS silver.custom_activities (
    CUSTOM_ACTIVITY_ID           VARCHAR,
    CUSTOM_ACTIVITY_NAME         VARCHAR,
    CUSTOM_ACTIVITY_OUTCOME_ID   VARCHAR,
    CUSTOM_ACTIVITY_OUTCOME_NAME VARCHAR,
    MD5_HASH                     VARCHAR,
    INSERT_DATE                  TIMESTAMP_NTZ,
    UPDATE_DATE                  TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS silver.custom_activities_all_leads_details (
    LEAD_ID              VARCHAR,
    ACTIVITY_ID           VARCHAR,
    ACTIVITY_AT           TIMESTAMP_NTZ,
    WRAPPED_ACTIVITIES    VARIANT,
    INSERT_DATE           TIMESTAMP_NTZ,
    UPDATE_DATE           TIMESTAMP_NTZ,
    MD5_HASH              VARCHAR
);

CREATE TABLE IF NOT EXISTS silver.lead_activities_processed_transient (
    JSON_OBJECT  VARIANT,
    INSERT_DATE  TIMESTAMP_NTZ
);

-- ── LEAD_ACTIVITIES_PROCESSED — now SCD Type 2 ────────────────────
-- New columns: EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT.
-- A hash change no longer overwrites the row in place — the old
-- version is closed out (IS_CURRENT=FALSE, EFFECTIVE_END_DATE set)
-- and a new current row is inserted. Any query that only wants the
-- current state must filter WHERE IS_CURRENT = TRUE.
CREATE OR REPLACE TABLE silver.lead_activities_processed (
    ACTIVITY_ID           VARCHAR,
    LEAD_ID               VARCHAR,
    CONTACT_ID            VARCHAR,
    USER_ID               VARCHAR,
    TYPE                  VARCHAR,
    TEXT                  VARCHAR,
    STATUS_CHANGE         VARCHAR,
    DIRECTION             VARCHAR,
    CREATED_BY_NAME       VARCHAR,
    UPDATED_BY_NAME       VARCHAR,
    ATTENDEES             VARCHAR,
    CUSTOM_ACTIVITY_ID    VARCHAR,
    MEETING_STARTS_AT     TIMESTAMP_NTZ,
    MEETING_END_AT        TIMESTAMP_NTZ,
    MEETING_STATUS        VARCHAR,
    ACTIVITY_AT           TIMESTAMP_NTZ,
    DATE_CREATED          TIMESTAMP_NTZ,
    DATE_UPDATED          TIMESTAMP_NTZ,
    CREATED_BY_USER       VARCHAR,
    UPDATED_BY_USER       VARCHAR,
    INSERT_DATE           TIMESTAMP_NTZ,
    UPDATE_DATE           TIMESTAMP_NTZ,
    MD5_HASH              VARCHAR,
    EFFECTIVE_START_DATE  TIMESTAMP_NTZ,
    EFFECTIVE_END_DATE    TIMESTAMP_NTZ,
    IS_CURRENT            BOOLEAN
);

-- ── LEAD_ACTIVITIES_EMAIL — MD5_HASH added for CDC ────────────────
CREATE OR REPLACE TABLE silver.lead_activities_email (
    LEAD_ID             VARCHAR,
    ACTIVITY_AT         TIMESTAMP_NTZ,
    ATTACHMENTS_JSON    VARCHAR,
    BODY_TEXT           VARCHAR,
    DATE_CREATED        TIMESTAMP_NTZ,
    DATE_UPDATED        TIMESTAMP_NTZ,
    DIRECTION           VARCHAR,
    ENVELOPE_JSON       VARCHAR,
    SENDER_JSON         VARCHAR,
    ACTIVITY_ID         VARCHAR,
    OPENS_JSON          VARCHAR,
    SENDER              VARCHAR,
    STATUS              VARCHAR,
    SUBJECT              VARCHAR,
    JSON_PAYLOAD        VARIANT,
    USER_ID              VARCHAR,
    USER_NAME            VARCHAR,
    TEMPLATE_ID          VARCHAR,
    TEMPLATE_NAME        VARCHAR,
    DATE_SENT            TIMESTAMP_NTZ,
    HAS_REPLY            BOOLEAN,
    INSERT_DATE          TIMESTAMP_NTZ,
    UPDATE_DATE          TIMESTAMP_NTZ,
    MD5_HASH             VARCHAR
);

CREATE TABLE IF NOT EXISTS silver.leads_activities_summary_transient (
    ACTIVITY_ID                   VARCHAR,
    LEAD_ID                       VARCHAR,
    CONTACT_ID                    VARCHAR,
    USER_ID                       VARCHAR,
    TYPE                          VARCHAR,
    TEXT                          VARCHAR,
    STATUS_CHANGE                 VARCHAR,
    CREATED_BY_NAME               VARCHAR,
    UPDATED_BY_NAME               VARCHAR,
    ATTENDEES                     VARCHAR,
    CUSTOM_ACTIVITY_ID            VARCHAR,
    MEETING_STARTS_AT             TIMESTAMP_NTZ,
    MEETING_END_AT                TIMESTAMP_NTZ,
    MEETING_STATUS                VARCHAR,
    ACTIVITY_AT                   TIMESTAMP_NTZ,
    DATE_CREATED                  TIMESTAMP_NTZ,
    DATE_UPDATED                  TIMESTAMP_NTZ,
    CREATED_BY_USER               VARCHAR,
    UPDATED_BY_USER               VARCHAR,
    INSERT_DATE                   TIMESTAMP_NTZ,
    UPDATE_DATE                   TIMESTAMP_NTZ,
    MD5_HASH                      VARCHAR,
    WRAPPED_ACTIVITIES            VARIANT,
    CUSTOM_ACTIVITY                VARCHAR,
    CUSTOM_ACTIVITY_OUTCOME_NAME   VARCHAR,
    CUSTOM_ACTIVITY_OUTCOME        VARCHAR,
    DEA_INTERNAL_NAME              VARCHAR,
    DEA_INTERNAL_EMAIL             VARCHAR
);

CREATE TABLE IF NOT EXISTS silver.leads_activities_summary (
    ACTIVITY_ID                   VARCHAR,
    LEAD_ID                       VARCHAR,
    CONTACT_ID                    VARCHAR,
    USER_ID                       VARCHAR,
    TYPE                          VARCHAR,
    TEXT                          VARCHAR,
    STATUS_CHANGE                 VARCHAR,
    CREATED_BY_NAME               VARCHAR,
    UPDATED_BY_NAME               VARCHAR,
    ATTENDEES                     VARCHAR,
    CUSTOM_ACTIVITY_ID            VARCHAR,
    MEETING_STARTS_AT             TIMESTAMP_NTZ,
    MEETING_END_AT                TIMESTAMP_NTZ,
    MEETING_STATUS                VARCHAR,
    ACTIVITY_AT                   TIMESTAMP_NTZ,
    DATE_CREATED                  TIMESTAMP_NTZ,
    DATE_UPDATED                  TIMESTAMP_NTZ,
    CREATED_BY_USER               VARCHAR,
    UPDATED_BY_USER               VARCHAR,
    INSERT_DATE                   TIMESTAMP_NTZ,
    UPDATE_DATE                   TIMESTAMP_NTZ,
    WRAPPED_ACTIVITIES             VARIANT,
    CUSTOM_ACTIVITY                 VARCHAR,
    CUSTOM_ACTIVITY_OUTCOME_NAME    VARCHAR,
    CUSTOM_ACTIVITY_OUTCOME         VARCHAR,
    DEA_INTERNAL_NAME               VARCHAR,
    DEA_INTERNAL_EMAIL              VARCHAR,
    MD5_HASH                        VARCHAR
);

-- ============================================================
-- STEP 8 — GOLD SCHEMA
-- No DDL needed here — all 5 Gold objects are views, created by
-- silver_to_gold.py's CREATE OR REPLACE VIEW statements once the
-- pipeline actually runs. Nothing to pre-create.
-- ============================================================

-- ============================================================
-- STEP 9 — SANITY CHECKS (run after DAGs have loaded at least once)
-- ============================================================
-- SELECT COUNT(*) FROM bronze.lead_activites_raw;
-- SELECT TRY_PARSE_JSON(silver.repair_json_users(JSON_OBJECT)) IS NULL AS still_invalid, COUNT(*)
--   FROM bronze.close_crm_users_raw GROUP BY 1;
-- SELECT TRY_PARSE_JSON(silver.repair_json_activities(JSON_OBJECT)) IS NULL AS still_invalid, COUNT(*)
--   FROM bronze.custom_activites_raw GROUP BY 1;

-- ============================================================
-- AFTER RUNNING THIS SCRIPT:
--   1. Update the snowflake_default Airflow connection with the
--      new account identifier + credentials.
--   2. Confirm dags/ folder has the target_date-parameterized
--      versions of postgres_to_s3.py and s3_to_snowflake_bronze.py
--      (see decisions-and-learnings.md — Backfill capability fix).
--   3. Trigger postgres_to_s3 and let the chain run.
-- ============================================================