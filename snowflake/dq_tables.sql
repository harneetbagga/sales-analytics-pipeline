-- Run once, as ACCOUNTADMIN (or a role that can create tables in SILVER).
USE DATABASE sales_analytics;
USE SCHEMA silver;

CREATE TABLE IF NOT EXISTS silver.dq_rejected_rows (
    REJECTED_AT   TIMESTAMP_NTZ,
    DAG_RUN_ID    VARCHAR,
    SOURCE_TABLE  VARCHAR,
    REJECT_REASON VARCHAR,
    ACTIVITY_ID   VARCHAR,
    LEAD_ID       VARCHAR,
    TYPE          VARCHAR,
    ROW_HASH      VARCHAR,      -- MD5 of the raw record: one quarantine row per distinct bad record
    RAW_RECORD    VARIANT
);

CREATE TABLE IF NOT EXISTS silver.dq_run_log (
    RUN_TS        TIMESTAMP_NTZ,
    DAG_RUN_ID    VARCHAR,
    TABLE_NAME    VARCHAR,
    CHECK_NAME    VARCHAR,      -- one row per expectation + one 'ROWS_REJECTED_ANY_RULE' row
    COLUMN_NAME   VARCHAR,
    TOTAL_ROWS    NUMBER,
    FAILED_ROWS   NUMBER,
    FAILED_PCT    FLOAT,
    THRESHOLD_PCT FLOAT,
    DECISION      VARCHAR       -- PASS / WARN / FAIL
);

-- The Airflow role owns and writes them (same pattern as the other Silver tables).
GRANT OWNERSHIP ON TABLE silver.dq_rejected_rows TO ROLE pipeline_transformer COPY CURRENT GRANTS;
GRANT OWNERSHIP ON TABLE silver.dq_run_log       TO ROLE pipeline_transformer COPY CURRENT GRANTS;