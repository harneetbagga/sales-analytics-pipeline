-- CLOSE_CRM_USERS_PROCESSED
CREATE OR REPLACE TABLE sales_analytics.silver.close_crm_users_processed (
    USER_ID      TEXT,
    EMAIL        TEXT,
    FIRST_NAME   TEXT,
    LAST_NAME    TEXT,
    MD5_HASH     TEXT,
    DATE_CREATED TIMESTAMP_NTZ,
    DATE_UPDATED TIMESTAMP_NTZ,
    INSERT_DATE  TIMESTAMP_NTZ,
    UPDATE_DATE  TIMESTAMP_NTZ
);

-- CUSTOM_ACTIVITIES_TRANSIENT
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.silver.custom_activities_transient (
    JSON_OBJECT  VARIANT,
    INSERT_DATE  TIMESTAMP_NTZ
);

-- CUSTOM_ACTIVITIES
CREATE OR REPLACE TABLE sales_analytics.silver.custom_activities (
    CUSTOM_ACTIVITY_ID           TEXT,
    CUSTOM_ACTIVITY_NAME         TEXT,
    CUSTOM_ACTIVITY_OUTCOME_ID   TEXT,
    CUSTOM_ACTIVITY_OUTCOME_NAME TEXT,
    MD5_HASH                     TEXT,
    INSERT_DATE                  TIMESTAMP_NTZ,
    UPDATE_DATE                  TIMESTAMP_NTZ
);

-- LEAD_ACTIVITIES_PROCESSED_TRANSIENT
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.silver.lead_activities_processed_transient (
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

-- CUSTOM_ACTIVITIES_ALL_LEADS_DETAILS
CREATE OR REPLACE TABLE sales_analytics.silver.custom_activities_all_leads_details (
    LEAD_ID             TEXT,
    ACTIVITY_ID         TEXT,
    ACTIVITY_AT         TIMESTAMP_NTZ,
    WRAPPED_ACTIVITIES  VARIANT,
    INSERT_DATE         TIMESTAMP_NTZ,
    UPDATE_DATE         TIMESTAMP_NTZ,
    MD5_HASH            TEXT
);

-- LEADS_ACTIVITIES_SUMMARY_TRANSIENT
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.silver.leads_activities_summary_transient (
    ACTIVITY_ID                  TEXT,
    LEAD_ID                      TEXT,
    CONTACT_ID                   TEXT,
    USER_ID                      TEXT,
    TYPE                         TEXT,
    TEXT                         TEXT,
    STATUS_CHANGE                TEXT,
    CREATED_BY_NAME              TEXT,
    UPDATED_BY_NAME              TEXT,
    ATTENDEES                    TEXT,
    CUSTOM_ACTIVITY_ID           TEXT,
    MEETING_STARTS_AT            TIMESTAMP_NTZ,
    MEETING_END_AT               TIMESTAMP_NTZ,
    MEETING_STATUS               TEXT,
    ACTIVITY_AT                  TIMESTAMP_NTZ,
    DATE_CREATED                 TIMESTAMP_NTZ,
    DATE_UPDATED                 TIMESTAMP_NTZ,
    CREATED_BY_USER              TEXT,
    UPDATED_BY_USER              TEXT,
    INSERT_DATE                  TIMESTAMP_NTZ,
    UPDATE_DATE                  TIMESTAMP_NTZ,
    MD5_HASH                     TEXT,
    WRAPPED_ACTIVITIES           VARIANT,
    CUSTOM_ACTIVITY              TEXT,
    CUSTOM_ACTIVITY_OUTCOME_NAME TEXT,
    CUSTOM_ACTIVITY_OUTCOME      TEXT,
    DEA_INTERNAL_NAME            TEXT,
    DEA_INTERNAL_EMAIL           TEXT
);

-- LEADS_ACTIVITIES_SUMMARY
CREATE OR REPLACE TABLE sales_analytics.silver.leads_activities_summary (
    ACTIVITY_ID                  TEXT,
    LEAD_ID                      TEXT,
    CONTACT_ID                   TEXT,
    USER_ID                      TEXT,
    TYPE                         TEXT,
    TEXT                         TEXT,
    STATUS_CHANGE                TEXT,
    CREATED_BY_NAME              TEXT,
    UPDATED_BY_NAME              TEXT,
    ATTENDEES                    TEXT,
    CUSTOM_ACTIVITY_ID           TEXT,
    MEETING_STARTS_AT            TIMESTAMP_NTZ,
    MEETING_END_AT               TIMESTAMP_NTZ,
    MEETING_STATUS               TEXT,
    ACTIVITY_AT                  TIMESTAMP_NTZ,
    DATE_CREATED                 TIMESTAMP_NTZ,
    DATE_UPDATED                 TIMESTAMP_NTZ,
    CREATED_BY_USER              TEXT,
    UPDATED_BY_USER              TEXT,
    INSERT_DATE                  TIMESTAMP_NTZ,
    UPDATE_DATE                  TIMESTAMP_NTZ,
    WRAPPED_ACTIVITIES           VARIANT,
    CUSTOM_ACTIVITY              TEXT,
    CUSTOM_ACTIVITY_OUTCOME_NAME TEXT,
    CUSTOM_ACTIVITY_OUTCOME      TEXT,
    DEA_INTERNAL_NAME            TEXT,
    DEA_INTERNAL_EMAIL           TEXT,
    MD5_HASH                     TEXT
);


-- Load close_crm_users_raw
COPY INTO sales_analytics.bronze.close_crm_users_raw (
    JSON_OBJECT, LOAD_DATE
)
FROM (
    SELECT
        $1::VARCHAR                                                    AS JSON_OBJECT,
        TO_DATE(
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'load_date=', 2), '/', 1)
        )                                                              AS LOAD_DATE
    FROM @sales_analytics.bronze.s3_raw_stage/close_crm_users_raw/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
FORCE = TRUE;

-- Verify close_crm_users_raw count
SELECT COUNT(*) FROM sales_analytics.bronze.close_crm_users_raw;

-- Load custom_activites_raw
COPY INTO sales_analytics.bronze.custom_activites_raw (
    JSON_OBJECT, LOAD_DATE
)
FROM (
    SELECT
        $1::VARCHAR                                                    AS JSON_OBJECT,
        TO_DATE(
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'load_date=', 2), '/', 1)
        )                                                              AS LOAD_DATE
    FROM @sales_analytics.bronze.s3_raw_stage/custom_activites_raw/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
FORCE = TRUE;

-- Load leads_raw
COPY INTO sales_analytics.bronze.leads_raw (
    JSON_OBJECT, LOAD_DATE
)
FROM (
    SELECT
        $1::VARCHAR                                                    AS JSON_OBJECT,
        TO_DATE(
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'load_date=', 2), '/', 1)
        )                                                              AS LOAD_DATE
    FROM @sales_analytics.bronze.s3_raw_stage/leads_raw/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
FORCE = TRUE;

-- Load lead_activites_raw (largest table — will take a few minutes)
COPY INTO sales_analytics.bronze.lead_activites_raw (
    JSON_OBJECT, LOAD_DATE
)
FROM (
    SELECT
        $1::VARCHAR                                                    AS JSON_OBJECT,
        TO_DATE(
            SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'load_date=', 2), '/', 1)
        )                                                              AS LOAD_DATE
    FROM @sales_analytics.bronze.s3_raw_stage/lead_activites_raw/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
FORCE = TRUE;

SELECT 'close_crm_users_raw' AS tbl, COUNT(*) AS cnt FROM sales_analytics.bronze.close_crm_users_raw
UNION ALL SELECT 'custom_activites_raw', COUNT(*) FROM sales_analytics.bronze.custom_activites_raw
UNION ALL SELECT 'leads_raw', COUNT(*) FROM sales_analytics.bronze.leads_raw
UNION ALL SELECT 'lead_activites_raw', COUNT(*) FROM sales_analytics.bronze.lead_activites_raw;

SELECT 'close_crm_users_processed' AS tbl, COUNT(*) AS cnt FROM sales_analytics.silver.close_crm_users_processed
UNION ALL SELECT 'custom_activities', COUNT(*) FROM sales_analytics.silver.custom_activities
UNION ALL SELECT 'custom_activities_all_leads_details', COUNT(*) FROM sales_analytics.silver.custom_activities_all_leads_details
UNION ALL SELECT 'lead_activities_processed', COUNT(*) FROM sales_analytics.silver.lead_activities_processed
UNION ALL SELECT 'lead_activities_email', COUNT(*) FROM sales_analytics.silver.lead_activities_email
UNION ALL SELECT 'leads_activities_summary', COUNT(*) FROM sales_analytics.silver.leads_activities_summary;

