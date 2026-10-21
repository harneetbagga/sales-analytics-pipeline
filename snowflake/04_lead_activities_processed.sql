-- ============================================================
-- FILE: 04_lead_activities_processed.sql
-- PURPOSE: Silver transformation for all lead activities
-- SOURCE:  sales_analytics.bronze.lead_activites_raw
-- TARGET:  sales_analytics.silver.lead_activities_processed
-- PATTERN: Transient staging table → MERGE INTO permanent table
--          ROW_NUMBER() deduplicates incremental loads
-- LAYER: Silver
-- SOURCE: bronze.lead_activites_raw (via transient staging table)
-- SCD TYPE (Batch 1 change): 2 — was Type 1. This is the one Silver
--            table where history genuinely matters for the business:
--            if a Strategy Call outcome gets corrected after the
--            fact, the business wants to know what it showed on the
--            day a dashboard snapshot was taken, not just the latest
--            value. Requires the updated DDL (see
--            silver_ddl_updates.sql) — 3 new columns:
--            EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT.
--            Downstream consumers MUST filter WHERE IS_CURRENT = TRUE
--            for current-state queries.
-- DEDUP KEY (Batch 1 change): ACTIVITY_ID + LEAD_ID (composite) —
--            was ACTIVITY_ID alone. Validated safe (see file 03).
-- "LATEST WINS" TIEBREAK (Batch 1 change): ACTIVITY_AT DESC — was
--            INSERT_DATE DESC.
--
-- IMPLEMENTATION NOTE: Snowflake MERGE cannot update-and-insert for
-- the same matched key in one statement, so SCD2 here is two
-- sequential statements sharing the same source_rows CTE logic:
--   Step 1 (CLOSE OUT): close any current row whose hash changed
--   Step 2 (INSERT NEW): insert a fresh current row for anything
--     new, or anything Step 1 just closed out
-- Run both statements in this file, in order, every time this
-- table is refreshed. In the DAG these are two separate tasks
-- (close_out_lead_activities_processed → insert_new_lead_activities_processed)
-- so a failure in either is independently visible/retryable.

-- ROWS:    900,908 unique activities (as of Jun 2026)
-- NOTES:
--   Activity types and row counts:
--     Call         436,277  STATUS_CHANGE from disposition, DIRECTION populated
--     SMS          250,134  STATUS_CHANGE from status, DIRECTION populated
--     Email        153,230  STATUS_CHANGE from status, DIRECTION populated
--     CustomActivity 28,237 STATUS_CHANGE from status
--     Meeting       12,833  STATUS_CHANGE from status
--     Note          10,768  STATUS_CHANGE NULL (expected)
--     Created        9,314  STATUS_CHANGE NULL (expected)
--     LeadMerge        115  STATUS_CHANGE from merge_status
--
--   STATUS_CHANGE logic:
--     COALESCE(disposition, merge_status, status)
--     NOTE: the business stakeholder confirmed STATUS_CHANGE in LEADS_ACTIVITIES_SUMMARY
--     should be a derived old-->new pipeline stage transition field.
--     Pending clarification from SME on final implementation.
--
--   TEXT logic:
--     COALESCE(note, text) — note for Call/Note/Meeting, text for SMS
--
--   DIRECTION: populated for Call, SMS, Email only
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA silver;

-- ── Transient load (unchanged) ────────────────────────────────
INSERT OVERWRITE INTO sales_analytics.silver.lead_activities_processed_transient
SELECT
    PARSE_JSON(JSON_OBJECT)  AS JSON_OBJECT,
    LOAD_DATE::TIMESTAMP_NTZ AS INSERT_DATE
FROM sales_analytics.bronze.lead_activites_raw;


-- ── Shared source CTE, used by both SCD2 steps below ───────────
-- (Snowflake doesn't support a named CTE shared across separate
-- statements, so this block is repeated verbatim in both steps —
-- keep them in sync if you edit the extraction logic.)

-- ── STEP 1: Close out current rows whose hash changed ──────────
UPDATE sales_analytics.silver.lead_activities_processed AS target
SET
    IS_CURRENT         = FALSE,
    EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(),
    UPDATE_DATE         = CURRENT_TIMESTAMP()
FROM (
    WITH source_rows AS (
        SELECT
            ACTIVITY_ID, LEAD_ID, CONTACT_ID, USER_ID, TYPE, TEXT,
            STATUS_CHANGE, DIRECTION, CREATED_BY_NAME, UPDATED_BY_NAME,
            ATTENDEES, CUSTOM_ACTIVITY_ID, MEETING_STARTS_AT, MEETING_END_AT,
            MEETING_STATUS, ACTIVITY_AT, DATE_CREATED, DATE_UPDATED,
            CREATED_BY_USER, UPDATED_BY_USER, INSERT_DATE, MD5_HASH
        FROM (
            SELECT
                act.value:id::VARCHAR                      AS ACTIVITY_ID,
                act.value:lead_id::VARCHAR                 AS LEAD_ID,
                act.value:contact_id::VARCHAR              AS CONTACT_ID,
                act.value:user_id::VARCHAR                 AS USER_ID,
                act.value:_type::VARCHAR                   AS TYPE,
                COALESCE(act.value:note::VARCHAR, act.value:text::VARCHAR) AS TEXT,
                COALESCE(
                    act.value:disposition::VARCHAR,
                    act.value:merge_status::VARCHAR,
                    act.value:status::VARCHAR
                )                                          AS STATUS_CHANGE,
                act.value:direction::VARCHAR               AS DIRECTION,
                act.value:created_by_name::VARCHAR         AS CREATED_BY_NAME,
                act.value:updated_by_name::VARCHAR         AS UPDATED_BY_NAME,
                act.value:attendees::VARCHAR               AS ATTENDEES,
                act.value:custom_activity_type_id::VARCHAR AS CUSTOM_ACTIVITY_ID,
                act.value:starts_at::TIMESTAMP_NTZ         AS MEETING_STARTS_AT,
                act.value:ends_at::TIMESTAMP_NTZ           AS MEETING_END_AT,
                act.value:status::VARCHAR                  AS MEETING_STATUS,
                act.value:activity_at::TIMESTAMP_NTZ       AS ACTIVITY_AT,
                act.value:date_created::TIMESTAMP_NTZ      AS DATE_CREATED,
                act.value:date_updated::TIMESTAMP_NTZ      AS DATE_UPDATED,
                act.value:created_by::VARCHAR              AS CREATED_BY_USER,
                act.value:updated_by::VARCHAR              AS UPDATED_BY_USER,
                src.INSERT_DATE                            AS INSERT_DATE,
                MD5(
                    CONCAT(
                        COALESCE(act.value:id::VARCHAR,           ''),
                        COALESCE(act.value:lead_id::VARCHAR,      ''),
                        COALESCE(act.value:activity_at::VARCHAR,  ''),
                        COALESCE(act.value:date_updated::VARCHAR, ''),
                        COALESCE(act.value:disposition::VARCHAR,  ''),
                        COALESCE(act.value:merge_status::VARCHAR, ''),
                        COALESCE(act.value:status::VARCHAR,       ''),
                        COALESCE(act.value:direction::VARCHAR,    '')
                    )
                )                                          AS MD5_HASH,
                ROW_NUMBER() OVER (
                    PARTITION BY act.value:id::VARCHAR, act.value:lead_id::VARCHAR
                    -- Latest VERSION wins: date_updated, then newest batch, then event time
                    ORDER BY act.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,
                             src.INSERT_DATE DESC,
                             act.value:activity_at::TIMESTAMP_NTZ DESC
                ) AS rn
            FROM sales_analytics.silver.lead_activities_processed_transient src,
            LATERAL FLATTEN(input => src.JSON_OBJECT:data) act
        )
        WHERE rn = 1
    )
    SELECT * FROM source_rows
) AS source
WHERE target.ACTIVITY_ID = source.ACTIVITY_ID
  AND target.LEAD_ID     = source.LEAD_ID
  AND target.IS_CURRENT  = TRUE
  AND target.MD5_HASH   <> source.MD5_HASH;


-- ── STEP 2: Insert a fresh current row for new/just-closed keys ─
INSERT INTO sales_analytics.silver.lead_activities_processed (
    ACTIVITY_ID, LEAD_ID, CONTACT_ID, USER_ID, TYPE, TEXT,
    STATUS_CHANGE, DIRECTION, CREATED_BY_NAME, UPDATED_BY_NAME,
    ATTENDEES, CUSTOM_ACTIVITY_ID, MEETING_STARTS_AT, MEETING_END_AT,
    MEETING_STATUS, ACTIVITY_AT, DATE_CREATED, DATE_UPDATED,
    CREATED_BY_USER, UPDATED_BY_USER,
    INSERT_DATE, UPDATE_DATE, MD5_HASH,
    EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT
)
SELECT
    source.ACTIVITY_ID, source.LEAD_ID, source.CONTACT_ID, source.USER_ID,
    source.TYPE, source.TEXT, source.STATUS_CHANGE, source.DIRECTION,
    source.CREATED_BY_NAME, source.UPDATED_BY_NAME,
    source.ATTENDEES, source.CUSTOM_ACTIVITY_ID,
    source.MEETING_STARTS_AT, source.MEETING_END_AT, source.MEETING_STATUS,
    source.ACTIVITY_AT, source.DATE_CREATED, source.DATE_UPDATED,
    source.CREATED_BY_USER, source.UPDATED_BY_USER,
    source.INSERT_DATE, CURRENT_TIMESTAMP(), source.MD5_HASH,
    CURRENT_TIMESTAMP(), NULL, TRUE
FROM (
    WITH source_rows AS (
        SELECT
            ACTIVITY_ID, LEAD_ID, CONTACT_ID, USER_ID, TYPE, TEXT,
            STATUS_CHANGE, DIRECTION, CREATED_BY_NAME, UPDATED_BY_NAME,
            ATTENDEES, CUSTOM_ACTIVITY_ID, MEETING_STARTS_AT, MEETING_END_AT,
            MEETING_STATUS, ACTIVITY_AT, DATE_CREATED, DATE_UPDATED,
            CREATED_BY_USER, UPDATED_BY_USER, INSERT_DATE, MD5_HASH
        FROM (
            SELECT
                act.value:id::VARCHAR                      AS ACTIVITY_ID,
                act.value:lead_id::VARCHAR                 AS LEAD_ID,
                act.value:contact_id::VARCHAR              AS CONTACT_ID,
                act.value:user_id::VARCHAR                 AS USER_ID,
                act.value:_type::VARCHAR                   AS TYPE,
                COALESCE(act.value:note::VARCHAR, act.value:text::VARCHAR) AS TEXT,
                COALESCE(
                    act.value:disposition::VARCHAR,
                    act.value:merge_status::VARCHAR,
                    act.value:status::VARCHAR
                )                                          AS STATUS_CHANGE,
                act.value:direction::VARCHAR               AS DIRECTION,
                act.value:created_by_name::VARCHAR         AS CREATED_BY_NAME,
                act.value:updated_by_name::VARCHAR         AS UPDATED_BY_NAME,
                act.value:attendees::VARCHAR               AS ATTENDEES,
                act.value:custom_activity_type_id::VARCHAR AS CUSTOM_ACTIVITY_ID,
                act.value:starts_at::TIMESTAMP_NTZ         AS MEETING_STARTS_AT,
                act.value:ends_at::TIMESTAMP_NTZ           AS MEETING_END_AT,
                act.value:status::VARCHAR                  AS MEETING_STATUS,
                act.value:activity_at::TIMESTAMP_NTZ       AS ACTIVITY_AT,
                act.value:date_created::TIMESTAMP_NTZ      AS DATE_CREATED,
                act.value:date_updated::TIMESTAMP_NTZ      AS DATE_UPDATED,
                act.value:created_by::VARCHAR              AS CREATED_BY_USER,
                act.value:updated_by::VARCHAR              AS UPDATED_BY_USER,
                src.INSERT_DATE                            AS INSERT_DATE,
                MD5(
                    CONCAT(
                        COALESCE(act.value:id::VARCHAR,           ''),
                        COALESCE(act.value:lead_id::VARCHAR,      ''),
                        COALESCE(act.value:activity_at::VARCHAR,  ''),
                        COALESCE(act.value:date_updated::VARCHAR, ''),
                        COALESCE(act.value:disposition::VARCHAR,  ''),
                        COALESCE(act.value:merge_status::VARCHAR, ''),
                        COALESCE(act.value:status::VARCHAR,       ''),
                        COALESCE(act.value:direction::VARCHAR,    '')
                    )
                )                                          AS MD5_HASH,
                ROW_NUMBER() OVER (
                    PARTITION BY act.value:id::VARCHAR, act.value:lead_id::VARCHAR
                    -- Latest VERSION wins: date_updated, then newest batch, then event time
                    ORDER BY act.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,
                             src.INSERT_DATE DESC,
                             act.value:activity_at::TIMESTAMP_NTZ DESC
                ) AS rn
            FROM sales_analytics.silver.lead_activities_processed_transient src,
            LATERAL FLATTEN(input => src.JSON_OBJECT:data) act
        )
        WHERE rn = 1
    )
    SELECT * FROM source_rows
) AS source
LEFT JOIN sales_analytics.silver.lead_activities_processed AS target
    ON  target.ACTIVITY_ID = source.ACTIVITY_ID
    AND target.LEAD_ID     = source.LEAD_ID
    AND target.IS_CURRENT  = TRUE
WHERE target.ACTIVITY_ID IS NULL;
