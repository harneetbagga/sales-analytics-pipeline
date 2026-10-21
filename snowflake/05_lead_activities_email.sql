-- ============================================================
-- FILE: 05_lead_activities_email.sql
-- PURPOSE: Silver transformation for Email activities with
--          full email-specific fields
-- SOURCE:  sales_analytics.bronze.lead_activites_raw
-- TARGET:  sales_analytics.silver.lead_activities_email
-- PATTERN: Inline parse from Bronze, filter Email type only
--          INSERT only (no UPDATE) — emails don't change once sent
-- ROWS:    153,230 unique email activities (as of Jun 2026)
-- NOTES:
--   - Filters WHERE _type = 'Email' only
--   - SUBJECT sourced from envelope:subject (not masked top-level field)
--   - SENDER sourced from envelope:from[0]:email (actual email address)
--   - SENDER_JSON is full envelope:from array as TEXT
--   - ENVELOPE_JSON stores full envelope object as TEXT
--   - BODY_TEXT is NULL in source data (stripped for privacy)
--   - 39,210 distinct leads have at least one email activity
--   - 153,022 rows have SENDER populated
--     (208 NULL = inbound emails from prospects)

-- SCD TYPE (Batch 1 change): 1 (real upsert) — was insert-only with
--            NO update path at all. Previously an email row was
--            never updated after first load (e.g. HAS_REPLY flipping
--            true after send was silently lost forever). Requires
--            the updated DDL (see silver_ddl_updates.sql) — adds
--            MD5_HASH column, needed for the new WHEN MATCHED branch.
-- DEDUP KEY (Batch 1 change): ACTIVITY_ID + LEAD_ID (composite) —
--            was ACTIVITY_ID alone.
-- "LATEST WINS" TIEBREAK (Batch 1 change): ACTIVITY_AT DESC — was
--            LOAD_DATE DESC.
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA silver;

MERGE INTO sales_analytics.silver.lead_activities_email AS target
USING (
    SELECT
        LEAD_ID, ACTIVITY_ID, ACTIVITY_AT, ATTACHMENTS_JSON, BODY_TEXT,
        DATE_CREATED, DATE_UPDATED, DIRECTION, ENVELOPE_JSON, SENDER_JSON,
        OPENS_JSON, SENDER, STATUS, SUBJECT, JSON_PAYLOAD, USER_ID,
        USER_NAME, TEMPLATE_ID, TEMPLATE_NAME, DATE_SENT, HAS_REPLY,
        INSERT_DATE, MD5_HASH
    FROM (
        SELECT
            act.value:lead_id::VARCHAR                AS LEAD_ID,
            act.value:id::VARCHAR                      AS ACTIVITY_ID,
            act.value:activity_at::TIMESTAMP_NTZ       AS ACTIVITY_AT,
            TO_VARCHAR(act.value:attachments)           AS ATTACHMENTS_JSON,
            act.value:body_text::VARCHAR                AS BODY_TEXT,
            act.value:date_created::TIMESTAMP_NTZ       AS DATE_CREATED,
            act.value:date_updated::TIMESTAMP_NTZ       AS DATE_UPDATED,
            act.value:direction::VARCHAR                AS DIRECTION,
            TO_VARCHAR(act.value:envelope)               AS ENVELOPE_JSON,
            TO_VARCHAR(act.value:envelope:from)          AS SENDER_JSON,
            TO_VARCHAR(act.value:opens)                  AS OPENS_JSON,
            act.value:envelope:from[0]:email::VARCHAR    AS SENDER,
            act.value:status::VARCHAR                    AS STATUS,
            act.value:envelope:subject::VARCHAR          AS SUBJECT,
            act.value                                    AS JSON_PAYLOAD,
            act.value:user_id::VARCHAR                   AS USER_ID,
            act.value:user_name::VARCHAR                 AS USER_NAME,
            act.value:template_id::VARCHAR               AS TEMPLATE_ID,
            act.value:template_name::VARCHAR             AS TEMPLATE_NAME,
            act.value:date_sent::TIMESTAMP_NTZ           AS DATE_SENT,
            act.value:has_reply::BOOLEAN                 AS HAS_REPLY,
            src.LOAD_DATE::TIMESTAMP_NTZ                 AS INSERT_DATE,
            MD5(
                CONCAT(
                    COALESCE(act.value:id::VARCHAR,            ''),
                    COALESCE(act.value:lead_id::VARCHAR,       ''),
                    COALESCE(act.value:activity_at::VARCHAR,   ''),
                    COALESCE(act.value:date_updated::VARCHAR,  ''),
                    COALESCE(act.value:status::VARCHAR,        ''),
                    COALESCE(act.value:has_reply::VARCHAR,     '')
                )
            ) AS MD5_HASH,
            ROW_NUMBER() OVER (
                PARTITION BY act.value:id::VARCHAR, act.value:lead_id::VARCHAR
                -- Latest VERSION wins: date_updated, then newest batch, then event time
                ORDER BY act.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,
                         src.LOAD_DATE DESC,
                         act.value:activity_at::TIMESTAMP_NTZ DESC
            ) AS rn
        FROM sales_analytics.bronze.lead_activites_raw src,
        LATERAL FLATTEN(input => PARSE_JSON(src.JSON_OBJECT):data) act
        WHERE act.value:_type::VARCHAR = 'Email'
    )
    WHERE rn = 1
) AS source
ON  target.ACTIVITY_ID = source.ACTIVITY_ID
AND target.LEAD_ID     = source.LEAD_ID

WHEN MATCHED AND target.MD5_HASH <> source.MD5_HASH THEN
    UPDATE SET
        target.ACTIVITY_AT        = source.ACTIVITY_AT,
        target.ATTACHMENTS_JSON   = source.ATTACHMENTS_JSON,
        target.BODY_TEXT          = source.BODY_TEXT,
        target.DATE_CREATED       = source.DATE_CREATED,
        target.DATE_UPDATED       = source.DATE_UPDATED,
        target.DIRECTION          = source.DIRECTION,
        target.ENVELOPE_JSON      = source.ENVELOPE_JSON,
        target.SENDER_JSON        = source.SENDER_JSON,
        target.OPENS_JSON         = source.OPENS_JSON,
        target.SENDER             = source.SENDER,
        target.STATUS             = source.STATUS,
        target.SUBJECT            = source.SUBJECT,
        target.JSON_PAYLOAD       = source.JSON_PAYLOAD,
        target.USER_ID            = source.USER_ID,
        target.USER_NAME          = source.USER_NAME,
        target.TEMPLATE_ID        = source.TEMPLATE_ID,
        target.TEMPLATE_NAME      = source.TEMPLATE_NAME,
        target.DATE_SENT          = source.DATE_SENT,
        target.HAS_REPLY          = source.HAS_REPLY,
        target.MD5_HASH           = source.MD5_HASH,
        target.UPDATE_DATE        = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        LEAD_ID, ACTIVITY_ID, ACTIVITY_AT, ATTACHMENTS_JSON, BODY_TEXT,
        DATE_CREATED, DATE_UPDATED, DIRECTION, ENVELOPE_JSON,
        SENDER_JSON, OPENS_JSON, SENDER, STATUS,
        SUBJECT, JSON_PAYLOAD, USER_ID, USER_NAME, TEMPLATE_ID,
        TEMPLATE_NAME, DATE_SENT, HAS_REPLY, INSERT_DATE, UPDATE_DATE, MD5_HASH
    )
    VALUES (
        source.LEAD_ID, source.ACTIVITY_ID, source.ACTIVITY_AT,
        source.ATTACHMENTS_JSON, source.BODY_TEXT, source.DATE_CREATED,
        source.DATE_UPDATED, source.DIRECTION, source.ENVELOPE_JSON,
        source.SENDER_JSON, source.OPENS_JSON, source.SENDER,
        source.STATUS, source.SUBJECT, source.JSON_PAYLOAD,
        source.USER_ID, source.USER_NAME, source.TEMPLATE_ID,
        source.TEMPLATE_NAME, source.DATE_SENT, source.HAS_REPLY,
        source.INSERT_DATE, CURRENT_TIMESTAMP(), source.MD5_HASH
    );
