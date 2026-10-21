-- ============================================================
-- FILE: 03_custom_activities_all_leads_details.sql
-- PURPOSE: Silver transformation for CustomActivity lead records
-- SOURCE:  sales_analytics.bronze.lead_activites_raw
-- TARGET:  sales_analytics.silver.custom_activities_all_leads_details
-- PATTERN: Inline parse from Bronze, filter CustomActivity type only
--          ROW_NUMBER() deduplicates incremental loads
-- ROWS:    28,237 unique custom activity records (as of Jun 2026)
-- NOTES:
--   - Filters WHERE _type = 'CustomActivity' only
--   - Stores full activity VARIANT in WRAPPED_ACTIVITIES
--   - WRAPPED_ACTIVITIES contains custom.cf_* field values
--     which are resolved via CUSTOM_ACTIVITIES lookup table
--   - 11,246 distinct leads have at least one CustomActivity

-- SCD TYPE: 1 (intermediate staging table feeding the fan-out
--            pivot in leads_activities_summary — not a reporting
--            surface itself, no history need)
-- DEDUP KEY (Batch 1 change): ACTIVITY_ID + LEAD_ID (composite) —
--            was ACTIVITY_ID alone. Validated safe: ACTIVITY_ID
--            confirmed globally unique across leads (0 collisions
--            / 31,585 activities / 10,072 leads), so this matches
--            spec and adds a safety margin, not fixing a live bug.
-- "LATEST WINS" TIEBREAK (Batch 1 change): ACTIVITY_AT DESC —
--            was INSERT_DATE/LOAD_DATE DESC (pipeline load time).
--            Now orders by the CRM's own event time per the
--            requirements doc's explicit instruction.
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA silver;

MERGE INTO sales_analytics.silver.custom_activities_all_leads_details AS target
USING (
    SELECT
        LEAD_ID, ACTIVITY_ID, ACTIVITY_AT,
        WRAPPED_ACTIVITIES, INSERT_DATE, MD5_HASH
    FROM (
        SELECT
            act.value:lead_id::VARCHAR              AS LEAD_ID,
            act.value:id::VARCHAR                   AS ACTIVITY_ID,
            act.value:activity_at::TIMESTAMP_NTZ    AS ACTIVITY_AT,
            act.value                               AS WRAPPED_ACTIVITIES,
            src.LOAD_DATE::TIMESTAMP_NTZ            AS INSERT_DATE,
            MD5(
                CONCAT(
                    COALESCE(act.value:id::VARCHAR,           ''),
                    COALESCE(act.value:lead_id::VARCHAR,      ''),
                    COALESCE(act.value:activity_at::VARCHAR,  ''),
                    COALESCE(act.value:date_updated::VARCHAR, '')
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
        WHERE act.value:_type::VARCHAR = 'CustomActivity'
    )
    WHERE rn = 1
) AS source
ON  target.ACTIVITY_ID = source.ACTIVITY_ID
AND target.LEAD_ID     = source.LEAD_ID

WHEN MATCHED AND target.MD5_HASH <> source.MD5_HASH THEN
    UPDATE SET
        target.ACTIVITY_AT        = source.ACTIVITY_AT,
        target.WRAPPED_ACTIVITIES = source.WRAPPED_ACTIVITIES,
        target.MD5_HASH           = source.MD5_HASH,
        target.UPDATE_DATE        = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        LEAD_ID, ACTIVITY_ID, ACTIVITY_AT,
        WRAPPED_ACTIVITIES, MD5_HASH,
        INSERT_DATE, UPDATE_DATE
    )
    VALUES (
        source.LEAD_ID, source.ACTIVITY_ID, source.ACTIVITY_AT,
        source.WRAPPED_ACTIVITIES, source.MD5_HASH,
        source.INSERT_DATE, CURRENT_TIMESTAMP()
    );
