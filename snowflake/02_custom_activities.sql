-- ============================================================
-- FILE: 02_custom_activities.sql
-- PURPOSE: Silver transformation for custom activity type definitions
-- SOURCE:  sales_analytics.bronze.custom_activites_raw
-- TARGET:  sales_analytics.silver.custom_activities
-- PATTERN: Inline parse from Bronze, double LATERAL FLATTEN
--          ROW_NUMBER() deduplicates daily snapshots
-- ROWS:    205 unique activity_type × field combinations (as of Jun 2026)
-- NOTES:
--   - repair_json_activities UDF neutralizes description fields
--     which contain embedded single quotes
--   - Double LATERAL FLATTEN: first on data[] (activity types),
--     then on act.value:fields[] (field definitions)
--   - One row per activity_type × field combination
--   - This is the lookup table for resolving cf_* field IDs
--     to human-readable names in lead activities
-- ============================================================

MERGE INTO sales_analytics.silver.custom_activities AS target
USING (
    SELECT
        CUSTOM_ACTIVITY_ID, CUSTOM_ACTIVITY_NAME,
        CUSTOM_ACTIVITY_OUTCOME_ID, CUSTOM_ACTIVITY_OUTCOME_NAME,
        INSERT_DATE, MD5_HASH
    FROM (
        SELECT
            act.value:id::VARCHAR        AS CUSTOM_ACTIVITY_ID,
            act.value:name::VARCHAR      AS CUSTOM_ACTIVITY_NAME,
            f.value:id::VARCHAR          AS CUSTOM_ACTIVITY_OUTCOME_ID,
            f.value:name::VARCHAR        AS CUSTOM_ACTIVITY_OUTCOME_NAME,
            src.LOAD_DATE                AS INSERT_DATE,
            MD5(
                CONCAT(
                    COALESCE(act.value:id::VARCHAR,   ''),
                    COALESCE(act.value:name::VARCHAR, ''),
                    COALESCE(f.value:id::VARCHAR,     ''),
                    COALESCE(f.value:name::VARCHAR,   '')
                )
            ) AS MD5_HASH,
            ROW_NUMBER() OVER (
                PARTITION BY act.value:id::VARCHAR, f.value:id::VARCHAR
                ORDER BY src.LOAD_DATE DESC
            ) AS rn
        FROM sales_analytics.bronze.custom_activites_raw src,
        LATERAL FLATTEN(
            input => TRY_PARSE_JSON(
                sales_analytics.silver.repair_json_activities(src.JSON_OBJECT)
            ):data
        ) act,
        LATERAL FLATTEN(input => act.value:fields) f
        WHERE TRY_PARSE_JSON(
            sales_analytics.silver.repair_json_activities(src.JSON_OBJECT)
        ) IS NOT NULL
    )
    WHERE rn = 1
) AS source
ON  target.CUSTOM_ACTIVITY_ID         = source.CUSTOM_ACTIVITY_ID
AND target.CUSTOM_ACTIVITY_OUTCOME_ID = source.CUSTOM_ACTIVITY_OUTCOME_ID

WHEN MATCHED AND target.MD5_HASH <> source.MD5_HASH THEN
    UPDATE SET
        target.CUSTOM_ACTIVITY_NAME         = source.CUSTOM_ACTIVITY_NAME,
        target.CUSTOM_ACTIVITY_OUTCOME_NAME = source.CUSTOM_ACTIVITY_OUTCOME_NAME,
        target.MD5_HASH                     = source.MD5_HASH,
        target.UPDATE_DATE                  = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        CUSTOM_ACTIVITY_ID, CUSTOM_ACTIVITY_NAME,
        CUSTOM_ACTIVITY_OUTCOME_ID, CUSTOM_ACTIVITY_OUTCOME_NAME,
        MD5_HASH, INSERT_DATE, UPDATE_DATE
    )
    VALUES (
        source.CUSTOM_ACTIVITY_ID, source.CUSTOM_ACTIVITY_NAME,
        source.CUSTOM_ACTIVITY_OUTCOME_ID, source.CUSTOM_ACTIVITY_OUTCOME_NAME,
        source.MD5_HASH, source.INSERT_DATE, CURRENT_TIMESTAMP()
    );


-- ── Validation ────────────────────────────────────────────
-- SELECT COUNT(*) AS total_rows,
--        COUNT(DISTINCT CUSTOM_ACTIVITY_ID || CUSTOM_ACTIVITY_OUTCOME_ID) AS distinct_combinations
-- FROM sales_analytics.silver.custom_activities;
-- Expected: 205 | 226
