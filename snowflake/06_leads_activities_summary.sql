-- ============================================================
-- FILE: 06_leads_activities_summary.sql
-- LAYER: Silver
-- SOURCE: silver.lead_activities_processed (current rows only),
--         silver.close_crm_users_processed,
--         silver.custom_activities_all_leads_details,
--         silver.custom_activities
-- SCD TYPE: 1 (the reporting fact table Gold views read from
--            directly. Since LAP is now Type 2, LAS only ingests
--            WHERE lap.IS_CURRENT = TRUE, so LAS stays a clean
--            current-state snapshot without duplicating LAP's own
--            history-tracking.)
-- BATCH FIX (2026-09, found via merge_leads_activities_summary
--            "Duplicate row detected" error on rebuilt data):
--            dedup key changed from ACTIVITY_ID + CUSTOM_ACTIVITY_OUTCOME_NAME
--            to ACTIVITY_ID + LEAD_ID + CUSTOM_ACTIVITY_OUTCOME_NAME.
--            The earlier assumption that "LAP has already resolved every
--            ACTIVITY_ID to exactly one LEAD_ID upstream, so LAS doesn't
--            need LEAD_ID in its key" was validated against the OLD
--            pre-rebuild dataset (0 collisions / 31,585 activities) and
--            does NOT hold on the rebuilt dataset: confirmed 1,313
--            activity_ids currently mapped to 2+ distinct leads in
--            LEAD_ACTIVITIES_PROCESSED. Without LEAD_ID in LAS's key,
--            two different leads sharing a custom field name on the
--            "same" activity_id collapsed into one MERGE target row,
--            which Snowflake correctly rejected as a duplicate-row error
--            rather than silently corrupting data.
-- BATCH 1 CHANGE: added "WHERE lap.IS_CURRENT = TRUE" to the
--            transient load — required now that LAP can carry
--            multiple (historical + current) rows per activity.
--            Without this filter, LAS would double-count every
--            activity that has ever changed.
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA silver;

-- ── Transient load ─────────────────────────────────────────────
INSERT OVERWRITE INTO sales_analytics.silver.leads_activities_summary_transient
SELECT
    lap.ACTIVITY_ID,
    lap.LEAD_ID,
    lap.CONTACT_ID,
    lap.USER_ID,
    lap.TYPE,
    lap.TEXT,
    lap.STATUS_CHANGE,
    lap.CREATED_BY_NAME,
    lap.UPDATED_BY_NAME,
    lap.ATTENDEES,
    lap.CUSTOM_ACTIVITY_ID,
    lap.MEETING_STARTS_AT,
    lap.MEETING_END_AT,
    lap.MEETING_STATUS,
    lap.ACTIVITY_AT,
    lap.DATE_CREATED,
    lap.DATE_UPDATED,
    lap.CREATED_BY_USER,
    lap.UPDATED_BY_USER,
    lap.INSERT_DATE,
    CURRENT_TIMESTAMP()                    AS UPDATE_DATE,
    MD5(
        CONCAT(
            COALESCE(lap.ACTIVITY_ID,              ''),
            COALESCE(lap.LEAD_ID,                  ''),   -- Fix: added — see file header
            COALESCE(lap.DATE_UPDATED::VARCHAR,    ''),
            COALESCE(f.cf_field_name,              ''),
            COALESCE(f.cf_field_value,             '')
        )
    )                                      AS MD5_HASH,
    cald.WRAPPED_ACTIVITIES,
    ca_name.CUSTOM_ACTIVITY_NAME           AS CUSTOM_ACTIVITY,
    f.cf_field_name                        AS CUSTOM_ACTIVITY_OUTCOME_NAME,
    f.cf_field_value                       AS CUSTOM_ACTIVITY_OUTCOME,
    COALESCE(u.FIRST_NAME || ' ' || u.LAST_NAME, u.EMAIL) AS DEA_INTERNAL_NAME,  -- Batch fix: falls back to email when name fields are null (e.g. user@example.com has no first/last name in source CRM data), rather than showing a blank "empty" setter row
    u.EMAIL                                AS DEA_INTERNAL_EMAIL
FROM sales_analytics.silver.lead_activities_processed lap
LEFT JOIN sales_analytics.silver.close_crm_users_processed u
    ON lap.USER_ID = u.USER_ID
LEFT JOIN (
    SELECT
        cald.ACTIVITY_ID,
        cald.LEAD_ID,
        cald.WRAPPED_ACTIVITIES,
        REPLACE(f.key, 'custom.', '')       AS cf_id,
        f.value::VARCHAR                     AS cf_field_value,
        ca.CUSTOM_ACTIVITY_NAME,
        ca.CUSTOM_ACTIVITY_OUTCOME_NAME      AS cf_field_name
    FROM sales_analytics.silver.custom_activities_all_leads_details cald
    JOIN sales_analytics.silver.custom_activities ca
        ON ca.CUSTOM_ACTIVITY_ID = cald.WRAPPED_ACTIVITIES:custom_activity_type_id::VARCHAR,
    LATERAL FLATTEN(input => cald.WRAPPED_ACTIVITIES) f
    WHERE f.key LIKE 'custom.%'
    AND ca.CUSTOM_ACTIVITY_OUTCOME_ID = REPLACE(f.key, 'custom.', '')
) f ON lap.ACTIVITY_ID = f.ACTIVITY_ID
   AND lap.LEAD_ID     = f.LEAD_ID
LEFT JOIN (
    SELECT DISTINCT CUSTOM_ACTIVITY_ID, CUSTOM_ACTIVITY_NAME
    FROM sales_analytics.silver.custom_activities
) ca_name ON lap.CUSTOM_ACTIVITY_ID = ca_name.CUSTOM_ACTIVITY_ID
LEFT JOIN sales_analytics.silver.custom_activities_all_leads_details cald
    ON lap.ACTIVITY_ID = cald.ACTIVITY_ID
   AND lap.LEAD_ID     = cald.LEAD_ID
WHERE lap.IS_CURRENT = TRUE;   -- ← Batch 1 addition: LAP is now SCD2


-- ── Merge (unchanged key) ──────────────────────────────────────
MERGE INTO sales_analytics.silver.leads_activities_summary AS target
USING (
    SELECT
        ACTIVITY_ID, LEAD_ID, CONTACT_ID, USER_ID, TYPE, TEXT,
        STATUS_CHANGE, CREATED_BY_NAME, UPDATED_BY_NAME, ATTENDEES,
        CUSTOM_ACTIVITY_ID, MEETING_STARTS_AT, MEETING_END_AT,
        MEETING_STATUS, ACTIVITY_AT, DATE_CREATED, DATE_UPDATED,
        CREATED_BY_USER, UPDATED_BY_USER, INSERT_DATE,
        WRAPPED_ACTIVITIES, CUSTOM_ACTIVITY,
        CUSTOM_ACTIVITY_OUTCOME_NAME, CUSTOM_ACTIVITY_OUTCOME,
        DEA_INTERNAL_NAME, DEA_INTERNAL_EMAIL,
        MD5_HASH
    FROM sales_analytics.silver.leads_activities_summary_transient
) AS source
ON  target.ACTIVITY_ID                                 = source.ACTIVITY_ID
AND target.LEAD_ID                                      = source.LEAD_ID
AND COALESCE(target.CUSTOM_ACTIVITY_OUTCOME_NAME, '') = COALESCE(source.CUSTOM_ACTIVITY_OUTCOME_NAME, '')

WHEN MATCHED AND target.MD5_HASH <> source.MD5_HASH THEN
    UPDATE SET
        target.LEAD_ID                     = source.LEAD_ID,
        target.CONTACT_ID                  = source.CONTACT_ID,
        target.USER_ID                     = source.USER_ID,
        target.TYPE                        = source.TYPE,
        target.TEXT                        = source.TEXT,
        target.STATUS_CHANGE               = source.STATUS_CHANGE,
        target.CREATED_BY_NAME             = source.CREATED_BY_NAME,
        target.UPDATED_BY_NAME             = source.UPDATED_BY_NAME,
        target.ATTENDEES                   = source.ATTENDEES,
        target.CUSTOM_ACTIVITY_ID          = source.CUSTOM_ACTIVITY_ID,
        target.MEETING_STARTS_AT           = source.MEETING_STARTS_AT,
        target.MEETING_END_AT              = source.MEETING_END_AT,
        target.MEETING_STATUS              = source.MEETING_STATUS,
        target.ACTIVITY_AT                 = source.ACTIVITY_AT,
        target.DATE_CREATED                = source.DATE_CREATED,
        target.DATE_UPDATED                = source.DATE_UPDATED,
        target.CREATED_BY_USER             = source.CREATED_BY_USER,
        target.UPDATED_BY_USER             = source.UPDATED_BY_USER,
        target.WRAPPED_ACTIVITIES          = source.WRAPPED_ACTIVITIES,
        target.CUSTOM_ACTIVITY             = source.CUSTOM_ACTIVITY,
        target.CUSTOM_ACTIVITY_OUTCOME     = source.CUSTOM_ACTIVITY_OUTCOME,
        target.DEA_INTERNAL_NAME           = source.DEA_INTERNAL_NAME,
        target.DEA_INTERNAL_EMAIL          = source.DEA_INTERNAL_EMAIL,
        target.MD5_HASH                    = source.MD5_HASH,
        target.UPDATE_DATE                 = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        ACTIVITY_ID, LEAD_ID, CONTACT_ID, USER_ID, TYPE, TEXT,
        STATUS_CHANGE, CREATED_BY_NAME, UPDATED_BY_NAME, ATTENDEES,
        CUSTOM_ACTIVITY_ID, MEETING_STARTS_AT, MEETING_END_AT,
        MEETING_STATUS, ACTIVITY_AT, DATE_CREATED, DATE_UPDATED,
        CREATED_BY_USER, UPDATED_BY_USER, INSERT_DATE,
        WRAPPED_ACTIVITIES, CUSTOM_ACTIVITY,
        CUSTOM_ACTIVITY_OUTCOME_NAME, CUSTOM_ACTIVITY_OUTCOME,
        DEA_INTERNAL_NAME, DEA_INTERNAL_EMAIL,
        MD5_HASH, UPDATE_DATE
    )
    VALUES (
        source.ACTIVITY_ID, source.LEAD_ID, source.CONTACT_ID,
        source.USER_ID, source.TYPE, source.TEXT,
        source.STATUS_CHANGE, source.CREATED_BY_NAME, source.UPDATED_BY_NAME,
        source.ATTENDEES, source.CUSTOM_ACTIVITY_ID,
        source.MEETING_STARTS_AT, source.MEETING_END_AT, source.MEETING_STATUS,
        source.ACTIVITY_AT, source.DATE_CREATED, source.DATE_UPDATED,
        source.CREATED_BY_USER, source.UPDATED_BY_USER, source.INSERT_DATE,
        source.WRAPPED_ACTIVITIES, source.CUSTOM_ACTIVITY,
        source.CUSTOM_ACTIVITY_OUTCOME_NAME, source.CUSTOM_ACTIVITY_OUTCOME,
        source.DEA_INTERNAL_NAME, source.DEA_INTERNAL_EMAIL,
        source.MD5_HASH, CURRENT_TIMESTAMP()
    );