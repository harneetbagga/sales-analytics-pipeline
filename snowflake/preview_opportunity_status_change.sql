-- ============================================================
-- FILE: preview_opportunity_status_change.sql
-- PURPOSE: PREVIEW (read-only) — generate synthetic
--          OpportunityStatusChange activity rows derived
--          from leads_raw.opportunities[].status_label
--          transitions across daily Bronze snapshots
-- SOURCE:  sales_analytics.bronze.leads_raw
-- STATUS:  ⚠️ FOR REVIEW ONLY — does not write to any table
--          Pending the business stakeholder's confirmation before being added
--          to the LEAD_ACTIVITIES_PROCESSED MERGE source
-- ============================================================

-- ── Context ───────────────────────────────────────────────
-- leads_raw is loaded as a DAILY SNAPSHOT (2 dates so far:
-- 2026-06-04 and 2026-06-08). Each lead's `opportunities[]`
-- array contains a `status_label` field that represents the
-- current pipeline stage of that opportunity at snapshot time.
--
-- By comparing status_label across consecutive daily snapshots
-- for the same opportunity_id, we can derive pipeline stage
-- transitions in the format:
--   "old_status_label --> new_status_label"
-- which matches the STATUS_CHANGE format the business stakeholder demonstrated
-- (e.g. "0. Strategy Call Scheduled --> 5. Short Term Farm- 3-6 Months")
--
-- DATA QUALITY NOTE:
-- Some leads have DUPLICATE opportunity entries within the same
-- snapshot (same opportunity_id, different date_updated) — this
-- appears to be Close CRM serializing historical opportunity
-- states within the lead record. We deduplicate by keeping the
-- entry with the latest opp.value:date_updated per
-- (lead_id, opportunity_id, LOAD_DATE).
--
-- RESULT: ~3,609 status transitions detected across the 2
-- available snapshots (will grow as more daily loads accumulate)

-- ── Proposed integration (PENDING CONFIRMATION) ──────────────
-- These rows would be added via UNION ALL into the
-- LEAD_ACTIVITIES_PROCESSED MERGE source as NEW rows only.
-- Existing 900,908 rows (Call/SMS/Email/etc.) remain UNCHANGED —
-- their STATUS_CHANGE stays as raw disposition/status/merge_status.
--
-- Synthetic ACTIVITY_ID convention:
--   'oppstatus_' || OPPORTUNITY_ID || '_' || OPP_DATE_UPDATED
--
-- Expected combined row count after integration:
--   900,908 (existing) + ~3,609 (new) = ~904,517

WITH opp_flat AS (
    SELECT
        PARSE_JSON(JSON_OBJECT):id::VARCHAR    AS LEAD_ID,
        LOAD_DATE,
        opp.value:id::VARCHAR                  AS OPPORTUNITY_ID,
        opp.value:status_label::VARCHAR        AS STATUS_LABEL,
        opp.value:date_updated::TIMESTAMP_NTZ  AS OPP_DATE_UPDATED,
        opp.value:user_id::VARCHAR             AS OPP_USER_ID,
        ROW_NUMBER() OVER (
            PARTITION BY PARSE_JSON(JSON_OBJECT):id::VARCHAR, opp.value:id::VARCHAR, LOAD_DATE
            ORDER BY opp.value:date_updated::TIMESTAMP_NTZ DESC
        ) AS rn
    FROM sales_analytics.bronze.leads_raw,
    LATERAL FLATTEN(input => PARSE_JSON(JSON_OBJECT):opportunities) opp
),
opp_dedup AS (
    SELECT * FROM opp_flat WHERE rn = 1
),
opp_transitions AS (
    SELECT
        LEAD_ID,
        OPPORTUNITY_ID,
        OPP_DATE_UPDATED,
        OPP_USER_ID,
        STATUS_LABEL AS NEW_STATUS,
        LAG(STATUS_LABEL) OVER (
            PARTITION BY OPPORTUNITY_ID ORDER BY OPP_DATE_UPDATED
        ) AS OLD_STATUS
    FROM opp_dedup
)

-- ── Preview: synthetic OpportunityStatusChange rows ──────────
SELECT
    'oppstatus_' || OPPORTUNITY_ID || '_' || OPP_DATE_UPDATED::VARCHAR AS ACTIVITY_ID,
    LEAD_ID,
    NULL                                AS CONTACT_ID,
    OPP_USER_ID                         AS USER_ID,
    'OpportunityStatusChange'           AS TYPE,
    NULL                                AS TEXT,
    OLD_STATUS || ' --> ' || NEW_STATUS AS STATUS_CHANGE,
    NULL                                AS DIRECTION,
    NULL                                AS CREATED_BY_NAME,
    NULL                                AS UPDATED_BY_NAME,
    NULL                                AS ATTENDEES,
    NULL                                AS CUSTOM_ACTIVITY_ID,
    NULL                                AS MEETING_STARTS_AT,
    NULL                                AS MEETING_END_AT,
    NULL                                AS MEETING_STATUS,
    OPP_DATE_UPDATED                    AS ACTIVITY_AT,
    OPP_DATE_UPDATED                    AS DATE_CREATED,
    OPP_DATE_UPDATED                    AS DATE_UPDATED,
    NULL                                AS CREATED_BY_USER,
    NULL                                AS UPDATED_BY_USER
FROM opp_transitions
WHERE OLD_STATUS IS NOT NULL
AND OLD_STATUS <> NEW_STATUS
ORDER BY OPP_DATE_UPDATED DESC;


-- ── Open questions for the business stakeholder ─────────────────────────────
-- 1. Does this OLD_STATUS --> NEW_STATUS format match what's
--    expected for STATUS_CHANGE?
-- 2. Should existing LEAD_ACTIVITIES_PROCESSED rows
--    (Call/SMS/Email/Meeting/CustomActivity/LeadMerge —
--    900,908 rows) keep their current STATUS_CHANGE values
--    (raw disposition/status/merge_status), with these new
--    synthetic rows added alongside via UNION ALL?
-- 3. Is the synthetic ACTIVITY_ID convention
--    ('oppstatus_' || opportunity_id || '_' || date_updated)
--    acceptable, or is there a preferred naming convention?
-- 4. CONTACT_ID, CREATED_BY_NAME/USER, UPDATED_BY_NAME/USER,
--    ATTENDEES, MEETING_*, CUSTOM_ACTIVITY_ID, DIRECTION, TEXT
--    are all NULL for these synthetic rows — confirm this is
--    acceptable for downstream Gold layer reports.
