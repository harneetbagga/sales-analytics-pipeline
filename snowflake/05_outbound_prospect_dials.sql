-- ============================================================
-- FILE: 05_outbound_prospect_dials.sql
-- LAYER: Gold (view)
-- SOURCE: silver.leads_activities_summary
-- BATCH 2 CHANGE (timezone fix — backported here 2026-09, see
--            01_inbound_strategies_booked.sql header for full
--            context). CONVERT_TIMEZONE computed once in the
--            activity_pivot CTE (see fan-out fix below), reused
--            for both ACTIVITY_LOG_DATE and PROSPECT_YEAR_WEEK.
-- FAN-OUT DEDUP FIX (2026-09, found while investigating an "empty"
--            setter row in the Outbound Setter Report — see
--            decisions-and-learnings.md): this view previously
--            selected straight from leads_activities_summary with
--            no dedup at all. Since LAS is fanned out one row per
--            custom field per activity (Avatar, Lead Quality,
--            Prospecting Call Outcome, etc.), this view was
--            silently returning multiple identical rows per real
--            activity — confirmed 5x duplication on at least one
--            activity in a live spot-check. That inflated
--            TOTAL_OUTBOUND_CALLS / TOTAL_LEADS_TOUCHED in every
--            Outbound Setter Report ever generated from this view.
--            Fixed using the same activity_pivot/flat CTE pattern
--            every other Gold view already uses — pivots just the
--            'Prospecting Call Outcome' field instead of returning
--            every fan-out row for the activity.
-- MULTI-LEAD PARTITION FIX (2026-09, found via
--            test_gold_regression.py after redeploying
--            silver_to_gold.py — see decisions-and-learnings.md):
--            the activity_pivot window functions partitioned by
--            ACTIVITY_ID alone. Since the same ACTIVITY_ID can
--            legitimately belong to multiple LEAD_IDs on the
--            rebuilt dataset (the same multi-lead collision pattern
--            fixed elsewhere in LAP/LAS), pivoting without LEAD_ID
--            in the partition blended two different leads' rows
--            together. Fixed by adding LEAD_ID to every PARTITION BY
--            in this view.
-- STATUS PIVOT FIX (2026-09, same investigation): MEETING_STATUS
--            (aliased STATUS) was selected as a bare passthrough
--            column, not pivoted. Confirmed via direct Silver query
--            that MEETING_STATUS is NOT constant across one
--            activity's fan-out rows — e.g. the 'Avatar' and
--            'Prospecting Call Outcome' fields on one real activity
--            showed 'published' while 'Date of Contact' and
--            'Prospecting Call Notes' on the SAME activity showed
--            'draft', all with identical INSERT_DATE (not a
--            timing/versioning artifact — apparently MEETING_STATUS
--            is per-field, not per-activity, in this source data).
--            Since STATUS wasn't pivoted, SELECT DISTINCT correctly
--            preserved both as genuinely different row images,
--            producing a real duplicate per activity_id (23 confirmed
--            on live data). Fixed by pivoting STATUS with MAX() —
--            'published' > 'draft' alphabetically, so this
--            deterministically prefers 'published' when both exist
--            for one activity, without needing a full CASE-based
--            priority rule for what is a low-stakes display field.
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA gold;

CREATE OR REPLACE VIEW sales_analytics.gold.outbound_prospect_dials AS

WITH activity_pivot AS (
    SELECT DISTINCT
        las.LEAD_ID,
        las.ACTIVITY_ID,
        CONVERT_TIMEZONE('UTC', 'America/New_York', las.ACTIVITY_AT) AS ACTIVITY_AT,
        las.CUSTOM_ACTIVITY,
        las.DEA_INTERNAL_EMAIL                AS SETTER_CLOSER_EMAIL,
        las.DEA_INTERNAL_NAME                 AS SETTER_CLOSER_NAME,
        MAX(las.MEETING_STATUS)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS STATUS,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Prospecting Call Outcome'
            THEN las.CUSTOM_ACTIVITY_OUTCOME_NAME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS CUSTOM_ACTIVITY_OUTCOME_NAME,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Prospecting Call Outcome'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS CUSTOM_ACTIVITY_OUTCOME
    FROM sales_analytics.silver.leads_activities_summary las
    WHERE las.CUSTOM_ACTIVITY IN ('1) Prospecting Activity', '2) Prospecting Follow Up')
)

SELECT DISTINCT
    LEAD_ID,
    ACTIVITY_ID,
    ACTIVITY_AT                          AS ACTIVITY_LOG_DATE,
    TO_CHAR(ACTIVITY_AT, 'YYYY') || '-W' ||
        LPAD(WEEKOFYEAR(ACTIVITY_AT)::VARCHAR, 2, '0')  AS PROSPECT_YEAR_WEEK,
    CUSTOM_ACTIVITY,
    STATUS,
    CUSTOM_ACTIVITY_OUTCOME_NAME,
    CUSTOM_ACTIVITY_OUTCOME,
    SETTER_CLOSER_EMAIL,
    SETTER_CLOSER_NAME
FROM activity_pivot;