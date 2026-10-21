-- ============================================================
-- FILE: 03_all_strategies_details.sql
-- PURPOSE: Gold view — strategy-call-level detail/audit fact
--          table, one row per Strategy Call activity (NOT
--          deduplicated to latest-per-lead, unlike the booking
--          funnel views)
-- SOURCE:  sales_analytics.silver.leads_activities_summary
-- TARGET:  sales_analytics.gold.all_strategies_details
-- SCHEMA (per Snowflake_Complete_Data_Model.pdf):
--   LEAD_ID, ACTIVITY_LOG_DATE, CUSTOM_ACTIVITY, STATUS,
--   CLOSER_EMAIL, CLOSER_NAME, STRATEGY_CALL_OUTCOME,
--   DATE_OF_STRATEGY_CALL, OFFER_PRESENTED, SETTER, SETTER_NAME
-- ROWS:    3,460 (every Strategy Call activity)
-- NOTES:
--   STATUS = MEETING_STATUS from Silver. Verified only 2 values
--     exist: 'published', 'draft' — this is the Close CRM activity
--     record status (whether the note is finalized), not a sales
--     funnel status.
--
--   SETTER = raw cf_Setter user ID (e.g. 'user_Bt4K3UWw...').
--     SETTER_NAME = resolved name via join to
--     CLOSE_CRM_USERS_PROCESSED. Verified cf_Setter values are
--     valid user IDs that resolve cleanly (15/15 sample match).
--     This differs from CLOSER_EMAIL/CLOSER_NAME pattern (which
--     resolves to email instead of raw ID) — intentional per
--     target schema column naming.
--
--   CLOSER_EMAIL/CLOSER_NAME resolved via cf_Closer user ID join
--     to CLOSE_CRM_USERS_PROCESSED.
--
--   ACTIVITY_LOG_DATE = ACTIVITY_AT. Verified ACTIVITY_AT and
--     DATE_CREATED are identical for activities in this dataset
--     (0 hour difference across sample) — no separate "logged"
--     timestamp exists to differentiate.
--
--   NOT deduplicated by lead — this view is the full strategy
--     call history/audit table. A lead with multiple strategy
--     calls (reschedules, follow-ups, repeat attempts) will have
--     multiple rows here, each representing one actual call.
--
--   Validated counts: 3460 total rows (matches known Strategy
--     Call activity count), 2825 distinct leads, ~100% resolution
--     on closer/setter/outcome/date/offer fields.

-- BATCH 2 CHANGE (timezone fix — backported here 2026-09, see
--            01_inbound_strategies_booked.sql header for full
--            context): las.ACTIVITY_AT wrapped in CONVERT_TIMEZONE
--            at entry to activity_pivot. This view also feeds
--            CLOSER_REPORT and OBJECTIONS_FACED_REPORT, so this
--            single fix corrects those two exported reports too.
--            Gold-level proof of the pre-fix bug: this view showed
--            ACTIVITY_LOG_DATE = 2026-07-09 (raw UTC) for an
--            activity that actually occurred 2026-07-08 21:01
--            Eastern — see decisions-and-learnings.md, Mahmoud
--            Toblany edge case.

-- NAME-FALLBACK FIX (2026-09, found investigating a blank "empty"
--            setter row in the Outbound Setter Report): CLOSER_NAME
--            and SETTER_NAME now COALESCE to EMAIL when FIRST_NAME/
--            LAST_NAME are both null in close_crm_users_processed
--            (e.g. user@example.com has no name on
--            file in source CRM data) — was previously showing a
--            fully blank name instead of any identifying value.
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA gold;

CREATE OR REPLACE VIEW sales_analytics.gold.all_strategies_details AS

WITH activity_pivot AS (
    SELECT DISTINCT
        las.LEAD_ID,
        las.ACTIVITY_ID,
        CONVERT_TIMEZONE('UTC', 'America/New_York', las.ACTIVITY_AT) AS ACTIVITY_AT,
        las.CUSTOM_ACTIVITY                   AS ACTIVITY_TYPE,
        las.MEETING_STATUS                    AS STATUS,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Strategy Call Outcome'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS STRATEGY_CALL_OUTCOME,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Date of Strategy Call'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS DATE_OF_STRATEGY_CALL_RAW,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Offer Presented'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS OFFER_PRESENTED,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Objections Faced?'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS OBJECTIONS_FACED,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Setter'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS SETTER_ID,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Closer'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS CLOSER_ID
    FROM sales_analytics.silver.leads_activities_summary las
    WHERE las.TYPE = 'CustomActivity'
    AND las.CUSTOM_ACTIVITY = '5) Strategy Call'
),
flat AS (
    SELECT DISTINCT * FROM activity_pivot
)

SELECT
    f.LEAD_ID,
    f.ACTIVITY_ID,
    f.ACTIVITY_AT                                  AS ACTIVITY_LOG_DATE,
    f.ACTIVITY_TYPE                                AS CUSTOM_ACTIVITY,
    f.STATUS,
    closer.EMAIL                                   AS CLOSER_EMAIL,
    COALESCE(closer.FIRST_NAME || ' ' || closer.LAST_NAME, closer.EMAIL) AS CLOSER_NAME,
    f.STRATEGY_CALL_OUTCOME,
    TRY_TO_DATE(f.DATE_OF_STRATEGY_CALL_RAW)       AS DATE_OF_STRATEGY_CALL,
    f.OFFER_PRESENTED,
    f.OBJECTIONS_FACED,
    f.SETTER_ID                                    AS SETTER,
    COALESCE(setter.FIRST_NAME || ' ' || setter.LAST_NAME, setter.EMAIL) AS SETTER_NAME
FROM flat f
LEFT JOIN sales_analytics.silver.close_crm_users_processed closer
    ON f.CLOSER_ID = closer.USER_ID
LEFT JOIN sales_analytics.silver.close_crm_users_processed setter
    ON f.SETTER_ID = setter.USER_ID;
