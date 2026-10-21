-- ============================================================
-- FILE: 02_outbound_strategies_booked.sql
-- PURPOSE: Gold view tracking outbound Prospecting Activity →
--          Strategy Call booking funnel for the Outbound Setter Report
-- SOURCE:  sales_analytics.silver.leads_activities_summary
-- TARGET:  sales_analytics.gold.outbound_strategies_booked
-- SCHEMA (per Snowflake_Complete_Data_Model.pdf):
--   LEAD_ID, ACTIVITY_LOG_DATE, SETTER_CLOSER_EMAIL,
--   SETTER_CLOSER_NAME, PROSPECT_CALL_DATE, STRATEGY_CALL_BOOKED,
--   SC_YEAR_WEEK
-- ROWS:    6,956 (one row per lead — latest Prospecting Activity)
-- NOTES:
--   Mirrors INBOUND_STRATEGIES_BOOKED pattern, rooted in
--     1) Prospecting Activity instead of 3) Triage Call.
--
--   STRATEGY_CALL_BOOKED is a BOOLEAN flag = (Prospecting Call
--     Outcome = '2. Strategy Call Scheduled').
--
--   SETTER_CLOSER_EMAIL/NAME resolved from DEA_INTERNAL_EMAIL/NAME
--     (activity owner via USER_ID). VERIFIED that 1) Prospecting
--     Activity has NO cf_Setter custom field at all (confirmed via
--     CUSTOM_ACTIVITIES field reference — only has Avatar, Date of
--     Contact, Follow Up Reason, Lead Quality, Next Action, Next
--     Action Date, Prospecting Call Notes, Prospecting Call Outcome,
--     Unqualified Reason). Activity owner is therefore the only
--     available source — 0 matches confirmed empirically (8209 rows,
--     0% match against non-existent cf_Setter field).
--
--   Deduplicated to ONE ROW PER LEAD (ROW_NUMBER by latest
--     ACTIVITY_AT), unlike INBOUND_STRATEGIES_BOOKED which keeps
--     one row per Triage Call activity. This is because Prospecting
--     Activity has much higher repeat-touch volume per lead
--     (8,209 activities / 6,956 leads) and the target schema's
--     PROSPECT_CALL_DATE singular column implies one row per lead's
--     most recent outbound touch, not every dial. Raw dial volume
--     is tracked separately in OUTBOUND_PROSPECT_DIALS.
--
--   SC_YEAR_WEEK derivation: same COALESCE(DATE_OF_STRATEGY_CALL,
--     STRATEGY_AT, PROSPECT_CALL_DATE) pattern as inbound view.
--
--   Validated counts: 6956 total rows = 6956 distinct leads (1:1,
--     confirms dedup correct), 1493 STRATEGY_CALL_BOOKED = TRUE,
--     100% SC_YEAR_WEEK populated, week range 2026-W11 to 2026-W24.
--     Note: 1493 is lower than the earlier pre-schema-fix validated
--     figure of 1642 — expected, since that included both
--     Prospecting Activity AND Prospecting Follow Up as dial
--     sources, while this view's correct scope is Prospecting
--     Activity only.

-- BATCH 2 CHANGE (timezone fix — backported here 2026-09, see
--            01_inbound_strategies_booked.sql header for full
--            context): las.ACTIVITY_AT wrapped in CONVERT_TIMEZONE
--            at entry to activity_pivot.
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA gold;

CREATE OR REPLACE VIEW sales_analytics.gold.outbound_strategies_booked AS

WITH activity_pivot AS (
    SELECT DISTINCT
        las.LEAD_ID,
        las.ACTIVITY_ID,
        CONVERT_TIMEZONE('UTC', 'America/New_York', las.ACTIVITY_AT) AS ACTIVITY_AT,
        las.CUSTOM_ACTIVITY                   AS ACTIVITY_TYPE,
        las.DEA_INTERNAL_NAME                 AS ASSIGNED_USER,
        las.DEA_INTERNAL_EMAIL                AS ASSIGNED_EMAIL,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Prospecting Call Outcome'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS PROSPECTING_OUTCOME,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Date of Strategy Call'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS DATE_OF_STRATEGY_CALL_RAW
    FROM sales_analytics.silver.leads_activities_summary las
    WHERE las.TYPE = 'CustomActivity'
),
flat AS (
    SELECT DISTINCT * FROM activity_pivot
),
prospecting AS (
    SELECT *
    FROM (
        SELECT
            LEAD_ID,
            ACTIVITY_ID                                          AS PROSPECT_ACTIVITY_ID,
            ACTIVITY_AT                                          AS PROSPECT_CALL_DATE,
            ASSIGNED_USER                                        AS SETTER_CLOSER_NAME,
            ASSIGNED_EMAIL                                       AS SETTER_CLOSER_EMAIL,
            PROSPECTING_OUTCOME = '2. Strategy Call Scheduled'   AS STRATEGY_CALL_BOOKED,
            ROW_NUMBER() OVER (
                PARTITION BY LEAD_ID
                ORDER BY ACTIVITY_AT DESC
            )                                                    AS rn
        FROM flat
        WHERE ACTIVITY_TYPE = '1) Prospecting Activity'
    )
    WHERE rn = 1
),
strategy AS (
    SELECT *
    FROM (
        SELECT
            LEAD_ID,
            ACTIVITY_AT                                          AS STRATEGY_AT,
            TRY_TO_DATE(DATE_OF_STRATEGY_CALL_RAW)               AS DATE_OF_STRATEGY_CALL,
            ROW_NUMBER() OVER (
                PARTITION BY LEAD_ID
                ORDER BY ACTIVITY_AT DESC
            )                                                    AS rn
        FROM flat
        WHERE ACTIVITY_TYPE = '5) Strategy Call'
    )
    WHERE rn = 1
)

SELECT
    p.LEAD_ID,
    p.PROSPECT_CALL_DATE                                        AS ACTIVITY_LOG_DATE,
    p.SETTER_CLOSER_EMAIL,
    p.SETTER_CLOSER_NAME,
    p.PROSPECT_CALL_DATE,
    p.STRATEGY_CALL_BOOKED,
    TO_CHAR(
        COALESCE(s.DATE_OF_STRATEGY_CALL, s.STRATEGY_AT, p.PROSPECT_CALL_DATE),
        'YYYY'
    ) || '-W' || LPAD(
        WEEKOFYEAR(COALESCE(s.DATE_OF_STRATEGY_CALL, s.STRATEGY_AT, p.PROSPECT_CALL_DATE))::VARCHAR,
        2, '0'
    )                                                            AS SC_YEAR_WEEK
FROM prospecting p
LEFT JOIN strategy s
    ON p.LEAD_ID = s.LEAD_ID
    AND (
        (s.DATE_OF_STRATEGY_CALL IS NOT NULL AND s.DATE_OF_STRATEGY_CALL >= p.PROSPECT_CALL_DATE::DATE)
        OR (s.DATE_OF_STRATEGY_CALL IS NULL AND s.STRATEGY_AT >= p.PROSPECT_CALL_DATE)
    );
