-- ============================================================
-- FILE: 01_inbound_strategies_booked.sql
-- PURPOSE: Gold view tracking inbound Triage Call → Strategy Call
--          booking funnel for the Inbound Setter Report
-- SOURCE:  sales_analytics.silver.leads_activities_summary
-- TARGET:  sales_analytics.gold.inbound_strategies_booked
-- SCHEMA (per Snowflake_Complete_Data_Model.pdf):
--   LEAD_ID, ACTIVITY_LOG_DATE, SETTER_CLOSER_EMAIL,
--   SETTER_CLOSER_NAME, TRIAGE_CALL_OUTCOME, TRIAGE_CALL_DATE,
--   STRATEGY_CALL_BOOKED, SC_YEAR_WEEK
-- ROWS:    2,617 (one row per Triage Call activity)
-- NOTES:
--   STRATEGY_CALL_BOOKED is a BOOLEAN flag = (Triage Call Outcome
--     = '1. Strategy Call Scheduled'). The actual strategy call
--     outcome detail lives in ALL_STRATEGIES_DETAILS, not here.
--
--   SETTER_CLOSER_EMAIL/NAME resolved from DEA_INTERNAL_EMAIL/NAME
--     (the Triage Call activity owner via USER_ID), NOT from the
--     cf_Setter custom field. Validated 99.54% match (2605/2617)
--     between activity owner and cf_Setter value — activity owner
--     chosen as the simpler, already-resolved source in Silver.
--
--   SC_YEAR_WEEK derivation: COALESCE(DATE_OF_STRATEGY_CALL,
--     STRATEGY_AT, TRIAGE_CALL_DATE) — prefers the business-entered
--     strategy call date field over the system ACTIVITY_AT
--     timestamp, falls back to TRIAGE_CALL_DATE when no strategy
--     call exists yet (i.e. STRATEGY_CALL_BOOKED = FALSE rows).
--     Format: 'YYYY-WNN' via TO_CHAR + WEEKOFYEAR.
--
--   Strategy call join condition: links the lead's most recent
--     Strategy Call (ROW_NUMBER dedup) where the call date is
--     on/after the triage date — same funnel-sequencing pattern
--     used across all Gold views.
--
--   Validated counts: 2617 total rows, 2487 distinct leads,
--     928 STRATEGY_CALL_BOOKED = TRUE (matches pre-schema-fix
--     validated figure), 100% SC_YEAR_WEEK populated,
--     week range 2026-W11 to 2026-W24.

-- BATCH 2 CHANGE (timezone fix — backported here 2026-09; was
--            previously applied live in Snowflake only, never
--            saved to this file until now): las.ACTIVITY_AT is now
--            wrapped in CONVERT_TIMEZONE('UTC', 'America/New_York', ...)
--            at the point it enters activity_pivot. Every downstream
--            ::DATE cast (TRIAGE_CALL_DATE, etc.) inherits the fix
--            automatically. Fixes a confirmed bug where activities
--            between ~8PM–midnight Eastern were bucketed into the
--            wrong calendar day. Verified against real business stakeholder
--            numbers for Chris Blanchette, Aug 21 2026 — see
--            decisions-and-learnings.md.
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA gold;

CREATE OR REPLACE VIEW sales_analytics.gold.inbound_strategies_booked AS

WITH activity_pivot AS (
    SELECT DISTINCT
        las.LEAD_ID,
        las.ACTIVITY_ID,
        CONVERT_TIMEZONE('UTC', 'America/New_York', las.ACTIVITY_AT) AS ACTIVITY_AT,
        las.CUSTOM_ACTIVITY                   AS ACTIVITY_TYPE,
        las.DEA_INTERNAL_NAME                 AS ASSIGNED_USER,
        las.DEA_INTERNAL_EMAIL                AS ASSIGNED_EMAIL,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Triage Call Outcome'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS TRIAGE_OUTCOME,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Date of Strategy Call'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS DATE_OF_STRATEGY_CALL_RAW
    FROM sales_analytics.silver.leads_activities_summary las
    WHERE las.TYPE = 'CustomActivity'
),
flat AS (
    SELECT DISTINCT * FROM activity_pivot
),
triage AS (
    SELECT
        LEAD_ID,
        ACTIVITY_ID                                          AS TRIAGE_ACTIVITY_ID,
        ACTIVITY_AT                                          AS TRIAGE_CALL_DATE,
        ASSIGNED_USER                                        AS SETTER_CLOSER_NAME,
        ASSIGNED_EMAIL                                       AS SETTER_CLOSER_EMAIL,
        TRIAGE_OUTCOME                                        AS TRIAGE_CALL_OUTCOME,
        TRIAGE_OUTCOME = '1. Strategy Call Scheduled'        AS STRATEGY_CALL_BOOKED
    FROM flat
    WHERE ACTIVITY_TYPE = '3) Triage Call'
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
    t.LEAD_ID,
    t.TRIAGE_CALL_DATE                                          AS ACTIVITY_LOG_DATE,
    t.SETTER_CLOSER_EMAIL,
    t.SETTER_CLOSER_NAME,
    t.TRIAGE_CALL_OUTCOME,
    t.TRIAGE_CALL_DATE,
    t.STRATEGY_CALL_BOOKED,
    TO_CHAR(
        COALESCE(s.DATE_OF_STRATEGY_CALL, s.STRATEGY_AT, t.TRIAGE_CALL_DATE),
        'YYYY'
    ) || '-W' || LPAD(
        WEEKOFYEAR(COALESCE(s.DATE_OF_STRATEGY_CALL, s.STRATEGY_AT, t.TRIAGE_CALL_DATE))::VARCHAR,
        2, '0'
    )                                                            AS SC_YEAR_WEEK
FROM triage t
LEFT JOIN strategy s
    ON t.LEAD_ID = s.LEAD_ID
    AND (
        (s.DATE_OF_STRATEGY_CALL IS NOT NULL AND s.DATE_OF_STRATEGY_CALL >= t.TRIAGE_CALL_DATE::DATE)
        OR (s.DATE_OF_STRATEGY_CALL IS NULL AND s.STRATEGY_AT >= t.TRIAGE_CALL_DATE)
    );
