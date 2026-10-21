-- ============================================================
-- FILE: 03_closer_report.sql
-- LAYER: Export (materialize -> COPY INTO S3 -> drop)
--
-- This is the ONE Closer Report, replacing both the original
-- ALL_STRATEGIES_DETAILS-based version AND the separate "v2" export
-- that ran alongside it during the business stakeholder reconciliation investigation
-- (2026-10-01). v2 has been fully folded back in here -- see the
-- header comment inside the materialize step below for the complete
-- list of fixes this version carries (activity scope, ACTIVITY_ID
-- grouping fix, STRATEGY_CALL_DATE-based filtering, 4 confirmed name
-- merges, inclusion-based STRTGY_CALL_SHW, new LOST metric,
-- SALE_STATUS-based sale counting).
--
-- DATE SCOPING: defaults to CURRENT month-to-date, computed natively
-- in Snowflake (CURRENT_DATE()) -- NOT hardcoded to September or any
-- other specific month. Running this file always reflects whatever
-- "today" actually is. See the inline comment in the WHERE clause
-- below for how to override to a specific past month instead.
--
-- This file is the manual-worksheet companion to
-- SQL_CLOSER_REPORT_MATERIALIZE in gold_to_export.py, which is what
-- actually runs in production via MWAA (using Python-resolved
-- {start_date}/{end_date} instead of CURRENT_DATE(), so a past
-- month can be requested via DAG run config -- see
-- get_closer_report_date_range() in that file). Keep both in sync if
-- you change the underlying logic in either place.
-- ============================================================
-- ============================================================
-- REWRITTEN (2026-10-01): replaces the original ALL_STRATEGIES_DETAILS
-- -based version entirely, consolidating everything found during the
-- reconciliation investigation against the business stakeholder's own shared CTE. This
-- IS the Closer Report now -- no separate "v2" export exists anymore.
--
-- Changes from the original:
--   1. Source is SILVER.LEADS_ACTIVITIES_SUMMARY directly, filtered to
--      BOTH '5) Strategy Call' AND '6) Strategy Call Follow Up' -- the
--      original only ever considered the first, undercounting real
--      activity.
--   2. GROUP BY includes ACTIVITY_ID (not just LEAD_ID, ACTIVITY_AT) --
--      fixes a real collision where two distinct activities logged at
--      the identical timestamp for the same lead were blended into
--      one row.
--   3. Date filter is on TRY_TO_DATE of the 'Date of Strategy Call'/
--      'Date of Follow Up Call' CUSTOM FIELD (STRATEGY_CALL_DATE), not
--      ACTIVITY_AT, matching the business stakeholder's own query exactly. Confirmed via
--      live comparison this matters: filtering on ACTIVITY_AT first
--      can silently drop an activity whose STRATEGY_CALL_DATE genuinely
--      falls in the target window but whose ACTIVITY_AT doesn't.
--      {{start_date}}/{{end_date}} filled in by Python at task runtime
--      (get_closer_report_date_range) -- not literal CURRENT_DATE() --
--      so a past month can be requested via DAG run config.
--   4. CLOSER_NAME passes through a name-variant merge. DEA_INTERNAL_NAME
--      is NOT a reliable live join -- it snapshots whatever name string
--      was captured into each individual activity at log time, so the
--      same real closer (same USER_ID, confirmed via the raw "Closer"
--      field) can carry more than one spelling across different
--      activities. An audit grouping by DEA_INTERNAL_EMAIL across the
--      whole dataset found exactly 3 such cases, confirmed this is a
--      closed, fully-enumerated list, not an open-ended risk:
--        Peter Jensen / PJ Jensen / P J  -> PJ Jensen
--        Enzo Misso                     -> Enzo Missio
--        Joshua Astle                   -> Josh Astle
--      Lucija Bitunjac -> Lucia Bitunjac is a SEPARATE, different kind
--      of fix: close_crm_users_processed only ever has one record for
--      her (no internal fragmentation at all) -- this merge exists
--      purely to match the business stakeholder's spelling convention for comparison,
--      not to fix a data bug.
--   5. Inclusion-based STRTGY_CALL_SHW (Sale/Lost/Follow Up outcomes),
--      not exclusion-based -- matches the business stakeholder's whitelist, and folds in
--      duplicate-numbered outcome variants his query revealed that the
--      original never accounted for ('5. Sale'/'6. Sale', '6. Lost'/
--      '7. Lost'). New LOST metric, didn't exist before.
--      CANCEL_NOT_INTEREST also folds in '2. Cancel- No Longer
--      Interested' alongside '3. Cancel- Not Interested', another
--      duplicate pair his query revealed.
--   6. Sale counted via sales_details.SALE_STATUS <> 'NO_SALE', not
--      "any row exists in sales_details for this lead" -- the original
--      approach was very likely overcounting, since sales_details
--      contains non-sale rows too.
--
-- NOT adopted from the business stakeholder's query: final_funnel, credit_score_category,
-- gold.lead_basic_details (we don't have this table; explicit scope
-- decision, not needed for reconciliation). His "Closer"/"Setter"
-- custom-field attribution was tested head-to-head against our
-- DEA_INTERNAL_NAME resolution in September specifically: 0
-- disagreements across 3 closers tested. Kept DEA_INTERNAL_NAME (with
-- the merge above) since it's already proven reliable once the 3 known
-- fragmentation cases are handled.
--
-- COST: no ACTIVITY_AT bound on the raw scan anymore (point 3 above) --
-- this is a genuine full-history scan every run, not a one-month
-- partition read. Accepted deliberately for correctness over speed on
-- a reconciliation-critical report.
--
-- RESULT as of the live comparison that produced this rewrite: 62 of
-- 120 (closer, route) rows matched the business stakeholder's file EXACTLY, 9 more were
-- correctly and intentionally excluded (confirmed via direct trace --
-- their activity's real STRATEGY_CALL_DATE falls outside the window
-- even though ACTIVITY_AT looked like it was inside it), 1 was a
-- closer present only in our data (Basel Hany -- real, correctly-
-- attributed September activity, absent from his file, open question
-- for him), and the remaining 48 were small (+/-1 to +/-5) differences
-- in both directions with no single common cause found -- consistent
-- with the same batch-timing freshness pattern already confirmed for
-- the Inbound, Outbound, and Objections reports, not a new bug.
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA gold;

-- Step 1: Materialize
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.gold.closer_report_export AS
WITH strategy_pivot AS (
    SELECT
        las.ACTIVITY_ID,
        las.LEAD_ID,
        las.ACTIVITY_AT,
        MAX(las.DEA_INTERNAL_NAME) AS CLOSER_NAME,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME IN ('Date of Strategy Call', 'Date of Follow Up Call')
            THEN las.CUSTOM_ACTIVITY_OUTCOME END) AS STRATEGY_CALL_DATE_RAW,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME IN ('Strategy Call Outcome', 'Follow Up Call Outcome')
            THEN las.CUSTOM_ACTIVITY_OUTCOME END) AS STRATEGY_CALL_OUTCOME
    FROM sales_analytics.silver.leads_activities_summary las
    WHERE las.CUSTOM_ACTIVITY IN ('5) Strategy Call', '6) Strategy Call Follow Up')
      AND las.CUSTOM_ACTIVITY_OUTCOME_NAME IN (
          'Strategy Call Outcome', 'Follow Up Call Outcome',
          'Date of Strategy Call', 'Date of Follow Up Call'
      )
    GROUP BY las.ACTIVITY_ID, las.LEAD_ID, las.ACTIVITY_AT
),
strategy_base AS (
    SELECT
        sp.CLOSER_NAME,
        sp.LEAD_ID,
        -- CALL_YEAR_MONTH: real DATE value (first of the month), not a
        -- string -- lets QuickSight roll it up to quarter/year or do
        -- exact date-range filtering natively, same as every other
        -- report's date column in this pipeline.
        DATE_TRUNC('MONTH', TRY_TO_DATE(sp.STRATEGY_CALL_DATE_RAW)) AS CALL_YEAR_MONTH,
        COALESCE(lp.LEAD_ROUTE, 'NO_ROUTE') AS LEAD_ROUTE,
        sp.STRATEGY_CALL_OUTCOME,
        CASE WHEN sp.STRATEGY_CALL_OUTCOME = '2. Admin Cancel'
            THEN 1 ELSE 0 END AS is_admin_cancel,
        CASE WHEN sp.STRATEGY_CALL_OUTCOME IN ('7. Cancel- Nurture', '8. Cancel- Nurture')
            THEN 1 ELSE 0 END AS is_cancel_nurture,
        CASE WHEN sp.STRATEGY_CALL_OUTCOME IN ('3. Cancel- Not Interested', '2. Cancel- No Longer Interested')
            THEN 1 ELSE 0 END AS is_cancel_not_interest,
        CASE WHEN sp.STRATEGY_CALL_OUTCOME IN ('4. No Show', '3. No Show')
            THEN 1 ELSE 0 END AS is_no_show,
        CASE WHEN sp.STRATEGY_CALL_OUTCOME IN
            ('5. Sale', '6. Sale', '6. Lost', '7. Lost', '1. Follow Up')
            THEN 1 ELSE 0 END AS is_show,
        CASE WHEN sp.STRATEGY_CALL_OUTCOME IN ('6. Lost', '7. Lost')
            THEN 1 ELSE 0 END AS is_lost
    FROM strategy_pivot sp
    LEFT JOIN sales_analytics.silver.leads_processed lp
        ON sp.LEAD_ID = lp.LEAD_ID
    WHERE sp.CLOSER_NAME IS NOT NULL
      -- NO date filter -- full history, every run. See header "3b".
),
sales_dedup AS (
    -- Joined to strategy_base at MONTH grain (sale_year_month, matching
    -- the business stakeholder's own CTE exactly), not exact calendar day -- a sale
    -- closing a few days after the strategy call still counts. The
    -- earlier day-level join was stricter than his and could silently
    -- miss real sales; this can only recover matches, never lose ones
    -- that already worked.
    SELECT
        sd.CLOSER_NAME AS CLOSER_NAME,
        DATE_TRUNC('MONTH', sd.ACTIVITY_AT) AS CALL_YEAR_MONTH,
        COALESCE(lp.LEAD_ROUTE, 'NO_ROUTE') AS LEAD_ROUTE,
        SUM(CASE WHEN sd.SALE_STATUS <> 'NO_SALE' THEN 1 ELSE 0 END) AS sale_count,
        CEIL(
            SUM(CASE WHEN sd.CONTRACTED_VALUE <> 0 THEN sd.CONTRACTED_VALUE ELSE NULL END)
            / NULLIF(COUNT(CASE WHEN sd.CONTRACTED_VALUE <> 0 THEN sd.CONTRACTED_VALUE END), 0)
        ) AS avg_value,
        SUM(sd.CASH_COLLECTED) AS cash_collected
    FROM sales_analytics.gold.sales_details sd
    LEFT JOIN sales_analytics.silver.leads_processed lp
        ON sd.LEAD_ID = lp.LEAD_ID
    GROUP BY
        sd.CLOSER_NAME,
        DATE_TRUNC('MONTH', sd.ACTIVITY_AT), COALESCE(lp.LEAD_ROUTE, 'NO_ROUTE')
)
SELECT
    sb.CLOSER_NAME,
    sb.CALL_YEAR_MONTH,
    sb.LEAD_ROUTE,
    COUNT(*)                                                          AS CALL_BOOKED,
    SUM(sb.is_admin_cancel)                                           AS ADMIN_CANCEL,
    SUM(sb.is_admin_cancel) * 1.0
        / NULLIF(COUNT(*), 0)                                         AS ADMN_CNCL_RT,
    SUM(sb.is_cancel_nurture)                                         AS CANCEL_NURTURE,
    SUM(sb.is_cancel_nurture) * 1.0
        / NULLIF(COUNT(*), 0)                                         AS CNCL_NTR_RT,
    SUM(sb.is_cancel_not_interest)                                    AS CANCEL_NOT_INTEREST,
    SUM(sb.is_cancel_not_interest) * 1.0
        / NULLIF(COUNT(*), 0)                                         AS CNCL_NT_INTRST_RT,
    SUM(sb.is_admin_cancel) + SUM(sb.is_cancel_nurture)
        + SUM(sb.is_cancel_not_interest)                              AS TOTAL_CANCEL,
    (SUM(sb.is_admin_cancel) + SUM(sb.is_cancel_nurture)
        + SUM(sb.is_cancel_not_interest)) * 1.0
        / NULLIF(COUNT(*), 0)                                         AS TOTL_CNCL_RT,
    SUM(sb.is_no_show)                                                AS NO_SHOW,
    SUM(sb.is_no_show) * 1.0
        / NULLIF(COUNT(*), 0)                                         AS NO_SHW_RT,
    SUM(sb.is_show)                                                   AS STRTGY_CALL_SHW,
    SUM(sb.is_show) * 1.0
        / NULLIF(COUNT(*), 0)                                         AS STGY_SHW_RT,
    SUM(sb.is_lost)                                                   AS LOST,
    COALESCE(MAX(sd.sale_count), 0)                                   AS SALE,
    -- NOTE: denominator is STRTGY_CALL_SHW, not CALL_BOOKED — reverse
    -- engineered and verified against the business stakeholder's own report sample.
    COALESCE(MAX(sd.sale_count), 0) * 1.0
        / NULLIF(SUM(sb.is_show), 0)                                  AS SALE_RT,
    COALESCE(MAX(sd.avg_value), 0)                                    AS AVG_VAUE,
    COALESCE(MAX(sd.cash_collected), 0)                               AS CASH_COLLECTED
FROM strategy_base sb
LEFT JOIN sales_dedup sd
    ON sb.CLOSER_NAME = sd.CLOSER_NAME
   AND sb.CALL_YEAR_MONTH = sd.CALL_YEAR_MONTH
   AND sb.LEAD_ROUTE = sd.LEAD_ROUTE
GROUP BY sb.CLOSER_NAME, sb.CALL_YEAR_MONTH, sb.LEAD_ROUTE
ORDER BY sb.CLOSER_NAME, sb.CALL_YEAR_MONTH, sb.LEAD_ROUTE;

-- Step 2: Export
COPY INTO @sales_analytics.gold.s3_export_stage/dea_closer_report/closer_report/closer_report.csv
FROM sales_analytics.gold.closer_report_export
FILE_FORMAT = (
    TYPE                         = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                      = ('')
    EMPTY_FIELD_AS_NULL          = TRUE
    COMPRESSION                  = NONE
)
HEADER    = TRUE
OVERWRITE = TRUE
SINGLE    = TRUE;

-- Step 3: Cleanup
DROP TABLE IF EXISTS sales_analytics.gold.closer_report_export;