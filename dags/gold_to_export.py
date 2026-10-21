"""
DAG: gold_to_export

Materializes and exports all 4 KPI reports from Gold layer
views to S3 as CSV files for downstream consumption by QuickSight.

Reports:
  1. Inbound Setter Report  → dea_setter_report/inbound/
  2. Outbound Setter Report → dea_setter_report/outbound/
  3. Closer Report          → dea_closer_report/closer_report/
  4. Objections Faced Report → dea_closer_report/objections_faced/

Pattern per report:
  Step 1: CREATE OR REPLACE TRANSIENT TABLE (materialize)
  Step 2: Row-count validation (fails BEFORE export if suspiciously
          empty/small — see materialize_validate_export_cleanup)
  Step 3: COPY INTO S3 stage (export)
  Step 4: DROP TABLE (cleanup, always runs)

Task order:
  run_gold_regression_tests (gate)
    -> [export_inbound_setter, export_outbound_setter,
        export_closer, export_objections] (parallel)
    -> refresh_quicksight_task

The regression test suite (tests/test_gold_regression.py) runs FIRST,
as a gate — if a known-answer Gold-layer check fails (e.g. the
timezone fix has silently regressed on one of the 5 Gold views), the
whole DAG fails before any report is exported or QuickSight is
refreshed with data built on a broken view. Credentials are pulled
from the same snowflake_default Airflow connection every other task
in this pipeline already uses — no separate manual environment setup
required when run through this task specifically (running the suite
manually from a terminal instead still requires exporting
SNOWFLAKE_ACCOUNT/USER/PASSWORD/etc. by hand — see the test file's
own docstring).

Schedule: Triggered by silver_to_gold DAG on success

CHANGE LOG:
  - 2026-09: fixed SQL_OUTBOUND_SETTER_REPORT — the original `combined`
    CTE LEFT JOINed lead-grain facts (outbound_set/strategy_dedup/
    sales_dedup, one row per lead) onto dial_base, which is CALL-grain
    (one row per individual dial). That duplicated every lead-level
    fact across every call belonging to that lead. total_leads_touched
    and outbound_set were protected because they used
    COUNT(DISTINCT lead_id), but total_closer_show, total_offer,
    total_sale, and total_revenue used plain SUM() over the same
    duplicated rows — silently multiplying those numbers (including
    real dollar revenue) by however many times a converting lead was
    dialed that day. Fixed by splitting into call_agg (call-grain,
    aggregated from dial_base directly) and lead_agg (lead-grain,
    aggregated from a new lead_level CTE built off the already-deduped
    outbound_set/strategy_dedup/sales_dedup chain, never touching
    dial_base), joined together only once at (dial_date, setter) in
    the final SELECT. See decisions-and-learnings.md for detail.
  - 2026-09: data quality + testing batch — export tasks converted
    from opaque SQLExecuteQueryOperator blobs to PythonOperator, each
    running materialize -> row-count validation -> export -> cleanup
    as separate checkable steps (see materialize_validate_export_cleanup).
    Added run_gold_regression_tests as a gate before all 4 exports,
    running the known-answer regression suite against real boundary
    cases already traced by hand this session.
"""

"""
DAG: gold_to_export

Builds the four KPI report tables in the Gold schema and refreshes QuickSight.

Reports (persistent tables, read by QuickSight through reporting_wh as the
read-only report_reader role):
  1. Inbound Setter Report   -> gold.rpt_inbound_setter_report
  2. Outbound Setter Report  -> gold.rpt_outbound_setter_report
  3. Closer Report           -> gold.rpt_closer_report
  4. Objections Faced Report -> gold.rpt_objections_faced_report

Pattern per report (build_report_table):
  Step 1: materialize into a transient staging table
  Step 2: row-count validation (fail BEFORE publishing if empty/small)
  Step 3: publish: INSERT OVERWRITE into the persistent rpt_ table (atomic)
  Step 4: grant SELECT to report_reader
  Step 5: drop the staging table (always runs)
  (LEGACY_S3_EXPORT=True additionally writes the old CSV to S3 during cutover.)

Task order:
  run_gold_regression_tests (gate: known-answer + pivot-isolation tests)
    -> [build_inbound_setter_report, build_outbound_setter_report,
        build_closer_report, build_objections_faced_report] (parallel)
    -> refresh_quicksight_datasets (SPICE refresh; skipped for direct query)

The regression suite runs FIRST, as a gate: if a known-answer Gold check or
the pivot-isolation check fails, the DAG fails before any report is published
or QuickSight is refreshed. Credentials come from the snowflake_default
Airflow connection.

Schedule: Triggered by silver_to_gold DAG on success

CHANGE LOG:
  - 2026-10: serving layer changed. Reports are now persistent Gold tables read
    by QuickSight through reporting_wh (report_reader role) instead of CSVs
    exported to S3 and read through manifests. Removed the four unused
    duplicate report SQL constants. Added test_gold_pivot_isolation.py to the
    gate. Objections pivot now groups by LEAD_ID as well as ACTIVITY_ID.
  - 2026-09: fixed the outbound report (call-grain vs lead-grain aggregation),
    added row-count validation, and added the Gold regression gate. See
    decisions-and-learnings.md for the incident detail.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.hooks.base import BaseHook

import boto3
import time
import logging
import os
import subprocess
from airflow.operators.python import PythonOperator

# AWS account that owns the QuickSight datasets. Set it in the environment
# (MWAA: an Airflow environment/config option); it is only used by the
# SPICE refresh path, which is off by default (QUICKSIGHT_REFRESH_SPICE).
QUICKSIGHT_ACCOUNT_ID = os.environ.get("QUICKSIGHT_ACCOUNT_ID", "")
QUICKSIGHT_REGION = "us-east-1"

QUICKSIGHT_DATASETS = {
    "inbound_setter_report": "6ea682b9-d12f-4c31-9ff6-b9f585088bc6",
    "outbound_setter_report": "b2b7cfb2-5bfc-4739-965c-e46d5787e649",
    "closer_report": "6ab7081a-50b9-4426-92bf-6c99bdada985",
    "objections_faced_report": "8b51f9ba-2be9-484c-b454-3629a6f59049",
}

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_default"
DATABASE          = "sales_analytics"
WAREHOUSE         = "sales_wh"
GOLD              = "gold"
EXPORT_STAGE      = "sales_analytics.gold.s3_export_stage"

USE_CONTEXT = f"""
    USE WAREHOUSE {WAREHOUSE};
    USE DATABASE {DATABASE};
    USE SCHEMA {GOLD};
"""

# ─────────────────────────────────────────────
# SQL DEFINITIONS
# ─────────────────────────────────────────────

# Report SQL (the versions that actually run). Each one builds a transient
# staging table; build_report_table() validates it and loads the persistent
# Gold report table QuickSight reads.

SQL_INBOUND_SETTER_REPORT_MATERIALIZE = USE_CONTEXT + """
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.gold.inbound_setter_report_export AS
WITH triage_base AS (
    SELECT
        LEAD_ID, TRIAGE_CALL_DATE, SETTER_CLOSER_EMAIL,
        SETTER_CLOSER_NAME, TRIAGE_CALL_OUTCOME, STRATEGY_CALL_BOOKED
    FROM sales_analytics.gold.inbound_strategies_booked
),
triage_enriched AS (
    SELECT *,
        COALESCE(
            TRIAGE_CALL_OUTCOME NOT IN ('6. No Show', '7. Reschedule', '8. Cancel'),
            FALSE
        ) AS IS_INBOUND_TAKEN
    FROM triage_base
),
strategy_matched AS (
    SELECT
        t.LEAD_ID, t.TRIAGE_CALL_DATE, t.SETTER_CLOSER_EMAIL,
        t.SETTER_CLOSER_NAME, t.STRATEGY_CALL_BOOKED, t.IS_INBOUND_TAKEN,
        asd.STRATEGY_CALL_OUTCOME, asd.OFFER_PRESENTED,
        -- Req doc 3.2.2 fix: sort ATTENDED attempts first (Sale/Lost/
        -- Follow Up), then earliest within that group. Picks the
        -- earliest genuinely-attended attempt if one exists anywhere
        -- for this lead, falling back to earliest attempt overall only
        -- if the lead was never attended at all. Was previously always
        -- earliest-by-date, which could permanently lock in an early
        -- No Show even when a later attempt was genuinely attended
        -- (e.g. a Lost sale) -- discarding real attendance the doc's
        -- own valid-outcome list (Follow Up/Sale/Lost) says should count.
        ROW_NUMBER() OVER (
            PARTITION BY t.LEAD_ID, t.TRIAGE_CALL_DATE
            ORDER BY
                CASE WHEN asd.STRATEGY_CALL_OUTCOME NOT IN (
                    '4. No Show', '3. Cancel- Not Interested', '2. Admin Cancel',
                    '7. Cancel- Nurture', '3. No Show', '8. Cancel- Nurture'
                ) THEN 0 ELSE 1 END ASC,
                asd.ACTIVITY_LOG_DATE ASC
        ) AS rn
    FROM triage_enriched t
    LEFT JOIN sales_analytics.gold.all_strategies_details asd
        ON t.LEAD_ID = asd.LEAD_ID
        AND asd.ACTIVITY_LOG_DATE::DATE >= t.TRIAGE_CALL_DATE::DATE
    WHERE COALESCE(t.STRATEGY_CALL_BOOKED, FALSE) = TRUE
),
strategy_dedup AS (
    SELECT
        LEAD_ID, TRIAGE_CALL_DATE, SETTER_CLOSER_EMAIL,
        SETTER_CLOSER_NAME, STRATEGY_CALL_BOOKED, IS_INBOUND_TAKEN,
        STRATEGY_CALL_OUTCOME, OFFER_PRESENTED,
        CASE
            WHEN STRATEGY_CALL_OUTCOME IS NULL THEN FALSE
            WHEN STRATEGY_CALL_OUTCOME IN (
                '4. No Show', '3. Cancel- Not Interested',
                '2. Admin Cancel', '7. Cancel- Nurture',
                '3. No Show', '8. Cancel- Nurture'
            ) THEN FALSE
            ELSE TRUE
        END AS IS_STRATEGY_TAKEN,
        CASE WHEN OFFER_PRESENTED = 'Yes' THEN TRUE ELSE FALSE END AS IS_OFFER_PRESENTED
    FROM strategy_matched
    WHERE rn = 1
),
triage_not_booked AS (
    SELECT
        LEAD_ID, TRIAGE_CALL_DATE, SETTER_CLOSER_EMAIL,
        SETTER_CLOSER_NAME, STRATEGY_CALL_BOOKED, IS_INBOUND_TAKEN,
        NULL AS STRATEGY_CALL_OUTCOME, NULL AS OFFER_PRESENTED,
        FALSE AS IS_STRATEGY_TAKEN, FALSE AS IS_OFFER_PRESENTED
    FROM triage_enriched
    WHERE COALESCE(STRATEGY_CALL_BOOKED, FALSE) = FALSE
),
all_triage AS (
    SELECT * FROM strategy_dedup
    UNION ALL
    SELECT * FROM triage_not_booked
),
sales_dedup AS (
    SELECT LEAD_ID, CONTRACTED_VALUE
    FROM (
        SELECT LEAD_ID, CONTRACTED_VALUE,
            ROW_NUMBER() OVER (PARTITION BY LEAD_ID ORDER BY ACTIVITY_AT DESC) AS rn
        FROM sales_analytics.gold.sales_details
    ) WHERE rn = 1
),
report_base AS (
    SELECT
        t.TRIAGE_CALL_DATE::DATE    AS triage_date,
        t.SETTER_CLOSER_NAME        AS setter,
        t.LEAD_ID,
        t.IS_INBOUND_TAKEN,
        t.STRATEGY_CALL_BOOKED,
        t.IS_STRATEGY_TAKEN,
        t.IS_OFFER_PRESENTED,
        sm.CONTRACTED_VALUE,
        CASE WHEN sm.LEAD_ID IS NOT NULL THEN TRUE ELSE FALSE END AS HAS_SALE
    FROM all_triage t
    LEFT JOIN sales_dedup sm ON t.LEAD_ID = sm.LEAD_ID
)
SELECT
    triage_date,
    setter,
    COUNT(*)                                                           AS inbound_booked,
    SUM(IS_INBOUND_TAKEN::INT)                                        AS inbound_taken,
    ROUND(SUM(IS_INBOUND_TAKEN::INT) * 100.0
        / NULLIF(COUNT(*), 0), 1)                                     AS triage_set_rate,
    SUM(COALESCE(STRATEGY_CALL_BOOKED::INT, 0))                       AS strategy_call_booked,
    SUM(IS_STRATEGY_TAKEN::INT)                                       AS strategy_call_taken,
    ROUND(SUM(IS_STRATEGY_TAKEN::INT) * 100.0
        / NULLIF(SUM(COALESCE(STRATEGY_CALL_BOOKED::INT, 0)), 0), 1)  AS show_rate,
    ROUND(SUM(IS_OFFER_PRESENTED::INT) * 100.0
        / NULLIF(SUM(IS_STRATEGY_TAKEN::INT), 0), 1)                  AS offer_rate,
    SUM(HAS_SALE::INT)                                                AS total_sales,
    ROUND(SUM(HAS_SALE::INT) * 100.0
        / NULLIF(SUM(IS_STRATEGY_TAKEN::INT), 0), 1)                  AS sale_rate,
    ROUND(AVG(CASE WHEN HAS_SALE THEN CONTRACTED_VALUE END), 0)       AS average_order_value
FROM report_base
GROUP BY triage_date, setter
ORDER BY triage_date, setter;
"""

SQL_OUTBOUND_SETTER_REPORT_MATERIALIZE = USE_CONTEXT + """
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.gold.outbound_setter_report_export AS
WITH
dial_base AS (
    SELECT DISTINCT
        ACTIVITY_LOG_DATE::DATE   AS dial_date,
        LEAD_ID,
        ACTIVITY_ID,
        SETTER_CLOSER_NAME        AS setter,
        SETTER_CLOSER_EMAIL       AS setter_email,
        CUSTOM_ACTIVITY_OUTCOME   AS prospecting_outcome
    FROM sales_analytics.gold.outbound_prospect_dials
    WHERE CUSTOM_ACTIVITY_OUTCOME_NAME = 'Prospecting Call Outcome'
),
outbound_set AS (
    SELECT DISTINCT dial_date, LEAD_ID, setter
    FROM dial_base
    WHERE prospecting_outcome = '2. Strategy Call Scheduled'
),
strategy_for_outbound AS (
    SELECT
        os.LEAD_ID, os.dial_date, os.setter,
        asd.STRATEGY_CALL_OUTCOME, asd.OFFER_PRESENTED,
        CASE
            WHEN asd.STRATEGY_CALL_OUTCOME IS NULL THEN FALSE
            WHEN asd.STRATEGY_CALL_OUTCOME IN (
                '4. No Show', '3. Cancel- Not Interested',
                '2. Admin Cancel', '7. Cancel- Nurture',
                '3. No Show', '8. Cancel- Nurture'
            ) THEN FALSE
            ELSE TRUE
        END AS IS_STRATEGY_TAKEN,
        CASE WHEN asd.OFFER_PRESENTED = 'Yes' THEN TRUE ELSE FALSE END AS IS_OFFER_PRESENTED,
        -- Req doc 3.2.2 fix: same reasoning as the Inbound report --
        -- sort ATTENDED attempts (Sale/Lost/Follow Up) first, then
        -- earliest within that group, so a later genuinely-attended
        -- attempt is never discarded in favor of an earlier No Show.
        ROW_NUMBER() OVER (
            PARTITION BY os.LEAD_ID, os.dial_date
            ORDER BY
                CASE WHEN asd.STRATEGY_CALL_OUTCOME NOT IN (
                    '4. No Show', '3. Cancel- Not Interested', '2. Admin Cancel',
                    '7. Cancel- Nurture', '3. No Show', '8. Cancel- Nurture'
                ) THEN 0 ELSE 1 END ASC,
                asd.ACTIVITY_LOG_DATE ASC
        ) AS rn
    FROM outbound_set os
    LEFT JOIN sales_analytics.gold.all_strategies_details asd
        ON os.LEAD_ID = asd.LEAD_ID
        AND asd.ACTIVITY_LOG_DATE::DATE >= os.dial_date
),
strategy_dedup AS (SELECT * FROM strategy_for_outbound WHERE rn = 1),
sales_dedup AS (
    SELECT LEAD_ID, CONTRACTED_VALUE
    FROM (
        SELECT LEAD_ID, CONTRACTED_VALUE,
            ROW_NUMBER() OVER (PARTITION BY LEAD_ID ORDER BY ACTIVITY_AT DESC) AS rn
        FROM sales_analytics.gold.sales_details
    ) WHERE rn = 1
),

-- Lead-grain funnel facts: exactly ONE row per (dial_date, setter, LEAD_ID)
-- that was set — built directly from the already-deduped lead-grain CTEs
-- above, never from the call-duplicated dial_base rows.
lead_level AS (
    SELECT
        os.dial_date,
        os.setter,
        os.LEAD_ID,
        COALESCE(sd.IS_STRATEGY_TAKEN, FALSE)  AS IS_STRATEGY_TAKEN,
        COALESCE(sd.IS_OFFER_PRESENTED, FALSE) AS IS_OFFER_PRESENTED,
        CASE WHEN sm.LEAD_ID IS NOT NULL
             AND COALESCE(sd.IS_STRATEGY_TAKEN, FALSE) THEN TRUE ELSE FALSE END AS HAS_SALE,
        sm.CONTRACTED_VALUE
    FROM outbound_set os
    LEFT JOIN strategy_dedup sd ON os.LEAD_ID = sd.LEAD_ID AND os.dial_date = sd.dial_date
    LEFT JOIN sales_dedup sm ON sd.LEAD_ID = sm.LEAD_ID
        AND COALESCE(sd.IS_STRATEGY_TAKEN, FALSE) = TRUE
),

-- Call-grain volume metrics, aggregated at their native grain
call_agg AS (
    SELECT
        dial_date,
        setter,
        COUNT(DISTINCT ACTIVITY_ID) AS total_outbound_calls,
        COUNT(DISTINCT LEAD_ID)     AS total_leads_touched
    FROM dial_base
    GROUP BY dial_date, setter
),

-- Lead-grain funnel/revenue metrics, aggregated at THEIR native grain —
-- SUM() here is safe because lead_level has exactly one row per lead,
-- no duplication to inflate.
lead_agg AS (
    SELECT
        dial_date,
        setter,
        COUNT(DISTINCT LEAD_ID)                              AS outbound_set,
        SUM(IS_STRATEGY_TAKEN::INT)                          AS total_closer_show,
        SUM(IS_OFFER_PRESENTED::INT)                         AS total_offer,
        SUM(HAS_SALE::INT)                                   AS total_sale,
        SUM(CASE WHEN HAS_SALE THEN CONTRACTED_VALUE ELSE 0 END) AS total_revenue
    FROM lead_level
    GROUP BY dial_date, setter
)

SELECT
    c.dial_date,
    c.setter,
    c.total_outbound_calls,
    c.total_leads_touched,
    COALESCE(l.outbound_set, 0)        AS outbound_set,
    COALESCE(l.total_closer_show, 0)   AS total_closer_show,
    COALESCE(l.total_offer, 0)         AS total_offer,
    COALESCE(l.total_sale, 0)          AS total_sale,
    ROUND(COALESCE(l.outbound_set, 0) * 100.0
        / NULLIF(c.total_outbound_calls, 0), 1)              AS dial_to_set_rate,
    ROUND(COALESCE(l.total_closer_show, 0) * 100.0
        / NULLIF(l.outbound_set, 0), 1)                       AS set_to_show_rate,
    ROUND(COALESCE(l.total_sale, 0) * 100.0
        / NULLIF(l.total_closer_show, 0), 1)                  AS show_to_sale_rate,
    COALESCE(l.total_revenue, 0)       AS total_revenue,
    ROUND(l.total_revenue / NULLIF(l.total_sale, 0), 0)       AS average_order_value
FROM call_agg c
LEFT JOIN lead_agg l ON c.dial_date = l.dial_date AND c.setter = l.setter
ORDER BY c.dial_date, c.setter;
"""

SQL_CLOSER_REPORT_MATERIALIZE = USE_CONTEXT + """
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
--   3. Date comes from TRY_TO_DATE of the 'Date of Strategy Call'/
--      'Date of Follow Up Call' CUSTOM FIELD (STRATEGY_CALL_DATE), not
--      ACTIVITY_AT, matching the business stakeholder's own query exactly. Confirmed via
--      live comparison this matters: filtering on ACTIVITY_AT first
--      can silently drop an activity whose STRATEGY_CALL_DATE genuinely
--      falls in the target window but whose ACTIVITY_AT doesn't.
--   3b. REVISED (2026-10-01, same day): NO date filter at all anymore,
--      and NO Python-templated {{start_date}}/{{end_date}} -- this table
--      now accumulates EVERY month, permanently, like every other Gold
--      export in this pipeline. CALL_YEAR_MONTH (DATE_TRUNC('MONTH', ...)
--      of STRATEGY_CALL_DATE) is now a real output column, matching
--      the business stakeholder's own CTE which has always grouped by this exact field.
--      Originally ("Option A", confirmed with the business stakeholder earlier in the
--      rebuild) month was treated as a filter applied before export,
--      producing a single-month snapshot each run -- this meant business
--      could never see a prior month without someone manually
--      re-triggering with a {"report_month": "..."} override, which
--      also destroyed whatever month was there before. Reversed once
--      that limitation became a real, named requirement: QuickSight's
--      own CALL_YEAR_MONTH filter control already existed in the
--      dashboard, unusable only because the export never populated it.
--      get_closer_report_date_range() and its DAG run config override
--      are now DEAD CODE for this report specifically -- left in place
--      only if something else still calls it; worth removing outright
--      if not.
--   4. CLOSER_NAME comes straight from silver.leads_activities_summary.
--      An earlier version merged name spellings here by hand. The root
--      cause was in Silver: the user's name is copied onto each activity
--      row when it loads, the MERGE only rewrites a row when its hash
--      changes, and the hash excludes the name, so a name edited in the
--      CRM left older rows with the old spelling (one real person had
--      three spellings, same USER_ID and email). Fixed once at the
--      source: bronze_to_silver.py now re-syncs every row's name and
--      email from the users dimension on each run (task
--      refresh_las_user_identity), so there is one name per person and
--      no merge is needed in any report. Names are shown as the user
--      table spells them.
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
-- disagreements across 3 closers tested. Kept DEA_INTERNAL_NAME, which
-- is now kept in sync with the users dimension by Silver (see point 4).
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
"""

SQL_OBJECTIONS_FACED_REPORT_MATERIALIZE = USE_CONTEXT + """
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.gold.objections_faced_report_export AS
WITH strategy_with_objections AS (
    SELECT
        CLOSER_NAME                AS closer_name,
        ACTIVITY_LOG_DATE::DATE    AS activity_date,
        LEAD_ID,
        ACTIVITY_ID,
        OBJECTIONS_FACED
    FROM sales_analytics.gold.all_strategies_details
    WHERE CLOSER_NAME IS NOT NULL
),
objections_exploded AS (
    SELECT
        s.closer_name, s.activity_date, s.LEAD_ID, s.ACTIVITY_ID,
        TRIM(obj.value::VARCHAR, '"') AS objection
    FROM strategy_with_objections s,
    LATERAL FLATTEN(input => TRY_PARSE_JSON(s.OBJECTIONS_FACED), OUTER => TRUE) obj
),
objections_pivot AS (
    SELECT
        closer_name, activity_date, ACTIVITY_ID, LEAD_ID,
        MAX(CASE WHEN LOWER(objection) = 'money' THEN 1 ELSE 0 END)             AS money_flag,
        MAX(CASE WHEN LOWER(objection) = 'fear' THEN 1 ELSE 0 END)              AS fear_flag,
        MAX(CASE WHEN LOWER(objection) IN ('hung up','hung_up') THEN 1 ELSE 0 END) AS hung_up_flag,
        MAX(CASE WHEN LOWER(objection) = 'logistical' THEN 1 ELSE 0 END)        AS logistical_flag,
        MAX(CASE WHEN LOWER(objection) IN ('no objections','no objection') THEN 1 ELSE 0 END) AS no_obj_flag,
        MAX(CASE WHEN LOWER(objection) IN ('other coaches','other coach') THEN 1 ELSE 0 END) AS other_coaches_flag,
        MAX(CASE WHEN LOWER(objection) = 'partner' THEN 1 ELSE 0 END)           AS partner_flag,
        MAX(CASE WHEN LOWER(objection) IN ('think about it','think abt it') THEN 1 ELSE 0 END) AS think_abt_it_flag,
        MAX(CASE WHEN LOWER(objection) = 'time' THEN 1 ELSE 0 END)              AS time_flag,
        MAX(CASE WHEN LOWER(objection) = 'trust' THEN 1 ELSE 0 END)             AS trust_flag,
        MAX(CASE WHEN LOWER(objection) = 'value' THEN 1 ELSE 0 END)             AS value_flag,
        MAX(CASE WHEN LOWER(objection) LIKE '%wasn%looking%offered%'
            OR LOWER(objection) = 'wasnt looking for what we offered'
            THEN 1 ELSE 0 END)                                                  AS not_looking_flag
    FROM objections_exploded
    GROUP BY closer_name, activity_date, ACTIVITY_ID, LEAD_ID
)
SELECT
    closer_name, activity_date,
    COUNT(*)                                               AS total_calls,
    SUM(money_flag)                                        AS money_count,
    SUM(fear_flag)                                         AS fear_count,
    SUM(hung_up_flag)                                      AS hung_up_count,
    SUM(logistical_flag)                                   AS logistical_count,
    SUM(no_obj_flag)                                       AS no_obj_count,
    SUM(other_coaches_flag)                                AS other_coaches_count,
    SUM(partner_flag)                                      AS partner_count,
    SUM(think_abt_it_flag)                                 AS think_abt_it_count,
    SUM(time_flag)                                         AS time_count,
    SUM(trust_flag)                                        AS trust_count,
    SUM(value_flag)                                        AS value_count,
    SUM(not_looking_flag)                                  AS not_looking_count,
    ROUND(SUM(money_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)          AS "Money%",
    ROUND(SUM(fear_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)           AS "Fear%",
    ROUND(SUM(hung_up_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)        AS "Hung Up%",
    ROUND(SUM(logistical_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)     AS "Logistical%",
    ROUND(SUM(no_obj_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)         AS "No Obj%",
    ROUND(SUM(other_coaches_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)  AS "Other Coaches%",
    ROUND(SUM(partner_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)        AS "Partner%",
    ROUND(SUM(think_abt_it_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)   AS "Think Abt It%",
    ROUND(SUM(time_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)           AS "Time%",
    ROUND(SUM(trust_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)          AS "Trust%",
    ROUND(SUM(value_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)          AS "Value%",
    ROUND(SUM(not_looking_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)    AS "Wsn't Lkng Fr Wht We Offrd%"
FROM objections_pivot
GROUP BY closer_name, activity_date
ORDER BY closer_name, activity_date;
"""


# Staging tables are transient and dropped after every run. The persistent
# tables QuickSight reads live in the same schema with an rpt_ prefix.
REPORTER_ROLE = "report_reader"

# Cutover safety switch: when True the old S3 CSV export also runs, so
# QuickSight can be switched over and checked before the S3 path is retired.
LEGACY_S3_EXPORT = False

EXPORT_REPORTS = {
    "inbound_setter_report": {
        "materialize_sql": SQL_INBOUND_SETTER_REPORT_MATERIALIZE,
        "staging_table": "sales_analytics.gold.inbound_setter_report_export",
        "target_table":  "sales_analytics.gold.rpt_inbound_setter_report",
        "s3_path": "dea_setter_report/inbound/inbound_setter_report.csv",
    },
    "outbound_setter_report": {
        "materialize_sql": SQL_OUTBOUND_SETTER_REPORT_MATERIALIZE,
        "staging_table": "sales_analytics.gold.outbound_setter_report_export",
        "target_table":  "sales_analytics.gold.rpt_outbound_setter_report",
        "s3_path": "dea_setter_report/outbound/outbound_setter_report.csv",
    },
    "closer_report": {
        "materialize_sql": SQL_CLOSER_REPORT_MATERIALIZE,
        "staging_table": "sales_analytics.gold.closer_report_export",
        "target_table":  "sales_analytics.gold.rpt_closer_report",
        "s3_path": "dea_closer_report/closer_report/closer_report.csv",
    },
    "objections_faced_report": {
        "materialize_sql": SQL_OBJECTIONS_FACED_REPORT_MATERIALIZE,
        "staging_table": "sales_analytics.gold.objections_faced_report_export",
        "target_table":  "sales_analytics.gold.rpt_objections_faced_report",
        "s3_path": "dea_closer_report/objections_faced/objections_faced_report.csv",
    },
}


def build_report_table(report_key, min_rows=1, **context):
    """
    Build one report and publish it to the persistent Gold table QuickSight reads.

      1. Materialize the report into a transient staging table.
      2. Validate: refuse to publish an empty / suspiciously small report.
      3. Publish: create the persistent rpt_ table if it does not exist, then
         INSERT OVERWRITE it from staging. The overwrite is one atomic
         statement, so a dashboard query never sees a half-loaded table, and
         the table keeps its identity and its grants.
      4. Grant SELECT on the table to the read-only reporting role (idempotent).
      5. (Only if LEGACY_S3_EXPORT) also write the old CSV to S3.
      6. Always drop the staging table, success or failure.

    If a report's columns change, drop the rpt_ table once so it is recreated
    with the new shape (INSERT OVERWRITE does not change a table's columns).
    """
    report = EXPORT_REPORTS[report_key]
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    staging = report["staging_table"]
    target = report["target_table"]

    logging.info("Materializing %s...", staging)
    hook.run(report["materialize_sql"])

    try:
        row_count = hook.get_first(f"SELECT COUNT(*) FROM {staging}")[0]
        logging.info("%s materialized with %s rows", staging, row_count)
        if row_count < min_rows:
            raise ValueError(
                f"{staging} has {row_count} rows, below the minimum of {min_rows}; "
                f"refusing to publish a suspiciously empty report to {target}."
            )

        hook.run(f"{USE_CONTEXT}\nCREATE TABLE IF NOT EXISTS {target} LIKE {staging};")
        hook.run(f"{USE_CONTEXT}\nINSERT OVERWRITE INTO {target} SELECT * FROM {staging};")
        published = hook.get_first(f"SELECT COUNT(*) FROM {target}")[0]
        if published != row_count:
            raise RuntimeError(
                f"{target} has {published} rows after publishing but staging had "
                f"{row_count}; the publish did not complete cleanly."
            )
        hook.run(f"{USE_CONTEXT}\nGRANT SELECT ON TABLE {target} TO ROLE {REPORTER_ROLE};")
        logging.info("Published %s rows to %s (SELECT granted to %s)",
                     published, target, REPORTER_ROLE)

        if LEGACY_S3_EXPORT:
            hook.run(f"""
                {USE_CONTEXT}
                COPY INTO @{EXPORT_STAGE}/{report['s3_path']}
                FROM {staging}
                FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                               NULL_IF = ('') EMPTY_FIELD_AS_NULL = TRUE COMPRESSION = NONE)
                HEADER = TRUE OVERWRITE = TRUE SINGLE = TRUE;
            """)
            logging.info("Legacy S3 export written to %s", report["s3_path"])
    finally:
        hook.run(f"{USE_CONTEXT}\nDROP TABLE IF EXISTS {staging};")
        logging.info("Cleaned up %s", staging)


def run_gold_regression_tests(**context):
    """
    Runs the known-answer Gold regression suite (tests/test_gold_regression.py)
    as a gate before any report is exported. Pulls credentials from the same
    snowflake_default Airflow connection every other task in this pipeline
    uses, so no separate manual environment setup is needed at runtime.

    A failing test here means a fix that was hard-won through manual
    reconciliation (the timezone bug, the fan-out dedup, the LAS composite
    key) has silently regressed — most likely a Gold view was redeployed
    without the CONVERT_TIMEZONE wrapping, or a CREATE OR REPLACE reverted
    a fix. Failing the whole DAG here, before export, is deliberate: we'd
    rather block today's reports than ship a regressed number to the business stakeholder's
    dashboard.
    """
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = conn.extra_dejson

    test_env = os.environ.copy()
    test_env.update({
        "SNOWFLAKE_ACCOUNT":   extra.get("account", ""),
        "SNOWFLAKE_USER":      conn.login or "",
        "SNOWFLAKE_PASSWORD":  conn.password or "",
        "SNOWFLAKE_WAREHOUSE": extra.get("warehouse", WAREHOUSE),
        "SNOWFLAKE_DATABASE":  extra.get("database", DATABASE),
        "SNOWFLAKE_ROLE":      extra.get("role", ""),
    })

    tests_dir = os.path.join(os.path.dirname(__file__), "tests")
    test_files = [
        os.path.join(tests_dir, "test_gold_regression.py"),
        os.path.join(tests_dir, "test_gold_pivot_isolation.py"),
    ]
    logging.info("Running Gold regression suite: %s", test_files)

    result = subprocess.run(
        ["pytest", *test_files, "-v", "--tb=short"],
        env=test_env,
        capture_output=True,
        text=True,
    )

    logging.info("pytest stdout:\n%s", result.stdout)
    if result.stderr:
        logging.info("pytest stderr:\n%s", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Gold regression suite FAILED (exit code {result.returncode}) — "
            f"a previously-fixed bug may have regressed. See task log above "
            f"for which test(s) failed. Blocking export until resolved."
        )

    logging.info("Gold regression suite passed — proceeding to export.")


# True when the QuickSight datasets are SPICE datasets built on the Gold report
# tables (refreshed here after each load). Set to False if they are switched to
# direct query: those read Snowflake live and have nothing to refresh.
# NOTE (2026-10-15): the QuickSight account was removed, so the refresh is OFF.
# Set back to True when a QuickSight account with SPICE datasets exists again.
QUICKSIGHT_REFRESH_SPICE = False


def refresh_quicksight_datasets(**context):
    if not QUICKSIGHT_REFRESH_SPICE:
        logging.info("QuickSight datasets use direct query; no SPICE refresh needed.")
        return
    client = boto3.client("quicksight", region_name=QUICKSIGHT_REGION)
    run_id = int(time.time())
    failures = []

    for name, dataset_id in QUICKSIGHT_DATASETS.items():
        ingestion_id = f"{dataset_id.replace('-', '')}-{run_id}"
        try:
            resp = client.create_ingestion(
                DataSetId=dataset_id,
                IngestionId=ingestion_id,
                AwsAccountId=QUICKSIGHT_ACCOUNT_ID,
                IngestionType="FULL_REFRESH",
            )
            logging.info(
                "Triggered SPICE refresh for %s (dataset_id=%s, ingestion_id=%s): %s",
                name, dataset_id, ingestion_id, resp["IngestionStatus"],
            )
        except Exception as e:
            logging.error("Failed to trigger refresh for %s: %s", name, e)
            failures.append(name)

    if failures:
        raise RuntimeError(f"QuickSight refresh failed for: {failures}")


refresh_quicksight_task = PythonOperator(
    task_id="refresh_quicksight_datasets",
    python_callable=refresh_quicksight_datasets,
)

# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="gold_to_export",
    description="Build the 4 KPI report tables in Gold and refresh QuickSight",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,  # triggered by silver_to_gold
    catchup=False,
    max_active_runs=1,
    tags=["gold", "snowflake", "reports", "quicksight"],
) as dag:

    # Gate: known-answer Gold regression suite runs FIRST. If it fails,
    # nothing downstream (export or QuickSight refresh) runs.
    regression_gate = PythonOperator(
        task_id="run_gold_regression_tests",
        python_callable=run_gold_regression_tests,
    )

    # All 4 reports run in parallel — no inter-dependencies between them.
    # Each build runs: materialize -> row-count validation -> atomic publish
    # to the persistent rpt_ table -> grant to report_reader -> cleanup.
    export_inbound_setter = PythonOperator(
        task_id="build_inbound_setter_report",
        python_callable=build_report_table,
        op_kwargs={"report_key": "inbound_setter_report", "min_rows": 1},
    )

    export_outbound_setter = PythonOperator(
        task_id="build_outbound_setter_report",
        python_callable=build_report_table,
        op_kwargs={"report_key": "outbound_setter_report", "min_rows": 1},
    )

    export_closer = PythonOperator(
        task_id="build_closer_report",
        python_callable=build_report_table,
        op_kwargs={"report_key": "closer_report", "min_rows": 1},
    )

    export_objections = PythonOperator(
        task_id="build_objections_faced_report",
        python_callable=build_report_table,
        op_kwargs={"report_key": "objections_faced_report", "min_rows": 1},
    )

    # regression_gate must pass before any export runs; all 4 exports run
    # in parallel with each other, then all feed the QuickSight refresh.
    # (Closer Report v2, added 2026-10 as a separate parallel export while
    # under investigation, has now been folded back into "closer_report"
    # itself — see SQL_CLOSER_REPORT_MATERIALIZE's header for the full
    # list of fixes. Back to 4 reports, one Closer Report.)
    regression_gate >> [
        export_inbound_setter,
        export_outbound_setter,
        export_closer,
        export_objections,
    ] >> refresh_quicksight_task