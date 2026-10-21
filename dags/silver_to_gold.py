"""
DAG: silver_to_gold

Builds/refreshes Gold layer views in Snowflake from Silver layer tables.
All 5 views use CREATE OR REPLACE VIEW — idempotent, no transient/merge
staging required since views compute live off Silver tables.

Views:
  1. inbound_strategies_booked   <- from silver.leads_activities_summary
  2. outbound_strategies_booked  <- from silver.leads_activities_summary
  3. all_strategies_details      <- from silver.leads_activities_summary
  4. sales_details                <- from silver.leads_activities_summary
  5. outbound_prospect_dials      <- from silver.leads_activities_summary

All 5 views read independently from the same Silver source table
(leads_activities_summary) plus close_crm_users_processed for
setter/closer name resolution -- no inter-view dependencies, so all
5 run in parallel.

Schedule: Triggered by bronze_to_silver DAG on success

CHANGE LOG (2026-09) -- CRITICAL BACKPORT:
  This file had NEVER received the timezone fix or the
  outbound_prospect_dials fan-out dedup fix, despite both being
  applied live in Snowflake worksheets and verified extensively
  earlier this session. Every fix existed only in standalone
  reference .sql files and live (worksheet-applied) Snowflake state
  -- never in this actual DAG. Since silver_to_gold runs CREATE OR
  REPLACE VIEW on every execution, every re-run of this DAG silently
  REVERTED all 5 Gold views back to their original, unfixed
  definitions -- discovered only when the new known-answer regression
  suite (tests/test_gold_regression.py) failed 5 of 6 tests against
  live data, reproducing the exact pre-fix numbers documented earlier
  in this project (e.g. Chris Blanchette Aug 21 showing 2 instead of
  3; outbound_prospect_dials showing 74,849 duplicate rows). This is
  the reason that regression suite exists as a gate before every
  export -- to catch exactly this class of silent regression before
  it reaches a stakeholder's dashboard again. All 5 views below now
  include:
    - CONVERT_TIMEZONE('UTC', 'America/New_York', ...) wrapping
      ACTIVITY_AT at the earliest point it enters each view's pivot,
      so every downstream ::DATE cast/grouping is Eastern-correct
    - outbound_prospect_dials rebuilt with an activity_pivot CTE +
      MAX(CASE WHEN...) pattern (matching every other view) instead
      of a bare SELECT with no dedup, fixing the fan-out duplication
See decisions-and-learnings.md for full incident detail.
"""

"""
DAG: silver_to_gold

Builds/refreshes Gold layer views in Snowflake from Silver layer tables.
All 5 views use CREATE OR REPLACE VIEW — idempotent, no transient/merge
staging required since views compute live off Silver tables.

Views:
  1. inbound_strategies_booked   <- from silver.leads_activities_summary
  2. outbound_strategies_booked  <- from silver.leads_activities_summary
  3. all_strategies_details      <- from silver.leads_activities_summary
  4. sales_details                <- from silver.leads_activities_summary
  5. outbound_prospect_dials      <- from silver.leads_activities_summary

All 5 views read independently from the same Silver source table
(leads_activities_summary) plus close_crm_users_processed for
setter/closer name resolution -- no inter-view dependencies, so all
5 run in parallel.

Schedule: Triggered by bronze_to_silver DAG on success

CHANGE LOG (2026-09) -- CRITICAL BACKPORT:
  This file had NEVER received the timezone fix or the
  outbound_prospect_dials fan-out dedup fix, despite both being
  applied live in Snowflake worksheets and verified extensively
  earlier this session. Every fix existed only in standalone
  reference .sql files and live (worksheet-applied) Snowflake state
  -- never in this actual DAG. Since silver_to_gold runs CREATE OR
  REPLACE VIEW on every execution, every re-run of this DAG silently
  REVERTED all 5 Gold views back to their original, unfixed
  definitions -- discovered only when the new known-answer regression
  suite (tests/test_gold_regression.py) failed 5 of 6 tests against
  live data, reproducing the exact pre-fix numbers documented earlier
  in this project (e.g. Chris Blanchette Aug 21 showing 2 instead of
  3; outbound_prospect_dials showing 74,849 duplicate rows). This is
  the reason that regression suite exists as a gate before every
  export -- to catch exactly this class of silent regression before
  it reaches a stakeholder's dashboard again. All 5 views below now
  include:
    - CONVERT_TIMEZONE('UTC', 'America/New_York', ...) wrapping
      ACTIVITY_AT at the earliest point it enters each view's pivot,
      so every downstream ::DATE cast/grouping is Eastern-correct
    - outbound_prospect_dials rebuilt with an activity_pivot CTE +
      MAX(CASE WHEN...) pattern (matching every other view) instead
      of a bare SELECT with no dedup, fixing the fan-out duplication
See decisions-and-learnings.md for full incident detail.

CHANGE LOG (2026-10) -- PIVOT KEY FIX:
  Four views (inbound_strategies_booked, outbound_strategies_booked,
  all_strategies_details, sales_details) pivoted outcomes with
  PARTITION BY ACTIVITY_ID alone. About 1,313 ACTIVITY_IDs belong to
  more than one lead, so for those activities MAX(CASE ...) could take
  the value from the OTHER lead's rows. Every pivot now partitions by
  ACTIVITY_ID + LEAD_ID, matching the Silver key (and the pattern
  outbound_prospect_dials already used). tests/test_gold_pivot_isolation.py
  guards this: a source check that no pivot partitions by ACTIVITY_ID alone,
  plus a live check on all_strategies_details against Silver.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# -----------------------------------------------
# SETTINGS
# -----------------------------------------------
SNOWFLAKE_CONN_ID = "snowflake_default"
DATABASE          = "sales_analytics"
WAREHOUSE         = "sales_wh"
GOLD              = "gold"

USE_CONTEXT = f"""
    USE WAREHOUSE {WAREHOUSE};
    USE DATABASE {DATABASE};
    USE SCHEMA {GOLD};
"""

# -----------------------------------------------
# SQL DEFINITIONS -- all 5 corrected (timezone + fan-out fixes)
# -----------------------------------------------

# -- 1. INBOUND_STRATEGIES_BOOKED ------------------------------------------------------------
SQL_INBOUND_STRATEGIES_BOOKED = USE_CONTEXT + """
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
"""

# -- 2. OUTBOUND_STRATEGIES_BOOKED ------------------------------------------------------------
SQL_OUTBOUND_STRATEGIES_BOOKED = USE_CONTEXT + """
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
"""

# -- 3. ALL_STRATEGIES_DETAILS ------------------------------------------------------------
SQL_ALL_STRATEGIES_DETAILS = USE_CONTEXT + """
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
"""

# -- 4. SALES_DETAILS ------------------------------------------------------------
SQL_SALES_DETAILS = USE_CONTEXT + """
CREATE OR REPLACE VIEW sales_analytics.gold.sales_details AS

WITH activity_pivot AS (
    SELECT DISTINCT
        las.LEAD_ID,
        las.ACTIVITY_ID,
        CONVERT_TIMEZONE('UTC', 'America/New_York', las.ACTIVITY_AT) AS ACTIVITY_AT,
        las.CUSTOM_ACTIVITY                   AS ACTIVITY_TYPE,
        las.MEETING_STATUS                    AS SALE_STATUS,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Contract Value'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS CONTRACT_VALUE,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Cash Collected'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS CASH_COLLECTED,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Date of Sale'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS DATE_OF_SALE_RAW,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Program'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS PROGRAM,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Setter'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS SETTER_ID,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Closer'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS CLOSER_ID
    FROM sales_analytics.silver.leads_activities_summary las
    WHERE las.TYPE = 'CustomActivity'
    AND las.CUSTOM_ACTIVITY IN ('7) New Sale', '8) New Sale [Custom Payment Plan]')
),
flat AS (
    SELECT DISTINCT * FROM activity_pivot
)

SELECT
    f.LEAD_ID,
    f.ACTIVITY_AT,
    f.SALE_STATUS,
    setter.EMAIL                                   AS SETTER_EMAIL,
    COALESCE(setter.FIRST_NAME || ' ' || setter.LAST_NAME, setter.EMAIL) AS SETTER_NAME,
    closer.EMAIL                                   AS CLOSER_EMAIL,
    COALESCE(closer.FIRST_NAME || ' ' || closer.LAST_NAME, closer.EMAIL) AS CLOSER_NAME,
    TRY_TO_NUMERIC(f.CONTRACT_VALUE)               AS CONTRACTED_VALUE,
    COALESCE(TRY_TO_DATE(f.DATE_OF_SALE_RAW), f.ACTIVITY_AT::DATE) AS DATE_OF_SALE,
    f.PROGRAM,
    TRY_TO_NUMERIC(f.CASH_COLLECTED)               AS CASH_COLLECTED
FROM flat f
LEFT JOIN sales_analytics.silver.close_crm_users_processed setter
    ON f.SETTER_ID = setter.USER_ID
LEFT JOIN sales_analytics.silver.close_crm_users_processed closer
    ON f.CLOSER_ID = closer.USER_ID;
"""

# -- 5. OUTBOUND_PROSPECT_DIALS ------------------------------------------------------------
SQL_OUTBOUND_PROSPECT_DIALS = USE_CONTEXT + """
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
"""

# -----------------------------------------------
# DAG
# -----------------------------------------------
default_args = {
    "owner":            "data-engineering",
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="silver_to_gold",
    description="Build/refresh Gold layer views in Snowflake from Silver layer tables",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,  # triggered by bronze_to_silver
    catchup=False,
    max_active_runs=1,
    tags=["gold", "snowflake", "views"],
) as dag:

    # -- All 5 views read independently from Silver -- run in parallel --
    build_inbound_strategies_booked = SQLExecuteQueryOperator(
        task_id="build_inbound_strategies_booked",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_INBOUND_STRATEGIES_BOOKED,
    )

    build_outbound_strategies_booked = SQLExecuteQueryOperator(
        task_id="build_outbound_strategies_booked",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_OUTBOUND_STRATEGIES_BOOKED,
    )

    build_all_strategies_details = SQLExecuteQueryOperator(
        task_id="build_all_strategies_details",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_ALL_STRATEGIES_DETAILS,
    )

    build_sales_details = SQLExecuteQueryOperator(
        task_id="build_sales_details",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_SALES_DETAILS,
    )

    build_outbound_prospect_dials = SQLExecuteQueryOperator(
        task_id="build_outbound_prospect_dials",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_OUTBOUND_PROSPECT_DIALS,
    )

    # -- Trigger Export/Reports layer --
    trigger_export = TriggerDagRunOperator(
        task_id="trigger_gold_to_export",
        trigger_dag_id="gold_to_export",
        wait_for_completion=False,
    )

    # -- Dependencies -- all 5 views run in parallel, no inter-dependencies --
    [
        build_inbound_strategies_booked,
        build_outbound_strategies_booked,
        build_all_strategies_details,
        build_sales_details,
        build_outbound_prospect_dials,
    ] >> trigger_export