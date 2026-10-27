"""
DAG: bronze_to_silver

Transforms Bronze layer tables into Silver layer in Snowflake.

CHANGE LOG (this version — Batch 1 rebuild):
  - Dedup key: MERGE ON / ROW_NUMBER PARTITION BY changed from ACTIVITY_ID
    alone to the composite (ACTIVITY_ID, LEAD_ID) for the three tables that
    carry real activity records (LEAD_ACTIVITIES_PROCESSED,
    CUSTOM_ACTIVITIES_ALL_LEADS_DETAILS, LEAD_ACTIVITIES_EMAIL), matching
    the requirements doc's stated key. LEADS_ACTIVITIES_SUMMARY keeps its
    existing (ACTIVITY_ID, CUSTOM_ACTIVITY_OUTCOME_NAME) key unchanged —
    that's its genuine natural key at the fan-out grain, not an instance of
    this bug. CLOSE_CRM_USERS_PROCESSED and CUSTOM_ACTIVITIES are dimension/
    metadata tables, not activity records, so also unchanged.
  - "Latest wins" tiebreak: ROW_NUMBER ORDER BY changed from INSERT_DATE
    (pipeline load time) to ACTIVITY_AT (the CRM's own event time) for the
    same three activity-record tables, per the requirements doc's explicit
    "keep the latest activity record using: activity_at" instruction.
  - LEAD_ACTIVITIES_PROCESSED is now SCD Type 2: a hash change closes out
    the existing current row (IS_CURRENT=FALSE, EFFECTIVE_END_DATE set)
    instead of overwriting it, and inserts a new current row. Downstream
    consumers that want current-state-only must filter WHERE IS_CURRENT.
    Requires the updated DDL in silver_ddl_updates.sql.
  - LEAD_ACTIVITIES_EMAIL: added MD5_HASH + a real WHEN MATCHED branch.
    Previously only had WHEN NOT MATCHED THEN INSERT — an email row was
    never updated after first load (e.g. HAS_REPLY flipping true was
    silently lost). Requires the updated DDL in silver_ddl_updates.sql.
  - Task dependency fix: merge_email no longer depends on merge_lap.
    SQL_EMAIL reads directly from bronze.lead_activites_raw — it never
    touched LAP's transient or merged output, so the dependency was never
    real; it just inherited unnecessary blast radius from sharing a DAG.
    merge_email now runs in the first parallel batch alongside
    transform_users / transform_custom_activities / transform_custom_all_leads.

Transformations:
  1. close_crm_users_processed          → from bronze.close_crm_users_raw
  2. custom_activities                   → from bronze.custom_activites_raw
  3. custom_activities_all_leads_details → from bronze.lead_activites_raw
  4. lead_activities_processed           → transient → merge (SCD2, from bronze.lead_activites_raw)
  5. lead_activities_email               → from bronze.lead_activites_raw (independent, parallel)
  6. leads_activities_summary            → transient → merge (from steps 1-4)

Dependency order:
  [users, custom_activities, custom_all_leads, email]  ← run in parallel
            ↓ (only users/custom_activities/custom_all_leads feed this)
  [lead_activities_processed_transient]
            ↓
  [lead_activities_processed_merge]
            ↓
  [leads_activities_summary_transient]  ← waits for users + custom + LAP merge
            ↓
  [leads_activities_summary_merge]

Schedule: Triggered by s3_to_snowflake_bronze DAG on success

CHANGE LOG (2026-10) -- INCREMENTAL SILVER:
  Every Silver step used to flatten ALL of Bronze history on every run. Bronze
  receives one batch per table per day and each daily extract restates the full
  history of every lead it touches, so a step only needs the batches since the
  last fully successful run.
  - init_run plans the window per Bronze source in silver.etl_watermark;
    windowed steps read it back; commit_watermark advances it only after every
    step succeeded (a failed run is re-read next time). First run = FULL.
  - Params (trigger with config): full_refresh=true rebuilds from all of Bronze;
    since_date=YYYY-MM-DD reprocesses from a date (backfill); lookback_days
    (default 1) re-reads that many days before the watermark as a safety overlap.
  - LAS rebuilds only the activities (re)written since the last good run, or
    everything when the custom-activity definitions changed.
  - Dedup tiebreak: the newest Bronze batch now wins a tie on activity_at
    (before, ties were arbitrary).
  - Dimension tables (users, custom_activities) stay full: they are tiny.
  - Quarantine life cycle (quarantine.py): every quarantined row ends RESOLVED as
    REPLAYED / SUPERSEDED / ACCEPTED_LOSS; trigger with reprocess_quarantine=true to
    replay open rows after fixing the cause. See docs/QUARANTINE_RUNBOOK.md.
  See silver_incremental.py, tests/test_silver_incremental*.py and
  docs/INCREMENTAL_SILVER_RUNBOOK.md.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
try:                                    # Airflow 3
    from airflow.sdk import Param
except ImportError:                     # Airflow 2
    from airflow.models.param import Param

from silver_incremental import (
    DDL_WATERMARK, SOURCES, WATERMARK_TABLE, apply_tokens, compute_window,
    transaction_batch)
import alerts
import quarantine as qr
from alerts import notify_failure

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_default"
DATABASE          = "sales_analytics"
WAREHOUSE         = "sales_wh"
BRONZE            = "bronze"
SILVER            = "silver"

USE_CONTEXT = f"""
    USE WAREHOUSE {WAREHOUSE};
    USE DATABASE {DATABASE};
    USE SCHEMA {SILVER};
"""

# ── Data quality settings (Great Expectations at Silver) ─────────────────
# Rows that break a constraint are quarantined (silver.dq_rejected_rows) and
# never reach the MERGE. If the share of rejected rows reaches the threshold
# the run fails, Gold is not triggered, and an email goes out.
DQ_FAIL_THRESHOLD_PCT = 10.0          # >= this % rejected -> stop the pipeline
# Alerting (SNS topic first, SMTP as the backup) lives in alerts.py, shared with the
# failure callback of all five DAGs.
DQ_GX_ROLE_DEFAULT   = "PIPELINE_TRANSFORMER"

LAP_KNOWN_TYPES = ("Call", "SMS", "Email", "Note", "Meeting", "Created",
                   "CustomActivity", "LeadMerge")
_lap_types_sql = ", ".join(f"'{t}'" for t in LAP_KNOWN_TYPES)

# ONE definition of a "good" lead-activity row, used by the MERGE source
# (keeps good rows) and by the quarantine step (keeps the opposite).
# COALESCE(..., FALSE) so a NULL comparison counts as a rejected row.
LAP_GOOD_ROW_FILTER = f"""COALESCE(
                    act.value:id::VARCHAR IS NOT NULL
                    AND act.value:lead_id::VARCHAR IS NOT NULL
                    AND act.value:activity_at::TIMESTAMP_NTZ IS NOT NULL
                    AND act.value:_type::VARCHAR IN ({_lap_types_sql}),
                    FALSE)"""

# ─────────────────────────────────────────────
# SQL DEFINITIONS
# ─────────────────────────────────────────────

# ── 1. CLOSE_CRM_USERS_PROCESSED — windowed to new Bronze batches ─────────────────────────────
SQL_USERS = USE_CONTEXT + """
MERGE INTO sales_analytics.silver.close_crm_users_processed AS target
USING (
    SELECT
        USER_ID, EMAIL, FIRST_NAME, LAST_NAME,
        DATE_CREATED, DATE_UPDATED, INSERT_DATE, MD5_HASH
    FROM (
        SELECT
            u.value:id::VARCHAR                  AS USER_ID,
            u.value:email::VARCHAR               AS EMAIL,
            u.value:first_name::VARCHAR          AS FIRST_NAME,
            u.value:last_name::VARCHAR           AS LAST_NAME,
            u.value:date_created::TIMESTAMP_NTZ  AS DATE_CREATED,
            u.value:date_updated::TIMESTAMP_NTZ  AS DATE_UPDATED,
            src.LOAD_DATE                        AS INSERT_DATE,
            MD5(
                CONCAT(
                    COALESCE(u.value:id::VARCHAR,           ''),
                    COALESCE(u.value:email::VARCHAR,        ''),
                    COALESCE(u.value:first_name::VARCHAR,   ''),
                    COALESCE(u.value:last_name::VARCHAR,    ''),
                    COALESCE(u.value:date_updated::VARCHAR, '')
                )
            ) AS MD5_HASH,
            ROW_NUMBER() OVER (
                PARTITION BY u.value:id::VARCHAR
                ORDER BY u.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,
                         src.LOAD_DATE DESC,
                         MD5(u.value::VARCHAR)
            ) AS rn
        FROM sales_analytics.bronze.close_crm_users_raw src,
        LATERAL FLATTEN(
            input => TRY_PARSE_JSON(
                sales_analytics.silver.repair_json_users(src.JSON_OBJECT)
            ):data
        ) u
        WHERE TRY_PARSE_JSON(
            sales_analytics.silver.repair_json_users(src.JSON_OBJECT)
        ) IS NOT NULL
          AND __WIN__
    )
    WHERE rn = 1
) AS source
ON target.USER_ID = source.USER_ID

WHEN MATCHED AND target.MD5_HASH <> source.MD5_HASH THEN
    UPDATE SET
        target.EMAIL        = source.EMAIL,
        target.FIRST_NAME   = source.FIRST_NAME,
        target.LAST_NAME    = source.LAST_NAME,
        target.DATE_CREATED = source.DATE_CREATED,
        target.DATE_UPDATED = source.DATE_UPDATED,
        target.MD5_HASH     = source.MD5_HASH,
        target.UPDATE_DATE  = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        USER_ID, EMAIL, FIRST_NAME, LAST_NAME,
        MD5_HASH, DATE_CREATED, DATE_UPDATED,
        INSERT_DATE, UPDATE_DATE
    )
    VALUES (
        source.USER_ID, source.EMAIL, source.FIRST_NAME, source.LAST_NAME,
        source.MD5_HASH, source.DATE_CREATED, source.DATE_UPDATED,
        source.INSERT_DATE, CURRENT_TIMESTAMP()
    );
"""

# ── 2. CUSTOM_ACTIVITIES — unchanged ──────────────────────────────────────
SQL_CUSTOM_ACTIVITIES = USE_CONTEXT + """
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
"""

# ── 3. CUSTOM_ACTIVITIES_ALL_LEADS_DETAILS — composite key + activity_at tiebreak
SQL_CUSTOM_ALL_LEADS = USE_CONTEXT + """
MERGE INTO sales_analytics.silver.custom_activities_all_leads_details AS target
USING (
    SELECT
        LEAD_ID, ACTIVITY_ID, ACTIVITY_AT,
        WRAPPED_ACTIVITIES, INSERT_DATE, MD5_HASH
    FROM (
        SELECT
            act.value:lead_id::VARCHAR              AS LEAD_ID,
            act.value:id::VARCHAR                   AS ACTIVITY_ID,
            act.value:activity_at::TIMESTAMP_NTZ    AS ACTIVITY_AT,
            act.value                               AS WRAPPED_ACTIVITIES,
            src.LOAD_DATE::TIMESTAMP_NTZ            AS INSERT_DATE,
            MD5(
                CONCAT(
                    COALESCE(act.value:id::VARCHAR,           ''),
                    COALESCE(act.value:lead_id::VARCHAR,      ''),
                    COALESCE(act.value:activity_at::VARCHAR,  ''),
                    COALESCE(act.value:date_updated::VARCHAR, '')
                )
            ) AS MD5_HASH,
            -- Composite key (ACTIVITY_ID, LEAD_ID); tiebreak on the
            -- activity's own event time, not pipeline load time.
            ROW_NUMBER() OVER (
                PARTITION BY act.value:id::VARCHAR, act.value:lead_id::VARCHAR
                -- Latest VERSION wins: newest date_updated, then newest batch,
                -- then event time. (Event time first picked stale versions when
                -- the CRM later changed activity_at.)
                ORDER BY act.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,
                         src.LOAD_DATE DESC,
                         act.value:activity_at::TIMESTAMP_NTZ DESC
            ) AS rn
        FROM sales_analytics.bronze.lead_activites_raw src,
        LATERAL FLATTEN(input => PARSE_JSON(src.JSON_OBJECT):data) act
        WHERE act.value:_type::VARCHAR = 'CustomActivity'
          AND __WIN__
    )
    WHERE rn = 1
) AS source
ON  target.ACTIVITY_ID = source.ACTIVITY_ID
AND target.LEAD_ID     = source.LEAD_ID

WHEN MATCHED AND target.MD5_HASH <> source.MD5_HASH THEN
    UPDATE SET
        target.ACTIVITY_AT        = source.ACTIVITY_AT,
        target.WRAPPED_ACTIVITIES = source.WRAPPED_ACTIVITIES,
        target.MD5_HASH           = source.MD5_HASH,
        target.UPDATE_DATE        = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        LEAD_ID, ACTIVITY_ID, ACTIVITY_AT,
        WRAPPED_ACTIVITIES, MD5_HASH,
        INSERT_DATE, UPDATE_DATE
    )
    VALUES (
        source.LEAD_ID, source.ACTIVITY_ID, source.ACTIVITY_AT,
        source.WRAPPED_ACTIVITIES, source.MD5_HASH,
        source.INSERT_DATE, CURRENT_TIMESTAMP()
    );
"""

# ── 4a. LEAD_ACTIVITIES_PROCESSED — transient load (unchanged) ───────────
SQL_LAP_TRANSIENT = USE_CONTEXT + """
INSERT OVERWRITE INTO sales_analytics.silver.lead_activities_processed_transient
SELECT
    PARSE_JSON(JSON_OBJECT)  AS JSON_OBJECT,
    LOAD_DATE::TIMESTAMP_NTZ AS INSERT_DATE
FROM sales_analytics.bronze.lead_activites_raw
WHERE __WIN__;
"""

# ── 4b. LEAD_ACTIVITIES_PROCESSED — SCD2 merge (two-step) ────────────────
# Step 1: close out any current row whose hash changed.
# Step 2: insert a fresh current row for anything new or just-closed-out.
# Snowflake MERGE can't do "update old + insert new" for the same matched
# key in one statement, so SCD2 is implemented as two statements sharing
# the same source CTE logic, run back to back in the same task.
SQL_LAP_SOURCE_SUBQUERY = """
(
    WITH source_rows AS (
        SELECT
            ACTIVITY_ID, LEAD_ID, CONTACT_ID, USER_ID, TYPE, TEXT,
            STATUS_CHANGE, DIRECTION, CREATED_BY_NAME, UPDATED_BY_NAME,
            ATTENDEES, CUSTOM_ACTIVITY_ID, MEETING_STARTS_AT, MEETING_END_AT,
            MEETING_STATUS, ACTIVITY_AT, DATE_CREATED, DATE_UPDATED,
            CREATED_BY_USER, UPDATED_BY_USER, INSERT_DATE, MD5_HASH
        FROM (
            SELECT
                act.value:id::VARCHAR                      AS ACTIVITY_ID,
                act.value:lead_id::VARCHAR                 AS LEAD_ID,
                act.value:contact_id::VARCHAR              AS CONTACT_ID,
                act.value:user_id::VARCHAR                 AS USER_ID,
                act.value:_type::VARCHAR                   AS TYPE,
                COALESCE(
                    act.value:note::VARCHAR,
                    act.value:text::VARCHAR
                )                                          AS TEXT,
                COALESCE(
                    act.value:disposition::VARCHAR,
                    act.value:merge_status::VARCHAR,
                    act.value:status::VARCHAR
                )                                          AS STATUS_CHANGE,
                act.value:direction::VARCHAR               AS DIRECTION,
                act.value:created_by_name::VARCHAR         AS CREATED_BY_NAME,
                act.value:updated_by_name::VARCHAR         AS UPDATED_BY_NAME,
                act.value:attendees::VARCHAR               AS ATTENDEES,
                act.value:custom_activity_type_id::VARCHAR AS CUSTOM_ACTIVITY_ID,
                act.value:starts_at::TIMESTAMP_NTZ         AS MEETING_STARTS_AT,
                act.value:ends_at::TIMESTAMP_NTZ           AS MEETING_END_AT,
                act.value:status::VARCHAR                  AS MEETING_STATUS,
                act.value:activity_at::TIMESTAMP_NTZ       AS ACTIVITY_AT,
                act.value:date_created::TIMESTAMP_NTZ      AS DATE_CREATED,
                act.value:date_updated::TIMESTAMP_NTZ      AS DATE_UPDATED,
                act.value:created_by::VARCHAR              AS CREATED_BY_USER,
                act.value:updated_by::VARCHAR              AS UPDATED_BY_USER,
                src.INSERT_DATE                            AS INSERT_DATE,
                MD5(
                    CONCAT(
                        COALESCE(act.value:id::VARCHAR,           ''),
                        COALESCE(act.value:lead_id::VARCHAR,      ''),
                        COALESCE(act.value:activity_at::VARCHAR,  ''),
                        COALESCE(act.value:date_updated::VARCHAR, ''),
                        COALESCE(act.value:disposition::VARCHAR,  ''),
                        COALESCE(act.value:merge_status::VARCHAR, ''),
                        COALESCE(act.value:status::VARCHAR,       ''),
                        COALESCE(act.value:direction::VARCHAR,    '')
                    )
                )                                          AS MD5_HASH,
                -- Composite key (ACTIVITY_ID, LEAD_ID); tiebreak on the
                -- activity's own event time, not pipeline load time.
                ROW_NUMBER() OVER (
                    PARTITION BY act.value:id::VARCHAR, act.value:lead_id::VARCHAR
                    -- Latest VERSION wins (see CALD/EMAIL): date_updated, batch, event time.
                    ORDER BY act.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,
                             src.INSERT_DATE DESC,
                             act.value:activity_at::TIMESTAMP_NTZ DESC
                ) AS rn
            FROM sales_analytics.silver.lead_activities_processed_transient src,
            LATERAL FLATTEN(input => src.JSON_OBJECT:data) act
            -- Great Expectations constraints: only good rows reach the MERGE;
            -- the rest are quarantined by validate_lap_staging.
            WHERE """ + LAP_GOOD_ROW_FILTER + """
        )
        WHERE rn = 1
    )
    SELECT * FROM source_rows
)
"""
# NOTE: this is a derived-table expression (parenthesized, ending in a
# plain SELECT), not a bare "WITH ... " prefix — Snowflake does not
# support a top-level WITH clause leading directly into UPDATE or
# INSERT (only into SELECT, or embedded as a subquery as done here).
# Interpolate this directly after FROM/USING, always followed by "AS source".

SQL_LAP_CLOSE_OUT = USE_CONTEXT + """
UPDATE sales_analytics.silver.lead_activities_processed AS target
SET
    IS_CURRENT         = FALSE,
    EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(),
    UPDATE_DATE         = CURRENT_TIMESTAMP()
FROM """ + SQL_LAP_SOURCE_SUBQUERY + """ AS source
WHERE target.ACTIVITY_ID = source.ACTIVITY_ID
  AND target.LEAD_ID     = source.LEAD_ID
  AND target.IS_CURRENT  = TRUE
  AND target.MD5_HASH   <> source.MD5_HASH;
"""

SQL_LAP_INSERT_NEW = USE_CONTEXT + """
INSERT INTO sales_analytics.silver.lead_activities_processed (
    ACTIVITY_ID, LEAD_ID, CONTACT_ID, USER_ID, TYPE, TEXT,
    STATUS_CHANGE, DIRECTION, CREATED_BY_NAME, UPDATED_BY_NAME,
    ATTENDEES, CUSTOM_ACTIVITY_ID, MEETING_STARTS_AT, MEETING_END_AT,
    MEETING_STATUS, ACTIVITY_AT, DATE_CREATED, DATE_UPDATED,
    CREATED_BY_USER, UPDATED_BY_USER,
    INSERT_DATE, UPDATE_DATE, MD5_HASH,
    EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT
)
SELECT
    source.ACTIVITY_ID, source.LEAD_ID, source.CONTACT_ID, source.USER_ID,
    source.TYPE, source.TEXT, source.STATUS_CHANGE, source.DIRECTION,
    source.CREATED_BY_NAME, source.UPDATED_BY_NAME,
    source.ATTENDEES, source.CUSTOM_ACTIVITY_ID,
    source.MEETING_STARTS_AT, source.MEETING_END_AT, source.MEETING_STATUS,
    source.ACTIVITY_AT, source.DATE_CREATED, source.DATE_UPDATED,
    source.CREATED_BY_USER, source.UPDATED_BY_USER,
    source.INSERT_DATE, CURRENT_TIMESTAMP(), source.MD5_HASH,
    CURRENT_TIMESTAMP(), NULL, TRUE
FROM """ + SQL_LAP_SOURCE_SUBQUERY + """ AS source
LEFT JOIN sales_analytics.silver.lead_activities_processed AS target
    ON  target.ACTIVITY_ID = source.ACTIVITY_ID
    AND target.LEAD_ID     = source.LEAD_ID
    AND target.IS_CURRENT  = TRUE
-- No current row exists yet: either a brand-new key, or one just
-- closed out by SQL_LAP_CLOSE_OUT above.
WHERE target.ACTIVITY_ID IS NULL;
"""

# ── 5. LEAD_ACTIVITIES_EMAIL — composite key, activity_at tiebreak, real upsert
# ── 6. LEADS_PROCESSED — new (2026-09), Closer Report LEAD_ROUTE rebuild ──
# 2026-10: CREATE OR REPLACE -> CREATE TABLE IF NOT EXISTS. The table used to be
# dropped and rebuilt on every run, so the MERGE always saw an empty target (all
# rows inserted, never updated) and UPDATE_DATE could never move. It is now a
# persistent table: the MERGE only changes rows whose MD5_HASH changed.
# Pulls all meaningful FLAT scalar fields from bronze.leads_raw. Deliberately
# excludes tasks[]/contacts[]/opportunities[] (each needs its own fan-out
# table, same class of work as the CustomActivity pivot — not needed by any
# current report) and ad-tracking noise (handl_*/utm_* fields). LEAD_ROUTE
# values kept as raw codes ('1'/'2'/'3'/'NO_ROUTE'), matching the business stakeholder's own
# report format exactly — meaning of the codes is an open question with him,
# not needed to build or validate this table. See decisions-and-learnings.md.
# Independent of every other Silver table — reads only from bronze.leads_raw.
SQL_LEADS_PROCESSED = USE_CONTEXT + """
CREATE TABLE IF NOT EXISTS silver.leads_processed (
    LEAD_ID              VARCHAR,
    STATUS_LABEL         VARCHAR,
    LEAD_ROUTE           VARCHAR,
    AVATAR               VARCHAR,
    FUNNEL               VARCHAR,
    SETTER_ID            VARCHAR,
    LEAD_OWNER_ID        VARCHAR,
    LEAD_QUALITY         VARCHAR,
    LEAD_INTENT          VARCHAR,
    VISA_STATUS          VARCHAR,
    CURRENT_SALARY       VARCHAR,
    DATE_CREATED         TIMESTAMP_NTZ,
    DATE_UPDATED         TIMESTAMP_NTZ,
    INSERT_DATE          TIMESTAMP_NTZ,
    UPDATE_DATE          TIMESTAMP_NTZ,
    MD5_HASH             VARCHAR
);

MERGE INTO sales_analytics.silver.leads_processed AS target
USING (
    SELECT
        LEAD_ID, STATUS_LABEL, LEAD_ROUTE, AVATAR, FUNNEL, SETTER_ID,
        LEAD_OWNER_ID, LEAD_QUALITY, LEAD_INTENT, VISA_STATUS,
        CURRENT_SALARY, DATE_CREATED, DATE_UPDATED, INSERT_DATE, MD5_HASH
    FROM (
        SELECT
            j:id::VARCHAR                                       AS LEAD_ID,
            j:status_label::VARCHAR                             AS STATUS_LABEL,
            COALESCE(j:custom:lead_route::VARCHAR, 'NO_ROUTE')  AS LEAD_ROUTE,
            j:custom:Avatar::VARCHAR                            AS AVATAR,
            j:custom:Funnel::VARCHAR                            AS FUNNEL,
            j:custom:Setter::VARCHAR                            AS SETTER_ID,
            j:custom:"Lead Owner"::VARCHAR                      AS LEAD_OWNER_ID,
            j:custom:"Lead Quality"::VARCHAR                    AS LEAD_QUALITY,
            j:custom:"Lead Intent"::VARCHAR                     AS LEAD_INTENT,
            j:custom:"Visa Status"::VARCHAR                     AS VISA_STATUS,
            j:custom:"Current Salary"::VARCHAR                  AS CURRENT_SALARY,
            j:date_created::TIMESTAMP_NTZ                       AS DATE_CREATED,
            j:date_updated::TIMESTAMP_NTZ                       AS DATE_UPDATED,
            src.LOAD_DATE::TIMESTAMP_NTZ                        AS INSERT_DATE,
            MD5(
                CONCAT(
                    COALESCE(j:id::VARCHAR,               ''),
                    COALESCE(j:date_updated::VARCHAR,     ''),
                    COALESCE(j:status_label::VARCHAR,     ''),
                    COALESCE(j:custom:lead_route::VARCHAR, '')
                )
            )                                                    AS MD5_HASH,
            ROW_NUMBER() OVER (
                PARTITION BY j:id::VARCHAR
                -- Latest VERSION wins: newest date_updated, then newest batch.
                ORDER BY j:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,
                         src.LOAD_DATE DESC
            ) AS rn
        FROM sales_analytics.bronze.leads_raw src,
             LATERAL (SELECT PARSE_JSON(src.JSON_OBJECT) AS j)
        WHERE __WIN__
    )
    WHERE rn = 1
) AS source
ON target.LEAD_ID = source.LEAD_ID
WHEN MATCHED AND target.MD5_HASH <> source.MD5_HASH THEN
    UPDATE SET
        target.STATUS_LABEL   = source.STATUS_LABEL,
        target.LEAD_ROUTE     = source.LEAD_ROUTE,
        target.AVATAR         = source.AVATAR,
        target.FUNNEL         = source.FUNNEL,
        target.SETTER_ID      = source.SETTER_ID,
        target.LEAD_OWNER_ID  = source.LEAD_OWNER_ID,
        target.LEAD_QUALITY   = source.LEAD_QUALITY,
        target.LEAD_INTENT    = source.LEAD_INTENT,
        target.VISA_STATUS    = source.VISA_STATUS,
        target.CURRENT_SALARY = source.CURRENT_SALARY,
        target.DATE_UPDATED   = source.DATE_UPDATED,
        target.MD5_HASH       = source.MD5_HASH,
        target.UPDATE_DATE    = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        LEAD_ID, STATUS_LABEL, LEAD_ROUTE, AVATAR, FUNNEL, SETTER_ID,
        LEAD_OWNER_ID, LEAD_QUALITY, LEAD_INTENT, VISA_STATUS,
        CURRENT_SALARY, DATE_CREATED, DATE_UPDATED, INSERT_DATE,
        UPDATE_DATE, MD5_HASH
    )
    VALUES (
        source.LEAD_ID, source.STATUS_LABEL, source.LEAD_ROUTE, source.AVATAR,
        source.FUNNEL, source.SETTER_ID, source.LEAD_OWNER_ID, source.LEAD_QUALITY,
        source.LEAD_INTENT, source.VISA_STATUS, source.CURRENT_SALARY,
        source.DATE_CREATED, source.DATE_UPDATED, source.INSERT_DATE,
        CURRENT_TIMESTAMP(), source.MD5_HASH
    );
"""

SQL_EMAIL = USE_CONTEXT + """
MERGE INTO sales_analytics.silver.lead_activities_email AS target
USING (
    SELECT
        LEAD_ID, ACTIVITY_ID, ACTIVITY_AT, ATTACHMENTS_JSON, BODY_TEXT,
        DATE_CREATED, DATE_UPDATED, DIRECTION, ENVELOPE_JSON, SENDER_JSON,
        OPENS_JSON, SENDER, STATUS, SUBJECT, JSON_PAYLOAD, USER_ID,
        USER_NAME, TEMPLATE_ID, TEMPLATE_NAME, DATE_SENT, HAS_REPLY,
        INSERT_DATE, MD5_HASH
    FROM (
        SELECT
            act.value:lead_id::VARCHAR                AS LEAD_ID,
            act.value:id::VARCHAR                      AS ACTIVITY_ID,
            act.value:activity_at::TIMESTAMP_NTZ       AS ACTIVITY_AT,
            TO_VARCHAR(act.value:attachments)           AS ATTACHMENTS_JSON,
            act.value:body_text::VARCHAR                AS BODY_TEXT,
            act.value:date_created::TIMESTAMP_NTZ       AS DATE_CREATED,
            act.value:date_updated::TIMESTAMP_NTZ       AS DATE_UPDATED,
            act.value:direction::VARCHAR                AS DIRECTION,
            TO_VARCHAR(act.value:envelope)               AS ENVELOPE_JSON,
            TO_VARCHAR(act.value:envelope:from)          AS SENDER_JSON,
            TO_VARCHAR(act.value:opens)                  AS OPENS_JSON,
            act.value:envelope:from[0]:email::VARCHAR    AS SENDER,
            act.value:status::VARCHAR                    AS STATUS,
            act.value:envelope:subject::VARCHAR          AS SUBJECT,
            act.value                                    AS JSON_PAYLOAD,
            act.value:user_id::VARCHAR                   AS USER_ID,
            act.value:user_name::VARCHAR                 AS USER_NAME,
            act.value:template_id::VARCHAR               AS TEMPLATE_ID,
            act.value:template_name::VARCHAR             AS TEMPLATE_NAME,
            act.value:date_sent::TIMESTAMP_NTZ           AS DATE_SENT,
            act.value:has_reply::BOOLEAN                 AS HAS_REPLY,
            src.LOAD_DATE::TIMESTAMP_NTZ                 AS INSERT_DATE,
            MD5(
                CONCAT(
                    COALESCE(act.value:id::VARCHAR,            ''),
                    COALESCE(act.value:lead_id::VARCHAR,       ''),
                    COALESCE(act.value:activity_at::VARCHAR,   ''),
                    COALESCE(act.value:date_updated::VARCHAR,  ''),
                    COALESCE(act.value:status::VARCHAR,        ''),
                    COALESCE(act.value:has_reply::VARCHAR,     '')
                )
            ) AS MD5_HASH,
            -- Composite key (ACTIVITY_ID, LEAD_ID); tiebreak on the
            -- activity's own event time, not pipeline load time.
            ROW_NUMBER() OVER (
                PARTITION BY act.value:id::VARCHAR, act.value:lead_id::VARCHAR
                -- Latest VERSION wins: newest date_updated, then newest batch,
                -- then event time. (Event time first picked stale versions when
                -- the CRM later changed activity_at.)
                ORDER BY act.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,
                         src.LOAD_DATE DESC,
                         act.value:activity_at::TIMESTAMP_NTZ DESC
            ) AS rn
        FROM sales_analytics.bronze.lead_activites_raw src,
        LATERAL FLATTEN(input => PARSE_JSON(src.JSON_OBJECT):data) act
        WHERE act.value:_type::VARCHAR = 'Email'
          AND __WIN__
    )
    WHERE rn = 1
) AS source
ON  target.ACTIVITY_ID = source.ACTIVITY_ID
AND target.LEAD_ID     = source.LEAD_ID

WHEN MATCHED AND target.MD5_HASH <> source.MD5_HASH THEN
    UPDATE SET
        target.ACTIVITY_AT        = source.ACTIVITY_AT,
        target.ATTACHMENTS_JSON   = source.ATTACHMENTS_JSON,
        target.BODY_TEXT          = source.BODY_TEXT,
        target.DATE_CREATED       = source.DATE_CREATED,
        target.DATE_UPDATED       = source.DATE_UPDATED,
        target.DIRECTION          = source.DIRECTION,
        target.ENVELOPE_JSON      = source.ENVELOPE_JSON,
        target.SENDER_JSON        = source.SENDER_JSON,
        target.OPENS_JSON         = source.OPENS_JSON,
        target.SENDER             = source.SENDER,
        target.STATUS             = source.STATUS,
        target.SUBJECT            = source.SUBJECT,
        target.JSON_PAYLOAD       = source.JSON_PAYLOAD,
        target.USER_ID            = source.USER_ID,
        target.USER_NAME          = source.USER_NAME,
        target.TEMPLATE_ID        = source.TEMPLATE_ID,
        target.TEMPLATE_NAME      = source.TEMPLATE_NAME,
        target.DATE_SENT          = source.DATE_SENT,
        target.HAS_REPLY          = source.HAS_REPLY,
        target.MD5_HASH           = source.MD5_HASH,
        target.UPDATE_DATE        = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        LEAD_ID, ACTIVITY_ID, ACTIVITY_AT, ATTACHMENTS_JSON, BODY_TEXT,
        DATE_CREATED, DATE_UPDATED, DIRECTION, ENVELOPE_JSON,
        SENDER_JSON, OPENS_JSON, SENDER, STATUS,
        SUBJECT, JSON_PAYLOAD, USER_ID, USER_NAME, TEMPLATE_ID,
        TEMPLATE_NAME, DATE_SENT, HAS_REPLY, INSERT_DATE, UPDATE_DATE, MD5_HASH
    )
    VALUES (
        source.LEAD_ID, source.ACTIVITY_ID, source.ACTIVITY_AT,
        source.ATTACHMENTS_JSON, source.BODY_TEXT, source.DATE_CREATED,
        source.DATE_UPDATED, source.DIRECTION, source.ENVELOPE_JSON,
        source.SENDER_JSON, source.OPENS_JSON, source.SENDER,
        source.STATUS, source.SUBJECT, source.JSON_PAYLOAD,
        source.USER_ID, source.USER_NAME, source.TEMPLATE_ID,
        source.TEMPLATE_NAME, source.DATE_SENT, source.HAS_REPLY,
        source.INSERT_DATE, CURRENT_TIMESTAMP(), source.MD5_HASH
    );
"""

# ── 6a. LEADS_ACTIVITIES_SUMMARY — transient load ────────────────────────
# Reads only current-state LAP rows (IS_CURRENT = TRUE) now that LAP is SCD2.
SQL_LAS_TRANSIENT = USE_CONTEXT + """
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
            COALESCE(lap.LEAD_ID,                  ''),   -- Fix: added — same ACTIVITY_ID can legitimately belong to multiple leads in this dataset (1,313 confirmed cases); without LEAD_ID here, two different leads' custom-field rows for the "same" activity_id+field collapse into one hash/key incorrectly.
            COALESCE(lap.DATE_UPDATED::VARCHAR,    ''),
            COALESCE(f.cf_field_name,              ''),
            COALESCE(f.cf_field_value,             '')
        )
    )                                      AS MD5_HASH,
    cald.WRAPPED_ACTIVITIES,
    ca_name.CUSTOM_ACTIVITY_NAME           AS CUSTOM_ACTIVITY,
    f.cf_field_name                        AS CUSTOM_ACTIVITY_OUTCOME_NAME,
    f.cf_field_value                       AS CUSTOM_ACTIVITY_OUTCOME,
    COALESCE(u.FIRST_NAME || ' ' || u.LAST_NAME, u.EMAIL) AS DEA_INTERNAL_NAME,
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
WHERE lap.IS_CURRENT = TRUE
  AND __LAS_SCOPE__;
"""

# ── 6b. LEADS_ACTIVITIES_SUMMARY — merge (unchanged key) ─────────────────
SQL_LAS_MERGE = USE_CONTEXT + """
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
"""

# ── 6c. LAS — re-sync user identity from the users dimension ─────────────
# WHY THIS EXISTS (found 2026-10, during the Closer Report reconciliation):
# DEA_INTERNAL_NAME / DEA_INTERNAL_EMAIL are copied onto each LAS row from
# close_crm_users_processed when the row is loaded (see SQL_LAS_TRANSIENT).
# The MERGE above only rewrites a row when its MD5_HASH changes, and the
# hash deliberately excludes these two columns, so when a user's name
# changes in the CRM, activities already loaded keep the OLD spelling while
# newly loaded ones get the new one. One real person ended up under three
# spellings (same USER_ID, same email). This step re-syncs every row's
# identity from the dimension on each run, so a name change propagates
# immediately and there is exactly one name per person. It only touches
# rows that actually differ, so on a normal day it updates nothing.
SQL_LAS_REFRESH_USER_IDENTITY = USE_CONTEXT + """
UPDATE sales_analytics.silver.leads_activities_summary las
SET DEA_INTERNAL_NAME  = COALESCE(u.FIRST_NAME || ' ' || u.LAST_NAME, u.EMAIL),
    DEA_INTERNAL_EMAIL = u.EMAIL
FROM sales_analytics.silver.close_crm_users_processed u
WHERE las.USER_ID = u.USER_ID
  AND (las.DEA_INTERNAL_NAME  IS DISTINCT FROM COALESCE(u.FIRST_NAME || ' ' || u.LAST_NAME, u.EMAIL)
    OR las.DEA_INTERNAL_EMAIL IS DISTINCT FROM u.EMAIL);
"""

# ── Silver data quality checks ────────────────────────────────
# Runs after merge_las, before trigger_gold. Distinguishes two
# severity levels deliberately:
#   - FAIL (raises, blocks the pipeline): null business keys,
#     duplicate current-state rows, or one email carrying more than one
#     display name (a split identity -- see refresh_las_user_identity) — these should be structurally
#     impossible given the composite-key fixes already in place, so
#     seeing any at all means something upstream broke silently.
#   - WARN (logs, does not block): multi-lead ACTIVITY_ID collisions
#     — these are a known, expected, already-handled pattern on this
#     dataset (1,313 confirmed on the rebuilt environment), not a
#     failure. Tracked as a trend metric, not gated on.
SQL_SILVER_DQ_CHECKS = USE_CONTEXT + """
SELECT
    (SELECT COUNT(*) FROM sales_analytics.silver.lead_activities_processed
     WHERE IS_CURRENT = TRUE
       AND (ACTIVITY_ID IS NULL OR LEAD_ID IS NULL OR ACTIVITY_AT IS NULL)
    ) AS lap_null_keys,
    (SELECT COUNT(*) FROM (
        SELECT ACTIVITY_ID, LEAD_ID, COUNT(*) AS c
        FROM sales_analytics.silver.lead_activities_processed
        WHERE IS_CURRENT = TRUE
        GROUP BY ACTIVITY_ID, LEAD_ID
        HAVING COUNT(*) > 1
    )) AS lap_duplicate_current_rows,
    (SELECT COUNT(*) FROM sales_analytics.silver.leads_activities_summary
     WHERE ACTIVITY_ID IS NULL OR LEAD_ID IS NULL
    ) AS las_null_keys,
    (SELECT COUNT(*) FROM (
        SELECT ACTIVITY_ID, LEAD_ID, CUSTOM_ACTIVITY_OUTCOME_NAME, COUNT(*) AS c
        FROM sales_analytics.silver.leads_activities_summary
        GROUP BY ACTIVITY_ID, LEAD_ID, CUSTOM_ACTIVITY_OUTCOME_NAME
        HAVING COUNT(*) > 1
    )) AS las_duplicate_rows,
    (SELECT COUNT(*) FROM (
        SELECT ACTIVITY_ID
        FROM sales_analytics.silver.lead_activities_processed
        WHERE IS_CURRENT = TRUE
        GROUP BY ACTIVITY_ID
        HAVING COUNT(DISTINCT LEAD_ID) > 1
    )) AS multi_lead_activity_count,
    (SELECT COUNT(*) FROM (
        SELECT DEA_INTERNAL_EMAIL
        FROM sales_analytics.silver.leads_activities_summary
        WHERE DEA_INTERNAL_EMAIL IS NOT NULL
        GROUP BY DEA_INTERNAL_EMAIL
        HAVING COUNT(DISTINCT DEA_INTERNAL_NAME) > 1
    )) AS las_split_identities;
"""


def validate_silver_data_quality(**context):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    row = hook.get_first(SQL_SILVER_DQ_CHECKS)
    (
        lap_null_keys,
        lap_duplicate_current_rows,
        las_null_keys,
        las_duplicate_rows,
        multi_lead_activity_count,
        las_split_identities,
    ) = row

    logging.info(
        "Silver DQ check results | lap_null_keys=%s | lap_duplicate_current_rows=%s "
        "| las_null_keys=%s | las_duplicate_rows=%s | multi_lead_activity_count=%s "
        "| las_split_identities=%s",
        lap_null_keys, lap_duplicate_current_rows,
        las_null_keys, las_duplicate_rows, multi_lead_activity_count,
        las_split_identities,
    )

    # WARN-level: known, expected, already handled by the composite key —
    # log for trending, never fail the pipeline on this alone.
    if multi_lead_activity_count and multi_lead_activity_count > 0:
        logging.warning(
            "%s ACTIVITY_ID(s) shared across multiple LEAD_IDs today — expected "
            "on this dataset, handled by the composite (ACTIVITY_ID, LEAD_ID) key. "
            "Logged for trend tracking only.",
            multi_lead_activity_count,
        )

    # FAIL-level: any of these should be structurally impossible given the
    # existing composite-key/SCD2 fixes. Seeing a nonzero count here means
    # something upstream broke silently and needs a human to look at it.
    failures = []
    if lap_null_keys and lap_null_keys > 0:
        failures.append(f"lap_null_keys={lap_null_keys}")
    if lap_duplicate_current_rows and lap_duplicate_current_rows > 0:
        failures.append(f"lap_duplicate_current_rows={lap_duplicate_current_rows}")
    if las_null_keys and las_null_keys > 0:
        failures.append(f"las_null_keys={las_null_keys}")
    if las_duplicate_rows and las_duplicate_rows > 0:
        failures.append(f"las_duplicate_rows={las_duplicate_rows}")
    if las_split_identities and las_split_identities > 0:
        failures.append(f"las_split_identities={las_split_identities}")

    if failures:
        raise ValueError(
            "Silver data quality check FAILED — one or more conditions that "
            "should be structurally impossible were found: " + ", ".join(failures)
        )


# ─────────────────────────────────────────────
# Great Expectations validation of the Silver staging table
# ─────────────────────────────────────────────
# Runs after the staging load and BEFORE the SCD2 merge. Bad rows are
# quarantined, good rows continue, counts go to silver.dq_run_log, and an
# email is sent on a warning or a threshold breach.

_LAP_FLATTEN_FROM = """
FROM sales_analytics.silver.lead_activities_processed_transient src,
LATERAL FLATTEN(input => src.JSON_OBJECT:data) act
"""

# ONE snapshot of the flattened staging rows of EVERY load in the run's window
# (the daily file plus the look-back overlap, or all of Bronze on a full build).
# The row counts, the Great Expectations checks and the quarantine all read this
# table, so they always describe exactly the same rows (and the JSON is flattened
# only once). It used to keep only the newest load, so in a multi-day run the bad
# rows of older loads were dropped by the merge filter but never quarantined.
SQL_LAP_STAGING_SNAPSHOT = USE_CONTEXT + f"""
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.silver.dq_lap_staging_flat AS
SELECT act.value:id::VARCHAR                 AS ACTIVITY_ID,
       act.value:lead_id::VARCHAR            AS LEAD_ID,
       act.value:_type::VARCHAR              AS TYPE,
       act.value:activity_at::TIMESTAMP_NTZ  AS ACTIVITY_AT,
       src.INSERT_DATE                       AS INSERT_DATE,
       {LAP_GOOD_ROW_FILTER}                 AS IS_GOOD,
       MD5(TO_JSON(act.value))               AS ROW_HASH,
       act.value                             AS RAW_RECORD
{_LAP_FLATTEN_FROM};
"""

SQL_LAP_STAGING_COUNTS = USE_CONTEXT + """
SELECT COUNT(*) AS total_rows,
       COUNT_IF(NOT IS_GOOD) AS rejected_rows
FROM sales_analytics.silver.dq_lap_staging_flat;
"""


def _sql_str(value):
    return "'" + str(value).replace("'", "''") + "'"


def build_quarantine_sql(run_id):
    """Insert each distinct rejected record once (re-runs and repeated daily
    snapshots do not duplicate it)."""
    return USE_CONTEXT + f"""
INSERT INTO sales_analytics.silver.dq_rejected_rows
    (REJECTED_AT, DAG_RUN_ID, SOURCE_TABLE, REJECT_REASON,
     ACTIVITY_ID, LEAD_ID, TYPE, ROW_HASH, RAW_RECORD)
SELECT b.REJECTED_AT, b.DAG_RUN_ID, b.SOURCE_TABLE, b.REJECT_REASON,
       b.ACTIVITY_ID, b.LEAD_ID, b.TYPE, b.ROW_HASH, b.RAW_RECORD
FROM (
    SELECT CURRENT_TIMESTAMP()                 AS REJECTED_AT,
           {_sql_str(run_id)}                  AS DAG_RUN_ID,
           'lead_activities_processed'         AS SOURCE_TABLE,
           {qr.REJECT_REASON_CASE}             AS REJECT_REASON,
           ACTIVITY_ID, LEAD_ID, TYPE, ROW_HASH, RAW_RECORD
    FROM sales_analytics.silver.dq_lap_staging_flat
    WHERE NOT IS_GOOD
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ROW_HASH ORDER BY INSERT_DATE DESC) = 1
) b
LEFT JOIN sales_analytics.silver.dq_rejected_rows q
    ON q.ROW_HASH = b.ROW_HASH
WHERE q.ROW_HASH IS NULL;
"""


def decide_dq_outcome(total_rows, rejected_rows, threshold_pct=DQ_FAIL_THRESHOLD_PCT):
    """Pure function: returns (decision, pct_rejected). PASS / WARN / FAIL."""
    if not total_rows:
        return "FAIL", 100.0
    pct = rejected_rows * 100.0 / total_rows
    if pct >= threshold_pct:
        return "FAIL", pct
    if rejected_rows > 0:
        return "WARN", pct
    return "PASS", pct


def _gx_connection_details(hook):
    conn = hook.get_connection(SNOWFLAKE_CONN_ID)
    extra = conn.extra_dejson
    account = extra.get("account") or (conn.host or "").replace(".snowflakecomputing.com", "")
    if not conn.password:
        raise AirflowFailException(
            "snowflake_default has no password; Great Expectations validation "
            "needs password auth (key-pair auth is not wired up yet)."
        )
    return {
        "account":   account,
        "user":      conn.login,
        "password":  conn.password,
        "database":  DATABASE.upper(),
        "schema":    SILVER.upper(),
        "warehouse": (extra.get("warehouse") or WAREHOUSE).upper(),
        "role":      (extra.get("role") or DQ_GX_ROLE_DEFAULT).upper(),
    }


def run_gx_lap_checks(hook):
    """Run the Great Expectations suite on the staging rows.
    Returns one dict per expectation with the failed-row counts."""
    import great_expectations as gx
    import great_expectations.expectations as gxe

    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.data_sources.add_snowflake(
        name="silver_sf", connection_string=_gx_connection_details(hook))
    asset = ds.add_table_asset(
        name="lap_staging", table_name="dq_lap_staging_flat", schema_name=SILVER.upper())
    batch_def = asset.add_batch_definition_whole_table("whole")

    suite = ctx.suites.add(gx.ExpectationSuite(name="silver_lead_activities_staging"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="ACTIVITY_ID"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="LEAD_ID"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="ACTIVITY_AT"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
        column="TYPE", value_set=list(LAP_KNOWN_TYPES)))

    validation = ctx.validation_definitions.add(
        gx.ValidationDefinition(name="lap_staging_v", data=batch_def, suite=suite))
    result = validation.run(
        result_format={"result_format": "BASIC", "partial_unexpected_count": 0})

    checks = []
    for r in result.results:
        q = r.result
        checks.append({
            "check":  r.expectation_config.type,
            "column": r.expectation_config.kwargs.get("column"),
            "total":  int(q.get("element_count") or 0),
            "failed": int(q.get("unexpected_count") or 0),
            "pct":    float(q.get("unexpected_percent") or 0.0),
            "passed": bool(r.success),
        })
    return checks


def send_dq_email(subject, html_body):
    """SNS first, SMTP as the backup; never lets an alerting problem change the outcome (alerts.py)."""
    alerts.send_alert(subject, html_body)


def fmt_pct(pct):
    """Readable percentage that never rounds a real failure down to 0."""
    if pct == 0:
        return "0%"
    if pct >= 0.01:
        return f"{pct:.2f}%"
    return f"{pct:.7f}".rstrip("0").rstrip(".") + "%"


def build_dq_email(decision, run_id, total, rejected, pct, checks, reasons=None):
    rows = "".join(
        f"<tr><td>{c['column']}</td><td>{c['check']}</td>"
        f"<td align='right'>{c['failed']:,}</td><td align='right'>{fmt_pct(c['pct'])}</td></tr>"
        for c in checks)
    if decision == "FAIL":
        headline = (f"Silver data quality FAILED: {fmt_pct(pct)} of incoming lead activity "
                    f"rows broke constraints (threshold {DQ_FAIL_THRESHOLD_PCT:.0f}%). "
                    "The pipeline was stopped before the Silver merge; Gold was not triggered. "
                    "This usually means something changed in the upstream CRM/Postgres load.")
    else:
        headline = (f"Silver data quality warning: {rejected:,} incoming row(s) ({fmt_pct(pct)}) "
                    "broke constraints and were quarantined. The pipeline continued with the good rows.")
    return f"""
<p>{headline}</p>
<p><b>DAG run:</b> {run_id}<br>
<b>Rows checked:</b> {total:,}<br>
<b>Rows rejected:</b> {rejected:,} ({fmt_pct(pct)})<br>
<b>Good rows:</b> {total - rejected:,}</p>
{qr.build_reason_table(reasons)}
<p></p>
<table border='1' cellpadding='4' cellspacing='0'>
<tr><th>Column</th><th>Expectation</th><th>Failed rows</th><th>Failed %</th></tr>{rows}</table>
<p>Rejected records: <code>SELECT * FROM sales_analytics.silver.dq_rejected_rows
WHERE DAG_RUN_ID = '{run_id}'</code><br>
History: <code>silver.dq_run_log</code></p>
<p>Once the cause is fixed (for example a new activity type added to LAP_KNOWN_TYPES),
replay the quarantined rows by triggering bronze_to_silver with
<code>{{"reprocess_quarantine": true}}</code>. Steps: docs/QUARANTINE_RUNBOOK.md</p>
"""


def validate_lap_staging(**context):
    run_id = context.get("run_id", "manual")
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    hook.run(SQL_LAP_STAGING_SNAPSHOT)
    total, rejected = hook.get_first(SQL_LAP_STAGING_COUNTS)
    total, rejected = int(total or 0), int(rejected or 0)
    decision, pct = decide_dq_outcome(total, rejected)
    reasons = ([(r[0], r[1]) for r in hook.get_records(USE_CONTEXT + qr.reject_breakdown_sql())]
               if rejected else [])

    checks = run_gx_lap_checks(hook)
    gx_failed = sum(c["failed"] for c in checks)
    logging.info("DQ | decision=%s | total=%s | rejected=%s (%s) | gx_failed_cells=%s",
                 decision, total, rejected, fmt_pct(pct), gx_failed)
    for c in checks:
        logging.info("DQ | %s on %s | failed %s of %s (%s)",
                     c["check"], c["column"], c["failed"], c["total"], fmt_pct(c["pct"]))
    if (gx_failed == 0) != (rejected == 0):
        logging.warning("DQ | Great Expectations and the SQL row filter disagree "
                        "(gx_failed=%s, sql_rejected=%s); keep the two rule sets in sync.",
                        gx_failed, rejected)

    # Quarantine bad records and write the audit log (even when we are about to fail).
    log_values = ",\n".join(
        "(CURRENT_TIMESTAMP(), {run}, 'lead_activities_processed', {chk}, {col}, "
        "{tot}, {fail}, {pct}, {thr}, {dec})".format(
            run=_sql_str(run_id), chk=_sql_str(c["check"]), col=_sql_str(c["column"]),
            tot=c["total"], fail=c["failed"], pct=round(c["pct"], 10),
            thr=DQ_FAIL_THRESHOLD_PCT, dec=_sql_str(decision))
        for c in checks)
    log_values += ",\n(CURRENT_TIMESTAMP(), {run}, 'lead_activities_processed', "\
                  "'ROWS_REJECTED_ANY_RULE', NULL, {tot}, {fail}, {pct}, {thr}, {dec})".format(
        run=_sql_str(run_id), tot=total, fail=rejected, pct=round(pct, 10),
        thr=DQ_FAIL_THRESHOLD_PCT, dec=_sql_str(decision))
    hook.run(build_quarantine_sql(run_id))
    hook.run(USE_CONTEXT + f"""
INSERT INTO sales_analytics.silver.dq_run_log
    (RUN_TS, DAG_RUN_ID, TABLE_NAME, CHECK_NAME, COLUMN_NAME,
     TOTAL_ROWS, FAILED_ROWS, FAILED_PCT, THRESHOLD_PCT, DECISION)
VALUES
{log_values};
""")

    if decision == "PASS":
        return
    subject = (f"[Sales Analytics] Silver data quality {decision}: "
               f"{rejected:,} of {total:,} rows rejected ({fmt_pct(pct)})")
    send_dq_email(subject, build_dq_email(decision, run_id, total, rejected, pct, checks, reasons))
    if decision == "FAIL":
        raise AirflowFailException(
            f"Silver data quality FAILED: {rejected:,} of {total:,} staged rows "
            f"({fmt_pct(pct)}) broke constraints (threshold {DQ_FAIL_THRESHOLD_PCT:.0f}%). "
            "Gold will not be triggered.")


# ─────────────────────────────────────────────
# INCREMENTAL LOAD (2026-10)
# ─────────────────────────────────────────────
# Silver steps read only the Bronze batches since the last fully successful run
# (see silver_incremental.py for the design). init_run decides the window and
# stores it in silver.etl_watermark; each windowed step reads it back;
# commit_watermark advances it only after every step succeeded, so a failed run
# is simply re-read next time. Trigger with full_refresh=true to rebuild from
# all of Bronze, or since_date=YYYY-MM-DD to reprocess from a date.
def _hook():
    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


def init_run(**context):
    params = context["params"]
    hook = _hook()
    hook.run(DDL_WATERMARK)
    hook.run(USE_CONTEXT + ";\n".join(qr.DDL) + ";")   # RESOLVED_* columns on dq_rejected_rows
    for src in SOURCES:
        hook.run(
            f"MERGE INTO {WATERMARK_TABLE} t USING (SELECT %s AS S) s "
            f"ON t.SOURCE_TABLE = s.S WHEN NOT MATCHED THEN "
            f"INSERT (SOURCE_TABLE, UPDATED_AT) VALUES (s.S, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)",
            parameters=(src,))
        last = hook.get_first(
            f"SELECT LAST_LOAD_DATE FROM {WATERMARK_TABLE} WHERE SOURCE_TABLE = %s",
            parameters=(src,))[0]
        newest = hook.get_first(f"SELECT MAX(LOAD_DATE) FROM sales_analytics.bronze.{src}")[0]
        window = compute_window(
            last, newest,
            lookback_days=params.get("lookback_days", 1),
            full_refresh=bool(params.get("full_refresh", False)),
            since_date=params.get("since_date"))
        hook.run(
            f"UPDATE {WATERMARK_TABLE} SET WINDOW_START = %s::DATE, WINDOW_END = %s::DATE, "
            f"MODE = %s, RUN_STARTED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, "
            f"UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ WHERE SOURCE_TABLE = %s",
            parameters=(window["start"].isoformat() if window["start"] else None,
                        window["end"].isoformat() if window["end"] else None,
                        window["mode"], src))
        logging.info("Bronze %s: mode=%s window=%s..%s (last committed %s, newest %s)",
                     src, window["mode"], window["start"], window["end"], last, newest)


def run_windowed_sql(sql_name, source, **context):
    hook = _hook()
    mode, start, end = hook.get_first(
        f"SELECT MODE, WINDOW_START, WINDOW_END FROM {WATERMARK_TABLE} WHERE SOURCE_TABLE = %s",
        parameters=(source,))
    if mode is None:
        raise AirflowFailException(f"No window planned for {source}; init_run did not run.")
    sql = apply_tokens(globals()[sql_name], {"mode": mode, "start": start, "end": end}, mode)
    logging.info("%s on Bronze %s: mode=%s window=%s..%s", sql_name, source, mode, start, end)
    hook.run(sql)


def commit_watermark(**context):
    hook = _hook()
    for src in SOURCES:
        hook.run(
            f"UPDATE {WATERMARK_TABLE} SET LAST_LOAD_DATE = COALESCE(WINDOW_END, LAST_LOAD_DATE), "
            f"LAST_SUCCESS_STARTED_AT = RUN_STARTED_AT, UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ "
            f"WHERE SOURCE_TABLE = %s", parameters=(src,))
    logging.info("Watermark committed; next run reads from the latest committed load date.")


def run_scd2_in_transaction(statements):
    """
    Run SCD2 statements (close-out, then insert) as ONE transaction. Before, they were two
    tasks: a failure between them left rows closed out with no replacement current row
    until a retry. Now both are applied or neither is, so a retry is always safe.
    """
    hook = _hook()
    # Every hook.run() call opens its own session, so the USE statements must travel in the
    # same call, before BEGIN (they are not part of the transaction).
    use_statements = [u.strip() + ";" for u in USE_CONTEXT.split(";") if u.strip()]
    hook.run(use_statements + transaction_batch(statements, USE_CONTEXT), autocommit=True)


def merge_lap_scd2(**context):
    run_scd2_in_transaction([SQL_LAP_CLOSE_OUT, SQL_LAP_INSERT_NEW])


def replay_and_resolve_quarantine(**context):
    """
    Close the quarantine loop after the daily LAP merge and before LAS is built:
      1. SUPERSEDED: open rows whose activity Silver already holds at the same or a
         newer version are obsolete.
      2. REPLAYED (only when reprocess_quarantine=true): re-run the open rows through
         the CURRENT row filter and the same SCD2 statements as the daily load. Rows
         that now pass are merged and resolved; rows that still fail stay open.
         Replayed rows get a fresh EFFECTIVE_START_DATE, so LAS picks them up in this
         same run.
      3. Open rows older than quarantine.STALE_DAYS raise an email until resolved.
    ACCEPTED_LOSS is set by hand (docs/QUARANTINE_RUNBOOK.md).
    """
    run_id = context.get("run_id", "manual")
    hook = _hook()

    hook.run(USE_CONTEXT + qr.supersede_sql(run_id))

    if context["params"].get("reprocess_quarantine"):
        open_before = hook.get_first(USE_CONTEXT + qr.open_count_sql())[0]
        hook.run(USE_CONTEXT + qr.replay_prep_sql())
        eligible = hook.get_first(USE_CONTEXT + qr.replay_count_sql())[0]
        if eligible:
            run_scd2_in_transaction([qr.to_replay_source(SQL_LAP_CLOSE_OUT),
                                     qr.to_replay_source(SQL_LAP_INSERT_NEW)])
            hook.run(USE_CONTEXT + qr.replay_resolve_sql(run_id, LAP_GOOD_ROW_FILTER))
        open_after = hook.get_first(USE_CONTEXT + qr.open_count_sql())[0]
        logging.info("Quarantine replay: %s open before, %s eligible, %s open after "
                     "(rows still open fail the current row filter).",
                     open_before, eligible, open_after)

    stale = hook.get_records(USE_CONTEXT + qr.stale_sql())
    if stale:
        total = sum(int(r[1]) for r in stale)
        logging.warning("Quarantine: %s row(s) open for more than %s days.", total, qr.STALE_DAYS)
        send_dq_email(
            f"[Sales Analytics] {total:,} quarantined rows open for more than {qr.STALE_DAYS} days",
            qr.build_stale_email([(r[0], r[1], r[2]) for r in stale]))


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": notify_failure,   # one email per task that fails after retries
}

with DAG(
    dag_id="bronze_to_silver",
    description="Transform Bronze layer tables into Silver layer in Snowflake",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,  # triggered by s3_to_snowflake_bronze
    catchup=False,
    max_active_runs=1,
    tags=["silver", "snowflake", "transform"],
    params={
        "full_refresh": Param(False, type="boolean",
            description="Rebuild Silver from ALL of Bronze (slow). Use for backfills or after a logic change."),
        "lookback_days": Param(1, type="integer", minimum=0,
            description="Re-read this many days before the last processed batch (safety overlap)."),
        "since_date": Param(None, type=["null", "string"],
            description="Optional YYYY-MM-DD: reprocess Bronze batches from this date."),
        "reprocess_quarantine": Param(False, type="boolean",
            description="Also replay open rows from silver.dq_rejected_rows through the current "
                        "row filter (use after fixing the cause, e.g. a new activity type)."),
    },
) as dag:

    init_window = PythonOperator(task_id="init_run", python_callable=init_run)

    # ── First parallel batch — all 4 are independent of each other ────────
    # Windowed (2026-10): every daily users batch restates each user's full
    # version history, so reading the newest batches is enough. Before this,
    # the step re-parsed ALL historical batches on every run (11 min).
    transform_users = PythonOperator(
        task_id="transform_close_crm_users_processed",
        python_callable=run_windowed_sql,
        op_kwargs={"sql_name": "SQL_USERS", "source": "close_crm_users_raw"},
    )

    transform_custom_activities = SQLExecuteQueryOperator(
        task_id="transform_custom_activities",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_CUSTOM_ACTIVITIES,
    )

    transform_custom_all_leads = PythonOperator(
        task_id="transform_custom_activities_all_leads_details",
        python_callable=run_windowed_sql,
        op_kwargs={"sql_name": "SQL_CUSTOM_ALL_LEADS", "source": "lead_activites_raw"},
    )

    merge_email = PythonOperator(
        task_id="merge_lead_activities_email",
        python_callable=run_windowed_sql,
        op_kwargs={"sql_name": "SQL_EMAIL", "source": "lead_activites_raw"},
    )

    # New (2026-09) — independent of every other Silver table, reads only
    # from bronze.leads_raw. Runs in the first parallel batch.
    transform_leads_processed = PythonOperator(
        task_id="transform_leads_processed",
        python_callable=run_windowed_sql,
        op_kwargs={"sql_name": "SQL_LEADS_PROCESSED", "source": "leads_raw"},
    )

    # ── LAP transient (depends on nothing from the batch above) ───────────
    load_lap_transient = PythonOperator(
        task_id="load_lead_activities_processed_transient",
        python_callable=run_windowed_sql,
        op_kwargs={"sql_name": "SQL_LAP_TRANSIENT", "source": "lead_activites_raw"},
    )

    # ── LAP SCD2 merge — close-out and insert in ONE transaction ───────────
    merge_lap = PythonOperator(
        task_id="merge_lead_activities_processed_scd2",
        python_callable=merge_lap_scd2,
    )

    # ── LAS transient (needs users + custom + LAP's current state) ────────
    load_las_transient = PythonOperator(
        task_id="load_leads_activities_summary_transient",
        python_callable=run_windowed_sql,
        op_kwargs={"sql_name": "SQL_LAS_TRANSIENT", "source": "lead_activites_raw"},
    )

    # ── LAS merge ───────────────────────────────────────────────────────────
    merge_las = SQLExecuteQueryOperator(
        task_id="merge_leads_activities_summary",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_LAS_MERGE,
    )

    # ── LAS identity re-sync (names/emails follow the users dimension) ─────
    refresh_las_identity = SQLExecuteQueryOperator(
        task_id="refresh_las_user_identity",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_LAS_REFRESH_USER_IDENTITY,
    )

    # ── Trigger Gold layer DAG ─────────────────────────────────────────────
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="silver_to_gold",
        wait_for_completion=False,
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    #
    #  [users]  [custom_activities]  [custom_all_leads]  [email] ← all parallel, all independent
    #     |              |                    |
    #     └──────────────┴────────────────────┘
    #                     |
    #            [load_lap_transient]
    #                     |
    #            [merge_lap] (SCD2 close-out + insert, one transaction)
    #                     |
    #     (users, custom_activities, custom_all_leads, replay_quarantine)
    #                     |
    #            [load_las_transient]
    #                     |
    #               [merge_las]
    #                     |
    #             [trigger_gold]
    #
    #  merge_email has NO downstream dependents — it was never part of the
    #  LAP/LAS lineage, so it just runs and finishes independently.

    validate_lap = PythonOperator(
        task_id="validate_lap_staging",
        python_callable=validate_lap_staging,
    )

    replay_quarantine = PythonOperator(
        task_id="replay_and_resolve_quarantine",
        python_callable=replay_and_resolve_quarantine,
    )

    load_lap_transient >> validate_lap >> merge_lap >> replay_quarantine

    [
        transform_users,
        transform_custom_activities,
        transform_custom_all_leads,
        replay_quarantine,
    ] >> load_las_transient

    silver_dq_check = PythonOperator(
        task_id="validate_silver_data_quality",
        python_callable=validate_silver_data_quality,
    )

    commit_wm = PythonOperator(task_id="commit_watermark", python_callable=commit_watermark)

    load_las_transient >> merge_las >> refresh_las_identity >> silver_dq_check >> commit_wm
    init_window >> [
        transform_users,
        transform_custom_activities,
        transform_custom_all_leads,
        merge_email,
        transform_leads_processed,
        load_lap_transient,
    ]

    # merge_email and transform_leads_processed join the final trigger only
    # so the DAG doesn't mark "success" while either is still running, but
    # carry no data dependency on the LAP/LAS lineage. transform_leads_processed
    # DOES need to complete before Gold, though, since the rebuilt Closer
    # Report joins against silver.leads_processed for LEAD_ROUTE.
    # The watermark moves only after EVERY Silver step succeeded.
    merge_email >> commit_wm
    transform_leads_processed >> commit_wm
    commit_wm >> trigger_gold