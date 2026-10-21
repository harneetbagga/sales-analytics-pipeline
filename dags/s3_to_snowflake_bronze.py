"""
DAG: s3_to_snowflake_bronze

Loads S3 files into Snowflake Bronze layer, filtered to a specific
target_date's partition.

BACKFILL SUPPORT (this version):
  Previously COPY INTO pointed at the whole table prefix
  (@stage/{table_name}/, no date subfolder), relying entirely on
  Snowflake's own load-history dedup to skip already-loaded files. That
  meant there was no way to deliberately target one date's file — e.g.
  to force-reload a specific day's partition during a backfill.

  Fixed by scoping every COPY INTO to @stage/{table_name}/load_date={target_date}/
  and resolving target_date the same way postgres_to_s3 does:
    - Default: propagated via dag_run.conf from postgres_to_s3's trigger
      (falls back to data_interval_end = real "today" if run standalone
      with no conf, e.g. manually triggered on its own for testing).
    - Explicit backfill: pass target_date via dag_run.conf directly when
      triggering this DAG on its own, without going through postgres_to_s3
      first — useful if the S3 partition for that date already exists and
      you just need to (re)load Bronze from it.

  To backfill/reload a specific day directly into Bronze:
    UI:  Trigger DAG w/ config → {"target_date": "2026-08-20"}
    CLI: airflow dags trigger s3_to_snowflake_bronze --conf '{"target_date": "2026-08-20"}'
  Add {"force": true} to the conf to force-reload a partition even if
  Snowflake's load history already has it (sets FORCE=TRUE on the COPY).

Tables:
  - custom_activites_raw    → wrapper structure, load target_date's partition
  - close_crm_users_raw     → wrapper structure, load target_date's partition
  - leads_raw               → direct jsonb, load target_date's partition
  - lead_activites_raw      → direct jsonb, load target_date's partition

Schedule: Triggered by postgres_to_s3 DAG on success
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_default"
STAGE             = "sales_analytics.bronze.s3_raw_stage"
DATABASE          = "sales_analytics"
SCHEMA            = "bronze"
WAREHOUSE         = "sales_wh"

# Same resolution order as postgres_to_s3.py: explicit conf override wins,
# otherwise fall back to data_interval_end (= real "today" for a normally
# scheduled/triggered run). When triggered downstream by postgres_to_s3,
# TriggerDagRunOperator's conf={"target_date": ...} already supplies the
# override, so both DAGs load the exact same date's partition.
TARGET_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('target_date', data_interval_end.strftime('%Y-%m-%d')) }}"
)
FORCE_RELOAD_TEMPLATE = "{{ dag_run.conf.get('force', false) }}"


# ─────────────────────────────────────────────
# COPY INTO SQL — one per table, scoped to target_date's partition
# ─────────────────────────────────────────────
def copy_into_sql(table_name, target_date, force):
    force_clause = "FORCE = TRUE" if str(force).lower() == "true" else ""
    return f"""
        USE WAREHOUSE {WAREHOUSE};
        USE DATABASE {DATABASE};
        USE SCHEMA {SCHEMA};

        COPY INTO {DATABASE}.{SCHEMA}.{table_name} (
            JSON_OBJECT,
            SOURCE_INSERT_DATE,
            PG_INSERT_DATE,
            LOAD_DATE
        )
        FROM (
            SELECT
                $1:JSON_OBJECT::VARCHAR,
                $1:SOURCE_INSERT_DATE::VARCHAR,
                $1:PG_INSERT_DATE::VARCHAR,
                '{target_date}'::DATE
            FROM @{STAGE}/{table_name}/load_date={target_date}/
        )
        FILE_FORMAT = (
            TYPE              = 'JSON'
            STRIP_OUTER_ARRAY = FALSE
        )
        ON_ERROR = ABORT_STATEMENT
        {force_clause};
    """


def run_copy_into(table_name, target_date, force, **context):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql = copy_into_sql(table_name, target_date, force)
    logging.info(f"Loading {table_name} | target_date={target_date} | force={force}")
    logging.info(f"Running: {sql}")
    hook.run(sql)


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
    dag_id="s3_to_snowflake_bronze",
    description="Load S3 raw files into Snowflake Bronze layer, scoped to target_date's partition",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule=None,  # triggered by postgres_to_s3
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "snowflake", "s3"],
    params={
        "target_date": None,
        "force": False,
    },
) as dag:

    # ── Load each table, scoped to target_date's S3 partition ───────────────
    load_custom = PythonOperator(
        task_id="load_custom_activites_raw",
        python_callable=run_copy_into,
        op_kwargs={
            "table_name": "custom_activites_raw",
            "target_date": TARGET_DATE_TEMPLATE,
            "force": FORCE_RELOAD_TEMPLATE,
        },
    )

    load_users = PythonOperator(
        task_id="load_close_crm_users_raw",
        python_callable=run_copy_into,
        op_kwargs={
            "table_name": "close_crm_users_raw",
            "target_date": TARGET_DATE_TEMPLATE,
            "force": FORCE_RELOAD_TEMPLATE,
        },
    )

    load_leads = PythonOperator(
        task_id="load_leads_raw",
        python_callable=run_copy_into,
        op_kwargs={
            "table_name": "leads_raw",
            "target_date": TARGET_DATE_TEMPLATE,
            "force": FORCE_RELOAD_TEMPLATE,
        },
    )

    load_lead_activities = PythonOperator(
        task_id="load_lead_activites_raw",
        python_callable=run_copy_into,
        op_kwargs={
            "table_name": "lead_activites_raw",
            "target_date": TARGET_DATE_TEMPLATE,
            "force": FORCE_RELOAD_TEMPLATE,
        },
    )

    # ── Trigger next layer — bronze_to_silver ─────────────────────────
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="bronze_to_silver",
        wait_for_completion=False,
    )

    # ── Dependencies — all 4 load in parallel, then trigger silver ────
    [
        load_custom,
        load_users,
        load_leads,
        load_lead_activities,
    ] >> trigger_silver


# def backfill_bronze_lead_activites(**context):
#     from datetime import date, timedelta
    
#     hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
#     start_date   = date(2026, 5, 13)
#     end_date     = date(2026, 6, 6)
#     current_date = start_date

#     while current_date <= end_date:
#         ds = current_date.strftime("%Y-%m-%d")
#         logging.info(f"Loading lead_activites_raw for {ds}")

#         sql = f"""
#             COPY INTO {DATABASE}.{SCHEMA}.lead_activites_raw (
#                 JSON_OBJECT,
#                 SOURCE_INSERT_DATE,
#                 PG_INSERT_DATE,
#                 LOAD_DATE
#             )
#             FROM (
#                 SELECT
#                     $1:JSON_OBJECT::VARCHAR,
#                     $1:SOURCE_INSERT_DATE::VARCHAR,
#                     $1:PG_INSERT_DATE::VARCHAR,
#                     '{ds}'::DATE
#                 FROM @{DATABASE}.{SCHEMA}.s3_raw_stage/lead_activites_raw/load_date={ds}/
#             )
#             FILE_FORMAT = (
#                 TYPE              = 'JSON'
#                 STRIP_OUTER_ARRAY = FALSE
#             )
#             ON_ERROR = CONTINUE;
#         """

#         try:
#             hook.run(sql)
#             logging.info(f"Loaded {ds} successfully")
#         except Exception as e:
#             logging.warning(f"Skipping {ds} — {str(e)}")

#         current_date += timedelta(days=1)

#     logging.info("Bronze backfill complete for lead_activites_raw")

