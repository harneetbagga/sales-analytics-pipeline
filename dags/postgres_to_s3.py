"""
DAG: postgres_to_s3

Extracts all 4 source tables from PostgreSQL schema: raw → uploads to S3.
All 4 tables extract incrementally by INSERT_DATE, filtered to `target_date`.

BACKFILL SUPPORT (this version):
  Previously every task hardcoded `datetime.now()` / `CURRENT_DATE`, so a
  manually-triggered run for a past logical_date silently still pulled
  TODAY's real data — there was no way to actually backfill a specific day.

  Fixed by introducing `target_date`, resolved once per run:
    - Default (no override): {{ data_interval_end | ds }} — the Jinja
      macro that equals real "today" for a normally-scheduled daily run,
      so default behavior is unchanged from before.
    - Explicit backfill: pass target_date via dag_run.conf when
      triggering, e.g. {"target_date": "2026-08-20"} — every task
      (sensor + all 4 extractions) then checks/pulls that exact date.

  To backfill a specific day:
    UI:  Trigger DAG w/ config → {"target_date": "2026-08-20"}
    CLI: airflow dags trigger postgres_to_s3 --conf '{"target_date": "2026-08-20"}'

Tables (all incremental by INSERT_DATE, filtered to target_date):
  - lead_activites_raw      → direct jsonb ({ data: [...] })
  - leads_raw               → direct jsonb (lead object)
  - close_crm_users_raw     → wrapper structure (INSERT_DATE + JSON_OBJECT keys)
  - custom_activites_raw    → wrapper structure (INSERT_DATE + JSON_OBJECT keys)

Schedule: Every day at 8:00 AM America/New_York
"""

import json
import io
import os
import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from alerts import notify_failure
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
POSTGRES_CONN_ID = "postgres_close_crm"
AWS_CONN_ID      = "aws_default"
STAGING_BUCKET   = "sales-analyical-pipeline"
POSTGRES_SCHEMA  = "raw"

WRAPPER_TABLES = {"close_crm_users_raw", "custom_activites_raw"}

# Jinja expression used everywhere target_date is resolved: explicit conf
# override wins, otherwise fall back to data_interval_end (= real "today"
# for a normally-scheduled run). Centralized here so every task — the
# sensor and all 4 extractions — resolves the exact same value.
TARGET_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('target_date') or data_interval_end.strftime('%Y-%m-%d') }}"
)

# ─────────────────────────────────────────────
# SENSOR TIMEOUT — derived, not a flat constant (Batch 4)
# ─────────────────────────────────────────────
# Previously a flat SOURCE_SLA_BUFFER_SECONDS = 7200 with no relationship
# to the DAG's actual schedule or the source's actual SLA — exactly the
# gap that caused the original sensor timeout incident (sensor started
# polling ~04:12, gave up ~06:27, over an hour before the source's
# 07:30 EST SLA was even due). This derives the timeout instead:
#   - If the DAG's scheduled start is AFTER the source SLA (the normal
#     case today: DAG starts 08:00, SLA is 07:30), the timeout only
#     needs to cover variance in the source landing later than usual —
#     collapses to just SLA_VARIANCE_BUFFER_SECONDS.
#   - If the DAG's scheduled start is ever moved to BEFORE the source
#     SLA, the timeout automatically extends to cover that shortfall
#     plus the variance buffer, so the sensor can never structurally
#     time out before the source could possibly have landed — the
#     exact failure mode from the original incident becomes impossible
#     by construction, not just unlikely.
SOURCE_SLA_TIME   = pendulum.parse("07:30:00").time()
DAG_SCHEDULE_TIME = pendulum.parse("08:00:00").time()

# Variance buffer for the source landing later than its own SLA on a
# bad day. Starting assumption — tune against real observed
# landing-time variance once a few weeks of production history exist.
SLA_VARIANCE_BUFFER_SECONDS = 60 * 60 * 2  # 2 hours


def _compute_sensor_timeout():
    dag_start_seconds = DAG_SCHEDULE_TIME.hour * 3600 + DAG_SCHEDULE_TIME.minute * 60
    sla_seconds        = SOURCE_SLA_TIME.hour * 3600 + SOURCE_SLA_TIME.minute * 60
    gap = dag_start_seconds - sla_seconds  # positive if DAG starts after SLA

    if gap < 0:
        # DAG starts BEFORE the source's own SLA — timeout must at
        # minimum cover that gap, or it will always time out before
        # the source could possibly have landed.
        return abs(gap) + SLA_VARIANCE_BUFFER_SECONDS

    # DAG already starts after the SLA — timeout only needs to cover
    # the variance buffer for a late-landing day.
    return SLA_VARIANCE_BUFFER_SECONDS


SOURCE_SLA_BUFFER_SECONDS = _compute_sensor_timeout()


# ─────────────────────────────────────────────
# INCREMENTAL EXTRACT — used by all 4 tables
# ─────────────────────────────────────────────
def extract_incremental_table(table_name, s3_prefix, target_date, **context):
    logging.info(f"Starting incremental extraction | table={table_name} | target_date={target_date}")

    pg_hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    s3_hook   = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_client = s3_hook.get_conn()

    if table_name in WRAPPER_TABLES:
        sql = f"""
            SELECT
                RAW_DATA->>'JSON_OBJECT'  AS JSON_OBJECT,
                RAW_DATA->>'INSERT_DATE'  AS SOURCE_INSERT_DATE,
                INSERT_DATE               AS PG_INSERT_DATE
            FROM {POSTGRES_SCHEMA}.{table_name}
            WHERE INSERT_DATE::date = %(target_date)s
              AND RAW_DATA IS NOT NULL
        """
    else:
        sql = f"""
            SELECT
                RAW_DATA::text  AS JSON_OBJECT,
                NULL            AS SOURCE_INSERT_DATE,
                INSERT_DATE     AS PG_INSERT_DATE
            FROM {POSTGRES_SCHEMA}.{table_name}
            WHERE INSERT_DATE::date = %(target_date)s
              AND RAW_DATA IS NOT NULL
        """

    logging.info(f"Running: {sql} | params: target_date={target_date}")

    s3_key        = f"raw/{s3_prefix}/load_date={target_date}/{table_name}.json"
    mpu           = s3_client.create_multipart_upload(Bucket=STAGING_BUCKET, Key=s3_key)
    upload_id     = mpu["UploadId"]
    parts         = []
    part_num      = 1
    MIN_PART_SIZE = 5 * 1024 * 1024
    chunk_size    = 100 if table_name == "leads_raw" else 500
    total_rows    = 0
    null_rows     = 0
    buffer        = io.BytesIO()

    conn   = pg_hook.get_conn()
    # Server-side (named) cursor: Postgres keeps the result set and hands over
    # chunk_size rows per fetchmany(), so worker memory holds one chunk, not the whole
    # day (rows are ~60 KB; the largest daily file was 1 GB). A plain conn.cursor()
    # would download the entire result before the first fetchmany(). The read runs in
    # one open transaction for the few minutes the extract takes.
    cursor = conn.cursor(name=f"extract_{table_name}_{os.getpid()}")
    cursor.itersize = chunk_size

    try:
        cursor.execute(sql, {"target_date": target_date})

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break

            for row in rows:
                if row[0] is None:
                    null_rows += 1
                    continue
                line = json.dumps({
                    "JSON_OBJECT":        str(row[0]),
                    "SOURCE_INSERT_DATE": str(row[1]),
                    "PG_INSERT_DATE":     str(row[2])
                }) + "\n"
                buffer.write(line.encode("utf-8"))

            total_rows += len(rows)
            logging.info(f"Processed {total_rows} rows, {null_rows} nulls skipped...")

            if buffer.tell() >= MIN_PART_SIZE:
                buffer.seek(0)
                part = s3_client.upload_part(
                    Bucket=STAGING_BUCKET,
                    Key=s3_key,
                    PartNumber=part_num,
                    UploadId=upload_id,
                    Body=buffer.read()
                )
                parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
                part_num += 1
                buffer = io.BytesIO()

        cursor.close()

        if buffer.tell() > 0:
            buffer.seek(0)
            part = s3_client.upload_part(
                Bucket=STAGING_BUCKET,
                Key=s3_key,
                PartNumber=part_num,
                UploadId=upload_id,
                Body=buffer.read()
            )
            parts.append({"PartNumber": part_num, "ETag": part["ETag"]})

        if not parts:
            s3_client.abort_multipart_upload(
                Bucket=STAGING_BUCKET, Key=s3_key, UploadId=upload_id
            )
            raise ValueError(
                f"No rows in {POSTGRES_SCHEMA}.{table_name} "
                f"for INSERT_DATE::date = '{target_date}'. "
                f"Total: {total_rows}, Nulls: {null_rows}"
            )

        s3_client.complete_multipart_upload(
            Bucket=STAGING_BUCKET,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )

        logging.info(
            f"Upload complete | s3://{STAGING_BUCKET}/{s3_key} | "
            f"{total_rows} rows | {part_num} parts"
        )

    except Exception as e:
        s3_client.abort_multipart_upload(
            Bucket=STAGING_BUCKET, Key=s3_key, UploadId=upload_id
        )
        raise e
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": notify_failure,   # one email per task that fails after retries
}

with DAG(
    dag_id="postgres_to_s3",
    description="Extract all 4 source tables from PostgreSQL to S3, incremental by target_date",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 5, 1, tz="America/New_York"),
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "postgres", "s3"],
    params={
        "target_date": None,  # documents the conf key; UI's Trigger w/ config shows it as a hint
    },
) as dag:

    # ── Sensor — waits for target_date's row to exist ───────────────────────
    # Default runs: target_date = today, so this behaves exactly as before.
    # Backfill runs (conf override): waits for that specific date's row —
    # if it's already landed (a past day), this returns immediately.
    wait_for_postgres = SqlSensor(
        task_id="wait_for_postgres_refresh",
        conn_id=POSTGRES_CONN_ID,
        sql=(
            "SELECT 1 FROM raw.lead_activites_raw "
            f"WHERE INSERT_DATE::date = '{TARGET_DATE_TEMPLATE}' LIMIT 1"
        ),
        poke_interval=300,
        timeout=SOURCE_SLA_BUFFER_SECONDS,
        mode="reschedule",
        retries=0,
    )

    # ── All 4 tables, incremental, filtered to target_date ───────────────────
    extract_custom = PythonOperator(
        task_id="extract_custom_activites_raw",
        python_callable=extract_incremental_table,
        op_kwargs={
            "table_name": "custom_activites_raw",
            "s3_prefix":  "custom_activites_raw",
            "target_date": TARGET_DATE_TEMPLATE,
        },
    )

    extract_users = PythonOperator(
        task_id="extract_close_crm_users_raw",
        python_callable=extract_incremental_table,
        op_kwargs={
            "table_name": "close_crm_users_raw",
            "s3_prefix":  "close_crm_users_raw",
            "target_date": TARGET_DATE_TEMPLATE,
        },
    )

    extract_leads = PythonOperator(
        task_id="extract_leads_raw",
        python_callable=extract_incremental_table,
        op_kwargs={
            "table_name": "leads_raw",
            "s3_prefix":  "leads_raw",
            "target_date": TARGET_DATE_TEMPLATE,
        },
    )

    extract_lead_activities = PythonOperator(
        task_id="extract_lead_activites_raw",
        python_callable=extract_incremental_table,
        op_kwargs={
            "table_name": "lead_activites_raw",
            "s3_prefix":  "lead_activites_raw",
            "target_date": TARGET_DATE_TEMPLATE,
        },
    )

    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_s3_to_snowflake_bronze",
        trigger_dag_id="s3_to_snowflake_bronze",
        wait_for_completion=False,
        logical_date="{{ logical_date }}",
        conf={"target_date": TARGET_DATE_TEMPLATE},  # propagate to downstream Bronze load
    )

    # ── Dependencies — all 4 run in parallel after sensor passes ────────────
    wait_for_postgres >> [
        extract_custom,
        extract_users,
        extract_leads,
        extract_lead_activities,
    ] >> trigger_bronze

    
# def backfill_lead_activites(**context):
#     import io
#     from datetime import date, timedelta

#     pg_hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#     s3_hook   = S3Hook(aws_conn_id=AWS_CONN_ID)
#     s3_client = s3_hook.get_conn()

#     start_date = date(2026, 6, 5) 
#     end_date   = date(2026, 6, 5) # Missing June 5

#     current_date = start_date
#     while current_date <= end_date:
#         ds = current_date.strftime("%Y-%m-%d")
#         logging.info(f"Starting backfill for date: {ds}")

#         sql = f"""
#             SELECT
#                 RAW_DATA::text  AS JSON_OBJECT,
#                 NULL            AS SOURCE_INSERT_DATE,
#                 INSERT_DATE     AS PG_INSERT_DATE
#             FROM {POSTGRES_SCHEMA}.lead_activites_raw
#             WHERE INSERT_DATE::date = '{ds}'
#             AND RAW_DATA IS NOT NULL
#         """

#         s3_key        = f"raw/lead_activites_raw/load_date={ds}/lead_activites_raw.json"
#         mpu           = s3_client.create_multipart_upload(Bucket=STAGING_BUCKET, Key=s3_key)
#         upload_id     = mpu["UploadId"]
#         parts         = []
#         part_num      = 1
#         MIN_PART_SIZE = 5 * 1024 * 1024
#         chunk_size    = 500
#         total_rows    = 0
#         buffer        = io.BytesIO()

#         # ── Fresh connection per date ─────────────────────────────────
#         conn   = pg_hook.get_conn()
#         cursor = conn.cursor()

#         try:
#             cursor.execute(sql)

#             while True:
#                 rows = cursor.fetchmany(chunk_size)
#                 if not rows:
#                     break

#                 for row in rows:
#                     if row[0] is None:
#                         continue
#                     line = json.dumps({
#                         "JSON_OBJECT":        str(row[0]),
#                         "SOURCE_INSERT_DATE": str(row[1]),
#                         "PG_INSERT_DATE":     str(row[2])
#                     }) + "\n"
#                     buffer.write(line.encode("utf-8"))

#                 total_rows += len(rows)

#                 if buffer.tell() >= MIN_PART_SIZE:
#                     buffer.seek(0)
#                     part = s3_client.upload_part(
#                         Bucket=STAGING_BUCKET,
#                         Key=s3_key,
#                         PartNumber=part_num,
#                         UploadId=upload_id,
#                         Body=buffer.read()
#                     )
#                     parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
#                     part_num += 1
#                     buffer = io.BytesIO()

#             # Upload remaining buffer
#             if buffer.tell() > 0:
#                 buffer.seek(0)
#                 part = s3_client.upload_part(
#                     Bucket=STAGING_BUCKET,
#                     Key=s3_key,
#                     PartNumber=part_num,
#                     UploadId=upload_id,
#                     Body=buffer.read()
#                 )
#                 parts.append({"PartNumber": part_num, "ETag": part["ETag"]})

#             if not parts:
#                 s3_client.abort_multipart_upload(
#                     Bucket=STAGING_BUCKET, Key=s3_key, UploadId=upload_id
#                 )
#                 logging.warning(f"No rows found for {ds} — skipping")
#             else:
#                 s3_client.complete_multipart_upload(
#                     Bucket=STAGING_BUCKET,
#                     Key=s3_key,
#                     UploadId=upload_id,
#                     MultipartUpload={"Parts": parts}
#                 )
#                 logging.info(f"Uploaded {total_rows} rows for {ds}")

#         except Exception as e:
#             s3_client.abort_multipart_upload(
#                 Bucket=STAGING_BUCKET, Key=s3_key, UploadId=upload_id
#             )
#             raise e

#         finally:
#             # ── Always close connection after each date ───────────────
#             try:
#                 cursor.close()
#             except Exception:
#                 pass
#             try:
#                 conn.close()
#             except Exception:
#                 pass

#         current_date += timedelta(days=1)

#     logging.info("Backfill complete for all dates.")


    # def check_date_range(**context):
    #     pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    #     result = pg_hook.get_records("""
    #         SELECT
    #             MIN(INSERT_DATE::date) AS earliest_date,
    #             MAX(INSERT_DATE::date) AS latest_date,
    #             COUNT(*)               AS total_rows,
    #             COUNT(DISTINCT INSERT_DATE::date) AS distinct_dates
    #         FROM raw.lead_activites_raw
    #     """)
    #     logging.info("=== lead_activites_raw date range ===")
    #     for r in result:
    #         logging.info(f"  Earliest:       {r[0]}")
    #         logging.info(f"  Latest:         {r[1]}")
    #         logging.info(f"  Total rows:     {r[2]}")
    #         logging.info(f"  Distinct dates: {r[3]}")

    # date_range_task = PythonOperator(
    #     task_id='check_date_range',
    #     python_callable=check_date_range
    # )

# def check_leads_s3(**context):
#         s3_hook   = S3Hook(aws_conn_id=AWS_CONN_ID)
#         s3_client = s3_hook.get_conn()

#         obj = s3_client.get_object(
#             Bucket=STAGING_BUCKET,
#             Key="raw/leads_raw/load_date=2026-06-04/leads_raw.json"
#         )
#         content = obj['Body'].read().decode('utf-8')
#         lines = [l for l in content.split('\n') if l.strip()]
#         logging.info(f"Lines in S3 file: {len(lines)}")

# check_leads_s3 = PythonOperator(
#     task_id='check_leads_s3',
#     python_callable=check_leads_s3
#     )

    