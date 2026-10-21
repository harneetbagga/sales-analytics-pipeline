#!/usr/bin/env python
"""
One-off Bronze backfill: load every S3 date partition into Snowflake Bronze.

Why this exists
---------------
The s3_to_snowflake_bronze DAG loads ONE date partition per run and then
triggers Silver. After a Snowflake rebuild (new trial account, empty Bronze)
that would mean one Silver run per historical date. This script loads all the
partitions that exist in S3 directly, WITHOUT triggering Silver, so Silver can
be run once afterwards (an empty watermark makes that first run a FULL build).

It reuses the DAG's own COPY INTO builder (s3_to_snowflake_bronze.copy_into_sql),
so every partition is loaded exactly as the DAG would load it:
scoped to @stage/{table}/load_date={date}/, LOAD_DATE stamped from the path
date, ON_ERROR = ABORT_STATEMENT.

Idempotent: Snowflake's load history skips files it has already loaded, so
re-running is safe. Do NOT use --force on a populated table (it reloads
files and duplicates rows). Load metadata expires after 64 days.

Usage (from the repo root, with the project's Airflow environment active)
-------------------------------------------------------------------------
    export SNOWFLAKE_ACCOUNT=<orgname-accountname>
    export SNOWFLAKE_USER=<user>
    # either a password (prompted if unset) or a key pair:
    #   export SNOWFLAKE_PASSWORD=...        or
    #   export SNOWFLAKE_PRIVATE_KEY_FILE=~/.ssh/snowflake_key.p8
    export SNOWFLAKE_ROLE=PIPELINE_LOADER      # optional

    python scripts/backfill_bronze.py --dry-run
    python scripts/backfill_bronze.py --start 2026-03-01
    python scripts/backfill_bronze.py --tables lead_activites_raw --start 2026-10-01 --end 2026-10-22

AWS credentials come from the usual boto3 chain (aws configure / env vars).
Credentials are never read from, or written to, the repository.
"""
import argparse
import getpass
import logging
import os
import sys
from datetime import date

import boto3
import snowflake.connector

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "dags"))
import s3_to_snowflake_bronze as bronze  # noqa: E402  (reuse the DAG's COPY INTO builder)

BUCKET = os.environ.get("STAGING_BUCKET", "sales-analyical-pipeline")  # spelling matches the real bucket
RAW_PREFIX = "raw"
TABLES = ["custom_activites_raw", "close_crm_users_raw", "leads_raw", "lead_activites_raw"]

log = logging.getLogger("backfill_bronze")


def list_partitions(s3, table):
    """Return the sorted load dates that have files under raw/{table}/load_date=YYYY-MM-DD/."""
    prefix = f"{RAW_PREFIX}/{table}/"
    dates = set()
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            part = cp["Prefix"][len(prefix):].strip("/")      # load_date=2026-10-22
            if part.startswith("load_date="):
                try:
                    dates.add(date.fromisoformat(part[len("load_date="):]))
                except ValueError:
                    log.warning("Skipping unexpected prefix %s", cp["Prefix"])
    return sorted(dates)


def connect():
    params = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        warehouse=bronze.WAREHOUSE,
        database=bronze.DATABASE,
    )
    if os.environ.get("SNOWFLAKE_ROLE"):
        params["role"] = os.environ["SNOWFLAKE_ROLE"]
    key_file = os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE")
    if key_file:
        params["private_key_file"] = os.path.expanduser(key_file)
        if os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
            params["private_key_file_pwd"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]
    else:
        params["password"] = os.environ.get("SNOWFLAKE_PASSWORD") or getpass.getpass("Snowflake password: ")
    return snowflake.connector.connect(**params)


def rows_loaded(cursors):
    """Best-effort sum of rows loaded from COPY INTO's result (0 if already loaded / no files)."""
    total = 0
    for cur in cursors:
        try:
            cols = [c[0].lower() for c in cur.description or []]
            if "rows_loaded" in cols:
                i = cols.index("rows_loaded")
                total += sum(int(r[i]) for r in cur.fetchall())
        except Exception:  # result shape differs for USE statements / "0 files processed"
            continue
    return total


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=date.fromisoformat, help="first load_date to include (default: earliest in S3)")
    ap.add_argument("--end", type=date.fromisoformat, help="last load_date to include (default: latest in S3)")
    ap.add_argument("--tables", nargs="+", choices=TABLES, default=TABLES)
    ap.add_argument("--dry-run", action="store_true", help="list what would be loaded; touch nothing")
    ap.add_argument("--force", action="store_true", help="FORCE=TRUE (reloads files; duplicates rows on a populated table)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    s3 = boto3.client("s3")
    plan = []
    for table in args.tables:
        for d in list_partitions(s3, table):
            if (args.start and d < args.start) or (args.end and d > args.end):
                continue
            plan.append((table, d))
    plan.sort(key=lambda x: (x[1], x[0]))
    if not plan:
        log.error("No partitions found in s3://%s/%s/ for the requested tables/dates.", BUCKET, RAW_PREFIX)
        return 1

    log.info("%d partitions to load across %d tables (%s .. %s)",
             len(plan), len(args.tables), plan[0][1], plan[-1][1])
    if args.dry_run:
        for table, d in plan:
            print(f"{d}  {table}")
        return 0
    if args.force:
        log.warning("--force set: files already loaded will be loaded AGAIN.")

    conn = connect()
    failures = []
    try:
        for n, (table, d) in enumerate(plan, 1):
            sql = bronze.copy_into_sql(table, d.isoformat(), str(args.force).lower())
            try:
                loaded = rows_loaded(conn.execute_string(sql))
                log.info("[%d/%d] %s %s: %d rows loaded", n, len(plan), table, d, loaded)
            except Exception as exc:  # keep going; ABORT_STATEMENT already left this partition clean
                failures.append((table, d, str(exc)[:200]))
                log.error("[%d/%d] %s %s FAILED: %s", n, len(plan), table, d, str(exc)[:200])

        log.info("---- Bronze totals ----")
        cur = conn.cursor()
        for table in args.tables:
            cur.execute(f"SELECT COUNT(*), MIN(LOAD_DATE), MAX(LOAD_DATE), COUNT(DISTINCT LOAD_DATE) "
                        f"FROM {bronze.DATABASE}.{bronze.SCHEMA}.{table}")
            cnt, lo, hi, days = cur.fetchone()
            log.info("%-22s rows=%s  days=%s  %s .. %s", table, cnt, days, lo, hi)
    finally:
        conn.close()

    if failures:
        log.error("%d partition(s) failed:", len(failures))
        for table, d, msg in failures:
            log.error("  %s %s: %s", table, d, msg)
        return 2
    log.info("Done. Next: trigger bronze_to_silver once (first run is a FULL build).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
