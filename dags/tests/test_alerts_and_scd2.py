"""
Tests for the shared failure alert, the SCD2 single-transaction helper and the S3
lifecycle config (no database, no Airflow, no AWS needed).
Run:  pytest dags/tests/test_alerts_and_scd2.py
"""
import json
import os
import re
import sys
from types import SimpleNamespace

HERE = os.path.dirname(os.path.abspath(__file__))
DAGS = os.path.join(HERE, "..")
ROOT = os.path.join(HERE, "..", "..")
sys.path.insert(0, DAGS)

import alerts  # noqa: E402
from silver_incremental import transaction_batch  # noqa: E402

FIVE_DAGS = ("postgres_to_s3", "s3_to_snowflake_bronze", "bronze_to_silver",
             "silver_to_gold", "gold_to_export")


def _src(name):
    with open(os.path.join(DAGS, name + ".py"), encoding="utf-8") as f:
        return f.read()


# ── failure alert ─────────────────────────────────────────────────────────

def _ctx(exc=None, ti_extra=None):
    ti = SimpleNamespace(dag_id="postgres_to_s3", task_id="wait_for_postgres_refresh",
                         try_number=1, max_tries=0, **(ti_extra or {}))
    return {"task_instance": ti, "run_id": "scheduled__2026-10-20", "exception": exc}


def test_failure_info_and_email_name_the_dag_task_run_and_error():
    info = alerts.extract_failure_info(_ctx(RuntimeError("Snap. Time is OUT")))
    assert info["dag_id"] == "postgres_to_s3"
    assert info["task_id"] == "wait_for_postgres_refresh"
    assert info["run_id"] == "scheduled__2026-10-20"
    assert info["error"] == "RuntimeError: Snap. Time is OUT"
    subject, body = alerts.build_failure_email(info)
    assert subject == "[Sales Analytics] FAILED: postgres_to_s3.wait_for_postgres_refresh"
    assert "scheduled__2026-10-20" in body and "Snap. Time is OUT" in body
    assert "1 of 1" in body  # attempt 1, max_tries 0 -> one attempt allowed
    assert "target_date" in body and "ALERTING" in body


def test_error_text_is_escaped_and_truncated():
    info = alerts.extract_failure_info(_ctx(RuntimeError("<script>" + "x" * 5000)))
    _, body = alerts.build_failure_email(info)
    assert "<script>" not in body and "&lt;script&gt;" in body
    assert body.count("x") <= alerts.MAX_ERROR_CHARS


def test_missing_context_fields_do_not_break_the_callback():
    info = alerts.extract_failure_info({})
    assert info["dag_id"] == "unknown_dag" and info["task_id"] == "unknown_task"
    subject, body = alerts.build_failure_email(info)
    assert "FAILED" in subject and "(no exception text)" in body


def test_notify_failure_never_raises():
    def boom(*a, **k):
        raise RuntimeError("smtp is down")
    original = alerts.send_email
    alerts.send_email = boom
    try:
        alerts.notify_failure(_ctx(RuntimeError("x")))   # must not raise
        alerts.notify_failure(None)                      # garbage context must not raise either
    finally:
        alerts.send_email = original


def test_send_email_swallows_every_error():
    # no Airflow installed here: the Variable import fails inside send_email and is swallowed
    alerts.send_email("subject", "<p>body</p>")


def test_all_five_dags_use_the_shared_callback():
    for name in FIVE_DAGS:
        src = _src(name)
        assert re.search(r"^from alerts import notify_failure", src, re.M), name
        assert re.search(r'"on_failure_callback":\s*notify_failure', src), name


# ── SCD2 in one transaction ───────────────────────────────────────────────

USE = "\n    USE WAREHOUSE w;\n    USE DATABASE d;\n"


def test_transaction_batch_wraps_statements_and_strips_the_session_prefix():
    close_out = USE + "UPDATE t SET a = 1;"
    insert = USE + "INSERT INTO t SELECT 1;"
    batch = transaction_batch([close_out, insert], USE)
    assert batch == ["BEGIN", "UPDATE t SET a = 1;", "INSERT INTO t SELECT 1;", "COMMIT"]


def test_transaction_batch_rejects_multi_statement_bodies():
    try:
        transaction_batch([USE + "UPDATE t SET a = 1; UPDATE t SET b = 2;"], USE)
    except ValueError:
        return
    raise AssertionError("a body with two statements must be rejected")


def test_lap_scd2_is_one_task_one_transaction():
    src = _src("bronze_to_silver")
    assert "def run_scd2_in_transaction" in src
    assert "transaction_batch(" in src
    # USE statements and the transaction share ONE hook.run call (each call is its own session)
    body = src.split("def run_scd2_in_transaction", 1)[1].split("def merge_lap_scd2", 1)[0]
    assert "hook.run(use_statements + transaction_batch(" in body and "autocommit=True" in body
    assert body.count("hook.run(use_statements") == 1
    assert 'task_id="merge_lead_activities_processed_scd2"' in src
    # the two old separate tasks are gone
    assert 'task_id="close_out_lead_activities_processed"' not in src
    assert 'task_id="insert_new_lead_activities_processed"' not in src
    assert re.search(r"validate_lap\s*>>\s*merge_lap\s*>>\s*replay_quarantine", src)


def test_quarantine_replay_uses_the_same_transaction():
    src = _src("bronze_to_silver")
    body = src.split("def replay_and_resolve_quarantine", 1)[1].split("# ─", 1)[0]
    assert "run_scd2_in_transaction(" in body
    assert "hook.run(qr.to_replay_source(SQL_LAP_CLOSE_OUT))" not in body


# ── S3 lifecycle ──────────────────────────────────────────────────────────

def test_s3_lifecycle_never_expires_raw_files():
    with open(os.path.join(ROOT, "scripts", "s3_lifecycle.json"), encoding="utf-8") as f:
        rules = json.load(f)["Rules"]
    ids = [r["ID"] for r in rules]
    assert len(ids) == len(set(ids))
    for r in rules:
        assert r["Status"] == "Enabled"
        assert "Expiration" not in r, "raw files are the replay source; never expire current objects"
    raw = next(r for r in rules if r["Filter"]["Prefix"] == "raw/")
    t = raw["Transitions"][0]
    assert t["StorageClass"] == "GLACIER_IR" and t["Days"] >= 90
    # Snowflake cannot read Glacier Flexible Retrieval / Deep Archive objects
    assert all(x["StorageClass"] in ("GLACIER_IR", "STANDARD_IA", "INTELLIGENT_TIERING")
               for r in rules for x in r.get("Transitions", []))


# ── extract uses a server-side cursor ─────────────────────────────────────

def test_extract_streams_with_a_named_server_side_cursor():
    src = _src("postgres_to_s3")
    body = src.split("def extract_incremental_table", 1)[1].split("# ─", 1)[0]
    assert re.search(r"conn\.cursor\(\s*name=", body), "extract must use a named (server-side) cursor"
    assert "fetchmany(chunk_size)" in body
    assert "finally:" in body and "conn.close()" in body.split("finally:", 1)[1]
