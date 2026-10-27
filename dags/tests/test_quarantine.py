"""
Unit tests for the quarantine life cycle (no database, no Airflow needed) plus
source checks that bronze_to_silver wires it in.
Run:  pytest dags/tests/test_quarantine.py
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DAGS = os.path.join(HERE, "..")
sys.path.insert(0, DAGS)

import quarantine as qr  # noqa: E402

with open(os.path.join(DAGS, "bronze_to_silver.py"), encoding="utf-8") as f:
    SILVER_SRC = f.read()
with open(os.path.join(HERE, "..", "..", "snowflake", "dq_tables.sql"), encoding="utf-8") as f:
    DQ_TABLES_SQL = f.read()

GOOD_FILTER = "act.value:id::VARCHAR IS NOT NULL"


def test_sql_str_escapes_quotes():
    assert qr.sql_str("a'b") == "'a''b'"
    assert qr.sql_str("manual__2026-10-24") == "'manual__2026-10-24'"


def test_resolution_columns_exist_in_ddl_and_table_script():
    for col in ("RESOLVED_AT", "RESOLUTION", "RESOLVED_RUN_ID", "RESOLUTION_NOTE"):
        assert any(col in stmt and "IF NOT EXISTS" in stmt for stmt in qr.DDL), col
        assert col in DQ_TABLES_SQL, col
    assert len(qr.DDL) == 4


def test_supersede_only_touches_open_rows_and_never_null_ids():
    sql = qr.supersede_sql("run'1")
    assert "RESOLUTION      = 'SUPERSEDED'" in sql
    assert "q.RESOLVED_AT IS NULL" in sql
    assert "q.ACTIVITY_ID IS NOT NULL" in sql
    assert "p.IS_CURRENT  = TRUE" in sql
    assert "p.DATE_UPDATED >= q.RAW_RECORD:date_updated::TIMESTAMP_NTZ" in sql
    assert "'run''1'" in sql


def test_replay_prep_skips_rows_silver_already_has_at_same_or_newer_version():
    sql = qr.replay_prep_sql()
    # anti-join: keep only rows with NO current Silver row at >= the record's date_updated
    assert "LEFT JOIN" in sql and "p.ACTIVITY_ID IS NULL" in sql
    assert "p.DATE_UPDATED >= q.RAW_RECORD:date_updated::TIMESTAMP_NTZ" in sql
    assert "q.RESOLVED_AT IS NULL" in sql
    # same shape as the daily staging table: JSON_OBJECT:data is an array of activities
    assert "OBJECT_CONSTRUCT('data', ARRAY_CONSTRUCT(q.RAW_RECORD))" in sql
    assert qr.REPLAY_TABLE in sql


def test_replay_points_the_daily_scd2_statements_at_the_replay_table():
    daily = f"... FROM {qr.TRANSIENT_TABLE} src, LATERAL FLATTEN(...) ..."
    out = qr.to_replay_source(daily)
    assert qr.REPLAY_TABLE in out and qr.TRANSIENT_TABLE not in out


def test_replay_refuses_a_statement_that_does_not_read_the_staging_table():
    try:
        qr.to_replay_source("SELECT 1")
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_daily_scd2_statements_really_read_the_transient_table_once():
    # to_replay_source relies on this; if the SCD2 SQL is refactored this fails loudly.
    block = SILVER_SRC.split("SQL_LAP_SOURCE_SUBQUERY = \"\"\"", 1)[1].split("\"\"\"", 1)[0]
    assert block.count(qr.TRANSIENT_TABLE) == 1


def test_replay_resolve_uses_the_current_row_filter():
    sql = qr.replay_resolve_sql("r1", GOOD_FILTER)
    assert "RESOLUTION      = 'REPLAYED'" in sql
    assert GOOD_FILTER in sql
    assert "q.ROW_HASH IN" in sql and "q.RESOLVED_AT IS NULL" in sql


def test_stale_query_and_email():
    assert "DATEADD(day, -7," in qr.stale_sql()
    assert "DATEADD(day, -3," in qr.stale_sql(3)
    html = qr.build_stale_email([("TYPE not in known set", 12, "2026-10-10"),
                                 ("ACTIVITY_ID is null", 3, "2026-10-12")])
    assert "15 quarantined" in html
    assert "reprocess_quarantine" in html and "QUARANTINE_RUNBOOK" in html


def test_reason_table():
    assert qr.build_reason_table([]) == ""
    html = qr.build_reason_table([("TYPE not in known set", 1234)])
    assert "TYPE not in known set" in html and "1,234" in html


def test_reject_reason_order_is_first_failing_rule_wins():
    case = qr.REJECT_REASON_CASE
    order = [case.index(x) for x in ("ACTIVITY_ID is null", "LEAD_ID is null",
                                      "ACTIVITY_AT is null", "TYPE not in known set")]
    assert order == sorted(order)


# ── wiring in bronze_to_silver.py (source checks) ─────────────────────────

def test_dag_uses_the_shared_reason_case_and_replay_task():
    assert "{qr.REJECT_REASON_CASE}" in SILVER_SRC
    assert "reprocess_quarantine" in SILVER_SRC
    assert re.search(
        r"merge_lap\s*>>\s*replay_quarantine", SILVER_SRC), "replay must follow the LAP merge"
    # LAS (built from LAP) must wait for the replay so replayed rows are summarised
    assert re.search(r"replay_quarantine,\s*\]\s*>>\s*load_las_transient", SILVER_SRC)


def test_staging_snapshot_covers_every_load_in_the_window():
    snap = SILVER_SRC.split("SQL_LAP_STAGING_SNAPSHOT = USE_CONTEXT", 1)[1].split("SQL_LAP_STAGING_COUNTS", 1)[0]
    assert "MAX(INSERT_DATE)" not in snap, "validate and quarantine the whole window, not just the newest load"
    assert "dq_lap_staging_flat" in snap


def test_init_run_adds_the_resolution_columns():
    init = SILVER_SRC.split("def init_run(", 1)[1].split("def run_windowed_sql", 1)[0]
    assert "qr.DDL" in init
