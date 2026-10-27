"""
Live parity test: an incremental Silver run must leave the same data as a full
rebuild. Needs Snowflake credentials (same env vars as the other live tests)
and SILVER_PARITY_BASELINE, the name of a zero-copy clone of silver taken
BEFORE a full-refresh run (see docs/INCREMENTAL_SILVER_RUNBOOK.md):

    CREATE SCHEMA sales_analytics.silver_baseline CLONE sales_analytics.silver;
    -- trigger bronze_to_silver with {"full_refresh": true}
    SILVER_PARITY_BASELINE=silver_baseline pytest dags/tests/test_silver_incremental_parity.py

Each table is compared in both directions with EXCEPT on its business key plus
MD5_HASH (and the derived columns for LAS), so a missing, extra or changed row
is reported with a count.
"""
import os

import pytest

DB = "sales_analytics"
CHECKS = {
    "lead_activities_processed (current rows)": (
        "lead_activities_processed",
        "ACTIVITY_ID, LEAD_ID, MD5_HASH", "IS_CURRENT = TRUE"),
    "custom_activities_all_leads_details": (
        "custom_activities_all_leads_details",
        "ACTIVITY_ID, LEAD_ID, MD5_HASH", "TRUE"),
    "lead_activities_email": (
        "lead_activities_email",
        "ACTIVITY_ID, LEAD_ID, MD5_HASH", "TRUE"),
    "leads_processed": (
        "leads_processed", "LEAD_ID, MD5_HASH", "TRUE"),
    "leads_activities_summary": (
        "leads_activities_summary",
        "ACTIVITY_ID, LEAD_ID, COALESCE(CUSTOM_ACTIVITY_OUTCOME_NAME,'') AS OUTCOME_NAME, "
        "MD5_HASH, CUSTOM_ACTIVITY_OUTCOME, DEA_INTERNAL_NAME, DEA_INTERNAL_EMAIL", "TRUE"),
}


def _connect():
    snowflake = pytest.importorskip("snowflake.connector")
    needed = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    if not all(os.environ.get(k) for k in needed):
        pytest.skip("Snowflake credentials not set")
    if not os.environ.get("SILVER_PARITY_BASELINE"):
        pytest.skip("SILVER_PARITY_BASELINE not set")
    kwargs = dict(account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
                  password=os.environ["SNOWFLAKE_PASSWORD"],
                  warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE") or "sales_wh",
                  database=DB)
    if os.environ.get("SNOWFLAKE_ROLE"):
        kwargs["role"] = os.environ["SNOWFLAKE_ROLE"]
    return snowflake.connect(**kwargs)


@pytest.mark.parametrize("label", list(CHECKS))
def test_incremental_state_equals_full_rebuild(label):
    table, cols, where = CHECKS[label]
    baseline = os.environ.get("SILVER_PARITY_BASELINE")
    if not baseline:
        pytest.skip("manual parity check: set SILVER_PARITY_BASELINE (see the docstring at the top)")
    a = f"{DB}.{baseline}.{table}"     # state left by the incremental runs
    b = f"{DB}.silver.{table}"         # state after the full refresh
    conn = _connect()
    try:
        cur = conn.cursor()
        def n(sql):
            cur.execute(sql); return cur.fetchone()[0]
        only_inc = n(f"SELECT COUNT(*) FROM (SELECT {cols} FROM {a} WHERE {where} "
                     f"EXCEPT SELECT {cols} FROM {b} WHERE {where})")
        only_full = n(f"SELECT COUNT(*) FROM (SELECT {cols} FROM {b} WHERE {where} "
                      f"EXCEPT SELECT {cols} FROM {a} WHERE {where})")
    finally:
        conn.close()
    assert only_inc == 0 and only_full == 0, (
        f"{label}: {only_inc} rows only in the incremental result, "
        f"{only_full} rows only in the full rebuild")
