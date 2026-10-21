"""
Pivot-isolation tests for the Gold layer.

Background: about 1,313 ACTIVITY_IDs belong to more than one lead. A pivot
written as

    MAX(CASE WHEN outcome_name = 'X' THEN outcome END) OVER (PARTITION BY ACTIVITY_ID)

takes the value from ALL rows that share the activity id, including the other
lead's rows. The correct pivot partitions by ACTIVITY_ID + LEAD_ID.

Two tests:
  1. test_no_pivot_partitions_by_activity_id_alone  (no database needed)
     Reads silver_to_gold.py and fails if any PARTITION BY that mentions
     ACTIVITY_ID does not also mention LEAD_ID. This catches the exact failure
     that happened before: a fix that never reached the DAG file.
  2. test_all_strategies_details_matches_silver     (needs Snowflake)
     Recomputes the strategy-call pivot straight from Silver, grouped by
     (ACTIVITY_ID, LEAD_ID), and compares it with the Gold view. Any mismatch
     means one lead picked up another lead's value.

Credentials come from the same environment variables as test_gold_regression.py
(SNOWFLAKE_ACCOUNT, _USER, _PASSWORD, _WAREHOUSE, _DATABASE, _ROLE).
"""
import os
import re

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
GOLD_DAG = os.path.join(HERE, "..", "silver_to_gold.py")


def test_no_pivot_partitions_by_activity_id_alone():
    with open(GOLD_DAG, encoding="utf-8") as f:
        src = f.read()

    # Ignore the module docstring and SQL comments; only real SQL counts.
    blank = lambda m: "\n" * m.group(0).count("\n")  # keep line numbers
    src = re.sub(r'^\s*"""[\s\S]*?"""', blank, src, count=1)
    src = re.sub(r"--[^\n]*", "", src)

    bad = []
    # Only real window clauses count: OVER (PARTITION BY ...). Prose that merely
    # mentions the words (docstrings, comments) is not an OVER clause.
    for m in re.finditer(r"OVER\s*\(\s*PARTITION\s+BY\s+([^)]*)\)", src, flags=re.IGNORECASE):
        cols = m.group(1).upper()
        if "ACTIVITY_ID" in cols and "LEAD_ID" not in cols:
            line_no = src[: m.start()].count("\n") + 1
            bad.append(f"line {line_no}: PARTITION BY {m.group(1).strip()}")

    assert not bad, (
        "These pivots partition by ACTIVITY_ID without LEAD_ID, so an activity "
        "id shared by two leads can pick up the other lead's value:\n  "
        + "\n  ".join(bad)
    )


def _connect():
    snowflake = pytest.importorskip("snowflake.connector")
    needed = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    if not all(os.environ.get(k) for k in needed):
        pytest.skip("Snowflake credentials not set")
    kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE") or "sales_wh",
        database=os.environ.get("SNOWFLAKE_DATABASE") or "sales_analytics",
    )
    if os.environ.get("SNOWFLAKE_ROLE"):
        kwargs["role"] = os.environ["SNOWFLAKE_ROLE"]
    return snowflake.connect(**kwargs)


def test_all_strategies_details_matches_silver():
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            WITH expected AS (
                SELECT ACTIVITY_ID, LEAD_ID,
                       MAX(CASE WHEN CUSTOM_ACTIVITY_OUTCOME_NAME = 'Strategy Call Outcome'
                                THEN CUSTOM_ACTIVITY_OUTCOME END) AS EXP_OUTCOME,
                       MAX(CASE WHEN CUSTOM_ACTIVITY_OUTCOME_NAME = 'Offer Presented'
                                THEN CUSTOM_ACTIVITY_OUTCOME END) AS EXP_OFFER
                FROM sales_analytics.silver.leads_activities_summary
                WHERE TYPE = 'CustomActivity'
                  AND CUSTOM_ACTIVITY = '5) Strategy Call'
                GROUP BY ACTIVITY_ID, LEAD_ID
            )
            SELECT COUNT(*) AS MISMATCHES
            FROM sales_analytics.gold.all_strategies_details v
            JOIN expected e
              ON v.ACTIVITY_ID = e.ACTIVITY_ID AND v.LEAD_ID = e.LEAD_ID
            WHERE v.STRATEGY_CALL_OUTCOME IS DISTINCT FROM e.EXP_OUTCOME
               OR v.OFFER_PRESENTED       IS DISTINCT FROM e.EXP_OFFER
        """)
        mismatches = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT ACTIVITY_ID
                FROM sales_analytics.silver.leads_activities_summary
                WHERE TYPE = 'CustomActivity' AND CUSTOM_ACTIVITY = '5) Strategy Call'
                GROUP BY ACTIVITY_ID HAVING COUNT(DISTINCT LEAD_ID) > 1
            )
        """)
        shared = cur.fetchone()[0]
    finally:
        conn.close()

    assert mismatches == 0, (
        f"{mismatches} strategy-call rows in gold.all_strategies_details differ "
        f"from the value computed per (ACTIVITY_ID, LEAD_ID) in Silver "
        f"({shared} strategy-call activity ids are shared by more than one lead)."
    )