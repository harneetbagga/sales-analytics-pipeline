"""
test_gold_regression.py

Known-answer regression tests for the Gold layer, built around the
real boundary-case activities found and manually traced during this
project's timezone-bug investigation. Each test asserts a specific,
already-verified-by-hand result — if any of these ever start failing,
it means someone has silently regressed a fix that was hard-won
through hours of manual reconciliation (most likely: the
CONVERT_TIMEZONE wrapping in one of the 5 Gold views was reverted or
never redeployed after a CREATE OR REPLACE).

Normal path: runs automatically as the "run_gold_regression_tests"
task at the start of the gold_to_export DAG, as a gate before any
report is exported — credentials are pulled there from the
snowflake_default Airflow connection, no manual setup needed.

Manual/standalone run (for local debugging outside Airflow):
    export SNOWFLAKE_ACCOUNT=...
    export SNOWFLAKE_USER=...
    export SNOWFLAKE_PASSWORD=...
    export SNOWFLAKE_WAREHOUSE=sales_wh
    export SNOWFLAKE_DATABASE=sales_analytics
    pytest test_gold_regression.py -v

File location: must live at dags/tests/test_gold_regression.py,
sitting alongside gold_to_export.py in the dags/ folder — the Airflow
task locates it via a path relative to gold_to_export.py's own
location, not an absolute path.
"""

import os
import pytest
import snowflake.connector


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture(scope="module")
def sf_conn():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "sales_wh"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "sales_analytics"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    yield conn
    conn.close()


def _fetchone(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params or {})
        return cur.fetchone()
    finally:
        cur.close()


def _fetchall(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params or {})
        return cur.fetchall()
    finally:
        cur.close()


# ─────────────────────────────────────────────
# Case 1 — Chris Blanchette, Aug 22 2026, Triage Calls Booked
#
# Manually verified across Postgres, Bronze, Silver, and Gold: the raw
# activity timestamp is 2026-08-22T01:01:18 UTC, which is
# 2026-08-21 ~21:01 Eastern — the PRIOR calendar day. An unfixed,
# UTC-only date filter double-counts this: it shows up under BOTH
# Aug 21 and Aug 22 depending on which layer's raw ::DATE cast you
# use. The fixed (CONVERT_TIMEZONE) version correctly places it on
# Aug 21 only, matching the business stakeholder's confirmed expected value.
# ─────────────────────────────────────────────

CHRIS_USER_ID = "user_Dm9463lCTMPgUNfgHfr9EymJtgCeXnKfWlzudANv2W2"


def test_chris_blanchette_not_on_aug22(sf_conn):
    """
    The edge-case activity (acti_b4Lgc2ut...) must NOT appear under
    Aug 22 in INBOUND_STRATEGIES_BOOKED — it belongs to Aug 21 Eastern.
    If this fails, the CONVERT_TIMEZONE fix has regressed in
    INBOUND_STRATEGIES_BOOKED.
    """
    row = _fetchone(sf_conn, """
        SELECT COUNT(*)
        FROM sales_analytics.gold.inbound_strategies_booked
        WHERE SETTER_CLOSER_EMAIL ILIKE '%chrisblanchette%'
          AND TRIAGE_CALL_DATE::DATE = '2026-08-22'
          AND LEAD_ID = 'lead_qTldTGfpoa0nRbOgHHmkMoKijRRZXtswbC3nDZ9tirM'
    """)
    assert row[0] == 0, (
        "Edge-case activity acti_b4Lgc2ut... incorrectly appears under "
        "Aug 22 — timezone fix has regressed in INBOUND_STRATEGIES_BOOKED"
    )


def test_chris_blanchette_aug21_totals(sf_conn):
    """
    Known-answer check against the business stakeholder's confirmed real numbers for
    Chris Blanchette, Aug 21 2026: 3 booked, 3 taken, 3 strategy
    booked (strategy_call_taken is deliberately excluded here — see
    decisions-and-learnings.md, that one has an open business-logic
    question with the business stakeholder, not a fixed expected value).
    """
    row = _fetchone(sf_conn, """
        SELECT
            COUNT(*) AS inbound_booked,
            SUM(CASE WHEN TRIAGE_CALL_OUTCOME NOT IN
                ('6. No Show', '7. Reschedule', '8. Cancel')
                THEN 1 ELSE 0 END) AS inbound_taken,
            SUM(CASE WHEN STRATEGY_CALL_BOOKED THEN 1 ELSE 0 END) AS strategy_booked
        FROM sales_analytics.gold.inbound_strategies_booked
        WHERE SETTER_CLOSER_EMAIL ILIKE '%chrisblanchette%'
          AND TRIAGE_CALL_DATE::DATE = '2026-08-21'
    """)
    inbound_booked, inbound_taken, strategy_booked = row
    assert inbound_booked == 3, f"Expected inbound_booked=3, got {inbound_booked}"
    assert inbound_taken == 3, f"Expected inbound_taken=3, got {inbound_taken}"
    assert strategy_booked == 3, f"Expected strategy_booked=3, got {strategy_booked}"


# ─────────────────────────────────────────────
# Case 2 — Mahmoud Toblany, Jul 8 2026, Strategy Call
#
# Manually verified: acti_6SZ52b... is at 2026-07-09T01:01:53 UTC =
# 2026-07-08 ~21:01 Eastern. This is the sharper failure mode — an
# unfixed UTC-only filter doesn't double-count, it silently DROPS
# this activity from Jul 8 entirely (it only shows under Jul 9).
# ─────────────────────────────────────────────

MAHMOUD_ACTIVITY_ID = "acti_6SZ52bTmPkfgcZkB2MejKISjjOimEPkfSEUfQlgCguZ"


def test_mahmoud_edge_case_on_jul8_not_jul9(sf_conn):
    """
    ALL_STRATEGIES_DETAILS.ACTIVITY_LOG_DATE for this specific
    activity must resolve to Jul 8, not Jul 9. If this fails, the
    CONVERT_TIMEZONE fix has regressed in ALL_STRATEGIES_DETAILS —
    this is the same view CLOSER_REPORT and OBJECTIONS_FACED_REPORT
    both read from, so a regression here is high-blast-radius.
    """
    row = _fetchone(sf_conn, """
        SELECT ACTIVITY_LOG_DATE::DATE
        FROM sales_analytics.gold.all_strategies_details
        WHERE ACTIVITY_ID = %(activity_id)s
    """, {"activity_id": MAHMOUD_ACTIVITY_ID})
    assert row is not None, f"Activity {MAHMOUD_ACTIVITY_ID} not found at all"
    assert str(row[0]) == "2026-07-08", (
        f"Expected ACTIVITY_LOG_DATE=2026-07-08, got {row[0]} — "
        f"timezone fix has regressed in ALL_STRATEGIES_DETAILS"
    )


def test_mahmoud_jul8_strategy_call_count(sf_conn):
    """
    Two genuine Strategy Call activities exist for Mahmoud on Jul 8
    Eastern (acti_iTFTeE... and acti_6SZ52b...). An unfixed version
    would show only 1 (silently dropping the edge case).
    """
    row = _fetchone(sf_conn, """
        SELECT COUNT(*)
        FROM sales_analytics.gold.all_strategies_details
        WHERE CLOSER_EMAIL ILIKE '%mahmoud%'
          AND ACTIVITY_LOG_DATE::DATE = '2026-07-08'
    """)
    assert row[0] == 2, f"Expected 2 Strategy Calls for Mahmoud on Jul 8, got {row[0]}"


# ─────────────────────────────────────────────
# Case 3 — OUTBOUND_PROSPECT_DIALS fan-out regression guard
#
# This view previously had zero dedup and produced up to 5x duplicate
# rows per real activity (confirmed 80.7% inflation on live data
# before the fix). This test doesn't pin to a specific date/setter —
# it asserts the structural invariant the fix was supposed to
# guarantee: one row per ACTIVITY_ID, always, on any day with data.
# ─────────────────────────────────────────────

def test_outbound_prospect_dials_no_duplicate_composite_keys(sf_conn):
    """
    NOTE (2026-09, corrected): originally asserted COUNT(*) ==
    COUNT(DISTINCT ACTIVITY_ID) — but that assumes ACTIVITY_ID alone
    is unique in this view, which the multi-lead-collision
    investigation (1,313 confirmed cases) disproved: the same
    ACTIVITY_ID can legitimately belong to 2+ distinct LEAD_IDs, and
    this view correctly produces one row per (ACTIVITY_ID, LEAD_ID)
    pair. The real invariant is uniqueness on the COMPOSITE key, not
    ACTIVITY_ID alone. Confirmed via manual trace: the remaining
    "duplicates" under the old assertion were genuine distinct-lead
    pairs with identical other column values (same setter, same
    outcome, same timestamp) — correct output, not a regression.
    """
    row = _fetchone(sf_conn, """
        SELECT COUNT(*) FROM (
            SELECT ACTIVITY_ID, LEAD_ID, COUNT(*) AS c
            FROM sales_analytics.gold.outbound_prospect_dials
            GROUP BY ACTIVITY_ID, LEAD_ID
            HAVING COUNT(*) > 1
        )
    """)
    assert row[0] == 0, (
        f"Found {row[0]} (ACTIVITY_ID, LEAD_ID) pairs with genuine duplicate "
        f"rows in OUTBOUND_PROSPECT_DIALS — the fan-out dedup fix has regressed"
    )


# ─────────────────────────────────────────────
# Case 4 — LEADS_ACTIVITIES_SUMMARY multi-lead collision guard
#
# Confirmed on the rebuilt dataset: 1,313 ACTIVITY_IDs legitimately
# map to multiple LEAD_IDs. LAS's composite (ACTIVITY_ID, LEAD_ID,
# CUSTOM_ACTIVITY_OUTCOME_NAME) key must handle this without ever
# producing a true duplicate row.
# ─────────────────────────────────────────────

def test_las_no_duplicate_composite_keys(sf_conn):
    row = _fetchone(sf_conn, """
        SELECT COUNT(*) FROM (
            SELECT ACTIVITY_ID, LEAD_ID, CUSTOM_ACTIVITY_OUTCOME_NAME, COUNT(*) AS c
            FROM sales_analytics.silver.leads_activities_summary
            GROUP BY ACTIVITY_ID, LEAD_ID, CUSTOM_ACTIVITY_OUTCOME_NAME
            HAVING COUNT(*) > 1
        )
    """)
    assert row[0] == 0, (
        f"Found {row[0]} duplicate (ACTIVITY_ID, LEAD_ID, "
        f"CUSTOM_ACTIVITY_OUTCOME_NAME) combinations in LEADS_ACTIVITIES_SUMMARY "
        f"— the LEAD_ID join fix has regressed (see decisions-and-learnings.md, "
        f"'LAS fan-out fix, part 2')"
    )