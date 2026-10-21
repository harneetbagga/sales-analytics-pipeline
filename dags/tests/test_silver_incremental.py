"""
Unit tests for the incremental-load helpers (no database needed) plus a source
check that every Silver step reading a big Bronze table is windowed.
Run:  pytest dags/tests/test_silver_incremental.py
"""
import os
import re
import sys
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from silver_incremental import (  # noqa: E402
    apply_tokens, compute_window, las_scope_predicate, window_predicate)


def test_first_run_is_full_bootstrap():
    w = compute_window(None, date(2026, 10, 14))
    assert w == {"mode": "FULL", "start": None, "end": date(2026, 10, 14)}
    assert window_predicate(w) == "LOAD_DATE <= '2026-10-14'"


def test_incremental_uses_lookback():
    w = compute_window(date(2026, 10, 13), date(2026, 10, 14), lookback_days=1)
    assert w["mode"] == "INCREMENTAL"
    assert w["start"] == date(2026, 10, 12) and w["end"] == date(2026, 10, 14)


def test_nothing_new_still_reprocesses_latest_day():
    # watermark ahead of Bronze (e.g. after a rollback): never start past the newest batch
    w = compute_window(date(2026, 10, 20), date(2026, 10, 14), lookback_days=0)
    assert w["start"] == date(2026, 10, 14)


def test_full_refresh_overrides_everything():
    w = compute_window(date(2026, 10, 13), date(2026, 10, 14), full_refresh=True,
                       since_date="2026-09-01")
    assert w["mode"] == "FULL" and w["start"] is None


def test_since_date_backfill():
    w = compute_window(date(2026, 10, 13), date(2026, 10, 14), since_date="2026-09-01")
    assert w["mode"] == "SINCE" and w["start"] == date(2026, 9, 1)


def test_negative_or_missing_lookback_is_safe():
    w = compute_window(date(2026, 10, 13), date(2026, 10, 14), lookback_days=-5)
    assert w["start"] == date(2026, 10, 13)


def test_las_scope_full_is_unrestricted_incremental_is_scoped():
    assert las_scope_predicate("FULL") == "TRUE"
    inc = las_scope_predicate("INCREMENTAL")
    assert "EFFECTIVE_START_DATE >=" in inc and "custom_activities" in inc


def test_apply_tokens_replaces_all_placeholders():
    w = compute_window(date(2026, 10, 13), date(2026, 10, 14))
    out = apply_tokens("SELECT 1 WHERE __WIN__ AND __LAS_SCOPE__", w, "INCREMENTAL")
    assert "__WIN__" not in out and "__LAS_SCOPE__" not in out


def _dag_source():
    with open(os.path.join(HERE, "..", "bronze_to_silver.py"), encoding="utf-8") as f:
        return f.read()


def test_every_big_bronze_read_is_windowed():
    """Each SQL string that reads lead_activites_raw, leads_raw or close_crm_users_raw must carry __WIN__."""
    src = _dag_source()
    offenders = []
    for m in re.finditer(r'^(SQL_[A-Z_]+) = [^\n]*?"""\n(.*?)\n"""', src, flags=re.S | re.M):
        name, body = m.group(1), m.group(2)
        if re.search(r"bronze\.(lead_activites_raw|leads_raw|close_crm_users_raw)\b", body) and "__WIN__" not in body:
            offenders.append(name)
    assert not offenders, f"These Silver steps read Bronze without a window: {offenders}"


def test_dedup_picks_latest_version():
    """The newest VERSION (date_updated) must win, then the newest batch; never event time first."""
    src = _dag_source()
    for name in ("SQL_CUSTOM_ALL_LEADS", "SQL_EMAIL"):
        block = src.split(f"{name} = ", 1)[1].split('\n"""', 1)[0]
        assert re.search(r"ORDER BY act\.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,\s*src\.LOAD_DATE DESC", block), name
    lap = src.split("SQL_LAP_SOURCE_SUBQUERY = ", 1)[1].split("SQL_LAP_CLOSE_OUT", 1)[0]
    assert re.search(r"ORDER BY act\.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,\s*src\.INSERT_DATE DESC", lap)
    leads = src.split("SQL_LEADS_PROCESSED = ", 1)[1].split('\n"""', 1)[0]
    assert re.search(r"ORDER BY j:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,\s*src\.LOAD_DATE DESC", leads)


def test_users_dedup_is_deterministic_and_latest_wins():
    """Bronze users batches hold many versions per user; ties on LOAD_DATE alone are arbitrary."""
    src = _dag_source()
    users = src.split("SQL_USERS = ", 1)[1].split('\n"""', 1)[0]
    assert re.search(r"ORDER BY u\.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,\s*src\.LOAD_DATE DESC,\s*MD5\(u\.value::VARCHAR\)", users)
    assert "__WIN__" in users
