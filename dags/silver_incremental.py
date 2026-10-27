"""
Incremental-load helpers for the bronze_to_silver DAG (no Airflow imports, so
they can be unit-tested anywhere).

IDEA
  Bronze receives one batch per source table per day (LOAD_DATE is a DATE) and
  each daily extract restates the full history of every lead it touches. So a
  Silver step only has to read the Bronze batches since the last successful
  run, not all of history. Every Silver step is a hash-gated MERGE (or SCD2
  close-out + insert), so re-reading a batch is harmless: unchanged rows are
  ignored. That makes a small overlap (lookback) a free safety margin.

CONTROL TABLE  silver.etl_watermark  (one row per Bronze source table)
  LAST_LOAD_DATE           newest Bronze LOAD_DATE fully processed (committed
                           only after the whole run succeeded)
  LAST_SUCCESS_STARTED_AT  start time of the last fully successful run; used
                           to find Silver rows touched since then
  WINDOW_START/WINDOW_END, MODE, RUN_STARTED_AT   this run's plan

MODES
  FULL         no lower bound: rebuild from all of Bronze. Used for the first
               run (bootstrap), when full_refresh=true, and as the safe default.
  SINCE        start at since_date (backfill / reprocess a date range).
  INCREMENTAL  start at LAST_LOAD_DATE - lookback_days.
"""
import re
from datetime import date, datetime, timedelta

WATERMARK_TABLE = "sales_analytics.silver.etl_watermark"

DDL_WATERMARK = f"""
CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
    SOURCE_TABLE             VARCHAR,
    LAST_LOAD_DATE           DATE,
    LAST_SUCCESS_STARTED_AT  TIMESTAMP_NTZ,
    WINDOW_START             DATE,
    WINDOW_END               DATE,
    MODE                     VARCHAR,
    RUN_STARTED_AT           TIMESTAMP_NTZ,
    UPDATED_AT               TIMESTAMP_NTZ
)
"""

SOURCES = ("lead_activites_raw", "leads_raw", "close_crm_users_raw")


def _as_date(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def compute_window(last_load_date, max_load_date, lookback_days=1,
                   full_refresh=False, since_date=None):
    """Return {'mode', 'start', 'end'} for one Bronze source (start None = no lower bound)."""
    last = _as_date(last_load_date)
    mx = _as_date(max_load_date)
    since = _as_date(since_date)
    lookback = max(int(lookback_days or 0), 0)

    if full_refresh or (since is None and last is None):
        return {"mode": "FULL", "start": None, "end": mx}
    if since is not None:
        return {"mode": "SINCE", "start": since, "end": mx}
    start = last - timedelta(days=lookback)
    if mx is not None and start > mx:      # nothing newer than the watermark
        start = mx
    return {"mode": "INCREMENTAL", "start": start, "end": mx}


def window_predicate(window, column="LOAD_DATE"):
    """SQL predicate selecting the Bronze batches in the window."""
    parts = []
    if window["start"] is not None:
        parts.append(f"{column} >= '{window['start'].isoformat()}'")
    if window["end"] is not None:
        parts.append(f"{column} <= '{window['end'].isoformat()}'")
    return " AND ".join(parts) if parts else "TRUE"


def las_scope_predicate(mode, source="lead_activites_raw"):
    """
    Which LAP rows the LAS transient rebuilds.
      FULL  -> everything.
      else  -> current rows (re)written since the last fully successful run,
               plus everything if the custom-activity definitions changed since
               then (a new outcome field affects old activities too).
    """
    if mode == "FULL":
        return "TRUE"
    last_ok = (f"(SELECT LAST_SUCCESS_STARTED_AT FROM {WATERMARK_TABLE} "
               f"WHERE SOURCE_TABLE = '{source}')")
    return (f"(lap.EFFECTIVE_START_DATE >= {last_ok} "
            f"OR EXISTS (SELECT 1 FROM sales_analytics.silver.custom_activities ca_chg "
            f"WHERE ca_chg.UPDATE_DATE >= {last_ok}))")


def apply_tokens(sql, window, mode):
    """Replace the __WIN__ and __LAS_SCOPE__ placeholders in a SQL string."""
    return (sql.replace("__WIN__", window_predicate(window))
               .replace("__LAS_SCOPE__", las_scope_predicate(mode)))


def transaction_batch(statements, use_context):
    """
    ["BEGIN", stmt, ..., "COMMIT"] for SnowflakeHook.run(batch, autocommit=True): the
    statements succeed or fail together (an open transaction is rolled back when the
    session closes after an error). The session prefix (USE WAREHOUSE/DATABASE/SCHEMA)
    is stripped because it must be run before BEGIN, and every body must be exactly one
    statement so a failure cannot leave a half-applied step behind.
    """
    body = []
    for sql in statements:
        stmt = sql.replace(use_context, "", 1).strip()
        without_comments = re.sub(r"--[^\n]*", "", stmt).strip().rstrip(";")
        if ";" in without_comments:
            raise ValueError("transaction_batch needs exactly one statement per entry")
        body.append(stmt)
    return ["BEGIN", *body, "COMMIT"]
