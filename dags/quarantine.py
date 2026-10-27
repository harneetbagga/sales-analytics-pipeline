"""
Quarantine life cycle for Silver (pure SQL builders, no Airflow imports, so the
module can be unit-tested without a database).

bronze_to_silver.validate_lap_staging quarantines every row that breaks a
constraint into silver.dq_rejected_rows. This module adds the other half: every
quarantined row ends up RESOLVED with a reason, instead of sitting in the table
forever. A row with RESOLVED_AT NULL is "open".

  REPLAYED       the row passes the current row filter now (for example a new
                 activity type was added to LAP_KNOWN_TYPES), so it was merged.
  SUPERSEDED     Silver already holds the same activity at the same or a newer
                 version, so the quarantined copy is obsolete.
  ACCEPTED_LOSS  a person reviewed it and decided it cannot be loaded (for
                 example a record with no ID). Set by hand; see
                 docs/QUARANTINE_RUNBOOK.md.

Replay safety:
  * A quarantined (older) version never overwrites a newer current Silver row:
    only rows with no current Silver row at the same or a newer date_updated are
    replayed.
  * The replay runs through the same SCD2 close-out and insert statements as the
    daily load (hash-based, so repeating it is harmless), against a small
    transient table that has the same shape as the daily staging table.
"""

SCHEMA = "sales_analytics.silver"
QUARANTINE_TABLE = f"{SCHEMA}.dq_rejected_rows"
LAP_TABLE = f"{SCHEMA}.lead_activities_processed"
STAGING_FLAT_TABLE = f"{SCHEMA}.dq_lap_staging_flat"
TRANSIENT_TABLE = f"{SCHEMA}.lead_activities_processed_transient"
REPLAY_TABLE = f"{SCHEMA}.lead_activities_processed_replay"
SOURCE_NAME = "lead_activities_processed"

# Open rows older than this many days raise an email until someone resolves them.
STALE_DAYS = 7

RESOLUTIONS = ("REPLAYED", "SUPERSEDED", "ACCEPTED_LOSS")

# Existing deployments already have the table, so the new columns are added
# idempotently (init_run runs these on every run; snowflake/dq_tables.sql has the
# same columns for fresh builds).
DDL = [
    f"ALTER TABLE {QUARANTINE_TABLE} ADD COLUMN IF NOT EXISTS RESOLVED_AT TIMESTAMP_NTZ",
    f"ALTER TABLE {QUARANTINE_TABLE} ADD COLUMN IF NOT EXISTS RESOLUTION VARCHAR",
    f"ALTER TABLE {QUARANTINE_TABLE} ADD COLUMN IF NOT EXISTS RESOLVED_RUN_ID VARCHAR",
    f"ALTER TABLE {QUARANTINE_TABLE} ADD COLUMN IF NOT EXISTS RESOLUTION_NOTE VARCHAR",
]

# Why a staged row was rejected. Order matters: the first failing rule wins.
REJECT_REASON_CASE = """CASE
               WHEN ACTIVITY_ID IS NULL THEN 'ACTIVITY_ID is null'
               WHEN LEAD_ID IS NULL     THEN 'LEAD_ID is null'
               WHEN ACTIVITY_AT IS NULL THEN 'ACTIVITY_AT is null'
               ELSE 'TYPE not in known set'
           END"""


def sql_str(value):
    """Single-quoted SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def reject_breakdown_sql():
    """Rejected staged rows of the current run, grouped by reason."""
    return f"""
SELECT {REJECT_REASON_CASE} AS REASON, COUNT(*) AS ROWS_REJECTED
FROM {STAGING_FLAT_TABLE}
WHERE NOT IS_GOOD
GROUP BY 1
ORDER BY 2 DESC;
"""


def supersede_sql(run_id):
    """Resolve open rows whose activity Silver already holds at the same or a newer version."""
    return f"""
UPDATE {QUARANTINE_TABLE} AS q
SET RESOLVED_AT     = CURRENT_TIMESTAMP(),
    RESOLUTION      = 'SUPERSEDED',
    RESOLVED_RUN_ID = {sql_str(run_id)}
FROM {LAP_TABLE} AS p
WHERE q.RESOLVED_AT IS NULL
  AND q.SOURCE_TABLE = {sql_str(SOURCE_NAME)}
  AND q.ACTIVITY_ID IS NOT NULL
  AND p.ACTIVITY_ID = q.ACTIVITY_ID
  AND p.LEAD_ID     = q.LEAD_ID
  AND p.IS_CURRENT  = TRUE
  AND p.DATE_UPDATED >= q.RAW_RECORD:date_updated::TIMESTAMP_NTZ;
"""


def replay_prep_sql():
    """
    Build the replay staging table from the open quarantine rows, in the same
    shape as lead_activities_processed_transient (JSON_OBJECT:data is an array of
    activities), skipping rows Silver already has at the same or a newer version.
    """
    return f"""
CREATE OR REPLACE TRANSIENT TABLE {REPLAY_TABLE} AS
SELECT OBJECT_CONSTRUCT('data', ARRAY_CONSTRUCT(q.RAW_RECORD)) AS JSON_OBJECT,
       q.REJECTED_AT                                           AS INSERT_DATE,
       q.ROW_HASH                                              AS ROW_HASH
FROM {QUARANTINE_TABLE} q
LEFT JOIN {LAP_TABLE} p
       ON  p.ACTIVITY_ID = q.ACTIVITY_ID
       AND p.LEAD_ID     = q.LEAD_ID
       AND p.IS_CURRENT  = TRUE
       AND p.DATE_UPDATED >= q.RAW_RECORD:date_updated::TIMESTAMP_NTZ
WHERE q.RESOLVED_AT IS NULL
  AND q.SOURCE_TABLE = {sql_str(SOURCE_NAME)}
  AND p.ACTIVITY_ID IS NULL;
"""


def replay_count_sql():
    return f"SELECT COUNT(*) FROM {REPLAY_TABLE};"


def to_replay_source(lap_sql):
    """
    Point one of the daily SCD2 statements (SQL_LAP_CLOSE_OUT / SQL_LAP_INSERT_NEW)
    at the replay table. Keeping a single copy of the SCD2 logic means a replayed
    row goes through exactly the same row filter, hash and tie-break as a daily one.
    """
    if TRANSIENT_TABLE not in lap_sql:
        raise ValueError("statement does not read the LAP staging table; cannot replay with it")
    return lap_sql.replace(TRANSIENT_TABLE, REPLAY_TABLE)


def replay_resolve_sql(run_id, good_row_filter):
    """Mark the replayed rows that now pass the row filter as REPLAYED."""
    return f"""
UPDATE {QUARANTINE_TABLE} AS q
SET RESOLVED_AT     = CURRENT_TIMESTAMP(),
    RESOLUTION      = 'REPLAYED',
    RESOLVED_RUN_ID = {sql_str(run_id)}
WHERE q.RESOLVED_AT IS NULL
  AND q.SOURCE_TABLE = {sql_str(SOURCE_NAME)}
  AND q.ROW_HASH IN (
      SELECT r.ROW_HASH
      FROM {REPLAY_TABLE} r,
           LATERAL FLATTEN(input => r.JSON_OBJECT:data) act
      WHERE {good_row_filter}
  );
"""


def open_count_sql():
    return (f"SELECT COUNT(*) FROM {QUARANTINE_TABLE} "
            f"WHERE RESOLVED_AT IS NULL AND SOURCE_TABLE = {sql_str(SOURCE_NAME)};")


def stale_sql(days=STALE_DAYS):
    """Open rows older than `days`, grouped by reason."""
    return f"""
SELECT REJECT_REASON, COUNT(*) AS OPEN_ROWS, MIN(REJECTED_AT) AS OLDEST
FROM {QUARANTINE_TABLE}
WHERE RESOLVED_AT IS NULL
  AND SOURCE_TABLE = {sql_str(SOURCE_NAME)}
  AND REJECTED_AT < DATEADD(day, -{int(days)}, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
GROUP BY 1
ORDER BY 2 DESC;
"""


def build_stale_email(rows, days=STALE_DAYS):
    """HTML body listing open quarantine rows that have waited longer than `days`."""
    body = "".join(
        f"<tr><td>{reason}</td><td align='right'>{int(n):,}</td><td>{oldest}</td></tr>"
        for reason, n, oldest in rows)
    total = sum(int(n) for _, n, _ in rows)
    return f"""
<p>{total:,} quarantined lead-activity row(s) have been open for more than {days} days.
They are excluded from Silver, Gold and the dashboards until resolved.</p>
<table border='1' cellpadding='4' cellspacing='0'>
<tr><th>Reason</th><th>Open rows</th><th>Oldest</th></tr>{body}</table>
<p>Review the rows, then either fix the cause and replay them (trigger bronze_to_silver with
<code>{{"reprocess_quarantine": true}}</code>) or mark them ACCEPTED_LOSS with a note.
Steps: docs/QUARANTINE_RUNBOOK.md</p>
"""


def build_reason_table(reasons):
    """HTML table of (reason, count) for the data-quality alert email."""
    if not reasons:
        return ""
    body = "".join(
        f"<tr><td>{reason}</td><td align='right'>{int(n):,}</td></tr>" for reason, n in reasons)
    return ("<table border='1' cellpadding='4' cellspacing='0'>"
            f"<tr><th>Why rejected</th><th>Rows</th></tr>{body}</table>")
