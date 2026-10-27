# Quarantine runbook

Rows that break a data-quality rule are kept out of Silver and stored, untouched, in
`silver.dq_rejected_rows`. The Gold reports and dashboards exclude them, so they are
understated by the rejected fraction (`silver.dq_run_log` records it every run).
Nothing is dropped silently: every quarantined row is either **open** (`RESOLVED_AT IS NULL`)
or **resolved** with a reason.

| `RESOLUTION` | Meaning | Set by |
|---|---|---|
| `REPLAYED` | Passes the current row filter now and was merged into Silver | `bronze_to_silver` (with `reprocess_quarantine`) |
| `SUPERSEDED` | Silver already holds this activity at the same or a newer version | `bronze_to_silver`, every run |
| `ACCEPTED_LOSS` | Reviewed by a person; it cannot be loaded | by hand (below) |

## What happens automatically

- **Every run** (`validate_lap_staging`): the whole run window is checked, bad rows go to
  quarantine once each (de-duplicated by `ROW_HASH`), and the check is logged.
  Under 10% rejected is a WARN (good rows merge, an email goes out); 10% or more is a FAIL
  (nothing merges, Gold is not triggered, the watermark does not move).
- **Every run** (`replay_and_resolve_quarantine`, after the LAP merge): obsolete rows are marked
  `SUPERSEDED`. Open rows older than 7 days trigger an email until they are resolved.

## Daily routine (about five minutes, only when an alert arrives)

1. Read the alert email: counts and the reason breakdown.
2. See what is open:
   ```sql
   SELECT REJECT_REASON, COUNT(*) AS open_rows, MIN(REJECTED_AT) AS oldest
   FROM sales_analytics.silver.dq_rejected_rows
   WHERE RESOLVED_AT IS NULL
   GROUP BY 1 ORDER BY 2 DESC;
   ```
3. Decide by reason:

| Reason | Cause | Action |
|---|---|---|
| `TYPE not in known set` | Our rule is too strict (a new activity type) | Add the type to `LAP_KNOWN_TYPES` in `bronze_to_silver.py`, deploy, then replay |
| `ACTIVITY_ID is null` / `LEAD_ID is null` / `ACTIVITY_AT is null` | Source defect; the record has no identity so it cannot be merged | Report to the source owner. A corrected record arrives on its own when the lead next changes (the source re-sends a changed lead's full history, and the corrected record has a different hash). If it never arrives, mark `ACCEPTED_LOSS` |

## Replay after fixing the cause

Trigger `bronze_to_silver` with:

```json
{"reprocess_quarantine": true}
```

The run does its normal daily work, then re-runs the open rows through the current row filter
and the same SCD2 close-out and insert as the daily load (hash-based, so safe to repeat).
It never overwrites a newer current Silver row with an older quarantined version. Replayed rows
get a fresh `EFFECTIVE_START_DATE`, so `leads_activities_summary` is rebuilt for them in the same
run, and Gold follows as usual. The log line `Quarantine replay: N open before, M eligible, K open after`
shows the outcome; rows still open fail the current filter.

## Accepting a loss by hand

```sql
UPDATE sales_analytics.silver.dq_rejected_rows
SET RESOLVED_AT = CURRENT_TIMESTAMP(), RESOLUTION = 'ACCEPTED_LOSS',
    RESOLVED_RUN_ID = 'manual:<your name>',
    RESOLUTION_NOTE = '<why it cannot be loaded, who was told>'
WHERE RESOLVED_AT IS NULL
  AND REJECT_REASON = 'ACTIVITY_ID is null'
  AND REJECTED_AT < '<cut-off>';
```

## Retention

Keep resolved rows for 90 days for audit, then archive or delete them. Never delete open rows.

## Notes

- Validation now covers every load in the run window (the daily file plus the look-back overlap).
  A full build or a long `since_date` backfill therefore validates millions of staged rows and can add
  several minutes; that is expected and rare.
- Quarantine covers `lead_activities_processed` only.
- The replay runs the SCD2 close-out and the insert in one transaction, like the daily load.
