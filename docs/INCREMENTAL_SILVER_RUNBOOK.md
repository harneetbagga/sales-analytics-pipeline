# Incremental Silver: deploy, verify, backfill

## What changed
`bronze_to_silver` now reads only the Bronze batches since the last fully successful run
(watermark table `silver.etl_watermark`) instead of all history. Files:
`dags/bronze_to_silver.py`, `dags/silver_incremental.py` (new), `dags/tests/test_silver_incremental.py`
and `test_silver_incremental_parity.py` (new). No schema changes to existing tables; the watermark
table is created automatically.

## Deploy and first runs
1. Put the files in place (and copy to `~/airflow/dags` if Airflow runs from there).
2. Run `pytest dags/tests/test_silver_incremental.py` (no database needed).
3. Trigger `bronze_to_silver` normally. **The first run is a FULL run** (bootstrap, no watermark yet):
   expect the old duration. Task log for `init_run` shows `mode=FULL`.
4. Run the whole chain again the next day (or trigger again). `init_run` should now log
   `mode=INCREMENTAL window=<yesterday>..<newest>`, and the transient/merge tasks should be much faster.

## What the batches contain (check, and how to read it)
The incremental design needs only this: **every activity that is new or changed lands in the Bronze batch of
the day it changes** (whatever the extract does for that is the same for the old full approach). It does NOT
need each daily extract to restate whole lead histories. This query shows which kind of batch you have:
```sql
-- activities of leads in the newest batch that exist ONLY in older batches
-- (large = batches are deltas, which is fine; or activities were deleted in the CRM)
WITH latest AS (SELECT MAX(LOAD_DATE) AS d FROM sales_analytics.bronze.lead_activites_raw),
newest AS (
  SELECT DISTINCT act.value:id::VARCHAR AS activity_id, act.value:lead_id::VARCHAR AS lead_id
  FROM sales_analytics.bronze.lead_activites_raw src, LATERAL FLATTEN(PARSE_JSON(src.JSON_OBJECT):data) act
  WHERE src.LOAD_DATE = (SELECT d FROM latest)),
all_acts AS (
  SELECT DISTINCT act.value:id::VARCHAR AS activity_id, act.value:lead_id::VARCHAR AS lead_id
  FROM sales_analytics.bronze.lead_activites_raw src, LATERAL FLATTEN(PARSE_JSON(src.JSON_OBJECT):data) act
  WHERE act.value:lead_id::VARCHAR IN (SELECT lead_id FROM newest))
SELECT COUNT(*) AS only_in_older_batches
FROM all_acts a LEFT JOIN newest n USING (activity_id, lead_id) WHERE n.activity_id IS NULL;
```
Result on this data: 183,420. So the daily batches are mostly deltas, which suits incremental loading: each batch is
processed once and the watermark moves on. The proof that incremental equals a full rebuild is the parity test below,
not this query.

## Backfill and rebuild
Trigger `bronze_to_silver` with config:
- `{"full_refresh": true}`: rebuild from all of Bronze (use after a logic change or if in doubt).
- `{"since_date": "2026-09-01"}`: reprocess from that date (e.g. after a Bronze backfill of old dates).
- `{"lookback_days": 3}`: widen the safety overlap.

## Parity test (incremental == full rebuild)
Run after at least one incremental run:
```sql
CREATE SCHEMA sales_analytics.silver_baseline CLONE sales_analytics.silver;   -- zero-copy snapshot
```
Trigger `bronze_to_silver` with `{"full_refresh": true}`, then:
```
SILVER_PARITY_BASELINE=silver_baseline pytest dags/tests/test_silver_incremental_parity.py -v
```
Five tests (LAP current rows, custom details, email, leads, LAS) compare the snapshot with the rebuilt data in
both directions. Then `DROP SCHEMA sales_analytics.silver_baseline;`.
Note: ties on `activity_at` used to be resolved arbitrarily; they now go to the newest batch. If the very first
parity run shows a few differences, check whether they are those tie cases before treating it as a bug.

## Failure behaviour
The watermark moves only in `commit_watermark`, after every Silver step succeeded. A failed run changes nothing
there, so the next run re-reads the same batches (every step is a hash-gated MERGE, so this is safe).
