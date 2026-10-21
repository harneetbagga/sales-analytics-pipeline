# QuickSight cutover: S3 CSV -> Snowflake Gold tables

## Deploy order
1. Run `reporting_setup.sql` in Snowsight (warehouse, role, grants, quicksight_svc user).
2. Replace in Airflow dags folder: `silver_to_gold.py`, `gold_to_export.py`; add `tests/test_gold_pivot_isolation.py` next to `tests/test_gold_regression.py`.
3. Set `LEGACY_S3_EXPORT = True` in gold_to_export.py for the first runs (old CSVs keep updating, nothing breaks).
4. Run silver_to_gold, then gold_to_export. Expect: both test files pass, four `rpt_*` tables created and granted.
   - If the live pivot test FAILS, that is the old bug showing in Gold data; check silver_to_gold was really replaced and the views were recreated.
5. Verify as report_reader: `SELECT COUNT(*)` works, `DELETE` fails.

## QuickSight console
1. Manage QuickSight -> Datasets -> New dataset -> Snowflake. Server = <account>.snowflakecomputing.com, Database = SALES_ANALYTICS, Warehouse = REPORTING_WH, user quicksight_svc.
2. Pick the four `RPT_*` tables. Choose SPICE (then the DAG refresh task keeps it fresh) or Direct query (then set `QUICKSIGHT_REFRESH_SPICE = False`).
3. Easiest low-risk path: on each EXISTING dataset use "Edit dataset -> Switch source" so analyses keep their visuals; otherwise build the new datasets, copy the analysis, repoint.
4. Check every visual. Columns that were CSV strings are now typed (dates, numbers); fix field types or calculated fields that break.
5. If you create NEW datasets, put their IDs in `QUICKSIGHT_DATASETS` in gold_to_export.py.

## Retire the S3 path
After a few clean runs and a checked dashboard: set `LEGACY_S3_EXPORT = False`, remove the S3 manifests/datasets, then mark drift item 8 resolved.

## Rollback
Set `LEGACY_S3_EXPORT = True` and point the datasets back at the S3 manifests. Nothing was deleted.

## Notes
- If a report's columns change, `DROP TABLE gold.rpt_<name>` once; the next run recreates it.
- Not run against Snowflake/QuickSight from here; verify steps 4-5 on your side.