# Snowflake rebuild runbook

Use this when the Snowflake account is replaced (for example an expired trial)
and the warehouse is empty. PostgreSQL, S3 and MWAA are untouched; only
Snowflake and the things that point at it need to be redone.

## Why a normal daily run is not enough

* Bronze loads **one** S3 date partition per run (`load_date={target_date}`).
* Silver builds from whatever is in Bronze. With an empty watermark its first
  run is a **FULL** build automatically (no `full_refresh` flag needed).
* So an empty Bronze plus a daily run gives you one day of data. Load every S3
  partition first (step 5), then run Silver once.

## Steps

1. **Create the account** and sign in as ACCOUNTADMIN. Note the account
   identifier (`orgname-accountname`).
2. **Run the setup SQL** in this order:
   1. `snowflake/snowflake_complete_environment_setup.sql` (warehouse,
      database and schemas, file format, stage, Bronze tables, repair UDFs,
      Silver tables, Gold schema). Use the storage-integration stage (Option A);
      do not paste AWS keys into the script, and never commit them.
   2. `snowflake/rbac_rules.sql` (roles, `reporting_wh`, grants).
   3. `snowflake/dq_tables.sql` (`silver.dq_rejected_rows`, `silver.dq_run_log`).
   4. `snowflake/reporting_setup.sql`, then `snowflake/serving_layer.sql` to verify.
   `silver.etl_watermark` is created by the DAG on its first run.
3. **Re-link S3.** A new account has a new storage-integration identity.
   Run `DESC INTEGRATION <name>;` and copy `STORAGE_AWS_IAM_USER_ARN` and
   `STORAGE_AWS_EXTERNAL_ID` into the trust policy of the IAM role the
   integration uses. Check with `LIST @sales_analytics.bronze.s3_raw_stage;`.
4. **Re-point the consumers.** Update the `snowflake_default` Airflow
   connection (account, user, role, warehouse) in MWAA and in local Airflow,
   and the QuickSight data source; then re-link the four datasets. If MWAA reads
   connections from Secrets Manager, update the secret.
5. **Backfill Bronze** from S3 without triggering Silver:
   ```
   python scripts/backfill_bronze.py --dry-run            # see the plan
   python scripts/backfill_bronze.py                      # load all partitions
   ```
   Check the printed totals (rows, days, min/max `LOAD_DATE`) per table.
6. **Run Silver once**, with no parameters, from the Airflow UI
   (`bronze_to_silver`). The empty watermark makes it a FULL build; it then
   triggers `silver_to_gold`, which triggers `gold_to_export`.
7. **Verify**
   * `SELECT COUNT(DISTINCT ACTIVITY_ID) FROM silver.lead_activities_processed WHERE IS_CURRENT;`
     compare with the previous figure (1,980,479 on 2026-10-19).
   * `SELECT * FROM silver.dq_run_log ORDER BY RUN_TS DESC;`
   * Gold report row counts (`gold.rpt_*`) and the four QuickSight dashboards.
   * Run the Gold regression tests (`pytest dags/tests`).
8. From the next morning, the 08:00 schedule loads one day at a time again.

## Notes

* S3 may not contain a partition for every day (a day with no source data
  produces none). Because each daily extract restates the history of the
  leads it touches, Silver deduplicates across partitions, but compare the
  step 7 count with the old figure before trusting the rebuild.
* The Bronze `COPY INTO` load history expires after 64 days. Re-running the
  backfill later without `--force` is safe; with `--force` it duplicates rows.
