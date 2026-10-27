-- Bronze lineage columns: which file a row came from and when it was loaded.
--
-- Run ONCE on an existing environment, as the owner of the Bronze tables, BEFORE deploying the
-- COPY INTO that writes them. Adding nullable columns is backward compatible: the old COPY INTO
-- (which lists its columns explicitly) keeps working until the new code is deployed.
-- Fresh builds already get these columns from snowflake_complete_environment_setup.sql.

USE DATABASE sales_analytics;
USE SCHEMA bronze;

ALTER TABLE leads_raw            ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR;
ALTER TABLE leads_raw            ADD COLUMN IF NOT EXISTS INGESTED_AT TIMESTAMP_NTZ;
ALTER TABLE lead_activites_raw   ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR;
ALTER TABLE lead_activites_raw   ADD COLUMN IF NOT EXISTS INGESTED_AT TIMESTAMP_NTZ;
ALTER TABLE close_crm_users_raw  ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR;
ALTER TABLE close_crm_users_raw  ADD COLUMN IF NOT EXISTS INGESTED_AT TIMESTAMP_NTZ;
ALTER TABLE custom_activites_raw ADD COLUMN IF NOT EXISTS SOURCE_FILE_NAME VARCHAR;
ALTER TABLE custom_activites_raw ADD COLUMN IF NOT EXISTS INGESTED_AT TIMESTAMP_NTZ;

-- Rows loaded before these columns existed have NULL in both. For clean lineage, reload Bronze
-- (TRUNCATE TABLE also clears the load metadata, so the files can be loaded again), then:
--   python scripts/backfill_bronze.py
-- Silver does not read the new columns, so it does not need to be rebuilt.
