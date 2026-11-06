-- PIPELINE_RUN_ID audit column on the Silver tables.
--
-- You normally do NOT need to run this: bronze_to_silver.init_run adds the column to every
-- Silver table (ADD COLUMN IF NOT EXISTS) at the start of each run, as the transformer role.
-- It is here for fresh builds you want ready before the first run, and as the record of the change.
-- Bronze is separate: run snowflake/bronze_metadata_columns.sql once, as the table owner,
-- BEFORE deploying the new COPY INTO.
--
-- Rows written before this column existed keep NULL until a run touches them.

USE DATABASE sales_analytics;
USE SCHEMA silver;

ALTER TABLE close_crm_users_processed          ADD COLUMN IF NOT EXISTS PIPELINE_RUN_ID VARCHAR;
ALTER TABLE custom_activities                  ADD COLUMN IF NOT EXISTS PIPELINE_RUN_ID VARCHAR;
ALTER TABLE custom_activities_all_leads_details ADD COLUMN IF NOT EXISTS PIPELINE_RUN_ID VARCHAR;
ALTER TABLE leads_processed                    ADD COLUMN IF NOT EXISTS PIPELINE_RUN_ID VARCHAR;
ALTER TABLE lead_activities_email              ADD COLUMN IF NOT EXISTS PIPELINE_RUN_ID VARCHAR;
ALTER TABLE lead_activities_processed          ADD COLUMN IF NOT EXISTS PIPELINE_RUN_ID VARCHAR;
ALTER TABLE leads_activities_summary           ADD COLUMN IF NOT EXISTS PIPELINE_RUN_ID VARCHAR;
