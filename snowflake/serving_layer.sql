-- 1. Gold report tables exist, are populated, and are fresh
USE ROLE pipeline_transformer;  -- or whichever role runs gold_to_export
SELECT table_name, row_count, bytes, created, last_altered
FROM sales_analytics.information_schema.tables
WHERE table_schema = 'GOLD' AND table_name LIKE 'RPT_%'
ORDER BY table_name;

SELECT 'inbound'    AS report, COUNT(*) FROM sales_analytics.gold.rpt_inbound_setter_report
UNION ALL SELECT 'outbound',   COUNT(*) FROM sales_analytics.gold.rpt_outbound_setter_report
UNION ALL SELECT 'closer',     COUNT(*) FROM sales_analytics.gold.rpt_closer_report
UNION ALL SELECT 'objections', COUNT(*) FROM sales_analytics.gold.rpt_objections_faced_report;

-- 2. The read-only role: what it can and can’t do
SHOW GRANTS TO ROLE report_reader;          -- only usage and SELECT, no write privileges
SHOW GRANTS TO USER quicksight_svc;         -- the service user holds only report_reader

USE ROLE report_reader;
USE WAREHOUSE reporting_wh;
SELECT COUNT(*) FROM sales_analytics.gold.rpt_closer_report;   -- works
DELETE FROM sales_analytics.gold.rpt_closer_report;            -- must FAIL (insufficient privileges)
SELECT * FROM sales_analytics.silver.leads_processed LIMIT 1;  -- must FAIL (no Silver access)
USE ROLE ACCOUNTADMIN;
-- Save a screenshot of the two failures. They’re the proof of least privilege.
-- 3. The dedicated reporting warehouse
SHOW WAREHOUSES LIKE 'reporting_wh';        -- size XSMALL, auto_suspend 60, auto_resume true

-- Reporting cost kept separate from ingestion (needs ACCOUNTADMIN or a monitoring grant)
SELECT warehouse_name, SUM(credits_used) AS credits
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name ORDER BY credits DESC;

-- Queries that ran on it, and who ran them
SELECT start_time, user_name, role_name, query_text
FROM snowflake.account_usage.query_history
WHERE warehouse_name = 'REPORTING_WH'
ORDER BY start_time DESC LIMIT 20;

-- account_usage views lag by up to a few hours, so recent queries may not show yet.

-- 4. The atomic, validated publish
-- The overwrite statements the DAG ran, with their timestamps
SELECT start_time, query_text, rows_inserted
FROM TABLE(sales_analytics.information_schema.query_history(RESULT_LIMIT => 1000))
WHERE query_text ILIKE 'INSERT OVERWRITE INTO%rpt_%'
ORDER BY start_time DESC;

SELECT * FROM sales_analytics.silver.etl_watermark;
SELECT 'bronze leads' AS src, MAX(LOAD_DATE) AS newest, COUNT(DISTINCT LOAD_DATE) AS days FROM sales_analytics.bronze.leads_raw
UNION ALL SELECT 'bronze activities', MAX(LOAD_DATE), COUNT(DISTINCT LOAD_DATE) FROM sales_analytics.bronze.lead_activites_raw;
SELECT 'baseline' AS s, MAX(INSERT_DATE) FROM sales_analytics.silver_baseline.leads_processed
UNION ALL SELECT 'live', MAX(INSERT_DATE) FROM sales_analytics.silver.leads_processed;

SELECT COUNT(*) AS diff_rows,
  COUNT_IF(a.STATUS_LABEL IS DISTINCT FROM b.STATUS_LABEL) AS status_diff,
  COUNT_IF(a.LEAD_ROUTE IS DISTINCT FROM b.LEAD_ROUTE) AS route_diff,
  COUNT_IF(a.DATE_UPDATED IS DISTINCT FROM b.DATE_UPDATED) AS date_updated_diff
FROM sales_analytics.silver_baseline.leads_processed a
JOIN sales_analytics.silver.leads_processed b USING (LEAD_ID)
WHERE a.MD5_HASH <> b.MD5_HASH;

SELECT a.ACTIVITY_ID, a.LEAD_ID, a.ACTIVITY_AT AS inc_activity_at, b.ACTIVITY_AT AS full_activity_at,
       a.DATE_UPDATED AS inc_updated, b.DATE_UPDATED AS full_updated
FROM sales_analytics.silver_baseline.lead_activities_processed a
JOIN sales_analytics.silver.lead_activities_processed b
  ON a.ACTIVITY_ID = b.ACTIVITY_ID AND a.LEAD_ID = b.LEAD_ID AND a.IS_CURRENT AND b.IS_CURRENT
WHERE a.MD5_HASH <> b.MD5_HASH;

SELECT COUNT(*) AS rows_differing,
  COUNT_IF(a.MD5_HASH IS DISTINCT FROM b.MD5_HASH) AS hash_diff,
  COUNT_IF(a.CUSTOM_ACTIVITY_OUTCOME IS DISTINCT FROM b.CUSTOM_ACTIVITY_OUTCOME) AS outcome_diff,
  COUNT_IF(a.DEA_INTERNAL_NAME IS DISTINCT FROM b.DEA_INTERNAL_NAME) AS name_diff,
  COUNT_IF(a.DEA_INTERNAL_EMAIL IS DISTINCT FROM b.DEA_INTERNAL_EMAIL) AS email_diff
FROM sales_analytics.silver_baseline.leads_activities_summary a
JOIN sales_analytics.silver.leads_activities_summary b
  ON a.ACTIVITY_ID = b.ACTIVITY_ID AND a.LEAD_ID = b.LEAD_ID
 AND COALESCE(a.CUSTOM_ACTIVITY_OUTCOME_NAME,'') = COALESCE(b.CUSTOM_ACTIVITY_OUTCOME_NAME,'')
WHERE a.MD5_HASH IS DISTINCT FROM b.MD5_HASH
   OR a.CUSTOM_ACTIVITY_OUTCOME IS DISTINCT FROM b.CUSTOM_ACTIVITY_OUTCOME
   OR a.DEA_INTERNAL_NAME IS DISTINCT FROM b.DEA_INTERNAL_NAME
   OR a.DEA_INTERNAL_EMAIL IS DISTINCT FROM b.DEA_INTERNAL_EMAIL;

   SELECT a.DEA_INTERNAL_EMAIL, a.DEA_INTERNAL_NAME AS incremental_name, b.DEA_INTERNAL_NAME AS full_name, COUNT(*) AS rows_affected
FROM sales_analytics.silver_baseline.leads_activities_summary a
JOIN sales_analytics.silver.leads_activities_summary b
  ON a.ACTIVITY_ID = b.ACTIVITY_ID AND a.LEAD_ID = b.LEAD_ID
 AND COALESCE(a.CUSTOM_ACTIVITY_OUTCOME_NAME,'') = COALESCE(b.CUSTOM_ACTIVITY_OUTCOME_NAME,'')
WHERE a.DEA_INTERNAL_NAME IS DISTINCT FROM b.DEA_INTERNAL_NAME
GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 20;

SELECT u.value:email::VARCHAR AS email, src.LOAD_DATE,
       u.value:date_updated::TIMESTAMP_NTZ AS date_updated,
       u.value:first_name::VARCHAR AS first_name, u.value:last_name::VARCHAR AS last_name,
       COUNT(*) AS copies
FROM sales_analytics.bronze.close_crm_users_raw src,
     LATERAL FLATTEN(input => TRY_PARSE_JSON(sales_analytics.silver.repair_json_users(src.JSON_OBJECT)):data) u
WHERE u.value:email::VARCHAR IN ('gregory@dataengineeracademy.com','paddy@dataengineeracademy.com','peterj@dataengineeracademy.com')
GROUP BY ALL ORDER BY 1, 2 DESC, 3 DESC;

DROP SCHEMA sales_analytics.silver_baseline;