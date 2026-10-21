-- reporting_setup.sql  (run once, as ACCOUNTADMIN or SECURITYADMIN + SYSADMIN)
-- Purpose: QuickSight reads the Gold rpt_ tables through reporting_wh as the
-- read-only report_reader role. Adjust names if yours differ.

-- 1. Dedicated reporting warehouse (isolated from ingestion cost)
CREATE WAREHOUSE IF NOT EXISTS reporting_wh
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

-- 2. Read-only role: warehouse + database + schema usage, SELECT on report tables only
CREATE ROLE IF NOT EXISTS report_reader;
GRANT USAGE ON WAREHOUSE reporting_wh              TO ROLE report_reader;
GRANT USAGE ON DATABASE  sales_analytics           TO ROLE report_reader;
GRANT USAGE ON SCHEMA    sales_analytics.gold      TO ROLE report_reader;
-- The DAG grants SELECT on each rpt_ table after publishing. These cover tables
-- that already exist and any created later:
GRANT SELECT ON ALL    TABLES IN SCHEMA sales_analytics.gold TO ROLE report_reader;
GRANT SELECT ON FUTURE TABLES IN SCHEMA sales_analytics.gold TO ROLE report_reader;

-- 3. The role that runs the DAG must be able to create tables in gold and grant
--    on them. If gold_to_export runs as pipeline_transformer, it needs:
GRANT CREATE TABLE ON SCHEMA sales_analytics.gold TO ROLE pipeline_transformer;
-- (it owns the tables it creates, so it can GRANT SELECT on them)

-- 4. Dedicated QuickSight service user (password or, better, key-pair)
CREATE USER IF NOT EXISTS quicksight_svc
  DEFAULT_ROLE = report_reader DEFAULT_WAREHOUSE = reporting_wh
  PASSWORD = '<set-a-strong-password>' MUST_CHANGE_PASSWORD = FALSE;
GRANT ROLE report_reader TO USER quicksight_svc;

-- 5. Checks
SHOW GRANTS TO ROLE report_reader;
-- After the first gold_to_export run:
--   SELECT COUNT(*) FROM sales_analytics.gold.rpt_closer_report;   -- as report_reader
-- Prove it is read-only (this must FAIL):
--   DELETE FROM sales_analytics.gold.rpt_closer_report;