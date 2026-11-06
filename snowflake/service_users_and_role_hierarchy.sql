-- service_users_and_role_hierarchy.sql
-- Run once, as ACCOUNTADMIN (or SECURITYADMIN + SYSADMIN), after rbac_rules.sql.
-- Purpose: automated access runs through service users with RSA key pairs (no passwords,
-- no personal logins), and the roles form a proper hierarchy.
--
--   pipeline_loader        writes Bronze only
--   pipeline_transformer   builds Silver and Gold; inherits pipeline_loader
--   report_reader          read-only on Gold
--   analyst_role           for human analysts; inherits report_reader only
--   sysadmin               inherits pipeline_transformer (and through it pipeline_loader)
--
-- Steps 3 and 4 need a public key: see docs/SERVICE_USERS_RUNBOOK.md for generating the pair.

USE ROLE ACCOUNTADMIN;

-- 1. Role hierarchy ------------------------------------------------------------------
GRANT ROLE pipeline_loader TO ROLE pipeline_transformer;   -- already in rbac_rules.sql; idempotent
GRANT ROLE pipeline_transformer TO ROLE sysadmin;

CREATE ROLE IF NOT EXISTS analyst_role;
GRANT ROLE report_reader TO ROLE analyst_role;
GRANT ROLE analyst_role  TO ROLE sysadmin;
-- Human analysts get analyst_role, never a pipeline role:
--   GRANT ROLE analyst_role TO USER <analyst_login>;

-- 2. Privileges the split roles need --------------------------------------------------
-- The Bronze COPY now runs as pipeline_loader, so it needs the external stage.
GRANT USAGE ON STAGE sales_analytics.bronze.s3_raw_stage TO ROLE pipeline_loader;

-- The transformer builds and replaces Silver and Gold objects (CREATE OR REPLACE VIEW, ALTER,
-- DROP), which needs ownership, not just ALL. Move ownership of what exists, and of what
-- is created later, to the transformer. COPY CURRENT GRANTS keeps existing grants.
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA sales_analytics.silver TO ROLE pipeline_transformer COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS  IN SCHEMA sales_analytics.silver TO ROLE pipeline_transformer COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA sales_analytics.gold   TO ROLE pipeline_transformer COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS  IN SCHEMA sales_analytics.gold   TO ROLE pipeline_transformer COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA sales_analytics.bronze TO ROLE pipeline_transformer COPY CURRENT GRANTS;
GRANT ALL ON FUTURE TABLES IN SCHEMA sales_analytics.silver TO ROLE pipeline_transformer;
GRANT ALL ON FUTURE TABLES IN SCHEMA sales_analytics.gold   TO ROLE pipeline_transformer;
GRANT ALL ON FUTURE VIEWS  IN SCHEMA sales_analytics.gold   TO ROLE pipeline_transformer;

-- 3. Airflow service user (MWAA) -----------------------------------------------------
-- TYPE = SERVICE: key-pair only, no password and no interactive login.
CREATE USER IF NOT EXISTS airflow_svc_user
  TYPE = SERVICE
  DEFAULT_ROLE = pipeline_transformer
  DEFAULT_WAREHOUSE = sales_wh
  DEFAULT_NAMESPACE = sales_analytics.silver
  COMMENT = 'Airflow (MWAA) service user. RSA key pair only.';
ALTER USER airflow_svc_user SET RSA_PUBLIC_KEY = '<paste public key body, no BEGIN/END lines>';
-- Granted both roles directly: the loader DAG switches to pipeline_loader explicitly, and a
-- user can only use roles granted to it, not ones it inherits through another role.
GRANT ROLE pipeline_loader      TO USER airflow_svc_user;
GRANT ROLE pipeline_transformer TO USER airflow_svc_user;

-- 4. QuickSight service user: run when the QuickSight account exists again ------------
-- (reporting_setup.sql creates it the same way for a fresh setup.)
-- If quicksight_svc already exists with a password, switch it to a key pair:
--   ALTER USER quicksight_svc SET TYPE = SERVICE;
--   ALTER USER quicksight_svc UNSET PASSWORD;
--   ALTER USER quicksight_svc SET RSA_PUBLIC_KEY = '<public key body>';

-- 5. Checks ---------------------------------------------------------------------------
SHOW GRANTS TO USER airflow_svc_user;                -- pipeline_loader, pipeline_transformer
SHOW GRANTS OF ROLE pipeline_loader;                 -- pipeline_transformer and airflow_svc_user
SHOW GRANTS OF ROLE pipeline_transformer;            -- sysadmin and airflow_svc_user
SHOW GRANTS OF ROLE report_reader;                   -- analyst_role (and the other grantees)

-- The loader can load Bronze and nothing else (the last two must FAIL):
USE ROLE pipeline_loader;
LIST @sales_analytics.bronze.s3_raw_stage;           -- works
SELECT COUNT(*) FROM sales_analytics.bronze.lead_activites_raw;   -- works
SELECT COUNT(*) FROM sales_analytics.silver.leads_processed;      -- must FAIL (no Silver access)
CREATE TABLE sales_analytics.gold.loader_should_not_create (x INT);  -- must FAIL

-- The transformer can do the rest (and reads Bronze through the loader):
USE ROLE pipeline_transformer;
SELECT COUNT(*) FROM sales_analytics.bronze.lead_activites_raw;   -- works
SELECT COUNT(*) FROM sales_analytics.silver.leads_processed;      -- works

USE ROLE ACCOUNTADMIN;
