-- pii_masking_policies.sql
-- Run once, as ACCOUNTADMIN, after service_users_and_role_hierarchy.sql.
-- Needs Snowflake Enterprise edition or higher (dynamic data masking). On Standard edition the
-- CREATE MASKING POLICY statement fails with "Unsupported feature"; nothing else is changed.
--
-- Purpose: lead contact details (email addresses, meeting attendees, message bodies, free-text
-- notes, raw rejected records) live in Silver. Only the pipeline roles see the real values; any
-- other role that is ever given SELECT on these columns sees ***MASKED***.
--
-- IS_ROLE_IN_SESSION('PIPELINE_TRANSFORMER') is true for the transformer and for roles that
-- inherit it (sysadmin, accountadmin), so the DAGs, the Great Expectations step, the quarantine
-- replay and admins are unaffected. The Gold layer and the four rpt_ report tables that
-- report_reader and QuickSight read contain none of these columns (dags/tests/test_masking_policies.py
-- enforces that), so dashboards are unaffected too.

USE ROLE ACCOUNTADMIN;

CREATE SCHEMA IF NOT EXISTS sales_analytics.governance;

CREATE MASKING POLICY IF NOT EXISTS sales_analytics.governance.mask_contact_string
  AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN IS_ROLE_IN_SESSION('PIPELINE_TRANSFORMER') THEN val
    WHEN val IS NULL THEN NULL
    ELSE '***MASKED***'
  END;

CREATE MASKING POLICY IF NOT EXISTS sales_analytics.governance.mask_contact_variant
  AS (val VARIANT) RETURNS VARIANT ->
  CASE
    WHEN IS_ROLE_IN_SESSION('PIPELINE_TRANSFORMER') THEN val
    WHEN val IS NULL THEN NULL
    ELSE TO_VARIANT('***MASKED***')
  END;

-- Attach (FORCE replaces a policy already attached, so the script can be re-run) ----------------
-- Email activities: sender and envelope addresses, body, open tracking, full payload.
ALTER TABLE sales_analytics.silver.lead_activities_email MODIFY COLUMN SENDER       SET MASKING POLICY sales_analytics.governance.mask_contact_string  FORCE;
ALTER TABLE sales_analytics.silver.lead_activities_email MODIFY COLUMN SENDER_JSON  SET MASKING POLICY sales_analytics.governance.mask_contact_string  FORCE;
ALTER TABLE sales_analytics.silver.lead_activities_email MODIFY COLUMN ENVELOPE_JSON SET MASKING POLICY sales_analytics.governance.mask_contact_string FORCE;
ALTER TABLE sales_analytics.silver.lead_activities_email MODIFY COLUMN BODY_TEXT    SET MASKING POLICY sales_analytics.governance.mask_contact_string  FORCE;
ALTER TABLE sales_analytics.silver.lead_activities_email MODIFY COLUMN OPENS_JSON   SET MASKING POLICY sales_analytics.governance.mask_contact_string  FORCE;
ALTER TABLE sales_analytics.silver.lead_activities_email MODIFY COLUMN JSON_PAYLOAD SET MASKING POLICY sales_analytics.governance.mask_contact_variant FORCE;

-- Activity notes and meeting attendees (current and summary tables).
ALTER TABLE sales_analytics.silver.lead_activities_processed MODIFY COLUMN TEXT      SET MASKING POLICY sales_analytics.governance.mask_contact_string FORCE;
ALTER TABLE sales_analytics.silver.lead_activities_processed MODIFY COLUMN ATTENDEES SET MASKING POLICY sales_analytics.governance.mask_contact_string FORCE;
ALTER TABLE sales_analytics.silver.leads_activities_summary  MODIFY COLUMN TEXT      SET MASKING POLICY sales_analytics.governance.mask_contact_string FORCE;
ALTER TABLE sales_analytics.silver.leads_activities_summary  MODIFY COLUMN ATTENDEES SET MASKING POLICY sales_analytics.governance.mask_contact_string FORCE;

-- Quarantine keeps the whole raw record of a rejected activity.
ALTER TABLE sales_analytics.silver.dq_rejected_rows MODIFY COLUMN RAW_RECORD SET MASKING POLICY sales_analytics.governance.mask_contact_variant FORCE;

-- Checks ---------------------------------------------------------------------------------------
-- 1. Where each policy is attached (expect 9 string columns and 2 variant columns):
SELECT policy_name, ref_entity_name, ref_column_name
FROM TABLE(sales_analytics.information_schema.policy_references(
       policy_name => 'sales_analytics.governance.mask_contact_string'))
UNION ALL
SELECT policy_name, ref_entity_name, ref_column_name
FROM TABLE(sales_analytics.information_schema.policy_references(
       policy_name => 'sales_analytics.governance.mask_contact_variant'))
ORDER BY 2, 3;

-- 2. A role that can read Silver but is not a pipeline role sees masked values.
CREATE ROLE IF NOT EXISTS pii_check_role;
GRANT USAGE ON WAREHOUSE reporting_wh TO ROLE pii_check_role;
GRANT USAGE ON DATABASE sales_analytics TO ROLE pii_check_role;
GRANT USAGE ON SCHEMA sales_analytics.silver TO ROLE pii_check_role;
GRANT SELECT ON TABLE sales_analytics.silver.lead_activities_email TO ROLE pii_check_role;
GRANT SELECT ON TABLE sales_analytics.silver.lead_activities_processed TO ROLE pii_check_role;
GRANT ROLE pii_check_role TO ROLE ACCOUNTADMIN;

USE ROLE pii_check_role;
USE WAREHOUSE reporting_wh;
SELECT SENDER, LEFT(BODY_TEXT, 20) AS body_start
FROM sales_analytics.silver.lead_activities_email WHERE SENDER IS NOT NULL LIMIT 3;     -- must show ***MASKED***
SELECT LEFT(TEXT, 20) AS text_start, ATTENDEES
FROM sales_analytics.silver.lead_activities_processed WHERE TEXT IS NOT NULL LIMIT 3;   -- must show ***MASKED***

-- 3. The transformer still sees the real values.
USE ROLE pipeline_transformer;
USE WAREHOUSE sales_wh;
SELECT SENDER, LEFT(BODY_TEXT, 20) AS body_start
FROM sales_analytics.silver.lead_activities_email WHERE SENDER IS NOT NULL LIMIT 3;     -- real values

-- 4. Clean up the test role.
USE ROLE ACCOUNTADMIN;
DROP ROLE IF EXISTS pii_check_role;
