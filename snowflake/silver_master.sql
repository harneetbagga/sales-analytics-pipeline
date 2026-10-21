-- ============================================================
-- FILE: silver_master.sql
-- PURPOSE: Master Silver layer execution file
--          Run all transformations in dependency order
-- PROJECT: Sales Analytics Pipeline
-- STACK:   PostgreSQL → S3 → Snowflake (Bronze/Silver/Gold)
-- SCHEMA:  sales_analytics.silver
-- ============================================================

-- ── Execution order and dependencies ─────────────────────
--
-- 00_udfs.sql                          (no dependencies)
--    ↓
-- 01_close_crm_users_processed.sql     (depends on: bronze.close_crm_users_raw)
--    ↓
-- 02_custom_activities.sql             (depends on: bronze.custom_activites_raw)
--    ↓
-- 03_custom_activities_all_leads_details.sql (depends on: bronze.lead_activites_raw)
--    ↓
-- 04_lead_activities_processed.sql     (depends on: bronze.lead_activites_raw)
--    ↓
-- 05_lead_activities_email.sql         (depends on: bronze.lead_activites_raw)
--    ↓
-- 06_leads_activities_summary.sql      (depends on: 01, 02, 03, 04 — PENDING SME)

-- ── Silver table inventory ────────────────────────────────
--
-- PERMANENT TABLES:
--   CLOSE_CRM_USERS_PROCESSED          381 rows
--   CUSTOM_ACTIVITIES                  205 rows
--   LEAD_ACTIVITIES_PROCESSED          900,908 rows
--   LEAD_ACTIVITIES_EMAIL              153,230 rows
--   CUSTOM_ACTIVITIES_ALL_LEADS_DETAILS 28,237 rows
--   LEADS_ACTIVITIES_SUMMARY           PENDING
--
-- TRANSIENT TABLES (staging only, no Fail-safe):
--   CUSTOM_ACTIVITIES_TRANSIENT        (used by 02)
--   LEAD_ACTIVITIES_PROCESSED_TRANSIENT (used by 04)
--   LEADS_ACTIVITIES_SUMMARY_TRANSIENT (used by 06 — PENDING)

-- ── UDF inventory ────────────────────────────────────────
--
--   sales_analytics.silver.repair_json_users(VARCHAR)
--     → Used by: 01_close_crm_users_processed.sql
--     → Handles: single-quoted JSON, apostrophes in names
--
--   sales_analytics.silver.repair_json_activities(VARCHAR)
--     → Used by: 02_custom_activities.sql
--     → Handles: same + neutralizes description fields with
--                embedded single quotes

-- ============================================================
-- STEP 0: Create/replace UDFs
-- ============================================================
-- See: 00_udfs.sql


-- ============================================================
-- STEP 1: CLOSE_CRM_USERS_PROCESSED
-- ============================================================
-- See: 01_close_crm_users_processed.sql


-- ============================================================
-- STEP 2: CUSTOM_ACTIVITIES
-- ============================================================
-- See: 02_custom_activities.sql


-- ============================================================
-- STEP 3: CUSTOM_ACTIVITIES_ALL_LEADS_DETAILS
-- ============================================================
-- See: 03_custom_activities_all_leads_details.sql


-- ============================================================
-- STEP 4: LEAD_ACTIVITIES_PROCESSED (transient + merge)
-- ============================================================
-- See: 04_lead_activities_processed.sql


-- ============================================================
-- STEP 5: LEAD_ACTIVITIES_EMAIL
-- ============================================================
-- See: 05_lead_activities_email.sql


-- ============================================================
-- STEP 6: LEADS_ACTIVITIES_SUMMARY (PENDING SME CONFIRMATION)
-- ============================================================
-- See: 06_leads_activities_summary.sql


-- ── Full validation suite ────────────────────────────────
SELECT 'CLOSE_CRM_USERS_PROCESSED'          AS table_name, COUNT(*) AS row_count FROM sales_analytics.silver.close_crm_users_processed
UNION ALL
SELECT 'CUSTOM_ACTIVITIES'                  AS table_name, COUNT(*) AS row_count FROM sales_analytics.silver.custom_activities
UNION ALL
SELECT 'CUSTOM_ACTIVITIES_ALL_LEADS_DETAILS'AS table_name, COUNT(*) AS row_count FROM sales_analytics.silver.custom_activities_all_leads_details
UNION ALL
SELECT 'LEAD_ACTIVITIES_PROCESSED'          AS table_name, COUNT(*) AS row_count FROM sales_analytics.silver.lead_activities_processed
UNION ALL
SELECT 'LEAD_ACTIVITIES_EMAIL'              AS table_name, COUNT(*) AS row_count FROM sales_analytics.silver.lead_activities_email
ORDER BY table_name;

-- Expected results:
-- CLOSE_CRM_USERS_PROCESSED            381
-- CUSTOM_ACTIVITIES                    205
-- CUSTOM_ACTIVITIES_ALL_LEADS_DETAILS  28237
-- LEAD_ACTIVITIES_EMAIL                153230
-- LEAD_ACTIVITIES_PROCESSED            900908
