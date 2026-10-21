-- ============================================================
-- FILE: 04_sales_details.sql
-- PURPOSE: Gold view — revenue/sale-level fact table, one row
--          per New Sale activity (primary revenue source table)
-- SOURCE:  sales_analytics.silver.leads_activities_summary
-- TARGET:  sales_analytics.gold.sales_details
-- SCHEMA (per Snowflake_Complete_Data_Model.pdf):
--   LEAD_ID, ACTIVITY_AT, SALE_STATUS, SETTER_EMAIL, SETTER_NAME,
--   CLOSER_EMAIL, CLOSER_NAME, CONTRACTED_VALUE, DATE_OF_SALE,
--   PROGRAM, CASH_COLLECTED
-- ROWS:    229 (7) New Sale + 8) New Sale [Custom Payment Plan])
-- NOTES:
--   SALE_STATUS = MEETING_STATUS from Silver ('published'/'draft')
--     — same generic Close CRM activity record status pattern
--     used in ALL_STRATEGIES_DETAILS.STATUS. Considered using
--     'Sales Type' field instead but verified its actual values
--     are 'Follow Up'/'Strategy Call' (describing call origin,
--     not a sale status) — not a fit for SALE_STATUS.
--
--   DATE_OF_SALE: COALESCE(Date of Sale cf_* field, ACTIVITY_AT::DATE).
--     IMPORTANT — verified 'Date of Sale' custom field EXISTS ONLY
--     on 7) New Sale (229/229 populated), and DOES NOT EXIST AT ALL
--     on 8) New Sale [Custom Payment Plan] (0 results on ILIKE
--     '%date%' search). The COALESCE fallback to ACTIVITY_AT::DATE
--     is therefore required, not optional, for Custom Payment Plan
--     rows. This was caught by explicitly verifying field existence
--     rather than trusting the original field-name reference list.
--
--   SETTER_EMAIL/NAME and CLOSER_EMAIL/NAME both resolved via
--     cf_Setter / cf_Closer user ID join to
--     CLOSE_CRM_USERS_PROCESSED. Setter and Closer are frequently
--     different people, confirming setter-to-closer handoff.
--
--   CONTRACTED_VALUE and CASH_COLLECTED cast via TRY_TO_NUMERIC
--     (stored as VARCHAR in Silver).
--
--   distinct_leads (216) < total_rows (229) — some leads have
--     multiple sale records (upsells, additional payment plan
--     activities on the same deal). Not deduplicated — this is
--     a full sale-event fact table.
--
--   Validated counts: 229 total rows, 216 distinct leads, 100%
--     closer_email/date_of_sale/program resolution, ~89% on
--     setter_email/contract_value/cash_collected (some legacy/
--     incomplete records).

-- LAYER: Gold (view)
-- SOURCE: silver.leads_activities_summary, silver.close_crm_users_processed
-- BATCH 2 CHANGE (timezone fix — backported here 2026-09, see
--            01_inbound_strategies_booked.sql header for full
--            context): las.ACTIVITY_AT wrapped in CONVERT_TIMEZONE
--            at entry to activity_pivot. DATE_OF_SALE's fallback
--            (f.ACTIVITY_AT::DATE) inherits the fix automatically
--            since f.ACTIVITY_AT is already Eastern-converted by
--            the time it reaches that expression.

-- NAME-FALLBACK FIX (2026-09, see 03_all_strategies_details.sql
--            header for full context): SETTER_NAME and CLOSER_NAME
--            now COALESCE to EMAIL when name fields are null.
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA gold;

CREATE OR REPLACE VIEW sales_analytics.gold.sales_details AS

WITH activity_pivot AS (
    SELECT DISTINCT
        las.LEAD_ID,
        las.ACTIVITY_ID,
        CONVERT_TIMEZONE('UTC', 'America/New_York', las.ACTIVITY_AT) AS ACTIVITY_AT,
        las.CUSTOM_ACTIVITY                   AS ACTIVITY_TYPE,
        las.MEETING_STATUS                    AS SALE_STATUS,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Contract Value'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS CONTRACT_VALUE,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Cash Collected'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS CASH_COLLECTED,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Date of Sale'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS DATE_OF_SALE_RAW,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Program'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS PROGRAM,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Setter'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS SETTER_ID,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Closer'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS CLOSER_ID
    FROM sales_analytics.silver.leads_activities_summary las
    WHERE las.TYPE = 'CustomActivity'
    AND las.CUSTOM_ACTIVITY IN ('7) New Sale', '8) New Sale [Custom Payment Plan]')
),
flat AS (
    SELECT DISTINCT * FROM activity_pivot
)

SELECT
    f.LEAD_ID,
    f.ACTIVITY_AT,
    f.SALE_STATUS,
    setter.EMAIL                                   AS SETTER_EMAIL,
    COALESCE(setter.FIRST_NAME || ' ' || setter.LAST_NAME, setter.EMAIL) AS SETTER_NAME,
    closer.EMAIL                                   AS CLOSER_EMAIL,
    COALESCE(closer.FIRST_NAME || ' ' || closer.LAST_NAME, closer.EMAIL) AS CLOSER_NAME,
    TRY_TO_NUMERIC(f.CONTRACT_VALUE)               AS CONTRACTED_VALUE,
    COALESCE(TRY_TO_DATE(f.DATE_OF_SALE_RAW), f.ACTIVITY_AT::DATE) AS DATE_OF_SALE,
    f.PROGRAM,
    TRY_TO_NUMERIC(f.CASH_COLLECTED)               AS CASH_COLLECTED
FROM flat f
LEFT JOIN sales_analytics.silver.close_crm_users_processed setter
    ON f.SETTER_ID = setter.USER_ID
LEFT JOIN sales_analytics.silver.close_crm_users_processed closer
    ON f.CLOSER_ID = closer.USER_ID;
