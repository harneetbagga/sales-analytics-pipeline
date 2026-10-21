-- Make sure Gold schema exists
CREATE SCHEMA IF NOT EXISTS sales_analytics.gold;

-- Create the export stage
CREATE OR REPLACE STAGE sales_analytics.gold.s3_export_stage
    URL = 's3://sales-analyical-pipeline/'
    STORAGE_INTEGRATION = SALES_S3_INTEGRATION
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('')
        EMPTY_FIELD_AS_NULL = TRUE
    );


SELECT 'inbound_strategies_booked' AS view_name, COUNT(*) AS cnt FROM sales_analytics.gold.inbound_strategies_booked
UNION ALL SELECT 'outbound_strategies_booked', COUNT(*) FROM sales_analytics.gold.outbound_strategies_booked
UNION ALL SELECT 'all_strategies_details', COUNT(*) FROM sales_analytics.gold.all_strategies_details
UNION ALL SELECT 'sales_details', COUNT(*) FROM sales_analytics.gold.sales_details
UNION ALL SELECT 'outbound_prospect_dials', COUNT(*) FROM sales_analytics.gold.outbound_prospect_dials;

CREATE OR REPLACE VIEW sales_analytics.gold.outbound_prospect_dials AS

SELECT
    las.LEAD_ID,
    las.ACTIVITY_ID,
    las.ACTIVITY_AT                          AS ACTIVITY_LOG_DATE,
    TO_CHAR(las.ACTIVITY_AT, 'YYYY') || '-W' ||
        LPAD(WEEKOFYEAR(las.ACTIVITY_AT)::VARCHAR, 2, '0')  AS PROSPECT_YEAR_WEEK,
    las.CUSTOM_ACTIVITY,
    las.MEETING_STATUS                       AS STATUS,
    las.CUSTOM_ACTIVITY_OUTCOME_NAME,
    las.CUSTOM_ACTIVITY_OUTCOME,
    las.DEA_INTERNAL_EMAIL                   AS SETTER_CLOSER_EMAIL,
    las.DEA_INTERNAL_NAME                    AS SETTER_CLOSER_NAME
FROM sales_analytics.silver.leads_activities_summary las
WHERE las.CUSTOM_ACTIVITY IN ('1) Prospecting Activity', '2) Prospecting Follow Up');


SELECT COLUMN_NAME
FROM sales_analytics.information_schema.columns
WHERE TABLE_SCHEMA = 'GOLD'
AND TABLE_NAME = 'OUTBOUND_PROSPECT_DIALS'
ORDER BY ORDINAL_POSITION;

SELECT COLUMN_NAME
FROM sales_analytics.information_schema.columns
WHERE TABLE_SCHEMA = 'GOLD'
AND TABLE_NAME = 'ALL_STRATEGIES_DETAILS'
ORDER BY ORDINAL_POSITION;

CREATE OR REPLACE VIEW sales_analytics.gold.all_strategies_details AS

WITH activity_pivot AS (
    SELECT DISTINCT
        las.LEAD_ID,
        las.ACTIVITY_ID,
        las.ACTIVITY_AT,
        las.CUSTOM_ACTIVITY                   AS ACTIVITY_TYPE,
        las.MEETING_STATUS                    AS STATUS,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Strategy Call Outcome'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS STRATEGY_CALL_OUTCOME,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Date of Strategy Call'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS DATE_OF_STRATEGY_CALL_RAW,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Offer Presented'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS OFFER_PRESENTED,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Objections Faced?'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS OBJECTIONS_FACED,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Setter'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS SETTER_ID,
        MAX(CASE WHEN las.CUSTOM_ACTIVITY_OUTCOME_NAME = 'Closer'
            THEN las.CUSTOM_ACTIVITY_OUTCOME END)
            OVER (PARTITION BY las.ACTIVITY_ID, las.LEAD_ID)   AS CLOSER_ID
    FROM sales_analytics.silver.leads_activities_summary las
    WHERE las.TYPE = 'CustomActivity'
    AND las.CUSTOM_ACTIVITY = '5) Strategy Call'
),
flat AS (
    SELECT DISTINCT * FROM activity_pivot
)

SELECT
    f.LEAD_ID,
    f.ACTIVITY_ID,
    f.ACTIVITY_AT                                  AS ACTIVITY_LOG_DATE,
    f.ACTIVITY_TYPE                                AS CUSTOM_ACTIVITY,
    f.STATUS,
    closer.EMAIL                                   AS CLOSER_EMAIL,
    closer.FIRST_NAME || ' ' || closer.LAST_NAME  AS CLOSER_NAME,
    f.STRATEGY_CALL_OUTCOME,
    TRY_TO_DATE(f.DATE_OF_STRATEGY_CALL_RAW)       AS DATE_OF_STRATEGY_CALL,
    f.OFFER_PRESENTED,
    f.OBJECTIONS_FACED,
    f.SETTER_ID                                    AS SETTER,
    setter.FIRST_NAME || ' ' || setter.LAST_NAME  AS SETTER_NAME
FROM flat f
LEFT JOIN sales_analytics.silver.close_crm_users_processed closer
    ON f.CLOSER_ID = closer.USER_ID
LEFT JOIN sales_analytics.silver.close_crm_users_processed setter
    ON f.SETTER_ID = setter.USER_ID;


SELECT 'inbound_strategies_booked' AS view_name, COUNT(*) AS cnt FROM sales_analytics.gold.inbound_strategies_booked
UNION ALL SELECT 'outbound_strategies_booked', COUNT(*) FROM sales_analytics.gold.outbound_strategies_booked
UNION ALL SELECT 'all_strategies_details', COUNT(*) FROM sales_analytics.gold.all_strategies_details
UNION ALL SELECT 'sales_details', COUNT(*) FROM sales_analytics.gold.sales_details
UNION ALL SELECT 'outbound_prospect_dials', COUNT(*) FROM sales_analytics.gold.outbound_prospect_dials;