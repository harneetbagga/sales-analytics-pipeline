-- Bronze tables (raw JSON ingestion)
CREATE OR REPLACE TABLE sales_analytics.bronze.close_crm_users_raw (
    JSON_OBJECT        VARCHAR,
    SOURCE_INSERT_DATE VARCHAR,
    PG_INSERT_DATE     VARCHAR,
    LOAD_DATE          DATE
);

CREATE OR REPLACE TABLE sales_analytics.bronze.custom_activites_raw (
    JSON_OBJECT        VARCHAR,
    SOURCE_INSERT_DATE VARCHAR,
    PG_INSERT_DATE     VARCHAR,
    LOAD_DATE          DATE
);

CREATE OR REPLACE TABLE sales_analytics.bronze.leads_raw (
    JSON_OBJECT        VARCHAR,
    SOURCE_INSERT_DATE VARCHAR,
    PG_INSERT_DATE     VARCHAR,
    LOAD_DATE          DATE
);

CREATE OR REPLACE TABLE sales_analytics.bronze.lead_activites_raw (
    JSON_OBJECT        VARCHAR,
    SOURCE_INSERT_DATE VARCHAR,
    PG_INSERT_DATE     VARCHAR,
    LOAD_DATE          DATE
);

SELECT TABLE_NAME, COLUMN_NAME
FROM sales_analytics.information_schema.columns
WHERE TABLE_SCHEMA = 'BRONZE'
ORDER BY TABLE_NAME, ORDINAL_POSITION;

SELECT 'close_crm_users_raw' AS tbl, COUNT(*) AS cnt FROM sales_analytics.bronze.close_crm_users_raw
UNION ALL SELECT 'custom_activites_raw', COUNT(*) FROM sales_analytics.bronze.custom_activites_raw
UNION ALL SELECT 'leads_raw', COUNT(*) FROM sales_analytics.bronze.leads_raw
UNION ALL SELECT 'lead_activites_raw', COUNT(*) FROM sales_analytics.bronze.lead_activites_raw;