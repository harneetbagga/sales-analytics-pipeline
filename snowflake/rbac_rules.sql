USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS reporting_wh
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Serves reports only; isolated from ingestion on sales_wh';

CREATE ROLE IF NOT EXISTS pipeline_loader;       -- writes Bronze
CREATE ROLE IF NOT EXISTS pipeline_transformer;  -- builds Silver, Gold, exports
CREATE ROLE IF NOT EXISTS report_reader;         -- read-only on Gold
GRANT ROLE pipeline_loader TO ROLE pipeline_transformer;  -- transformer inherits loader

GRANT USAGE ON WAREHOUSE sales_wh     TO ROLE pipeline_loader;
GRANT USAGE ON WAREHOUSE reporting_wh TO ROLE report_reader;
GRANT USAGE ON DATABASE sales_analytics TO ROLE pipeline_loader;
GRANT USAGE ON DATABASE sales_analytics TO ROLE pipeline_transformer;
GRANT USAGE ON DATABASE sales_analytics TO ROLE report_reader;

-- Loader: Bronze only
GRANT USAGE ON SCHEMA sales_analytics.bronze TO ROLE pipeline_loader;
GRANT ALL ON ALL TABLES IN SCHEMA sales_analytics.bronze TO ROLE pipeline_loader;
GRANT ALL ON FUTURE TABLES IN SCHEMA sales_analytics.bronze TO ROLE pipeline_loader;

-- Transformer: Silver and Gold (also needs the stage used for exports)
GRANT ALL ON SCHEMA sales_analytics.silver TO ROLE pipeline_transformer;
GRANT ALL ON SCHEMA sales_analytics.gold   TO ROLE pipeline_transformer;
GRANT ALL ON ALL TABLES IN SCHEMA sales_analytics.silver TO ROLE pipeline_transformer;
GRANT ALL ON ALL TABLES IN SCHEMA sales_analytics.gold   TO ROLE pipeline_transformer;
GRANT ALL ON ALL VIEWS  IN SCHEMA sales_analytics.gold   TO ROLE pipeline_transformer;
-- If you used the same stage for COPY INTO, also grant USAGE on the stage and the file format.

-- Reader: Gold read-only
GRANT USAGE  ON SCHEMA sales_analytics.gold TO ROLE report_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA sales_analytics.gold TO ROLE report_reader;
GRANT SELECT ON ALL VIEWS  IN SCHEMA sales_analytics.gold TO ROLE report_reader;
GRANT SELECT ON FUTURE TABLES IN SCHEMA sales_analytics.gold TO ROLE report_reader;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA sales_analytics.gold TO ROLE report_reader;

SELECT CURRENT_USER();

-- Set this to your own Snowflake login name before running.
SET pipeline_user = '<YOUR_SNOWFLAKE_USER>';
GRANT ROLE pipeline_transformer TO USER IDENTIFIER($pipeline_user);
GRANT ROLE report_reader        TO USER IDENTIFIER($pipeline_user);

GRANT OWNERSHIP ON ALL TABLES IN SCHEMA sales_analytics.bronze TO ROLE pipeline_transformer COPY CURRENT GRANTS;
-- repeat for views and tables in gold and bronze

SHOW STAGES       IN DATABASE sales_analytics;
SHOW FILE FORMATS IN DATABASE sales_analytics;

-- Bronze loading (loader role)
GRANT USAGE ON STAGE       sales_analytics.bronze.S3_RAW_STAGE  TO ROLE pipeline_loader;
GRANT USAGE ON FILE FORMAT sales_analytics.bronze.JSON_FORMAT   TO ROLE pipeline_loader;

-- Gold exports to S3 (transformer role)
GRANT USAGE ON STAGE       sales_analytics.gold.s3_export_stage          TO ROLE pipeline_transformer;
GRANT USAGE ON FILE FORMAT sales_analytics.bronze.JSON_FORMAT    TO ROLE pipeline_transformer;

SELECT referencing_object_name AS gold_object,
       referenced_schema || '.' || referenced_object_name AS source_object
FROM snowflake.account_usage.object_dependencies
WHERE referencing_database = 'SALES_ANALYTICS'
  AND referencing_schema   = 'GOLD'
ORDER BY 1, 2;

SELECT DISTINCT
       om.value:objectName::string  AS target_table,
       c.value:columnName::string   AS target_column,
       s.value:objectName::string   AS source_object,
       s.value:columnName::string   AS source_column
FROM snowflake.account_usage.access_history ah,
     LATERAL FLATTEN(input => ah.objects_modified) om,
     LATERAL FLATTEN(input => om.value:columns) c,
     LATERAL FLATTEN(input => c.value:directSources, OUTER => TRUE) s
WHERE ah.query_start_time > DATEADD('day', -2, CURRENT_TIMESTAMP())
  AND om.value:objectName::string LIKE 'SALES_ANALYTICS.SILVER.%'
ORDER BY 1, 2, 3, 4;