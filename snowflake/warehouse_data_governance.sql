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

GRANT ROLE pipeline_transformer TO USER <your_user>;
GRANT ROLE report_reader        TO USER <your_user>;