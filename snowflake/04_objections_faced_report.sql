-- ================================================================
-- FILE: 04_objections_faced_report.sql
-- PURPOSE: Objections Faced Report — daily aggregation by closer
--          tracking objection types raised during strategy calls
--          as both counts and percentages
-- TARGET:  s3://sales-analyical-pipeline/dea_closer_report/objections_faced/
--          Stage: sales_analytics.gold.s3_export_stage
-- SCHEMA:  closer_name, activity_date, total_calls, money_count,
--          fear_count, hung_up_count, logistical_count,
--          no_obj_count, other_coaches_count, partner_count,
--          think_abt_it_count, time_count, trust_count,
--          value_count, not_looking_count,
--          Money%, Fear%, Hung Up%, Logistical%, No Obj%,
--          Other Coaches%, Partner%, Think Abt It%, Time%,
--          Trust%, Value%, Wsn't Lkng Fr Wht We Offrd%
-- DEPENDS: sales_analytics.gold.all_strategies_details
-- ================================================================
-- KEY DESIGN DECISIONS:
--   Objections Faced? is stored as a JSON array in Silver:
--   e.g. ["Money","Value"] or ["No Show"] or ["No Objections"]
--
--   We use LATERAL FLATTEN on the OBJECTIONS_FACED column from
--   ALL_STRATEGIES_DETAILS to explode each objection into its
--   own row, then pivot back to one row per (closer, date).
--
--   OBJECTIONS_FACED comes from cf_aIN5Gtqq... custom field
--   on 5) Strategy Call activities.
--
--   Percentage columns = count / total_calls × 100 for each
--   objection type, rounded to 1 decimal place.
--
--   "Wsn't Lkng Fr Wht We Offrd%" = "Wasn't Looking For What
--   We Offered" — abbreviated per the data model schema.
-- ================================================================
-- NOTE ON OBJECTION VALUES (from Silver data reference):
--   Common values: Money, Fear, Hung Up, Logistical,
--   No Objections, Other Coaches, Partner, Think About It,
--   Time, Trust, Value, Wasn't Looking For What We Offered,
--   No Show (recorded as objection when lead no-showed)
-- ================================================================

-- ── Step 1: Materialize report ────────────────────────────────
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.gold.objections_faced_report_export AS
WITH strategy_with_objections AS (
    SELECT
        CLOSER_NAME                AS closer_name,
        ACTIVITY_LOG_DATE::DATE    AS activity_date,
        LEAD_ID,
        ACTIVITY_ID,
        OBJECTIONS_FACED
    FROM sales_analytics.gold.all_strategies_details
    WHERE CLOSER_NAME IS NOT NULL
),
objections_exploded AS (
    SELECT
        s.closer_name, s.activity_date, s.LEAD_ID, s.ACTIVITY_ID,
        TRIM(obj.value::VARCHAR, '"') AS objection
    FROM strategy_with_objections s,
    LATERAL FLATTEN(input => TRY_PARSE_JSON(s.OBJECTIONS_FACED), OUTER => TRUE) obj
),
objections_pivot AS (
    SELECT
        closer_name, activity_date, ACTIVITY_ID, LEAD_ID,
        MAX(CASE WHEN LOWER(objection) = 'money' THEN 1 ELSE 0 END)             AS money_flag,
        MAX(CASE WHEN LOWER(objection) = 'fear' THEN 1 ELSE 0 END)              AS fear_flag,
        MAX(CASE WHEN LOWER(objection) IN ('hung up','hung_up') THEN 1 ELSE 0 END) AS hung_up_flag,
        MAX(CASE WHEN LOWER(objection) = 'logistical' THEN 1 ELSE 0 END)        AS logistical_flag,
        MAX(CASE WHEN LOWER(objection) IN ('no objections','no objection') THEN 1 ELSE 0 END) AS no_obj_flag,
        MAX(CASE WHEN LOWER(objection) IN ('other coaches','other coach') THEN 1 ELSE 0 END) AS other_coaches_flag,
        MAX(CASE WHEN LOWER(objection) = 'partner' THEN 1 ELSE 0 END)           AS partner_flag,
        MAX(CASE WHEN LOWER(objection) IN ('think about it','think abt it') THEN 1 ELSE 0 END) AS think_abt_it_flag,
        MAX(CASE WHEN LOWER(objection) = 'time' THEN 1 ELSE 0 END)              AS time_flag,
        MAX(CASE WHEN LOWER(objection) = 'trust' THEN 1 ELSE 0 END)             AS trust_flag,
        MAX(CASE WHEN LOWER(objection) = 'value' THEN 1 ELSE 0 END)             AS value_flag,
        MAX(CASE WHEN LOWER(objection) LIKE '%wasn%looking%offered%'
            OR LOWER(objection) = 'wasnt looking for what we offered'
            THEN 1 ELSE 0 END)                                                  AS not_looking_flag
    FROM objections_exploded
    GROUP BY closer_name, activity_date, ACTIVITY_ID, LEAD_ID
)
SELECT
    closer_name, activity_date,
    COUNT(*)                                               AS total_calls,
    SUM(money_flag)                                        AS money_count,
    SUM(fear_flag)                                         AS fear_count,
    SUM(hung_up_flag)                                      AS hung_up_count,
    SUM(logistical_flag)                                   AS logistical_count,
    SUM(no_obj_flag)                                       AS no_obj_count,
    SUM(other_coaches_flag)                                AS other_coaches_count,
    SUM(partner_flag)                                      AS partner_count,
    SUM(think_abt_it_flag)                                 AS think_abt_it_count,
    SUM(time_flag)                                         AS time_count,
    SUM(trust_flag)                                        AS trust_count,
    SUM(value_flag)                                        AS value_count,
    SUM(not_looking_flag)                                  AS not_looking_count,
    ROUND(SUM(money_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)          AS "Money%",
    ROUND(SUM(fear_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)           AS "Fear%",
    ROUND(SUM(hung_up_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)        AS "Hung Up%",
    ROUND(SUM(logistical_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)     AS "Logistical%",
    ROUND(SUM(no_obj_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)         AS "No Obj%",
    ROUND(SUM(other_coaches_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)  AS "Other Coaches%",
    ROUND(SUM(partner_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)        AS "Partner%",
    ROUND(SUM(think_abt_it_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)   AS "Think Abt It%",
    ROUND(SUM(time_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)           AS "Time%",
    ROUND(SUM(trust_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)          AS "Trust%",
    ROUND(SUM(value_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)          AS "Value%",
    ROUND(SUM(not_looking_flag) * 100.0 / NULLIF(COUNT(*), 0), 1)    AS "Wsn't Lkng Fr Wht We Offrd%"
FROM objections_pivot
GROUP BY closer_name, activity_date
ORDER BY closer_name, activity_date;

-- ── Step 2: Export to S3 ──────────────────────────────────────
COPY INTO @sales_analytics.gold.s3_export_stage/dea_closer_report/objections_faced/objections_faced_report.csv
FROM sales_analytics.gold.objections_faced_report_export
FILE_FORMAT = (
    TYPE                         = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                      = ('')
    EMPTY_FIELD_AS_NULL          = TRUE
    COMPRESSION=NONE
)
HEADER    = TRUE
OVERWRITE = TRUE
SINGLE    = TRUE;

-- ── Step 3: Drop temp table ───────────────────────────────────
DROP TABLE IF EXISTS sales_analytics.gold.objections_faced_report_export;

-- ── Validation ────────────────────────────────────────────────
-- SELECT
--     COUNT(*) AS report_rows,
--     COUNT(DISTINCT closer_name) AS distinct_closers,
--     COUNT(DISTINCT activity_date) AS distinct_dates,
--     SUM(total_calls) AS total_strategy_calls,
--     SUM(money_count) AS total_money_objections
-- FROM (-- paste full report query above --);