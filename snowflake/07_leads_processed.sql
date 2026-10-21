-- ============================================================
-- FILE: 07_leads_processed.sql
-- LAYER: Silver
-- SOURCE: bronze.leads_raw
--
-- SCOPE DECISION (2026-09): leads_raw was explicitly descoped from
-- the rebuild earlier ("forget it for now"). The business stakeholder's new Closer
-- Report requirement (grain = closer x LEAD_ROUTE) forced reopening
-- it. Rather than extract just the one field needed right now, this
-- pulls every meaningful FLAT scalar field on the lead record, since
-- we're already here and future reports will likely need others
-- (Avatar, Funnel, Lead Quality, etc. all showed up in the business stakeholder's own
-- report sample this session).
--
-- Deliberately EXCLUDES tasks[], contacts[], and opportunities[] —
-- each is an array requiring its own fan-out table, the same class
-- of work as the CustomActivity pivot. None are needed for any
-- current report. Treat as a separate, larger future build if a
-- real requirement ever needs them.
--
-- Also excludes ad-tracking noise (handl_*, utm_* fields) and
-- placeholder junk values ("_______________________" fields like
-- AD TRACKING / LEAD INSIGHT / JOURNEY OWNERS) — present in the raw
-- data but not analytically meaningful.
--
-- STRUCTURE NOTE: unlike the other 3 Bronze tables, leads_raw.JSON_OBJECT
-- is ONE lead object per row (no wrapping "data" array, no single-quote
-- repair needed — confirmed via direct inspection that this is clean,
-- valid JSON). No LATERAL FLATTEN needed here.
--
-- DEDUP: ROW_NUMBER PARTITION BY LEAD_ID ORDER BY date_updated DESC, then LOAD_DATE DESC,
-- matching every other Silver table's pattern. MD5_HASH-based CDC,
-- same as every other _PROCESSED table.
--
-- LEAD_ROUTE values are stored as raw codes ('1', '2', '3') with
-- missing values mapped to 'NO_ROUTE' — matching the business stakeholder's own report
-- format exactly (his report displays these same raw codes, never
-- translated to friendly names). Meaning of 1/2/3 is an open question
-- with the business stakeholder — see decisions-and-learnings.md. Not needed to build
-- or validate the Closer Report, since we're matching his own
-- (also-untranslated) output.
-- ============================================================

-- CHANGE (2026-10): the table is created only if missing (was CREATE OR REPLACE,
-- which emptied it every run so the MERGE never updated anything and
-- UPDATE_DATE never moved). The MERGE below now does the real change detection.

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA silver;

CREATE TABLE IF NOT EXISTS silver.leads_processed (
    LEAD_ID              VARCHAR,
    STATUS_LABEL         VARCHAR,
    LEAD_ROUTE           VARCHAR,
    AVATAR               VARCHAR,
    FUNNEL               VARCHAR,
    SETTER_ID            VARCHAR,
    LEAD_OWNER_ID        VARCHAR,
    LEAD_QUALITY         VARCHAR,
    LEAD_INTENT          VARCHAR,
    VISA_STATUS          VARCHAR,
    CURRENT_SALARY       VARCHAR,
    DATE_CREATED         TIMESTAMP_NTZ,
    DATE_UPDATED         TIMESTAMP_NTZ,
    INSERT_DATE          TIMESTAMP_NTZ,
    UPDATE_DATE          TIMESTAMP_NTZ,
    MD5_HASH             VARCHAR
);

MERGE INTO sales_analytics.silver.leads_processed AS target
USING (
    SELECT
        LEAD_ID, STATUS_LABEL, LEAD_ROUTE, AVATAR, FUNNEL, SETTER_ID,
        LEAD_OWNER_ID, LEAD_QUALITY, LEAD_INTENT, VISA_STATUS,
        CURRENT_SALARY, DATE_CREATED, DATE_UPDATED, INSERT_DATE, MD5_HASH
    FROM (
        SELECT
            j:id::VARCHAR                                       AS LEAD_ID,
            j:status_label::VARCHAR                             AS STATUS_LABEL,
            COALESCE(j:custom:lead_route::VARCHAR, 'NO_ROUTE')  AS LEAD_ROUTE,
            j:custom:Avatar::VARCHAR                            AS AVATAR,
            j:custom:Funnel::VARCHAR                            AS FUNNEL,
            j:custom:Setter::VARCHAR                            AS SETTER_ID,
            j:custom:"Lead Owner"::VARCHAR                      AS LEAD_OWNER_ID,
            j:custom:"Lead Quality"::VARCHAR                    AS LEAD_QUALITY,
            j:custom:"Lead Intent"::VARCHAR                     AS LEAD_INTENT,
            j:custom:"Visa Status"::VARCHAR                     AS VISA_STATUS,
            j:custom:"Current Salary"::VARCHAR                  AS CURRENT_SALARY,
            j:date_created::TIMESTAMP_NTZ                       AS DATE_CREATED,
            j:date_updated::TIMESTAMP_NTZ                       AS DATE_UPDATED,
            src.LOAD_DATE::TIMESTAMP_NTZ                        AS INSERT_DATE,
            MD5(
                CONCAT(
                    COALESCE(j:id::VARCHAR,               ''),
                    COALESCE(j:date_updated::VARCHAR,     ''),
                    COALESCE(j:status_label::VARCHAR,     ''),
                    COALESCE(j:custom:lead_route::VARCHAR, '')
                )
            )                                                    AS MD5_HASH,
            ROW_NUMBER() OVER (
                PARTITION BY j:id::VARCHAR
                ORDER BY j:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,
                         src.LOAD_DATE DESC
            ) AS rn
        FROM sales_analytics.bronze.leads_raw src,
             LATERAL (SELECT PARSE_JSON(src.JSON_OBJECT) AS j)
    )
    WHERE rn = 1
) AS source
ON target.LEAD_ID = source.LEAD_ID
WHEN MATCHED AND target.MD5_HASH <> source.MD5_HASH THEN
    UPDATE SET
        target.STATUS_LABEL   = source.STATUS_LABEL,
        target.LEAD_ROUTE     = source.LEAD_ROUTE,
        target.AVATAR         = source.AVATAR,
        target.FUNNEL         = source.FUNNEL,
        target.SETTER_ID      = source.SETTER_ID,
        target.LEAD_OWNER_ID  = source.LEAD_OWNER_ID,
        target.LEAD_QUALITY   = source.LEAD_QUALITY,
        target.LEAD_INTENT    = source.LEAD_INTENT,
        target.VISA_STATUS    = source.VISA_STATUS,
        target.CURRENT_SALARY = source.CURRENT_SALARY,
        target.DATE_UPDATED   = source.DATE_UPDATED,
        target.MD5_HASH       = source.MD5_HASH,
        target.UPDATE_DATE    = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        LEAD_ID, STATUS_LABEL, LEAD_ROUTE, AVATAR, FUNNEL, SETTER_ID,
        LEAD_OWNER_ID, LEAD_QUALITY, LEAD_INTENT, VISA_STATUS,
        CURRENT_SALARY, DATE_CREATED, DATE_UPDATED, INSERT_DATE,
        UPDATE_DATE, MD5_HASH
    )
    VALUES (
        source.LEAD_ID, source.STATUS_LABEL, source.LEAD_ROUTE, source.AVATAR,
        source.FUNNEL, source.SETTER_ID, source.LEAD_OWNER_ID, source.LEAD_QUALITY,
        source.LEAD_INTENT, source.VISA_STATUS, source.CURRENT_SALARY,
        source.DATE_CREATED, source.DATE_UPDATED, source.INSERT_DATE,
        CURRENT_TIMESTAMP(), source.MD5_HASH
    );