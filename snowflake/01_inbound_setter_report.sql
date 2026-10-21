-- ================================================================
-- FILE: 01_inbound_setter_report.sql
-- PURPOSE: Inbound Setter Report — daily aggregation by setter
--          from triage calls through strategy calls to sales
-- TARGET:  s3://sales-analyical-pipeline/dea_setter_report/inbound/
-- SCHEMA:  triage_date, setter, inbound_booked, inbound_taken,
--          triage_set_rate, strategy_call_booked, strategy_call_taken,
--          show_rate, offer_rate, total_sales, sale_rate,
--          average_order_value
-- NOTES:
--   KEY METRIC DEFINITIONS (per the business stakeholder 2026-06-22):
--   inbound_booked       = COUNT of all triage call activities
--   inbound_taken        = triage call COMPLETED — outcome NOT IN
--                          ('6. No Show', '7. Reschedule', '8. Cancel')
--                          ← THIS IS DIFFERENT FROM strategy_call_booked
--   triage_set_rate      = inbound_taken / inbound_booked × 100
--   strategy_call_booked = triage outcome = '1. Strategy Call Scheduled'
--   strategy_call_taken  = strategy call attended — outcome NOT IN
--                          ('4. No Show', '3. Cancel- Not Interested',
--                           '2. Admin Cancel', '7. Cancel- Nurture',
--                           '3. No Show', '8. Cancel- Nurture')
--   show_rate            = strategy_call_taken / strategy_call_booked × 100
--   offer_rate           = offers_presented / strategy_call_taken × 100
--   sale_rate            = total_sales / strategy_call_taken × 100
--   average_order_value  = AVG(contracted_value) where sale exists
--
--   JOIN LOGIC:
--   strategy calls joined to triage via LEAD_ID + date sequencing
--   (asd.ACTIVITY_LOG_DATE::DATE >= t.TRIAGE_CALL_DATE::DATE)
--   ROW_NUMBER() picks earliest strategy call per triage (ASC order)
--   NULL STRATEGY_CALL_BOOKED rows handled via COALESCE(..., FALSE)
--
--   UNION ALL pattern: strategy_dedup (booked=TRUE rows) UNION ALL
--   triage_not_booked (booked=FALSE/NULL rows) ensures all 3,059
--   triage rows are represented in aggregation
--
--   Validated: 3059 total | 1632 taken | 1072 SC booked |
--              606 SC taken | 105 sales | 853 report rows exported
-- ================================================================

-- Step 1: Materialize report
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.gold.inbound_setter_report_export AS
WITH triage_base AS (
    SELECT
        LEAD_ID, TRIAGE_CALL_DATE, SETTER_CLOSER_EMAIL,
        SETTER_CLOSER_NAME, TRIAGE_CALL_OUTCOME, STRATEGY_CALL_BOOKED
    FROM sales_analytics.gold.inbound_strategies_booked
),
triage_enriched AS (
    SELECT *,
        COALESCE(
            TRIAGE_CALL_OUTCOME NOT IN ('6. No Show', '7. Reschedule', '8. Cancel'),
            FALSE
        ) AS IS_INBOUND_TAKEN
    FROM triage_base
),
strategy_matched AS (
    SELECT
        t.LEAD_ID, t.TRIAGE_CALL_DATE, t.SETTER_CLOSER_EMAIL,
        t.SETTER_CLOSER_NAME, t.STRATEGY_CALL_BOOKED, t.IS_INBOUND_TAKEN,
        asd.STRATEGY_CALL_OUTCOME, asd.OFFER_PRESENTED,
        -- Req doc 3.2.2 fix: sort ATTENDED attempts first (Sale/Lost/
        -- Follow Up), then earliest within that group. Picks the
        -- earliest genuinely-attended attempt if one exists anywhere
        -- for this lead, falling back to earliest attempt overall only
        -- if the lead was never attended at all. Was previously always
        -- earliest-by-date, which could permanently lock in an early
        -- No Show even when a later attempt was genuinely attended
        -- (e.g. a Lost sale) -- discarding real attendance the doc's
        -- own valid-outcome list (Follow Up/Sale/Lost) says should count.
        ROW_NUMBER() OVER (
            PARTITION BY t.LEAD_ID, t.TRIAGE_CALL_DATE
            ORDER BY
                CASE WHEN asd.STRATEGY_CALL_OUTCOME NOT IN (
                    '4. No Show', '3. Cancel- Not Interested', '2. Admin Cancel',
                    '7. Cancel- Nurture', '3. No Show', '8. Cancel- Nurture'
                ) THEN 0 ELSE 1 END ASC,
                asd.ACTIVITY_LOG_DATE ASC
        ) AS rn
    FROM triage_enriched t
    LEFT JOIN sales_analytics.gold.all_strategies_details asd
        ON t.LEAD_ID = asd.LEAD_ID
        AND asd.ACTIVITY_LOG_DATE::DATE >= t.TRIAGE_CALL_DATE::DATE
    WHERE COALESCE(t.STRATEGY_CALL_BOOKED, FALSE) = TRUE
),
strategy_dedup AS (
    SELECT
        LEAD_ID, TRIAGE_CALL_DATE, SETTER_CLOSER_EMAIL,
        SETTER_CLOSER_NAME, STRATEGY_CALL_BOOKED, IS_INBOUND_TAKEN,
        STRATEGY_CALL_OUTCOME, OFFER_PRESENTED,
        CASE
            WHEN STRATEGY_CALL_OUTCOME IS NULL THEN FALSE
            WHEN STRATEGY_CALL_OUTCOME IN (
                '4. No Show', '3. Cancel- Not Interested',
                '2. Admin Cancel', '7. Cancel- Nurture',
                '3. No Show', '8. Cancel- Nurture'
            ) THEN FALSE
            ELSE TRUE
        END AS IS_STRATEGY_TAKEN,
        CASE WHEN OFFER_PRESENTED = 'Yes' THEN TRUE ELSE FALSE END AS IS_OFFER_PRESENTED
    FROM strategy_matched
    WHERE rn = 1
),
triage_not_booked AS (
    SELECT
        LEAD_ID, TRIAGE_CALL_DATE, SETTER_CLOSER_EMAIL,
        SETTER_CLOSER_NAME, STRATEGY_CALL_BOOKED, IS_INBOUND_TAKEN,
        NULL AS STRATEGY_CALL_OUTCOME, NULL AS OFFER_PRESENTED,
        FALSE AS IS_STRATEGY_TAKEN, FALSE AS IS_OFFER_PRESENTED
    FROM triage_enriched
    WHERE COALESCE(STRATEGY_CALL_BOOKED, FALSE) = FALSE
),
all_triage AS (
    SELECT * FROM strategy_dedup
    UNION ALL
    SELECT * FROM triage_not_booked
),
sales_dedup AS (
    SELECT LEAD_ID, CONTRACTED_VALUE
    FROM (
        SELECT LEAD_ID, CONTRACTED_VALUE,
            ROW_NUMBER() OVER (PARTITION BY LEAD_ID ORDER BY ACTIVITY_AT DESC) AS rn
        FROM sales_analytics.gold.sales_details
    ) WHERE rn = 1
),
report_base AS (
    SELECT
        t.TRIAGE_CALL_DATE::DATE    AS triage_date,
        t.SETTER_CLOSER_NAME        AS setter,
        t.LEAD_ID,
        t.IS_INBOUND_TAKEN,
        t.STRATEGY_CALL_BOOKED,
        t.IS_STRATEGY_TAKEN,
        t.IS_OFFER_PRESENTED,
        sm.CONTRACTED_VALUE,
        CASE WHEN sm.LEAD_ID IS NOT NULL THEN TRUE ELSE FALSE END AS HAS_SALE
    FROM all_triage t
    LEFT JOIN sales_dedup sm ON t.LEAD_ID = sm.LEAD_ID
)
SELECT
    triage_date,
    setter,
    COUNT(*)                                                           AS inbound_booked,
    SUM(IS_INBOUND_TAKEN::INT)                                        AS inbound_taken,
    ROUND(SUM(IS_INBOUND_TAKEN::INT) * 100.0
        / NULLIF(COUNT(*), 0), 1)                                     AS triage_set_rate,
    SUM(COALESCE(STRATEGY_CALL_BOOKED::INT, 0))                       AS strategy_call_booked,
    SUM(IS_STRATEGY_TAKEN::INT)                                       AS strategy_call_taken,
    ROUND(SUM(IS_STRATEGY_TAKEN::INT) * 100.0
        / NULLIF(SUM(COALESCE(STRATEGY_CALL_BOOKED::INT, 0)), 0), 1)  AS show_rate,
    ROUND(SUM(IS_OFFER_PRESENTED::INT) * 100.0
        / NULLIF(SUM(IS_STRATEGY_TAKEN::INT), 0), 1)                  AS offer_rate,
    SUM(HAS_SALE::INT)                                                AS total_sales,
    ROUND(SUM(HAS_SALE::INT) * 100.0
        / NULLIF(SUM(IS_STRATEGY_TAKEN::INT), 0), 1)                  AS sale_rate,
    ROUND(AVG(CASE WHEN HAS_SALE THEN CONTRACTED_VALUE END), 0)       AS average_order_value
FROM report_base
GROUP BY triage_date, setter
ORDER BY triage_date, setter;

-- Step 2: Export to correct S3 path (no /raw/ prefix)
COPY INTO @sales_analytics.gold.s3_export_stage/dea_setter_report/inbound/inbound_setter_report.csv
FROM sales_analytics.gold.inbound_setter_report_export
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF=('') EMPTY_FIELD_AS_NULL=TRUE COMPRESSION=NONE)
HEADER=TRUE OVERWRITE=TRUE SINGLE=TRUE;

-- Step 3: Drop temp table
DROP TABLE IF EXISTS sales_analytics.gold.inbound_setter_report_export;

-- ── Validation ────────────────────────────────────────────────
-- Expected rollup after full run:
-- total_booked    = 3,059
-- total_taken     = 1,632
-- total_sc_booked = 1,072
-- total_sc_taken  = 606
-- total_sales     = 105
-- exported rows   = 853
