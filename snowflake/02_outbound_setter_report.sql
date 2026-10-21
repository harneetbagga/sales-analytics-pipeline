-- ================================================================
-- FILE: 02_outbound_setter_report.sql
-- PURPOSE: Outbound Setter Report — daily aggregation by setter
--          tracking dial volume, set rate, show rate, and sales
--          from outbound prospecting activities
-- TARGET:  s3://sales-analyical-pipeline/dea_setter_report/outbound/
--          Stage: sales_analytics.gold.s3_export_stage
-- SCHEMA:  dial_date, setter, total_outbound_calls, total_leads_touched,
--          outbound_set, total_closer_show, total_offer, total_sale,
--          dial_to_set_rate, set_to_show_rate, show_to_sale_rate,
--          total_revenue, average_order_value
-- DEPENDS: sales_analytics.gold.outbound_prospect_dials
--          sales_analytics.gold.all_strategies_details
--          sales_analytics.gold.sales_details
-- ================================================================
-- KEY METRIC DEFINITIONS:
--   total_outbound_calls = COUNT DISTINCT ACTIVITY_ID (raw dials)
--                          Includes both 1) Prospecting Activity
--                          AND 2) Prospecting Follow Up
--   total_leads_touched  = COUNT DISTINCT LEAD_ID per setter per day
--   outbound_set         = leads where Prospecting Call Outcome
--                          = '2. Strategy Call Scheduled'
--   total_closer_show    = strategy calls attended (NOT IN no-show/cancel)
--   total_offer          = strategy calls where Offer Presented = 'Yes'
--   total_sale           = sales from outbound-originated leads
--   dial_to_set_rate     = outbound_set / total_outbound_calls × 100
--   set_to_show_rate     = total_closer_show / outbound_set × 100
--   show_to_sale_rate    = total_sale / total_closer_show × 100
--   total_revenue        = SUM(contracted_value) for outbound sales
--   average_order_value  = AVG(contracted_value) where sale exists
-- ================================================================
-- KEY DESIGN DECISIONS:
--   OUTBOUND_PROSPECT_DIALS is unpivoted (one row per cf_* field
--   per activity). Filter to CUSTOM_ACTIVITY_OUTCOME_NAME =
--   'Prospecting Call Outcome' gives one row per activity for
--   counting. ACTIVITY_ID added to the view for correct dedup
--   (some leads had both Prospecting Activity AND Follow Up on
--   same day, both with 'Prospecting Call Outcome' field).
--   Strategy calls joined via LEAD_ID + date sequencing
--   (asd.ACTIVITY_LOG_DATE::DATE >= os.dial_date)
-- ================================================================
-- BUG FIX (2026-09, found during SQL walkthrough): the original
-- query built a single `combined` CTE by LEFT JOINing lead-grain
-- facts (outbound_set / strategy_dedup / sales_dedup — each one row
-- per lead) onto `dial_base`, which is CALL-grain (one row per
-- individual dial). That duplicated every lead-level fact across
-- every call belonging to that lead. `total_leads_touched` and
-- `outbound_set` were protected from this because they used
-- COUNT(DISTINCT lead_id) — but total_closer_show, total_offer,
-- total_sale, and total_revenue used plain SUM() over the same
-- duplicated rows, silently multiplying every one of those numbers
-- (and real dollar revenue) by however many times that lead was
-- dialed. A lead dialed 3 times who converted to a sale would count
-- as 3 shows, 3 sales, and 3x the actual contracted value.
--
-- FIX: split into two separately-aggregated CTEs at their correct
-- native grain — call_agg (call-grain: total_outbound_calls,
-- total_leads_touched, via dial_base directly) and lead_agg
-- (lead-grain: outbound_set, total_closer_show, total_offer,
-- total_sale, total_revenue, via a new lead_level CTE that has
-- exactly one row per (dial_date, setter, LEAD_ID) that was set —
-- built directly from outbound_set/strategy_dedup/sales_dedup, never
-- touching the call-duplicated dial_base rows). The two aggregates
-- are joined together at (dial_date, setter) only in the final
-- SELECT, so no metric is ever summed over duplicated rows.
-- ================================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA gold;
 
-- Step 1: Materialize
CREATE OR REPLACE TRANSIENT TABLE sales_analytics.gold.outbound_setter_report_export AS
WITH
dial_base AS (
    SELECT DISTINCT
        ACTIVITY_LOG_DATE::DATE   AS dial_date,
        LEAD_ID,
        ACTIVITY_ID,
        SETTER_CLOSER_NAME        AS setter,
        SETTER_CLOSER_EMAIL       AS setter_email,
        CUSTOM_ACTIVITY_OUTCOME   AS prospecting_outcome
    FROM sales_analytics.gold.outbound_prospect_dials
    WHERE CUSTOM_ACTIVITY_OUTCOME_NAME = 'Prospecting Call Outcome'
),
outbound_set AS (
    SELECT DISTINCT dial_date, LEAD_ID, setter
    FROM dial_base
    WHERE prospecting_outcome = '2. Strategy Call Scheduled'
),
strategy_for_outbound AS (
    SELECT
        os.LEAD_ID, os.dial_date, os.setter,
        asd.STRATEGY_CALL_OUTCOME, asd.OFFER_PRESENTED,
        CASE
            WHEN asd.STRATEGY_CALL_OUTCOME IS NULL THEN FALSE
            WHEN asd.STRATEGY_CALL_OUTCOME IN (
                '4. No Show', '3. Cancel- Not Interested',
                '2. Admin Cancel', '7. Cancel- Nurture',
                '3. No Show', '8. Cancel- Nurture'
            ) THEN FALSE
            ELSE TRUE
        END AS IS_STRATEGY_TAKEN,
        CASE WHEN asd.OFFER_PRESENTED = 'Yes' THEN TRUE ELSE FALSE END AS IS_OFFER_PRESENTED,
        -- Req doc 3.2.2 fix: same reasoning as the Inbound report --
        -- sort ATTENDED attempts (Sale/Lost/Follow Up) first, then
        -- earliest within that group, so a later genuinely-attended
        -- attempt is never discarded in favor of an earlier No Show.
        ROW_NUMBER() OVER (
            PARTITION BY os.LEAD_ID, os.dial_date
            ORDER BY
                CASE WHEN asd.STRATEGY_CALL_OUTCOME NOT IN (
                    '4. No Show', '3. Cancel- Not Interested', '2. Admin Cancel',
                    '7. Cancel- Nurture', '3. No Show', '8. Cancel- Nurture'
                ) THEN 0 ELSE 1 END ASC,
                asd.ACTIVITY_LOG_DATE ASC
        ) AS rn
    FROM outbound_set os
    LEFT JOIN sales_analytics.gold.all_strategies_details asd
        ON os.LEAD_ID = asd.LEAD_ID
        AND asd.ACTIVITY_LOG_DATE::DATE >= os.dial_date
),
strategy_dedup AS (SELECT * FROM strategy_for_outbound WHERE rn = 1),
sales_dedup AS (
    SELECT LEAD_ID, CONTRACTED_VALUE
    FROM (
        SELECT LEAD_ID, CONTRACTED_VALUE,
            ROW_NUMBER() OVER (PARTITION BY LEAD_ID ORDER BY ACTIVITY_AT DESC) AS rn
        FROM sales_analytics.gold.sales_details
    ) WHERE rn = 1
),

-- Lead-grain funnel facts: exactly ONE row per (dial_date, setter, LEAD_ID)
-- that was set — built directly from the already-deduped lead-grain CTEs
-- above, never from the call-duplicated dial_base rows.
lead_level AS (
    SELECT
        os.dial_date,
        os.setter,
        os.LEAD_ID,
        COALESCE(sd.IS_STRATEGY_TAKEN, FALSE)  AS IS_STRATEGY_TAKEN,
        COALESCE(sd.IS_OFFER_PRESENTED, FALSE) AS IS_OFFER_PRESENTED,
        CASE WHEN sm.LEAD_ID IS NOT NULL
             AND COALESCE(sd.IS_STRATEGY_TAKEN, FALSE) THEN TRUE ELSE FALSE END AS HAS_SALE,
        sm.CONTRACTED_VALUE
    FROM outbound_set os
    LEFT JOIN strategy_dedup sd ON os.LEAD_ID = sd.LEAD_ID AND os.dial_date = sd.dial_date
    LEFT JOIN sales_dedup sm ON sd.LEAD_ID = sm.LEAD_ID
        AND COALESCE(sd.IS_STRATEGY_TAKEN, FALSE) = TRUE
),

-- Call-grain volume metrics, aggregated at their native grain
call_agg AS (
    SELECT
        dial_date,
        setter,
        COUNT(DISTINCT ACTIVITY_ID) AS total_outbound_calls,
        COUNT(DISTINCT LEAD_ID)     AS total_leads_touched
    FROM dial_base
    GROUP BY dial_date, setter
),

-- Lead-grain funnel/revenue metrics, aggregated at THEIR native grain —
-- SUM() here is safe because lead_level has exactly one row per lead,
-- no duplication to inflate.
lead_agg AS (
    SELECT
        dial_date,
        setter,
        COUNT(DISTINCT LEAD_ID)                              AS outbound_set,
        SUM(IS_STRATEGY_TAKEN::INT)                          AS total_closer_show,
        SUM(IS_OFFER_PRESENTED::INT)                         AS total_offer,
        SUM(HAS_SALE::INT)                                   AS total_sale,
        SUM(CASE WHEN HAS_SALE THEN CONTRACTED_VALUE ELSE 0 END) AS total_revenue
    FROM lead_level
    GROUP BY dial_date, setter
)

SELECT
    c.dial_date,
    c.setter,
    c.total_outbound_calls,
    c.total_leads_touched,
    COALESCE(l.outbound_set, 0)        AS outbound_set,
    COALESCE(l.total_closer_show, 0)   AS total_closer_show,
    COALESCE(l.total_offer, 0)         AS total_offer,
    COALESCE(l.total_sale, 0)          AS total_sale,
    ROUND(COALESCE(l.outbound_set, 0) * 100.0
        / NULLIF(c.total_outbound_calls, 0), 1)              AS dial_to_set_rate,
    ROUND(COALESCE(l.total_closer_show, 0) * 100.0
        / NULLIF(l.outbound_set, 0), 1)                       AS set_to_show_rate,
    ROUND(COALESCE(l.total_sale, 0) * 100.0
        / NULLIF(l.total_closer_show, 0), 1)                  AS show_to_sale_rate,
    COALESCE(l.total_revenue, 0)       AS total_revenue,
    ROUND(l.total_revenue / NULLIF(l.total_sale, 0), 0)       AS average_order_value
FROM call_agg c
LEFT JOIN lead_agg l ON c.dial_date = l.dial_date AND c.setter = l.setter
ORDER BY c.dial_date, c.setter;

-- Step 2: Export
COPY INTO @sales_analytics.gold.s3_export_stage/dea_setter_report/outbound/outbound_setter_report.csv
FROM sales_analytics.gold.outbound_setter_report_export
FILE_FORMAT = (
    TYPE                         = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                      = ('')
    EMPTY_FIELD_AS_NULL          = TRUE
    COMPRESSION                  = NONE
)
HEADER    = TRUE
OVERWRITE = TRUE
SINGLE    = TRUE;
 
-- Step 3: Cleanup
DROP TABLE IF EXISTS sales_analytics.gold.outbound_setter_report_export;
 
 
-- ── Validation ────────────────────────────────────────────────
-- SELECT
--     SUM(total_outbound_calls)  AS total_dials,
--     SUM(total_leads_touched)   AS total_leads,
--     SUM(outbound_set)          AS total_set,
--     SUM(total_closer_show)     AS total_show,
--     SUM(total_sale)            AS total_sales
-- FROM (-- paste full report query above --);