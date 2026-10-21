-- ============================================================
-- FILE: 01_close_crm_users_processed.sql
-- LAYER: Silver
-- SOURCE: bronze.close_crm_users_raw
-- SCD TYPE: 1 (dimension table — only current user state matters
--            for setter/closer attribution; no business need to
--            track a user's prior email/name history)
-- DEDUP KEY: USER_ID
-- "LATEST WINS" TIEBREAK: LOAD_DATE DESC (pipeline receipt time —
--            no per-record activity_at concept applies here, this
--            is a dimension table not an activity record)
-- STATUS: unchanged in Batch 1 rebuild — this file was missing
--         from local disk; recreated here so local matches deployed.
-- ============================================================

USE WAREHOUSE sales_wh;
USE DATABASE sales_analytics;
USE SCHEMA silver;

MERGE INTO sales_analytics.silver.close_crm_users_processed AS target
USING (
    SELECT
        USER_ID, EMAIL, FIRST_NAME, LAST_NAME,
        DATE_CREATED, DATE_UPDATED, INSERT_DATE, MD5_HASH
    FROM (
        SELECT
            u.value:id::VARCHAR                  AS USER_ID,
            u.value:email::VARCHAR               AS EMAIL,
            u.value:first_name::VARCHAR          AS FIRST_NAME,
            u.value:last_name::VARCHAR           AS LAST_NAME,
            u.value:date_created::TIMESTAMP_NTZ  AS DATE_CREATED,
            u.value:date_updated::TIMESTAMP_NTZ  AS DATE_UPDATED,
            src.LOAD_DATE                        AS INSERT_DATE,
            MD5(
                CONCAT(
                    COALESCE(u.value:id::VARCHAR,           ''),
                    COALESCE(u.value:email::VARCHAR,        ''),
                    COALESCE(u.value:first_name::VARCHAR,   ''),
                    COALESCE(u.value:last_name::VARCHAR,    ''),
                    COALESCE(u.value:date_updated::VARCHAR, '')
                )
            ) AS MD5_HASH,
            ROW_NUMBER() OVER (
                PARTITION BY u.value:id::VARCHAR
                ORDER BY u.value:date_updated::TIMESTAMP_NTZ DESC NULLS LAST,
                         src.LOAD_DATE DESC,
                         MD5(u.value::VARCHAR)
            ) AS rn
        FROM sales_analytics.bronze.close_crm_users_raw src,
        LATERAL FLATTEN(
            input => TRY_PARSE_JSON(
                sales_analytics.silver.repair_json_users(src.JSON_OBJECT)
            ):data
        ) u
        WHERE TRY_PARSE_JSON(
            sales_analytics.silver.repair_json_users(src.JSON_OBJECT)
        ) IS NOT NULL
    )
    WHERE rn = 1
) AS source
ON target.USER_ID = source.USER_ID

WHEN MATCHED AND target.MD5_HASH <> source.MD5_HASH THEN
    UPDATE SET
        target.EMAIL        = source.EMAIL,
        target.FIRST_NAME   = source.FIRST_NAME,
        target.LAST_NAME    = source.LAST_NAME,
        target.DATE_CREATED = source.DATE_CREATED,
        target.DATE_UPDATED = source.DATE_UPDATED,
        target.MD5_HASH     = source.MD5_HASH,
        target.UPDATE_DATE  = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        USER_ID, EMAIL, FIRST_NAME, LAST_NAME,
        MD5_HASH, DATE_CREATED, DATE_UPDATED,
        INSERT_DATE, UPDATE_DATE
    )
    VALUES (
        source.USER_ID, source.EMAIL, source.FIRST_NAME, source.LAST_NAME,
        source.MD5_HASH, source.DATE_CREATED, source.DATE_UPDATED,
        source.INSERT_DATE, CURRENT_TIMESTAMP()
    );
