# Column Lineage: Sales Analytics (Postgres to Gold)

Generated 13 Oct 2026 from `SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY` (objects_modified, directSources) for one full five-DAG run, then completed by hand from the Silver SQL and `bronze_to_silver.py`. Items marked **manual** are not visible to Snowflake.

## How to read it
- "Source: table.column" = Snowflake recorded the column as copied or derived from that column (automatic).
- JSON key = the key read from the Bronze `JSON_OBJECT` by the Silver SQL. Snowflake lineage stops at the JSON flatten, so this hop is **manual**.
- "No source" = set by pipeline logic or the system at write time.

## End-to-end chain
Close CRM API -> Postgres `raw.*` (daily incremental) -> dated S3 file (`load_date=YYYY-MM-DD`) -> Bronze (as received) -> Silver -> Gold view -> report export. The first three hops are **manual** (design). Bronze to Silver is automatic for copied columns and manual (below) for JSON keys.

## Hops before Silver (from `postgres_to_s3.py` and `s3_to_snowflake_bronze.py`)

Source typos are kept as they exist in the systems (`activites`, bucket `sales-analyical-pipeline`).

### Postgres `raw.*` -> S3
- Filter on every table: `INSERT_DATE::date = target_date AND RAW_DATA IS NOT NULL` (`target_date` = run date, or the `target_date` value passed in the DAG run config for a backfill).
- S3 path: `s3://sales-analyical-pipeline/raw/<table>/load_date=<target_date>/<table>.json` (one JSON object per line, written as a multipart upload; the upload is aborted on any error and a day with zero rows fails the task).

| Postgres table | Type | S3 `JSON_OBJECT` comes from | S3 `SOURCE_INSERT_DATE` | S3 `PG_INSERT_DATE` |
|---|---|---|---|---|
| `raw.lead_activites_raw` | direct jsonb | `RAW_DATA::text` (`{ "data": [...] }`) | `NULL` | `INSERT_DATE` |
| `raw.leads_raw` | direct jsonb | `RAW_DATA::text` (one lead object) | `NULL` | `INSERT_DATE` |
| `raw.close_crm_users_raw` | wrapper | `RAW_DATA->>'JSON_OBJECT'` | `RAW_DATA->>'INSERT_DATE'` | `INSERT_DATE` |
| `raw.custom_activites_raw` | wrapper | `RAW_DATA->>'JSON_OBJECT'` | `RAW_DATA->>'INSERT_DATE'` | `INSERT_DATE` |

Each S3 line has exactly three keys: `JSON_OBJECT`, `SOURCE_INSERT_DATE`, `PG_INSERT_DATE` (all written as strings).

### S3 -> Bronze (`COPY INTO` per table, scoped to one `load_date=` folder)
| Bronze column | Source in the S3 line |
|---|---|
| JSON_OBJECT | `$1:JSON_OBJECT::VARCHAR` |
| SOURCE_INSERT_DATE | `$1:SOURCE_INSERT_DATE::VARCHAR` |
| PG_INSERT_DATE | `$1:PG_INSERT_DATE::VARCHAR` |
| LOAD_DATE | no source in the file: the `target_date` used for the run, cast to DATE (the same value as the `load_date=` folder) |

`ON_ERROR = ABORT_STATEMENT`; `FORCE = TRUE` only when the run config has `force: true`.

Bronze tables: `LEAD_ACTIVITES_RAW`, `CUSTOM_ACTIVITES_RAW`, `CLOSE_CRM_USERS_RAW`, `LEADS_RAW`. Each has `JSON_OBJECT`, `SOURCE_INSERT_DATE`, `PG_INSERT_DATE`, `LOAD_DATE`. Silver reads `JSON_OBJECT` and `LOAD_DATE` (it becomes INSERT_DATE in Silver).

## Silver run window and control table (Oct 2026, incremental Silver)
Silver no longer re-reads all of Bronze each run. `bronze_to_silver` first plans a window per Bronze source, runs every Silver step inside it, and commits the window only after the last step succeeds. This is design logic (manual lineage).

### `silver.etl_watermark` (one row per Bronze source: `lead_activites_raw`, `leads_raw`, `close_crm_users_raw`)
| Column | Meaning |
|---|---|
| SOURCE_TABLE | Bronze table the row controls |
| LAST_LOAD_DATE | newest Bronze `LOAD_DATE` fully processed; set from WINDOW_END by `commit_watermark` only after every Silver step and the quality check succeeded |
| LAST_SUCCESS_STARTED_AT | start time of the last fully successful run; used to find Silver rows written since then |
| WINDOW_START / WINDOW_END / MODE / RUN_STARTED_AT | this run's plan, written by `init_run` |
| UPDATED_AT | no source |

Modes: FULL (first run, or `full_refresh=true`; no lower bound), SINCE (`since_date`; backfill), INCREMENTAL (`LAST_LOAD_DATE` minus `lookback_days`, default 1). Which Bronze reads are windowed: LEAD_ACTIVITIES_PROCESSED (staging), CUSTOM_ACTIVITIES_ALL_LEADS_DETAILS, LEAD_ACTIVITIES_EMAIL, LEADS_PROCESSED and CLOSE_CRM_USERS_PROCESSED (`LOAD_DATE` in window). CUSTOM_ACTIVITIES stays a full read (small). The LEADS_ACTIVITIES_SUMMARY rebuild is scoped to current LAP rows with `EFFECTIVE_START_DATE >= LAST_SUCCESS_STARTED_AT`, plus a full rebuild if `custom_activities.UPDATE_DATE >= LAST_SUCCESS_STARTED_AT`.

### Which version of a record wins (all Silver dedups)
Latest `date_updated` first, then newest Bronze batch (`LOAD_DATE` / `INSERT_DATE`), then (activities only) `activity_at`. CLOSE_CRM_USERS_PROCESSED adds a deterministic last tie-break (`MD5` of the record) because each daily users batch holds every version of a user. This rule makes a windowed run and a full rebuild give the same rows.

## Silver tables

### CLOSE_CRM_USERS_PROCESSED (from CLOSE_CRM_USERS_RAW.JSON_OBJECT)
Path: `repair_json_users(JSON_OBJECT)` -> `:data` array -> flatten (`u.value`).
| Silver column | JSON key |
|---|---|
| USER_ID | `id` |
| EMAIL | `email` |
| FIRST_NAME | `first_name` |
| LAST_NAME | `last_name` |
| DATE_CREATED | `date_created` |
| DATE_UPDATED | `date_updated` |
| INSERT_DATE | Bronze `LOAD_DATE` (automatic) |
| MD5_HASH | `id`, `email`, `first_name`, `last_name`, `date_updated` |
| UPDATE_DATE | no source |
Dedup: one row per `id`, ordered by `date_updated` DESC, `LOAD_DATE` DESC, then `MD5(record)`. Read inside the run window.

### CUSTOM_ACTIVITIES (from CUSTOM_ACTIVITES_RAW.JSON_OBJECT)
Path: `repair_json_activities(JSON_OBJECT)` -> `:data` -> flatten (`act`) -> `act.value:fields` -> flatten (`f`). One row per activity type x field.
| Silver column | JSON key |
|---|---|
| CUSTOM_ACTIVITY_ID | `id` (activity type) |
| CUSTOM_ACTIVITY_NAME | `name` (activity type) |
| CUSTOM_ACTIVITY_OUTCOME_ID | `fields[].id` |
| CUSTOM_ACTIVITY_OUTCOME_NAME | `fields[].name` |
| INSERT_DATE | Bronze `LOAD_DATE` (automatic) |
| MD5_HASH | the four keys above |

### LEAD_ACTIVITIES_PROCESSED (SCD2; from LEAD_ACTIVITES_RAW.JSON_OBJECT)
Path: transient copy of `JSON_OBJECT` (automatic) -> `:data` -> flatten (`act`). Filter: all activity types.
| Silver column | JSON key |
|---|---|
| ACTIVITY_ID | `id` |
| LEAD_ID | `lead_id` |
| CONTACT_ID | `contact_id` |
| USER_ID | `user_id` |
| TYPE | `_type` |
| TEXT | `COALESCE(note, text)` |
| STATUS_CHANGE | `COALESCE(disposition, merge_status, status)` |
| DIRECTION | `direction` |
| CREATED_BY_NAME / UPDATED_BY_NAME | `created_by_name` / `updated_by_name` |
| ATTENDEES | `attendees` |
| CUSTOM_ACTIVITY_ID | `custom_activity_type_id` |
| MEETING_STARTS_AT / MEETING_END_AT | `starts_at` / `ends_at` |
| MEETING_STATUS | `status` |
| ACTIVITY_AT | `activity_at` |
| DATE_CREATED / DATE_UPDATED | `date_created` / `date_updated` |
| CREATED_BY_USER / UPDATED_BY_USER | `created_by` / `updated_by` |
| INSERT_DATE | Bronze `LOAD_DATE` (automatic) |
| MD5_HASH | `id`, `lead_id`, `activity_at`, `date_updated`, `disposition`, `merge_status`, `status`, `direction` |
| EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT, UPDATE_DATE | no source (SCD2 logic / system) |

Note: `status` feeds both STATUS_CHANGE (as last fallback) and MEETING_STATUS.

### CUSTOM_ACTIVITIES_ALL_LEADS_DETAILS (from LEAD_ACTIVITES_RAW.JSON_OBJECT)
Path: `PARSE_JSON(JSON_OBJECT):data` -> flatten, filter `_type = 'CustomActivity'`.
| Silver column | JSON key |
|---|---|
| LEAD_ID | `lead_id` |
| ACTIVITY_ID | `id` |
| ACTIVITY_AT | `activity_at` |
| WRAPPED_ACTIVITIES | the whole activity object (including every `custom.<field_id>` value) |
| MD5_HASH | `id`, `lead_id`, `activity_at`, `date_updated` |
| INSERT_DATE | Bronze `LOAD_DATE` |

### LEAD_ACTIVITIES_EMAIL (from LEAD_ACTIVITES_RAW.JSON_OBJECT)
Path: same flatten, filter `_type = 'Email'`.
| Silver column | JSON key |
|---|---|
| LEAD_ID / ACTIVITY_ID / ACTIVITY_AT | `lead_id` / `id` / `activity_at` |
| ATTACHMENTS_JSON / OPENS_JSON / ENVELOPE_JSON | `attachments` / `opens` / `envelope` (as text) |
| SENDER_JSON | `envelope.from` (as text) |
| SENDER | `envelope.from[0].email` |
| SUBJECT | `envelope.subject` |
| BODY_TEXT | `body_text` |
| DIRECTION, STATUS | `direction`, `status` |
| USER_ID, USER_NAME | `user_id`, `user_name` |
| TEMPLATE_ID, TEMPLATE_NAME | `template_id`, `template_name` |
| DATE_CREATED, DATE_UPDATED, DATE_SENT | `date_created`, `date_updated`, `date_sent` |
| HAS_REPLY | `has_reply` |
| JSON_PAYLOAD | whole activity object |
| MD5_HASH | `id`, `lead_id`, `activity_at`, `date_updated`, `status`, `has_reply` |
| INSERT_DATE | Bronze `LOAD_DATE` |

### LEADS_PROCESSED (from LEADS_RAW.JSON_OBJECT; one lead object per row, no flatten)
| Silver column | JSON path |
|---|---|
| LEAD_ID | `id` |
| STATUS_LABEL | `status_label` |
| LEAD_ROUTE | `custom.lead_route` (missing -> `'NO_ROUTE'`) |
| AVATAR | `custom.Avatar` |
| FUNNEL | `custom.Funnel` |
| SETTER_ID | `custom.Setter` |
| LEAD_OWNER_ID | `custom."Lead Owner"` |
| LEAD_QUALITY | `custom."Lead Quality"` |
| LEAD_INTENT | `custom."Lead Intent"` |
| VISA_STATUS | `custom."Visa Status"` |
| CURRENT_SALARY | `custom."Current Salary"` |
| DATE_CREATED / DATE_UPDATED | `date_created` / `date_updated` |
| MD5_HASH | `id`, `date_updated`, `status_label`, `custom.lead_route` |
| INSERT_DATE | Bronze `LOAD_DATE` |

### LEADS_ACTIVITIES_SUMMARY_TRANSIENT (staging)
- Copied from LEAD_ACTIVITIES_PROCESSED (current rows only, `IS_CURRENT = TRUE`), same column name: ACTIVITY_ID, LEAD_ID, CONTACT_ID, USER_ID, TYPE, TEXT, STATUS_CHANGE, ATTENDEES, CREATED_BY_NAME/USER, UPDATED_BY_NAME/USER, CUSTOM_ACTIVITY_ID, MEETING_*, ACTIVITY_AT, DATE_CREATED, DATE_UPDATED, INSERT_DATE.
- CUSTOM_ACTIVITY <- CUSTOM_ACTIVITIES.CUSTOM_ACTIVITY_NAME (join on CUSTOM_ACTIVITY_ID)
- CUSTOM_ACTIVITY_OUTCOME_NAME <- CUSTOM_ACTIVITIES.CUSTOM_ACTIVITY_OUTCOME_NAME
- CUSTOM_ACTIVITY_OUTCOME <- WRAPPED_ACTIVITIES key `custom.<CUSTOM_ACTIVITY_OUTCOME_ID>` (one row per field; matched by flattening WRAPPED_ACTIVITIES and joining the key to CUSTOM_ACTIVITIES.CUSTOM_ACTIVITY_OUTCOME_ID). Manual hop; Snowflake shows FLATTEN_STRUCT.
- WRAPPED_ACTIVITIES <- CUSTOM_ACTIVITIES_ALL_LEADS_DETAILS.WRAPPED_ACTIVITIES (join on ACTIVITY_ID + LEAD_ID)
- DEA_INTERNAL_NAME <- CLOSE_CRM_USERS_PROCESSED `COALESCE(FIRST_NAME || ' ' || LAST_NAME, EMAIL)` (join USER_ID)
- DEA_INTERNAL_EMAIL <- CLOSE_CRM_USERS_PROCESSED.EMAIL
- MD5_HASH <- ACTIVITY_ID, LEAD_ID, DATE_UPDATED, outcome name, outcome value
- UPDATE_DATE: no source

### LEADS_ACTIVITIES_SUMMARY (central fact table, SCD1)
- Every column copied from the TRANSIENT by MERGE on (ACTIVITY_ID, LEAD_ID, CUSTOM_ACTIVITY_OUTCOME_NAME).
- DEA_INTERNAL_NAME / DEA_INTERNAL_EMAIL also refreshed from CLOSE_CRM_USERS_PROCESSED by the `refresh_las_user_identity` task each run.
- UPDATE_DATE: no source.

## Gold views (column level, from `silver_to_gold.py`)
All five views read LEADS_ACTIVITIES_SUMMARY (LAS). Outcome columns are pivots: `MAX(CASE WHEN CUSTOM_ACTIVITY_OUTCOME_NAME = '<name>' THEN CUSTOM_ACTIVITY_OUTCOME END) OVER (PARTITION BY ACTIVITY_ID, LEAD_ID)` (all views partition by activity and lead; see drift 3). "Eastern" = `CONVERT_TIMEZONE('UTC','America/New_York', LAS.ACTIVITY_AT)`. User ids found in an outcome (`Setter`, `Closer`) are resolved to name and email through CLOSE_CRM_USERS_PROCESSED (`COALESCE(FIRST_NAME || ' ' || LAST_NAME, EMAIL)`).

### INBOUND_STRATEGIES_BOOKED (LAS rows with TYPE = `CustomActivity`; triage = CUSTOM_ACTIVITY `3) Triage Call`)
| Gold column | Source |
|---|---|
| LEAD_ID | LAS.LEAD_ID |
| ACTIVITY_LOG_DATE, TRIAGE_CALL_DATE | LAS.ACTIVITY_AT (Eastern) of the triage call |
| SETTER_CLOSER_EMAIL | LAS.DEA_INTERNAL_EMAIL |
| SETTER_CLOSER_NAME | LAS.DEA_INTERNAL_NAME |
| TRIAGE_CALL_OUTCOME | outcome `Triage Call Outcome` |
| STRATEGY_CALL_BOOKED | derived: `TRIAGE_CALL_OUTCOME = '1. Strategy Call Scheduled'` |
| SC_YEAR_WEEK | derived: year + ISO week of `COALESCE(strategy DATE_OF_STRATEGY_CALL, strategy ACTIVITY_AT, TRIAGE_CALL_DATE)`; the strategy call is the lead's latest `5) Strategy Call` (outcome `Date of Strategy Call`, LAS.ACTIVITY_AT) on or after the triage date |

### OUTBOUND_STRATEGIES_BOOKED (prospecting = CUSTOM_ACTIVITY `1) Prospecting Activity`, latest per lead)
| Gold column | Source |
|---|---|
| LEAD_ID | LAS.LEAD_ID |
| ACTIVITY_LOG_DATE, PROSPECT_CALL_DATE | LAS.ACTIVITY_AT (Eastern) of the prospecting activity |
| SETTER_CLOSER_EMAIL / SETTER_CLOSER_NAME | LAS.DEA_INTERNAL_EMAIL / DEA_INTERNAL_NAME |
| STRATEGY_CALL_BOOKED | derived: outcome `Prospecting Call Outcome` = `'2. Strategy Call Scheduled'` |
| SC_YEAR_WEEK | same derivation as the inbound view, using the prospect call date as the fallback |

### ALL_STRATEGIES_DETAILS (CUSTOM_ACTIVITY = `5) Strategy Call`)
| Gold column | Source |
|---|---|
| LEAD_ID, ACTIVITY_ID | LAS.LEAD_ID, LAS.ACTIVITY_ID |
| ACTIVITY_LOG_DATE | LAS.ACTIVITY_AT (Eastern) |
| CUSTOM_ACTIVITY | LAS.CUSTOM_ACTIVITY |
| STATUS | LAS.MEETING_STATUS |
| CLOSER_EMAIL / CLOSER_NAME | USERS via outcome `Closer` (user id) |
| SETTER | outcome `Setter` (raw user id) |
| SETTER_NAME | USERS via outcome `Setter` |
| STRATEGY_CALL_OUTCOME | outcome `Strategy Call Outcome` |
| DATE_OF_STRATEGY_CALL | outcome `Date of Strategy Call` (TRY_TO_DATE) |
| OFFER_PRESENTED | outcome `Offer Presented` |
| OBJECTIONS_FACED | outcome `Objections Faced?` (a JSON array stored as text) |

### SALES_DETAILS (CUSTOM_ACTIVITY in `7) New Sale`, `8) New Sale [Custom Payment Plan]`)
| Gold column | Source |
|---|---|
| LEAD_ID | LAS.LEAD_ID |
| ACTIVITY_AT | LAS.ACTIVITY_AT (Eastern) |
| SALE_STATUS | LAS.MEETING_STATUS |
| SETTER_EMAIL / SETTER_NAME | USERS via outcome `Setter` |
| CLOSER_EMAIL / CLOSER_NAME | USERS via outcome `Closer` |
| CONTRACTED_VALUE | outcome `Contract Value` (TRY_TO_NUMERIC) |
| DATE_OF_SALE | outcome `Date of Sale`, fallback to the ACTIVITY_AT date |
| PROGRAM | outcome `Program` |
| CASH_COLLECTED | outcome `Cash Collected` (TRY_TO_NUMERIC) |

### OUTBOUND_PROSPECT_DIALS (CUSTOM_ACTIVITY in `1) Prospecting Activity`, `2) Prospecting Follow Up`)
| Gold column | Source |
|---|---|
| LEAD_ID, ACTIVITY_ID | LAS.LEAD_ID, LAS.ACTIVITY_ID |
| ACTIVITY_LOG_DATE | LAS.ACTIVITY_AT (Eastern) |
| PROSPECT_YEAR_WEEK | derived: year + week of ACTIVITY_LOG_DATE |
| CUSTOM_ACTIVITY | LAS.CUSTOM_ACTIVITY |
| STATUS | `MAX(LAS.MEETING_STATUS)` over (ACTIVITY_ID, LEAD_ID) |
| CUSTOM_ACTIVITY_OUTCOME_NAME / CUSTOM_ACTIVITY_OUTCOME | outcome `Prospecting Call Outcome` (name and value) |
| SETTER_CLOSER_EMAIL / SETTER_CLOSER_NAME | LAS.DEA_INTERNAL_EMAIL / DEA_INTERNAL_NAME |

## Report columns (from `gold_to_export.py`; each report is materialised into staging, row-count checked, then published to a persistent `gold.rpt_*` table)
Rates are rounded to one decimal unless noted. The four exports are gated by the Gold regression suite.

### Inbound Setter Report
| Report column | Source / rule |
|---|---|
| triage_date | INBOUND_STRATEGIES_BOOKED.TRIAGE_CALL_DATE (date) |
| setter | INBOUND_STRATEGIES_BOOKED.SETTER_CLOSER_NAME |
| inbound_booked | count of triage rows |
| inbound_taken | TRIAGE_CALL_OUTCOME not in (`6. No Show`, `7. Reschedule`, `8. Cancel`) |
| triage_set_rate | inbound_taken / inbound_booked |
| strategy_call_booked | INBOUND_STRATEGIES_BOOKED.STRATEGY_CALL_BOOKED |
| strategy_call_taken | ALL_STRATEGIES_DETAILS.STRATEGY_CALL_OUTCOME of the lead's matching strategy call (same LEAD_ID, ACTIVITY_LOG_DATE on or after the triage date; an attended attempt wins, then the earliest), not in the no-show / cancel list |
| show_rate | strategy_call_taken / strategy_call_booked |
| offer_rate | ALL_STRATEGIES_DETAILS.OFFER_PRESENTED = `Yes`, over strategy_call_taken |
| total_sales | leads with a row in SALES_DETAILS (latest row per lead) |
| sale_rate | total_sales / strategy_call_taken |
| average_order_value | average SALES_DETAILS.CONTRACTED_VALUE for leads with a sale (0 decimals) |

### Outbound Setter Report
| Report column | Source / rule |
|---|---|
| dial_date | OUTBOUND_PROSPECT_DIALS.ACTIVITY_LOG_DATE (date), outcome name `Prospecting Call Outcome` only |
| setter | OUTBOUND_PROSPECT_DIALS.SETTER_CLOSER_NAME |
| total_outbound_calls | distinct ACTIVITY_ID |
| total_leads_touched | distinct LEAD_ID |
| outbound_set | distinct leads whose outcome = `2. Strategy Call Scheduled` |
| total_closer_show / total_offer | ALL_STRATEGIES_DETAILS.STRATEGY_CALL_OUTCOME / OFFER_PRESENTED of the set lead's matching strategy call (same attended-first rule) |
| total_sale | set leads that showed and have a SALES_DETAILS row |
| total_revenue / average_order_value | SALES_DETAILS.CONTRACTED_VALUE (sum / average over those sales) |
| dial_to_set_rate, set_to_show_rate, show_to_sale_rate | outbound_set / total_outbound_calls; total_closer_show / outbound_set; total_sale / total_closer_show |

Call-level and lead-level metrics are aggregated separately and joined once at (dial_date, setter), so lead facts are never summed over repeated dials.

### Closer Report (grain: closer x month x lead route)
| Report column | Source / rule |
|---|---|
| CLOSER_NAME | LAS.DEA_INTERNAL_NAME |
| CALL_YEAR_MONTH | month of outcome `Date of Strategy Call` / `Date of Follow Up Call` (business-entered date, not ACTIVITY_AT) |
| LEAD_ROUTE | LEADS_PROCESSED.LEAD_ROUTE (`NO_ROUTE` if missing) |
| CALL_BOOKED | count of `5) Strategy Call` and `6) Strategy Call Follow Up` activities (grouped by ACTIVITY_ID, LEAD_ID, ACTIVITY_AT) |
| ADMIN_CANCEL, CANCEL_NURTURE, CANCEL_NOT_INTEREST, NO_SHOW, STRTGY_CALL_SHW, LOST (+ their rates) | outcome `Strategy Call Outcome` / `Follow Up Call Outcome`, matched to the documented value lists (duplicate-numbered picklist values included) |
| TOTAL_CANCEL | ADMIN_CANCEL + CANCEL_NURTURE + CANCEL_NOT_INTEREST |
| SALE / SALE_RT | SALES_DETAILS rows with SALE_STATUS <> `NO_SALE`, by closer + month of ACTIVITY_AT + route; SALE_RT = SALE / STRTGY_CALL_SHW |
| AVG_VAUE | average non-zero SALES_DETAILS.CONTRACTED_VALUE (rounded up) |
| CASH_COLLECTED | sum of SALES_DETAILS.CASH_COLLECTED |

### Objections Faced Report (grain: closer x day)
| Report column | Source / rule |
|---|---|
| closer_name | ALL_STRATEGIES_DETAILS.CLOSER_NAME |
| activity_date | ALL_STRATEGIES_DETAILS.ACTIVITY_LOG_DATE (date) |
| total_calls | distinct strategy-call activities |
| *_count (money, fear, hung up, logistical, no objections, other coaches, partner, think about it, time, trust, value, not looking) | ALL_STRATEGIES_DETAILS.OBJECTIONS_FACED parsed as a JSON array and flattened; one flag per activity per objection |
| `Money%` ... `Wsn't Lkng Fr Wht We Offrd%` | each count / total_calls |

## Data-quality tables (new)
| Table | Written by | Content |
|---|---|---|
| `silver.dq_lap_staging_flat` | `validate_lap_staging` | the newest load's flattened activity rows (ACTIVITY_ID, LEAD_ID, TYPE, ACTIVITY_AT, INSERT_DATE, IS_GOOD, ROW_HASH, RAW_RECORD) from `lead_activities_processed_transient`; rebuilt every run |
| `silver.dq_rejected_rows` | `validate_lap_staging` | each distinct rejected record once (by ROW_HASH) with reason, DAG run id and the raw record |
| `silver.dq_run_log` | `validate_lap_staging` | one row per check per run (total, failed, failed %, threshold, decision) plus an overall row |

## Drift to verify (found while reading the code)
1. **Fixed.** DEA_INTERNAL_NAME now uses `COALESCE(first || ' ' || last, email)` in the DAG (transient load and refresh step), matching `06_leads_activities_summary.sql`.
2. **Fixed (2026-10).** `07_leads_processed.sql` and the DAG now use `CREATE TABLE IF NOT EXISTS` and then MERGE, so the table persists between runs and the MERGE only updates rows whose MD5_HASH changed; UPDATE_DATE now reflects real changes. Verify with two runs and `COUNT_IF(UPDATE_DATE > INSERT_DATE)`.
3. **Fixed and tested (2026-10).** Every outcome pivot in `silver_to_gold.py` (INBOUND_STRATEGIES_BOOKED, OUTBOUND_STRATEGIES_BOOKED, ALL_STRATEGIES_DETAILS, SALES_DETAILS) now partitions by `ACTIVITY_ID, LEAD_ID`, matching OUTBOUND_PROSPECT_DIALS; the objections report also groups by LEAD_ID. About 1,313 ACTIVITY_IDs belong to more than one lead, which is why the lead key matters. `tests/test_gold_pivot_isolation.py` (a source check plus a live Gold-vs-Silver comparison) runs in the regression gate; the live check passed on real data. The local SQL copies in `snowflake/` were updated to match.
4. **Resolved.** `01_inbound_setter_report.sql` now carries the same SQL as `gold_to_export.py` (attended-first rule included); the four report `.sql` files were synced from the DAG.
5. **Resolved.** The objections `not_looking_flag` logic in `04_objections_faced_report.sql` now matches `gold_to_export.py`.
6. **Resolved.** `03_closer_report.sql` now has a single header.
7. Timezone: the five Gold views convert to Eastern, the Closer Report uses the business-entered strategy call date. The Eastern vs UTC convention is still an open question with the business stakeholder, not a code defect.
8. **Resolved (2026-10).** `gold_to_export.py` builds four persistent Gold report tables (`gold.rpt_*`) through a validated, atomic publish (`INSERT OVERWRITE`), grants SELECT to the read-only `report_reader` role, and QuickSight reads them through `reporting_wh` under that role. The S3 CSV path remains only behind the `LEGACY_S3_EXPORT` switch (off), and the SPICE refresh behind `QUICKSIGHT_REFRESH_SPICE` (currently off).
9. **Resolved and validated (2026-10).** Silver is incremental (watermark, windowed Bronze reads, latest-`date_updated` dedup). It was checked against a full rebuild: a zero-copy clone of Silver, a full refresh, then EXCEPT comparisons in both directions on key and MD5_HASH (the summary table also compares outcome, name and email); all five tables match. The comparison first failed for two real reasons, both now fixed: event time was ranked ahead of `date_updated` when picking the newest activity version (5 activities), and the users dedup tied on `LOAD_DATE` (about 184,000 summary rows changed name spelling between runs). `dags/tests/test_silver_incremental.py` (unit and source checks) and `dags/tests/test_silver_incremental_parity.py` (live; needs `SILVER_PARITY_BASELINE`) cover this. Open: no scheduled reconciliation yet, and source deletions are not propagated.
