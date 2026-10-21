-- ============================================================
-- FILE: 00_udfs.sql
-- PURPOSE: Python UDFs for repairing single-quoted JSON
--          from Close CRM Bronze wrapper tables
-- SCHEMA: sales_analytics.silver
--
-- BATCH 3 HARDENING (2026-09): both UDFs now internally validate
-- their own output with json.loads() before returning it, and
-- RAISE a clear Python exception (with a truncated snippet of the
-- offending input) if the repaired string still isn't valid JSON,
-- instead of silently returning malformed text for a downstream
-- TRY_PARSE_JSON(...) to turn into a quiet NULL.
--
-- Consequence, deliberate: if even one row's repaired output fails
-- to parse, the ENTIRE calling SQL statement fails immediately —
-- not just that one row. This directly closes the gap from the
-- original incident, where the UDF silently returned NULL for
-- every row (a regex pattern mismatch after an environment
-- rebuild) and produced a zero-row Silver load with no alert
-- anywhere. A single genuinely malformed record now halts that
-- day's transform for that table rather than being silently
-- dropped — the cost of an actual alert instead of silence.
--
-- The downstream TRY_PARSE_JSON(...) IS NOT NULL filters in
-- bronze_to_silver.py are left in place as defense-in-depth (belt
-- and suspenders against any json.loads/TRY_PARSE_JSON parsing
-- discrepancy) but should now be dead code in normal operation —
-- if the UDF returns at all, its output is already guaranteed
-- valid JSON.
-- ============================================================

-- ── UDF 1: repair_json_users ──────────────────────────────
-- Used for: sales_analytics.bronze.close_crm_users_raw
-- Handles: O''Neal (double single quote) and O'hare (apostrophe)
-- Does NOT neutralize description fields (not needed for users)

CREATE OR REPLACE FUNCTION sales_analytics.silver.repair_json_users(input_str VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'repair_json_users'
AS $$
import re
import json

def repair_json_users(input_str):
    if input_str is None:
        return None

    result = input_str

    # Step 1: Protect double single quotes between letters (O''Neal)
    result = re.sub(r"([a-zA-Z])''([a-zA-Z])", r"\1__APOSTROPHE__\2", result)

    # Step 2: Protect single quotes between letters (O'hare)
    result = re.sub(r"([a-zA-Z])'([a-zA-Z])", r"\1__APOSTROPHE__\2", result)

    # Step 3: Replace single-quoted JSON strings with double quotes
    result = re.sub(
        r"'([^']*)'(\s*[:},\]])",
        lambda m: '"' + m.group(1).replace('"', '\\"') + '"' + m.group(2),
        result
    )

    # Step 4: Catch any remaining single quotes
    result = result.replace("'", '"')

    # Step 5: Restore apostrophes
    result = result.replace("__APOSTROPHE__", "'")

    # Step 6: Python literals to JSON literals
    result = result.replace(': None', ': null')
    result = result.replace(':None', ':null')
    result = result.replace(': True', ': true')
    result = result.replace(': False', ': false')
    result = result.replace(':True', ':true')
    result = result.replace(':False', ':false')

    # ── Batch 3 hardening: validate before returning ──
    # If this still isn't valid JSON, fail loudly right here rather
    # than handing malformed text downstream for TRY_PARSE_JSON to
    # silently turn into NULL.
    try:
        json.loads(result)
    except (json.JSONDecodeError, ValueError) as e:
        snippet = input_str[:300] if input_str else "(empty)"
        raise ValueError(
            f"repair_json_users: repaired output still failed to parse as "
            f"JSON ({e}). Original input (first 300 chars): {snippet}"
        )

    return result
$$;


-- ── UDF 2: repair_json_activities ────────────────────────
-- Used for: sales_analytics.bronze.custom_activites_raw
-- Handles: Same as above PLUS neutralizes description fields
-- which contain embedded single quotes like 'No Show'or 'Reschedule'
-- that would break standard quote replacement logic

CREATE OR REPLACE FUNCTION sales_analytics.silver.repair_json_activities(input_str VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'repair_json_activities'
AS $$
import re
import json

def neutralize_descriptions(s):
    """
    Walk the string character by character to find 'description': '...'
    patterns and replace the entire value with null.
    Handles embedded single quotes of any complexity.
    """
    result = []
    i = 0
    desc_key = "'description': '"
    while i < len(s):
        idx = s.find(desc_key, i)
        if idx == -1:
            result.append(s[i:])
            break
        # Append everything up to and including 'description':
        result.append(s[i:idx])
        result.append("'description': null")
        # Skip past the opening quote of the value
        i = idx + len(desc_key)
        # Find the closing quote — the one followed by , or } or ]
        while i < len(s):
            if s[i] == "'":
                j = i + 1
                while j < len(s) and s[j] == ' ':
                    j += 1
                if j < len(s) and s[j] in (',', '}', ']'):
                    i = i + 1  # skip past closing quote
                    break
                else:
                    i += 1  # embedded quote, keep scanning
            else:
                i += 1
    return ''.join(result)

def repair_json_activities(input_str):
    if input_str is None:
        return None

    result = input_str

    # Step 1: Neutralize description fields with embedded quotes
    result = neutralize_descriptions(result)

    # Step 2: Protect double single quotes between letters (O''Neal)
    result = re.sub(r"([a-zA-Z])''([a-zA-Z])", r"\1__APOSTROPHE__\2", result)

    # Step 3: Protect single quotes between letters (O'hare)
    result = re.sub(r"([a-zA-Z])'([a-zA-Z])", r"\1__APOSTROPHE__\2", result)

    # Step 4: Replace single-quoted JSON strings with double quotes
    result = re.sub(
        r"'([^']*)'(\s*[:},\]])",
        lambda m: '"' + m.group(1).replace('"', '\\"') + '"' + m.group(2),
        result
    )

    # Step 5: Catch any remaining single quotes
    result = result.replace("'", '"')

    # Step 6: Restore apostrophes
    result = result.replace("__APOSTROPHE__", "'")

    # Step 7: Python literals to JSON literals
    result = result.replace(': None', ': null')
    result = result.replace(':None', ':null')
    result = result.replace(': True', ': true')
    result = result.replace(': False', ': false')
    result = result.replace(':True', ':true')
    result = result.replace(':False', ':false')

    # ── Batch 3 hardening: validate before returning ──
    try:
        json.loads(result)
    except (json.JSONDecodeError, ValueError) as e:
        snippet = input_str[:300] if input_str else "(empty)"
        raise ValueError(
            f"repair_json_activities: repaired output still failed to parse "
            f"as JSON ({e}). Original input (first 300 chars): {snippet}"
        )

    return result
$$;


-- ============================================================
-- Automated validation, wired into the pipeline (replaces the
-- old commented-out manual-only queries below, which existed but
-- were never actually run as part of any DAG task).
-- ============================================================
-- Since the UDFs above now raise on any unparseable row, a failed
-- transform_users / transform_custom_activities task in
-- bronze_to_silver.py IS the alert — no separate check needed to
-- detect "the UDF is broken." These two queries remain useful as a
-- manual post-hoc sanity check (e.g. confirming today's row counts
-- look right after a run), not as failure detection:

-- SELECT COUNT(*) AS row_count
-- FROM sales_analytics.bronze.close_crm_users_raw;
-- Expected: matches today's known extraction count, no errors thrown
-- by transform_users if this query alone succeeds.

-- SELECT COUNT(*) AS row_count
-- FROM sales_analytics.bronze.custom_activites_raw;
-- Expected: matches today's known extraction count.