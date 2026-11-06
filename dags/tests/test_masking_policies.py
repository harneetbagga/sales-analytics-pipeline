"""
PII masking: the script only masks columns that exist, and nothing the reporting role reads
touches a masked column (otherwise a masked value would reach QuickSight).
No Snowflake needed.  Run:  pytest dags/tests/test_masking_policies.py
"""
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..", "..")


def _read(*parts):
    with open(os.path.join(ROOT, *parts), encoding="utf-8") as f:
        return f.read()


SCRIPT = _read("snowflake", "pii_masking_policies.sql")
# sales_analytics.silver.<table> MODIFY COLUMN <col> SET MASKING POLICY ...<string|variant>
MASKED = re.findall(
    r"ALTER TABLE sales_analytics\.silver\.(\w+)\s+MODIFY COLUMN\s+(\w+)\s+SET MASKING POLICY\s+"
    r"sales_analytics\.governance\.mask_contact_(string|variant)", SCRIPT)


def _table_columns(table):
    """Column name -> type, from whichever setup script defines the table."""
    for fname in ("snowflake_silver_setup.sql", "dq_tables.sql"):
        s = _read("snowflake", fname)
        m = re.search(rf"CREATE (?:OR REPLACE )?TABLE (?:IF NOT EXISTS )?(?:sales_analytics\.)?silver\.{table}\s*\((.*?)\n\);",
                      s, re.S | re.I)
        if m:
            cols = {}
            for line in m.group(1).splitlines():
                line = line.split("--")[0].strip().rstrip(",")
                parts = line.split()
                if len(parts) >= 2:
                    cols[parts[0].upper()] = parts[1].upper()
            return cols
    return {}


def test_script_attaches_eleven_policies():
    assert len(MASKED) == 11, MASKED


def test_every_masked_column_exists_with_the_matching_type():
    for table, col, kind in MASKED:
        cols = _table_columns(table)
        assert cols, f"no DDL found for silver.{table}"
        assert col.upper() in cols, f"{table}.{col} is not in the DDL"
        is_variant = cols[col.upper()] == "VARIANT"
        assert is_variant == (kind == "variant"), f"{table}.{col} is {cols[col.upper()]}, policy is {kind}"


def test_policies_let_the_transformer_see_real_values():
    assert SCRIPT.count("WHEN IS_ROLE_IN_SESSION('PIPELINE_TRANSFORMER') THEN val") == 2
    assert "WHEN val IS NULL THEN NULL" in SCRIPT  # masking must not hide NULLs


def test_gold_and_report_code_never_reads_a_masked_column():
    gold_sql = [_read("dags", "silver_to_gold.py"), _read("dags", "gold_to_export.py")]
    for name in os.listdir(os.path.join(ROOT, "snowflake")):
        if re.match(r"0\d_.*(strategies|details|dials|report)", name) or name == "snowflake_gold_setup.sql":
            gold_sql.append(_read("snowflake", name))
    blob = "\n".join(gold_sql)
    for _, col, _ in MASKED:
        # whole-word match, including aliases such as las.TEXT
        assert not re.search(rf"\b{col}\b", blob), f"Gold/report SQL reads masked column {col}"
