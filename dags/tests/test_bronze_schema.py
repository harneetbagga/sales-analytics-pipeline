"""
Bronze lineage columns (SOURCE_FILE_NAME, INGESTED_AT): the COPY statement and every
DDL/migration script must agree. No Airflow, Snowflake or AWS needed.
Run:  pytest dags/tests/test_bronze_schema.py
"""
import importlib
import os
import re
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
DAGS = os.path.join(HERE, "..")
ROOT = os.path.join(HERE, "..", "..")
sys.path.insert(0, DAGS)

BRONZE_TABLES = ("LEAD_ACTIVITES_RAW", "CUSTOM_ACTIVITES_RAW", "CLOSE_CRM_USERS_RAW", "LEADS_RAW")
NEW_COLUMNS = ("SOURCE_FILE_NAME", "INGESTED_AT")


def _stub(name, **attrs):
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    sys.modules.setdefault(name, mod)


def _load_dag_module():
    """Import the Bronze DAG file with Airflow stubbed out when Airflow is not installed."""
    try:
        import airflow  # noqa: F401
    except ImportError:
        class _Any:
            def __init__(self, *a, **k): pass
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def __rshift__(self, other): return other
            def __rrshift__(self, other): return self
            def __call__(self, *a, **k): return _Any()
        for n in ("airflow", "airflow.providers", "airflow.providers.snowflake",
                  "airflow.providers.snowflake.hooks", "airflow.operators",
                  "airflow.providers.common", "airflow.providers.common.sql",
                  "airflow.providers.common.sql.operators"):
            _stub(n)
        _stub("airflow.DAG", DAG=_Any)
        sys.modules["airflow"].DAG = _Any
        _stub("airflow.providers.snowflake.hooks.snowflake", SnowflakeHook=_Any)
        _stub("airflow.operators.python", PythonOperator=_Any)
        _stub("airflow.providers.common.sql.operators.sql", SQLExecuteQueryOperator=_Any)
        _stub("airflow.operators.trigger_dagrun", TriggerDagRunOperator=_Any)
    sys.modules.pop("s3_to_snowflake_bronze", None)
    return importlib.import_module("s3_to_snowflake_bronze")


def _read(*parts):
    with open(os.path.join(ROOT, *parts), encoding="utf-8") as f:
        return f.read()


def _split_top_level(s):
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(cur.strip()); cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def test_copy_lists_and_selects_the_same_number_of_columns():
    mod = _load_dag_module()
    sql = mod.copy_into_sql("LEADS_RAW", "2026-10-20", False)
    cols = re.search(r"COPY INTO .*?\((.*?)\)\s*FROM", sql, re.S).group(1)
    sel = re.search(r"SELECT(.*?)FROM @", sql, re.S).group(1)
    columns = [c.strip() for c in cols.split(",") if c.strip()]
    selects = _split_top_level(sel)
    assert columns == ["JSON_OBJECT", "SOURCE_INSERT_DATE", "PG_INSERT_DATE", "LOAD_DATE",
                       "SOURCE_FILE_NAME", "INGESTED_AT"]
    assert len(columns) == len(selects)


def test_copy_fills_file_name_and_utc_ingest_time():
    mod = _load_dag_module()
    sql = mod.copy_into_sql("LEADS_RAW", "2026-10-20", False)
    assert "METADATA$FILENAME" in sql
    assert "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ" in sql
    assert "load_date=2026-10-20/" in sql
    assert "FORCE = TRUE" not in sql
    assert "FORCE = TRUE" in mod.copy_into_sql("LEADS_RAW", "2026-10-20", True)


def test_setup_scripts_define_the_new_columns_on_every_bronze_table():
    for script in ("snowflake_complete_environment_setup.sql", "snowflake_bronze_setup.sql"):
        text = _read("snowflake", script)
        for table in BRONZE_TABLES:
            m = re.search(r"CREATE (?:OR REPLACE )?TABLE (?:IF NOT EXISTS )?[\w.]*" + table + r"\s*\((.*?)\);",
                          text, re.S | re.I)
            assert m, f"{script}: no CREATE for {table}"
            for col in NEW_COLUMNS:
                assert col in m.group(1).upper(), f"{script}: {table} lacks {col}"


def test_migration_adds_the_columns_idempotently():
    text = _read("snowflake", "bronze_metadata_columns.sql").upper()
    for table in BRONZE_TABLES:
        for col in NEW_COLUMNS:
            pat = rf"ALTER TABLE [\w.]*{table}\s+ADD COLUMN IF NOT EXISTS {col}"
            assert re.search(pat, text), f"migration missing {table}.{col}"
