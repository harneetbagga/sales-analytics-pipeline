"""
PIPELINE_RUN_ID audit column: one ID per pipeline run, written by every layer.

Static checks (no Snowflake, no Airflow): the DAG files are read as text, and the
small helper is exec'd from its own source.
"""
import ast
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
DAGS = os.path.join(HERE, "..")
SNOW = os.path.join(HERE, "..", "..", "snowflake")


def _read(*parts):
    with open(os.path.join(*parts), encoding="utf-8") as f:
        return f.read()


SILVER = _read(DAGS, "bronze_to_silver.py")
GOLD = _read(DAGS, "silver_to_gold.py")
SILVER_TABLES = ("close_crm_users_processed", "custom_activities", "custom_activities_all_leads_details",
                 "leads_processed", "lead_activities_email", "lead_activities_processed",
                 "leads_activities_summary")


def _load_helper():
    tree = ast.parse(SILVER)
    src = "\n".join(ast.get_source_segment(SILVER, n) for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name == "pipeline_run_id")
    ns = {}
    exec(src, ns)
    return ns["pipeline_run_id"]


class _Run:
    def __init__(self, conf):
        self.conf = conf


def test_run_id_prefers_the_upstream_conf_value():
    f = _load_helper()
    assert f({"dag_run": _Run({"pipeline_run_id": "scheduled__2026-10-27"}), "run_id": "x"}) == "scheduled__2026-10-27"


def test_run_id_falls_back_to_this_dag_run_then_manual():
    f = _load_helper()
    assert f({"dag_run": _Run({}), "run_id": "manual__1"}) == "manual__1"
    assert f({"dag_run": _Run(None), "run_id": "manual__2"}) == "manual__2"
    assert f({}) == "manual"


def test_run_id_cannot_break_out_of_a_sql_literal():
    f = _load_helper()
    assert "'" not in f({"dag_run": _Run({"pipeline_run_id": "a'; DROP TABLE x; --"})})


def test_every_silver_write_sets_the_run_id():
    # 6 merges x (update + insert) + LAP insert + LAS identity refresh + leads_processed DDL fallback
    assert SILVER.count("'__RUN_ID__'") >= 14
    for sql in re.findall(r'(SQL_\w+) = USE_CONTEXT \+ """(.*?)"""', SILVER, re.S):
        name, body = sql
        if "MERGE INTO" in body and "sales_analytics.silver." in body and "TRANSIENT" not in name:
            assert "__RUN_ID__" in body, name


def test_silver_adds_the_column_to_all_seven_tables():
    for t in SILVER_TABLES:
        assert f'"{t}"' in SILVER.split("SILVER_AUDIT_TABLES = (")[1].split(")")[0], t
        assert t in _read(SNOW, "pipeline_run_id_columns.sql")


def test_every_trigger_passes_the_run_id_downstream():
    for dag in ("postgres_to_s3.py", "s3_to_snowflake_bronze.py", "bronze_to_silver.py", "silver_to_gold.py"):
        assert "pipeline_run_id" in re.search(r"TriggerDagRunOperator\((.*?)\n    \)", _read(DAGS, dag), re.S).group(1), dag


def test_bronze_copy_writes_the_run_id():
    text = _read(DAGS, "s3_to_snowflake_bronze.py")
    assert "PIPELINE_RUN_ID" in text and "'{run_id}'" in text
    assert "ADD COLUMN IF NOT EXISTS PIPELINE_RUN_ID" in _read(SNOW, "bronze_metadata_columns.sql")


def test_gold_views_expose_the_run_id_without_changing_row_counts():
    views = re.findall(r'CREATE OR REPLACE VIEW sales_analytics\.gold\.(\w+) AS(.*?)\n"""', GOLD, re.S)
    assert len(views) == 5
    for name, body in views:
        # one value per activity (window MAX), so SELECT DISTINCT still collapses the same rows
        assert "MAX(las.PIPELINE_RUN_ID)" in body and "PARTITION BY las.ACTIVITY_ID, las.LEAD_ID" in body, name
        final = body.rsplit("\nSELECT", 1)[1]
        assert "PIPELINE_RUN_ID" in final, name
