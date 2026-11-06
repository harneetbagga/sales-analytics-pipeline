"""
Orchestration settings agreed in the SME review (29 Oct 2026).

Static checks: the DAG files are read as text, so no Airflow or database is needed.
"""
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
DAGS = os.path.join(HERE, "..")
DAG_FILES = ("postgres_to_s3.py", "s3_to_snowflake_bronze.py", "bronze_to_silver.py",
             "silver_to_gold.py", "gold_to_export.py")


def _read(name):
    with open(os.path.join(DAGS, name), encoding="utf-8") as f:
        return f.read()


def test_source_sensor_polls_every_15_minutes_in_reschedule_mode():
    src = _read("postgres_to_s3.py")
    block = src[src.index("wait_for_postgres = SqlSensor("):]
    block = block[:block.index("retries=0")]
    assert re.search(r"poke_interval=900\b", block)
    assert 'mode="reschedule"' in block


def test_sensor_timeout_stays_two_hours():
    src = _read("postgres_to_s3.py")
    assert "SLA_VARIANCE_BUFFER_SECONDS = 60 * 60 * 2" in src


def test_every_dag_is_single_run_without_catchup():
    for name in DAG_FILES:
        src = _read(name)
        assert re.search(r"catchup=False", src), name
        assert re.search(r"max_active_runs=1\b", src), name
