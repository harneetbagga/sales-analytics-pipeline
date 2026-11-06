"""
SPICE refresh: trigger returns ingestion IDs, the sensor polls them to completion and fails
on FAILED/CANCELLED. No Airflow, QuickSight or AWS needed (clients are faked).
Run:  pytest dags/tests/test_spice_refresh.py
"""
import importlib
import os
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
DAGS = os.path.join(HERE, "..")
sys.path.insert(0, DAGS)


def _stub(name, **attrs):
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    sys.modules.setdefault(name, mod)
    return mod


class _FailException(Exception):
    pass


def _load():
    before = set(sys.modules)
    try:
        return _import_gold()
    finally:
        # leave no stub behind: other test files stub the same module names differently
        for name in set(sys.modules) - before:
            sys.modules.pop(name, None)


def _import_gold():
    try:
        import airflow  # noqa: F401
        import boto3  # noqa: F401
    except ImportError:
        class _Any:
            def __init__(self, *a, **k): pass
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def __rshift__(self, other): return other
            def __rrshift__(self, other): return self
            def __call__(self, *a, **k): return _Any()
        for n in ("airflow", "airflow.providers", "airflow.providers.snowflake",
                  "airflow.providers.snowflake.hooks", "airflow.hooks", "airflow.operators",
                  "airflow.sensors", "airflow.exceptions"):
            _stub(n)
        sys.modules["airflow"].DAG = _Any
        _stub("airflow.providers.snowflake.hooks.snowflake", SnowflakeHook=_Any)
        _stub("airflow.hooks.base", BaseHook=_Any)
        _stub("airflow.operators.python", PythonOperator=_Any)
        _stub("airflow.sensors.python", PythonSensor=_Any)
        sys.modules["airflow.exceptions"].AirflowFailException = _FailException
        _stub("boto3")
        _stub("alerts", notify_failure=lambda *a, **k: None)
    sys.modules.pop("gold_to_export", None)
    return importlib.import_module("gold_to_export")


g = _load()
FAIL = g.AirflowFailException


class FakeQS:
    def __init__(self, statuses, error=None):
        self.statuses = statuses  # dataset_id -> status
        self.error = error
        self.created = []

    def create_ingestion(self, **kw):
        self.created.append(kw)
        return {"IngestionStatus": "INITIALIZED"}

    def describe_ingestion(self, DataSetId, IngestionId, AwsAccountId):
        ing = {"IngestionStatus": self.statuses[DataSetId]}
        if self.error:
            ing["ErrorInfo"] = self.error
        return {"Ingestion": ing}


def _ingestions():
    return {name: f"{ds.replace('-', '')}-1" for name, ds in g.QUICKSIGHT_DATASETS.items()}


def _all(status):
    return {ds: status for ds in g.QUICKSIGHT_DATASETS.values()}


def test_all_completed_returns_true():
    assert g.check_spice_ingestions(FakeQS(_all("COMPLETED")), "acct", _ingestions()) is True


@pytest.mark.parametrize("state", ["INITIALIZED", "QUEUED", "RUNNING"])
def test_in_progress_returns_false(state):
    assert g.check_spice_ingestions(FakeQS(_all(state)), "acct", _ingestions()) is False


def test_one_still_running_keeps_waiting():
    statuses = _all("COMPLETED")
    statuses[g.QUICKSIGHT_DATASETS["closer_report"]] = "RUNNING"
    assert g.check_spice_ingestions(FakeQS(statuses), "acct", _ingestions()) is False


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_failed_or_cancelled_raises_with_error_info(state):
    qs = FakeQS(_all(state), error={"Type": "DATA_SOURCE_CONNECTION", "Message": "warehouse unreachable"})
    with pytest.raises(FAIL) as exc:
        g.check_spice_ingestions(qs, "acct", _ingestions())
    assert state in str(exc.value) and "warehouse unreachable" in str(exc.value)


def test_disabled_trigger_returns_empty_and_sensor_passes(monkeypatch):
    monkeypatch.setattr(g, "QUICKSIGHT_REFRESH_SPICE", False)
    assert g.refresh_quicksight_datasets() == {}

    class TI:
        def xcom_pull(self, task_ids):
            return {}
    assert g.spice_refresh_complete(ti=TI()) is True


def test_trigger_returns_ingestion_ids_for_every_dataset(monkeypatch):
    qs = FakeQS(_all("RUNNING"))
    monkeypatch.setattr(g, "QUICKSIGHT_REFRESH_SPICE", True)
    monkeypatch.setattr(g, "QUICKSIGHT_ACCOUNT_ID", "acct")
    monkeypatch.setattr(g.boto3, "client", lambda *a, **k: qs, raising=False)
    started = g.refresh_quicksight_datasets()
    assert set(started) == set(g.QUICKSIGHT_DATASETS)
    assert len(qs.created) == 4
    assert all(c["IngestionType"] == "FULL_REFRESH" for c in qs.created)
    assert all(started[n].startswith(g.QUICKSIGHT_DATASETS[n].replace("-", "")) for n in started)


def test_dag_wires_sensor_after_refresh():
    src = open(os.path.join(DAGS, "gold_to_export.py"), encoding="utf-8").read()
    assert "refresh_quicksight_task >> wait_for_spice_refresh" in src
    assert 'mode="reschedule"' in src and "retries=0" in src
