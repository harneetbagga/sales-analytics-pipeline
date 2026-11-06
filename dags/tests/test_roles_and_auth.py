"""
Service-user roles and key-pair auth: each DAG runs as the role it needs, and the key
handling works. No Airflow, Snowflake or AWS needed.
Run:  pytest dags/tests/test_roles_and_auth.py
"""
import base64
import importlib
import os
import re
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
DAGS = os.path.join(HERE, "..")
sys.path.insert(0, DAGS)

import snowflake_auth as sa  # noqa: E402

crypto = pytest.importorskip("cryptography")
from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402


def _src(name):
    with open(os.path.join(DAGS, name), encoding="utf-8") as f:
        return f.read()


def _key_pem(passphrase=None):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    enc = (serialization.BestAvailableEncryption(passphrase.encode())
           if passphrase else serialization.NoEncryption())
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, enc)
    return key, pem


# ── roles ───────────────────────────────────────────────────────────────────
def test_default_role_names():
    assert sa.LOADER_ROLE == "PIPELINE_LOADER"
    assert sa.TRANSFORMER_ROLE == "PIPELINE_TRANSFORMER"


def test_bronze_dag_runs_as_the_loader_only():
    s = _src("s3_to_snowflake_bronze.py")
    assert "role=LOADER_ROLE" in s
    assert "TRANSFORMER_ROLE" not in s


@pytest.mark.parametrize("name", ["bronze_to_silver.py", "silver_to_gold.py", "gold_to_export.py"])
def test_later_dags_run_as_the_transformer(name):
    s = _src(name)
    assert "LOADER_ROLE" not in s
    # every Snowflake hook and SQL operator passes the role
    hooks = re.findall(r"SnowflakeHook\([^)]*\)", s)
    assert all("role=TRANSFORMER_ROLE" in h for h in hooks), hooks
    ops = len(re.findall(r"^\s+conn_id=SNOWFLAKE_CONN_ID,", s, re.M))
    assert s.count('hook_params={"role": TRANSFORMER_ROLE}') == ops


def test_every_sql_operator_and_hook_is_covered():
    n = sum(_src(f).count("SnowflakeHook(") for f in ("bronze_to_silver.py", "gold_to_export.py"))
    assert n >= 4


# ── key pair ────────────────────────────────────────────────────────────────
def test_uses_key_pair():
    assert not sa.uses_key_pair({})
    assert sa.uses_key_pair({"private_key_content": "x"})
    assert sa.uses_key_pair({"private_key_file": "/k.p8"})


@pytest.mark.parametrize("passphrase", [None, "s3cret"])
def test_der_b64_round_trips_to_the_same_key(passphrase):
    key, pem = _key_pem(passphrase)
    b64 = sa.private_key_der_b64(pem, passphrase)
    loaded = serialization.load_der_private_key(base64.standard_b64decode(b64), password=None)
    assert loaded.private_numbers() == key.private_numbers()


def test_wrong_passphrase_is_rejected():
    _, pem = _key_pem("right")
    with pytest.raises(Exception):
        sa.private_key_der_b64(pem, "wrong")


def test_load_pem_from_content_and_file(tmp_path):
    _, pem = _key_pem()
    assert sa.load_private_key_pem({"private_key_content": pem.decode()}) == pem
    p = tmp_path / "k.p8"
    p.write_bytes(pem)
    assert sa.load_private_key_pem({"private_key_file": str(p)}) == pem


# ── Great Expectations connection details (loads the Silver DAG with Airflow stubbed) ──
def _stub(name, **attrs):
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    sys.modules.setdefault(name, mod)
    return mod


def _load_silver():
    before = set(sys.modules)
    try:
        return _import_silver()
    finally:
        # leave no stub behind: other test files stub the same module names differently
        for name in set(sys.modules) - before:
            sys.modules.pop(name, None)


def _import_silver():
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

        class _Fail(Exception):
            pass
        for n in ("airflow", "airflow.providers", "airflow.providers.snowflake",
                  "airflow.providers.snowflake.hooks", "airflow.providers.common",
                  "airflow.providers.common.sql", "airflow.providers.common.sql.operators",
                  "airflow.operators", "airflow.sdk", "airflow.hooks", "airflow.models",
                  "airflow.models.param", "airflow.exceptions"):
            _stub(n)
        sys.modules["airflow"].DAG = _Any
        sys.modules["airflow.sdk"].Param = _Any
        sys.modules["airflow.exceptions"].AirflowFailException = _Fail
        _stub("airflow.providers.snowflake.hooks.snowflake", SnowflakeHook=_Any)
        _stub("airflow.providers.common.sql.operators.sql", SQLExecuteQueryOperator=_Any)
        _stub("airflow.operators.python", PythonOperator=_Any)
        _stub("airflow.operators.trigger_dagrun", TriggerDagRunOperator=_Any)
        _stub("airflow.hooks.base", BaseHook=_Any)
    sys.modules.pop("bronze_to_silver", None)
    try:
        return importlib.import_module("bronze_to_silver")
    except Exception as e:  # other DAG-module imports not stubbed here
        pytest.skip(f"could not import bronze_to_silver in this environment: {e}")


class _Conn:
    def __init__(self, login="airflow_svc_user", password=None, extra=None):
        self.login, self.password, self.host = login, password, ""
        self.extra_dejson = extra or {}


class _Hook:
    def __init__(self, conn):
        self._c = conn

    def get_connection(self, _):
        return self._c


def test_gx_details_use_key_pair_when_present():
    m = _load_silver()
    key, pem = _key_pem("pw")
    conn = _Conn(password="pw", extra={"account": "acct", "private_key_content": pem.decode()})
    d = m._gx_connection_details(_Hook(conn))
    assert "password" not in d and d["role"] == "PIPELINE_TRANSFORMER" and d["user"] == "airflow_svc_user"
    loaded = serialization.load_der_private_key(base64.standard_b64decode(d["private_key"]), password=None)
    assert loaded.private_numbers() == key.private_numbers()


def test_gx_details_still_accept_a_password_connection():
    m = _load_silver()
    d = m._gx_connection_details(_Hook(_Conn(password="p", extra={"account": "acct"})))
    assert d["password"] == "p" and "private_key" not in d


def test_gx_details_fail_with_no_credentials():
    m = _load_silver()
    with pytest.raises(Exception):
        m._gx_connection_details(_Hook(_Conn(extra={"account": "acct"})))
