"""
Snowflake service-user roles and key-pair helpers, shared by the DAGs.

No Airflow import here, so it can be unit-tested without Airflow.

Roles (least privilege, set per DAG so no task runs with more access than it needs):
  - s3_to_snowflake_bronze runs as PIPELINE_LOADER (writes Bronze only).
  - every later DAG runs as PIPELINE_TRANSFORMER (builds Silver and Gold, and inherits
    the loader's privileges, so it can also read Bronze).
The role names can be overridden with the PIPELINE_LOADER_ROLE / PIPELINE_TRANSFORMER_ROLE
environment variables, which is also the quick way back to a single role if a grant is missing.

Authentication: the snowflake_default connection can use an RSA key pair (the service
user, airflow_svc_user, has no password). Put the PEM private key in the connection's
Extra as "private_key_content" (or a path as "private_key_file"); if the key is encrypted,
its passphrase goes in the connection's Password field. A connection with only a password
keeps working.
"""
import base64
import os

LOADER_ROLE = os.environ.get("PIPELINE_LOADER_ROLE", "PIPELINE_LOADER").upper()
TRANSFORMER_ROLE = os.environ.get("PIPELINE_TRANSFORMER_ROLE", "PIPELINE_TRANSFORMER").upper()


def uses_key_pair(extra):
    """True when the connection Extra carries a private key (content or file path)."""
    return bool(extra.get("private_key_content") or extra.get("private_key_file"))


def load_private_key_pem(extra):
    """The PEM private key from the connection Extra, as bytes."""
    content = extra.get("private_key_content")
    if content:
        return content.encode() if isinstance(content, str) else content
    with open(extra["private_key_file"], "rb") as f:
        return f.read()


def private_key_der_b64(pem, passphrase=None):
    """Unencrypted PKCS8 DER of the key, base64 encoded.

    This is the form Great Expectations' Snowflake key-pair connection expects.
    """
    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(
        pem, password=passphrase.encode() if passphrase else None
    )
    der = key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return base64.standard_b64encode(der).decode()
