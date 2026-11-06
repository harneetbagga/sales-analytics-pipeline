# Service users and roles

Automated access never runs under a personal login. Airflow (MWAA) connects as `airflow_svc_user`
and QuickSight as `quicksight_svc`. Both use RSA key pairs (2048-bit); neither has a password.

| Who | Snowflake user | Role |
|---|---|---|
| `s3_to_snowflake_bronze` | `airflow_svc_user` | `pipeline_loader` (Bronze only) |
| the other four DAGs | `airflow_svc_user` | `pipeline_transformer` (Silver, Gold; inherits the loader) |
| QuickSight | `quicksight_svc` | `report_reader` (read-only Gold) |
| human analysts | their own login | `analyst_role` (inherits `report_reader`) |
| admin | their own login | `sysadmin` (inherits `pipeline_transformer`) |

The roles are chosen in code per DAG (`dags/snowflake_auth.py`), not by the connection.

## One-time setup

1. **Generate the key pair** (on your computer, never in the repo):
   ```
   openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -inform PEM -out airflow_svc_key.p8
   openssl rsa -in airflow_svc_key.p8 -pubout -out airflow_svc_key.pub
   ```
   It asks for a passphrase (keep it). For no passphrase use `-nocrypt` instead of `-v2 aes256`.
   Public key body = the `.pub` file without the BEGIN/END lines.
2. **Run `snowflake/service_users_and_role_hierarchy.sql`** as ACCOUNTADMIN after pasting the public key.
   Check that the two "must FAIL" statements fail.
3. **Update the `snowflake_default` connection** (in the AWS Secrets Manager secret
   `airflow/connections/snowflake_default` on MWAA):
   ```json
   {
     "conn_type": "snowflake",
     "login": "airflow_svc_user",
     "password": "<key passphrase, or empty if the key has none>",
     "extra": {
       "account": "<account identifier>",
       "warehouse": "sales_wh",
       "database": "sales_analytics",
       "private_key_content": "-----BEGIN ENCRYPTED PRIVATE KEY-----\n...\n-----END ENCRYPTED PRIVATE KEY-----\n"
     }
   }
   ```
   With a key present, the Password field is the key's passphrase, not a login password.
4. **Deploy** the DAG files (`snowflake_auth.py` is new and must be in `dags/` too).
5. **Verify**: run the five-DAG chain once. In Snowflake:
   ```sql
   SELECT user_name, role_name, COUNT(*) FROM snowflake.account_usage.query_history
   WHERE start_time > DATEADD(hour, -3, CURRENT_TIMESTAMP()) GROUP BY 1, 2;
   ```
   Bronze loads show `AIRFLOW_SVC_USER / PIPELINE_LOADER`; everything else `PIPELINE_TRANSFORMER`.
   Also confirm the Great Expectations step (it takes the key as base64 DER) and the Gold regression
   gate pass: they are the two places that read the key directly.

## If something fails
- **"Insufficient privileges" on the Bronze load**: a grant is missing for the loader. Set the
  environment variable `PIPELINE_LOADER_ROLE=PIPELINE_TRANSFORMER` on MWAA to run Bronze as the
  transformer again, then fix the grant.
- **Any other DAG**: same idea with `PIPELINE_TRANSFORMER_ROLE`. A password connection (no
  `private_key_content` in Extra) still works, so restoring the old secret is the full rollback.
