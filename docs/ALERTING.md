# Alerting

Every alert goes out through one function, `alerts.send_alert`:

1. **Amazon SNS (primary).** The message is published to the topic in the Airflow Variable
   `alert_sns_topic_arn`. The topic's subscribers (email, and later SMS or Slack) receive it.
   Authentication is the IAM role of the Airflow environment, so there is no password to store.
   SNS email is plain text, so the HTML body is converted to readable text.
2. **SMTP (backup).** Used only if no topic is configured or the SNS publish fails. It sends the
   full HTML email through the connection `smtp_default` to the addresses in the Variable
   `dq_alert_emails`.

Sending never raises. If both channels are down the run's outcome is unchanged and the problem is
logged in the task log.

| Alert | Subject | Sent by | When |
|---|---|---|---|
| Task failure | `[Sales Analytics] FAILED: <dag>.<task>` | `alerts.notify_failure`, set as `on_failure_callback` in all five DAGs | A task still fails after its retries. One alert per failed task; tasks that only fail because an upstream task failed send nothing. A sensor timeout counts as a failure. |
| Data quality | `[Sales Analytics] Silver data quality WARN/FAIL ...` | `validate_lap_staging` | Rejected rows in a Silver load (see `QUARANTINE_RUNBOOK.md`) |
| Stale quarantine | `[Sales Analytics] N quarantined rows open for more than 7 days` | `replay_and_resolve_quarantine` | Open quarantine rows older than 7 days |

## Setup (once per environment)

### SNS (primary)

1. Create the topic and subscribe the team:
   `REGION=us-east-1 scripts/setup_sns_alerts.sh you@example.com teammate@example.com`
   Each address gets a confirmation email from AWS; alerts start after it clicks **Confirm**.
2. Set the Airflow Variable `alert_sns_topic_arn` to the ARN the script prints
   (Admin > Variables, or `airflow variables set alert_sns_topic_arn <arn>`).
3. Allow the Airflow environment to publish. Local Airflow uses your AWS credentials through the
   `aws_default` connection. On MWAA, attach this inline policy to the environment's execution role
   (replace the ARN):
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": "sns:Publish",
       "Resource": "arn:aws:sns:us-east-1:<account-id>:sales-analytics-alerts"
     }]
   }
   ```
4. `apache-airflow-providers-amazon` must be installed (it is in `mwaa/requirements.txt`).

### SMTP (backup)

1. Install `apache-airflow-providers-smtp` (listed in `mwaa/requirements.txt`).
2. Create the connection `smtp_default` (host, port, login, password, TLS as your mail provider needs).
3. Set the Variable `dq_alert_emails` to a comma-separated list of addresses.

### Both

Upload `alerts.py` and `quarantine.py` next to the DAG files. A DAG that imports a module missing
from the dags folder fails to parse.

## Test it

Unpause `alert_smoke_test` and trigger it once. It fails on purpose, and the alert should arrive within
a minute through SNS (the task log says "Alert sent through SNS"). To test the backup, clear the
`alert_sns_topic_arn` Variable (or point it at a topic you cannot publish to) and trigger it again: the
log says "falling back to SMTP" and the HTML email arrives instead. Restore the Variable afterwards.

## When a failure alert arrives

1. Open the task log, find the cause.
2. Fix it, then clear the failed task. The run resumes from that task; completed tasks are not repeated.
3. If `postgres_to_s3` failed for a day the source delivered late or never delivered, reload the day:
   trigger `postgres_to_s3` with `{"target_date": "YYYY-MM-DD"}`. It extracts that day, then the chain
   (Bronze, Silver, Gold, export) follows.
4. If Silver stopped on the data-quality gate, follow `QUARANTINE_RUNBOOK.md`.
