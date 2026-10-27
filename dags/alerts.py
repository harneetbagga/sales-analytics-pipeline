"""
Shared alerting for all five DAGs (postgres_to_s3, s3_to_snowflake_bronze,
bronze_to_silver, silver_to_gold, gold_to_export).

Channels
  1. Amazon SNS (primary): the message is published to the topic named in the Airflow
     Variable `alert_sns_topic_arn`; the topic's email (or SMS, Slack, ...) subscribers
     receive it. Authentication is the IAM role of the Airflow environment, so there is
     no password to store. SNS email is plain text, so the HTML body is converted.
  2. SMTP (backup): used only if no topic is configured or the SNS publish fails. It
     sends the full HTML email through the Airflow connection `smtp_default` to the
     addresses in the Variable `dq_alert_emails`.

Sending never raises: an alerting problem must not change the outcome of a pipeline run
(it is logged instead).

Failure callback
  Every DAG sets  default_args["on_failure_callback"] = notify_failure, so a task that
  still fails after its retries sends ONE alert naming the DAG, task, run and error.
  Tasks that only fail because an upstream task failed (upstream_failed) do not alert,
  so one broken task is one alert, not a wall of them. A sensor that times out is a
  normal task failure and alerts the same way.

Needs: apache-airflow-providers-amazon (SNS), apache-airflow-providers-smtp (backup),
the Variable(s) above and, for SNS, sns:Publish on the topic for the environment's IAM
role. See docs/ALERTING.md.
"""
import html
import logging
import re

SUBJECT_PREFIX = "[Sales Analytics]"
MAX_ERROR_CHARS = 1500

# SNS (primary)
SNS_TOPIC_VARIABLE = "alert_sns_topic_arn"
AWS_CONN_ID = "aws_default"
SNS_SUBJECT_MAX = 100          # SNS limit
SNS_MESSAGE_MAX = 200_000      # SNS limit is 256 KB

# SMTP (backup)
SMTP_CONN_ID = "smtp_default"
ALERT_VARIABLE = "dq_alert_emails"


# ── configuration ─────────────────────────────────────────────────────────

def _variable(name):
    """Airflow Variable value, '' if missing (works on Airflow 2 and 3)."""
    try:
        from airflow.sdk import Variable
        return Variable.get(name, default="")
    except ImportError:  # Airflow 2
        from airflow.models import Variable
        return Variable.get(name, default_var="")


def sns_topic_arn():
    return str(_variable(SNS_TOPIC_VARIABLE) or "").strip()


def alert_recipients():
    raw = _variable(ALERT_VARIABLE)
    return [e.strip() for e in str(raw).split(",") if e.strip()]


# ── formatting ────────────────────────────────────────────────────────────

def html_to_text(body):
    """Readable plain text from the HTML alert bodies (SNS email is plain text)."""
    t = re.sub(r"(?i)<br\s*/?>", "\n", body)
    t = re.sub(r"(?i)</(p|tr|pre|table)>", "\n", t)
    t = re.sub(r"(?i)</(td|th)>", " | ", t)
    t = re.sub(r"<[^>]+>", "", t)
    t = html.unescape(t)
    lines = [re.sub(r"\s*\|\s*$", "", ln).strip() for ln in t.splitlines()]
    out = []
    for ln in lines:
        if ln or (out and out[-1]):
            out.append(ln)
    return "\n".join(out).strip()


def sns_subject(subject):
    """SNS subjects must be one line of ASCII, at most 100 characters."""
    one_line = " ".join(str(subject).split())
    return one_line.encode("ascii", "replace").decode("ascii")[:SNS_SUBJECT_MAX]


# ── channels ──────────────────────────────────────────────────────────────

def _publish_sns(topic_arn, subject, message):
    """Publish to the SNS topic using the environment's AWS connection (IAM role)."""
    from airflow.providers.amazon.aws.hooks.sns import SnsHook
    parts = topic_arn.split(":")
    region = parts[3] if len(parts) >= 6 else None      # arn:aws:sns:<region>:<account>:<name>
    client = SnsHook(aws_conn_id=AWS_CONN_ID, region_name=region).get_conn()
    client.publish(TopicArn=topic_arn, Subject=sns_subject(subject), Message=message[:SNS_MESSAGE_MAX])


def send_email(subject, html_body):
    """SMTP backup channel. Never raises."""
    try:
        recipients = alert_recipients()
        if not recipients:
            logging.warning("No recipients in Variable %s; email not sent: %s", ALERT_VARIABLE, subject)
            return
        from airflow.providers.smtp.hooks.smtp import SmtpHook
        with SmtpHook(smtp_conn_id=SMTP_CONN_ID) as smtp:
            smtp.send_email_smtp(to=recipients, subject=subject, html_content=html_body)
        logging.info("Alert email sent through SMTP to %s: %s", recipients, subject)
    except Exception:
        logging.exception("Could not send alert email (pipeline outcome unchanged): %s", subject)


def send_alert(subject, html_body):
    """SNS first; SMTP only if no topic is configured or the SNS publish fails. Never raises."""
    try:
        arn = sns_topic_arn()
    except Exception:
        logging.exception("Could not read Variable %s; using SMTP", SNS_TOPIC_VARIABLE)
        arn = ""
    if arn:
        try:
            _publish_sns(arn, subject, html_to_text(html_body))
            logging.info("Alert sent through SNS: %s", subject)
            return
        except Exception:
            logging.exception("SNS publish failed; falling back to SMTP: %s", subject)
    else:
        logging.info("No SNS topic configured (Variable %s); using SMTP", SNS_TOPIC_VARIABLE)
    send_email(subject, html_body)


# ── task-failure alert ────────────────────────────────────────────────────

def _attr(obj, name, default=None):
    value = getattr(obj, name, default)
    return default if value is None else value


def extract_failure_info(context):
    """Pull the fields we report out of an Airflow task-failure context, defensively
    (Airflow 2 and 3 expose slightly different objects)."""
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    exc = context.get("exception")
    return {
        "dag_id": _attr(ti, "dag_id", _attr(dag, "dag_id", "unknown_dag")),
        "task_id": _attr(ti, "task_id", "unknown_task"),
        "run_id": context.get("run_id") or _attr(dag_run, "run_id", _attr(ti, "run_id", "unknown_run")),
        "try_number": _attr(ti, "try_number", "?"),
        "max_tries": _attr(ti, "max_tries", None),
        "error": "" if exc is None else f"{type(exc).__name__}: {exc}",
    }


def build_failure_email(info):
    """(subject, html) for one failed task."""
    subject = f"{SUBJECT_PREFIX} FAILED: {info['dag_id']}.{info['task_id']}"
    attempts = (f"{info['try_number']} of {int(info['max_tries']) + 1}"
                if info.get("max_tries") not in (None, "") else str(info["try_number"]))
    error = html.escape(info["error"][:MAX_ERROR_CHARS]) or "(no exception text)"
    body = f"""
<p>A pipeline task failed after its retries were used up.</p>
<p><b>DAG:</b> {html.escape(str(info['dag_id']))}<br>
<b>Task:</b> {html.escape(str(info['task_id']))}<br>
<b>Run:</b> {html.escape(str(info['run_id']))}<br>
<b>Attempt:</b> {attempts}</p>
<p><b>Error</b></p>
<pre>{error}</pre>
<p><b>What to do:</b> open the task log in Airflow, fix the cause, then clear the failed task so the
run resumes from it (completed tasks are not repeated). A day the source delivered late or never
delivered is reloaded by triggering <code>postgres_to_s3</code> with
<code>{{"target_date": "YYYY-MM-DD"}}</code>. Steps: docs/ALERTING.md</p>
"""
    return subject, body


def notify_failure(context):
    """Airflow on_failure_callback. Never raises."""
    try:
        subject, body = build_failure_email(extract_failure_info(context))
        send_alert(subject, body)
    except Exception:
        logging.exception("failure callback itself failed (pipeline outcome unchanged)")
