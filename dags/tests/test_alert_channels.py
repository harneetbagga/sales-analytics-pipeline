"""
Tests for the alert channels: SNS first, SMTP as the backup (no AWS, no Airflow needed).
Run:  pytest dags/tests/test_alert_channels.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DAGS = os.path.join(HERE, "..")
sys.path.insert(0, DAGS)

import alerts  # noqa: E402

ARN = "arn:aws:sns:us-east-1:123456789012:sales-analytics-alerts"


class Spy:
    """Replace the module's channel functions and record the calls."""

    def __init__(self, arn="", sns_raises=False, smtp_raises=False):
        self.sns_calls, self.smtp_calls = [], []
        self._saved = (alerts.sns_topic_arn, alerts._publish_sns, alerts.send_email)

        def fake_arn():
            if arn == "RAISE":
                raise RuntimeError("variable store down")
            return arn

        def fake_sns(topic, subject, message):
            self.sns_calls.append((topic, subject, message))
            if sns_raises:
                raise RuntimeError("sns down")

        def fake_smtp(subject, body):
            self.smtp_calls.append((subject, body))
            if smtp_raises:
                raise RuntimeError("smtp down")

        alerts.sns_topic_arn, alerts._publish_sns, alerts.send_email = fake_arn, fake_sns, fake_smtp

    def restore(self):
        alerts.sns_topic_arn, alerts._publish_sns, alerts.send_email = self._saved


def _run(spy, fn):
    try:
        return fn()
    finally:
        spy.restore()


def test_sns_is_used_and_smtp_is_not_when_sns_works():
    spy = Spy(arn=ARN)
    _run(spy, lambda: alerts.send_alert("[Sales Analytics] FAILED: a.b", "<p>hello</p>"))
    assert len(spy.sns_calls) == 1 and spy.sns_calls[0][0] == ARN
    assert spy.smtp_calls == []


def test_smtp_is_the_backup_when_sns_fails():
    spy = Spy(arn=ARN, sns_raises=True)
    _run(spy, lambda: alerts.send_alert("subject", "<p>hello</p>"))
    assert len(spy.sns_calls) == 1
    assert len(spy.smtp_calls) == 1
    assert spy.smtp_calls[0] == ("subject", "<p>hello</p>")   # the backup keeps the full HTML


def test_smtp_is_used_when_no_topic_is_configured():
    spy = Spy(arn="")
    _run(spy, lambda: alerts.send_alert("subject", "<p>hello</p>"))
    assert spy.sns_calls == [] and len(spy.smtp_calls) == 1


def test_smtp_is_used_when_the_variable_cannot_be_read():
    spy = Spy(arn="RAISE")
    _run(spy, lambda: alerts.send_alert("subject", "<p>hello</p>"))
    assert spy.sns_calls == [] and len(spy.smtp_calls) == 1


def test_notify_failure_never_raises_even_if_both_channels_fail():
    spy = Spy(arn=ARN, sns_raises=True, smtp_raises=True)
    _run(spy, lambda: alerts.notify_failure({"exception": RuntimeError("x")}))
    _run(Spy(arn=ARN, sns_raises=True, smtp_raises=True), lambda: alerts.notify_failure(None))


def test_failure_alert_goes_through_sns_with_plain_text_body():
    spy = Spy(arn=ARN)
    ctx = {"exception": RuntimeError("boom <b>"), "run_id": "manual__1"}
    _run(spy, lambda: alerts.notify_failure(ctx))
    topic, subject, message = spy.sns_calls[0]
    assert subject == "[Sales Analytics] FAILED: unknown_dag.unknown_task"
    assert "<" not in message.replace("boom <b>", "")      # tags stripped (the error text is data)
    assert "Run: manual__1" in message and "RuntimeError: boom <b>" in message


def test_html_to_text_flattens_tables_and_paragraphs():
    body = ("<p>3 rows rejected.</p><table><tr><th>Why</th><th>Rows</th></tr>"
            "<tr><td>TYPE not in known set</td><td align='right'>3</td></tr></table>"
            "<p>Run <code>{\"reprocess_quarantine\": true}</code></p>")
    text = alerts.html_to_text(body)
    assert "<" not in text
    assert "3 rows rejected." in text
    assert "Why | Rows" in text and "TYPE not in known set | 3" in text
    assert '{"reprocess_quarantine": true}' in text
    assert "\n\n\n" not in text


def test_sns_subject_is_one_ascii_line_of_at_most_100_chars():
    s = alerts.sns_subject("[Sales Analytics]  Silver\nquality — FAIL: " + "x" * 200)
    assert "\n" not in s and len(s) == 100
    assert s.isascii()
    assert alerts.sns_subject("short") == "short"


def test_stale_and_dq_emails_convert_cleanly():
    import quarantine as qr
    text = alerts.html_to_text(qr.build_stale_email([("TYPE not in known set", 12, "2026-10-10")]))
    assert "12 quarantined" in text and "TYPE not in known set | 12 | 2026-10-10" in text
    assert "<" not in text


def test_all_alert_senders_use_send_alert():
    with open(os.path.join(DAGS, "bronze_to_silver.py"), encoding="utf-8") as f:
        src = f.read()
    body = src.split("def send_dq_email", 1)[1].split("def fmt_pct", 1)[0]
    assert "alerts.send_alert(" in body and "alerts.send_email(" not in body
