"""Email notifications for the pipeline (Gmail SMTP).

Reads its settings from the environment (put them in the .env file, which git ignores):
    ALERT_EMAIL_FROM          the Gmail address that sends the alerts
    ALERT_EMAIL_APP_PASSWORD  a Gmail *App Password* (16 characters), NOT your normal password
    ALERT_EMAIL_TO            where alerts go (optional; defaults to ALERT_EMAIL_FROM)

If a setting is missing, every function quietly does nothing: alerts must never break the pipeline.

Test:  docker compose exec airflow-scheduler python /opt/airflow/project/notify.py
"""

import mimetypes
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465            # SSL


def _settings() -> tuple[str, str, str] | None:
    sender = os.environ.get("ALERT_EMAIL_FROM", "").strip()
    password = os.environ.get("ALERT_EMAIL_APP_PASSWORD", "").replace(" ", "").strip()
    to = os.environ.get("ALERT_EMAIL_TO", "").strip() or sender
    return (sender, password, to) if sender and password else None


def _send(subject: str, body: str, attachment: Path | None = None) -> bool:
    settings = _settings()
    if settings is None:
        print("Email alerts not configured (ALERT_EMAIL_FROM / ALERT_EMAIL_APP_PASSWORD missing); skipping.")
        return False
    sender, password, to = settings

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"FII/DII Pipeline <{sender}>"
    msg["To"] = to
    msg.set_content(body)
    if attachment is not None and Path(attachment).exists():
        ctype = mimetypes.guess_type(str(attachment))[0] or "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        msg.add_attachment(Path(attachment).read_bytes(), maintype=maintype, subtype=subtype,
                           filename=Path(attachment).name)
    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        return True
    except Exception as exc:  # wrong password, no network...: report (never the password) and carry on
        print(f"Email alert failed: {type(exc).__name__}: {exc}")
        return False


def _subject(text: str) -> str:
    return text.strip().splitlines()[0][:150] if text.strip() else "FII/DII pipeline"


def send_message(text: str) -> bool:
    return _send(_subject(text), text)


def send_photo(path: Path, caption: str = "") -> bool:
    """Same name as before so the DAG doesn't change: the image goes as an attachment."""
    return _send(_subject(caption), caption, attachment=Path(path))


# ---------------------------------------------------------------------------------------------
# Message builders (pure functions, unit-tested)
# ---------------------------------------------------------------------------------------------

def failure_text(dag_id: str, task_id: str, run_date: str, try_number: int, error: str, log_url: str = "") -> str:
    lines = [
        f"🔴 Pipeline task failed: {task_id} ({run_date})",
        "",
        f"Task: {task_id}",
        f"Run date: {run_date}",
        f"Attempt: {try_number}",
        f"Error: {error.strip()[:500] or 'see log'}",
    ]
    if log_url:
        lines.append(f"Log: {log_url}")
    lines.append(f"DAG: {dag_id}")
    return "\n".join(lines)


def summary_text(run_date: str, states: dict[str, str]) -> str:
    """states: task_id -> state ('success', 'failed', 'upstream_failed', 'skipped', ...)."""
    total = len(states)
    ok = [t for t, s in states.items() if s == "success"]
    bad = sorted(t for t, s in states.items() if s in ("failed", "upstream_failed"))
    other = sorted(t for t, s in states.items() if s not in ("success", "failed", "upstream_failed"))
    if not bad and not other:
        return f"✅ {run_date}: all {total} tasks succeeded"
    if bad:
        return f"⚠️ {run_date}: {len(bad)} of {total} tasks failed: {', '.join(bad)}"
    return f"🟡 {run_date}: {len(ok)} of {total} succeeded; not run: {', '.join(other)}"


if __name__ == "__main__":
    settings = _settings()
    if settings is None:
        print("Not configured: add ALERT_EMAIL_FROM and ALERT_EMAIL_APP_PASSWORD to .env")
    else:
        sent = send_message("👋 Test from your FII/DII pipeline\n\nEmail alerts are working.")
        print(f"Sent to {settings[2]}! Check your inbox." if sent else "Sending failed; see the message above.")
