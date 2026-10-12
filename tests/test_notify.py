"""Tests for notify.py message building and the 'not configured' safety net (no network)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import notify  # noqa: E402


def test_unconfigured_never_raises(monkeypatch):
    for key in ("ALERT_EMAIL_FROM", "ALERT_EMAIL_APP_PASSWORD", "ALERT_EMAIL_TO"):
        monkeypatch.delenv(key, raising=False)
    assert notify.send_message("hi") is False


def test_recipient_defaults_to_sender(monkeypatch):
    monkeypatch.setenv("ALERT_EMAIL_FROM", "me@gmail.com")
    monkeypatch.setenv("ALERT_EMAIL_APP_PASSWORD", "abcd efgh ijkl mnop")
    monkeypatch.delenv("ALERT_EMAIL_TO", raising=False)
    assert notify._settings() == ("me@gmail.com", "abcdefghijklmnop", "me@gmail.com")


def test_subject_is_first_line():
    assert notify._subject("✅ 2026-10-12: all 10 tasks succeeded\n\nNifty fell") == "✅ 2026-10-12: all 10 tasks succeeded"


def test_summary_all_good():
    assert notify.summary_text("2026-10-12", {"a": "success", "b": "success"}) == "✅ 2026-10-12: all 2 tasks succeeded"


def test_summary_lists_failures():
    text = notify.summary_text("2026-10-12", {"a": "success", "spark": "failed", "sectors": "upstream_failed"})
    assert text.startswith("⚠️") and "2 of 3 tasks failed" in text and "spark" in text


def test_failure_text_has_task_and_error():
    text = notify.failure_text("dag", "extract_fii_dii", "2026-10-12", 7, "ValueError('late')", "http://x/log")
    assert text.startswith("🔴 Pipeline task failed: extract_fii_dii") and "late" in text and "http://x/log" in text
