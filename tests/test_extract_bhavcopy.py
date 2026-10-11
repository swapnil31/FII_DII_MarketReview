"""Unit tests for extract_bhavcopy.py: URLs and file validation (no network needed).

Run:  pytest -q
"""

import io
import sys
import zipfile
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import extract_bhavcopy as eb  # noqa: E402


def make_zip(header: list[str], rows: int, name: str = "file.csv") -> bytes:
    lines = [",".join(header)] + [",".join(["x"] * len(header)) for _ in range(rows)]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(name, "\n".join(lines))
    return buf.getvalue()


CLASSIC_HEADER = ["SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "LAST", "PREVCLOSE",
                  "TOTTRDQTY", "TOTTRDVAL", "TIMESTAMP", "TOTALTRADES", "ISIN"]
UDIFF_HEADER = ["TradDt", "BizDt", "Sgmt", "Src", "FinInstrmTp", "FinInstrmId", "ISIN", "TckrSymb",
                "SctySrs", "OpnPric", "HghPric", "LwPric", "ClsPric", "LastPric", "PrvsClsgPric",
                "TtlTradgVol", "TtlTrfVal", "TtlNbOfTxsExctd"]


def test_classic_url_format():
    assert eb.classic_url(date(2024, 6, 28)) == (
        "https://nsearchives.nseindia.com/content/historical/EQUITIES/2024/JUN/cm28JUN2024bhav.csv.zip"
    )


def test_udiff_url_format():
    assert eb.udiff_url(date(2026, 10, 9)) == (
        "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20261009_F_0000.csv.zip"
    )


def test_candidate_order_switches_at_udiff_start():
    assert eb.candidate_urls(date(2026, 10, 9))[0].endswith("20261009_F_0000.csv.zip")
    assert eb.candidate_urls(date(2020, 3, 2))[0].endswith("cm02MAR2020bhav.csv.zip")


def test_detects_classic_format():
    assert eb.detect_format(make_zip(CLASSIC_HEADER, 1800)) == ("classic", 1800)


def test_detects_udiff_format():
    assert eb.detect_format(make_zip(UDIFF_HEADER, 2500)) == ("udiff", 2500)


def test_rejects_unknown_header():
    with pytest.raises(ValueError, match="unknown header"):
        eb.detect_format(make_zip(["A", "B", "C"], 1000))


def test_rejects_too_few_rows():
    with pytest.raises(ValueError, match="only 10 rows"):
        eb.detect_format(make_zip(CLASSIC_HEADER, 10))


def test_weekdays_skip_weekends():
    days = eb.trading_weekdays(date(2026, 10, 9), date(2026, 10, 12))   # Fri..Mon
    assert days == [date(2026, 10, 9), date(2026, 10, 12)]
