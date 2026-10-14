"""Unit tests for the parsing logic in spark_bhavcopy.py (pure Python, no Spark needed).

Run:  pytest -q
"""

import io
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import spark_bhavcopy as sb  # noqa: E402


def zip_bytes(text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("bhav.csv", text)
    return buf.getvalue()


CLASSIC = (
    "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,\n"
    "RELIANCE,EQ,2900,2950,2880,2940,2941,2890,1000000,2940000000,28-JUN-2024,50000,INE002A01018,\n"
    "SOMEBOND,N1,100,100,100,100,100,100,10,1000,28-JUN-2024,1,INE000000000,\n"
)

UDIFF = (
    "TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,"
    "StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,"
    "SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,"
    "Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4\n"
    "2026-10-09,2026-10-09,CM,NSE,STK,2885,INE002A01018,RELIANCE,EQ,,,,,RELIANCE INDUSTRIES LTD,"
    "1400,1420,1395,1415,1416,1398,,1415,,,5000000,7075000000,120000,F1,1,,,,,\n"
)


def test_classic_row_is_unified_and_non_eq_dropped():
    rows = sb.parse_zip("x/cm28JUN2024bhav.csv.zip", zip_bytes(CLASSIC))
    assert len(rows) == 1                                    # bond series N1 dropped
    rec = dict(zip(sb.UNIFIED_FIELDS, rows[0]))
    assert rec["trade_date"] == "2024-06-28"
    assert rec["symbol"] == "RELIANCE" and rec["close"] == 2940.0 and rec["prev_close"] == 2890.0
    assert rec["volume"] == 1_000_000 and rec["source_format"] == "classic"
    assert rec["source_file"] == "cm28JUN2024bhav.csv.zip"


def test_udiff_row_is_unified():
    rows = sb.parse_zip("x/BhavCopy_NSE_CM_0_0_0_20261009_F_0000.csv.zip", zip_bytes(UDIFF))
    rec = dict(zip(sb.UNIFIED_FIELDS, rows[0]))
    assert rec["trade_date"] == "2026-10-09"
    assert rec["symbol"] == "RELIANCE" and rec["close"] == 1415.0 and rec["prev_close"] == 1398.0
    assert rec["turnover"] == 7_075_000_000 and rec["source_format"] == "udiff"


def test_both_formats_produce_identical_columns():
    a = sb.parse_zip("a.zip", zip_bytes(CLASSIC))[0]
    b = sb.parse_zip("b.zip", zip_bytes(UDIFF))[0]
    assert len(a) == len(b) == len(sb.UNIFIED_FIELDS)
    assert [type(x) for x in a] == [type(x) for x in b]


def test_bad_file_returns_error_marker_not_crash():
    rows = sb.parse_zip("x/broken.zip", b"not a zip at all")
    assert len(rows) == 1
    rec = dict(zip(sb.UNIFIED_FIELDS, rows[0]))
    assert rec["symbol"] == "__ERROR__" and rec["source_file"] == "broken.zip"
    assert len(rows[0]) == len(sb.UNIFIED_FIELDS)


def test_unknown_header_reported():
    rows = sb.parse_zip("x/odd.zip", zip_bytes("A,B,C\n1,2,3\n"))
    assert rows[0][1] == "__ERROR__" and "unknown header" in rows[0][2]


def test_blank_numbers_become_none():
    assert sb._num("") is None and sb._num("-") is None and sb._num(" 12.5 ") == 12.5


# ---- Incremental planning ---------------------------------------------------------------------

RAW = {"dt=2026-09-30/a.zip": 100, "dt=2026-10-12/b.zip": 200, "dt=2026-10-13/c.zip": 300}


def test_nothing_new_means_no_months():
    assert sb.plan_months(RAW, dict(RAW)) == []


def test_new_file_reprocesses_only_its_month():
    seen = {k: v for k, v in RAW.items() if k != "dt=2026-10-13/c.zip"}
    assert sb.plan_months(RAW, seen) == [(2026, 10)]


def test_changed_size_counts_as_new():
    seen = dict(RAW, **{"dt=2026-09-30/a.zip": 99})
    assert sb.plan_months(RAW, seen) == [(2026, 9)]


def test_empty_manifest_means_every_month():
    assert sb.plan_months(RAW, {}) == [(2026, 9), (2026, 10)]
