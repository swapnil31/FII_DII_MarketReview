"""Tests for weekly_report.py on a synthetic 3-stock week (no lake, database or network)."""

import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import weekly_report as wr  # noqa: E402

MON, FRI = date(2026, 10, 12), date(2026, 10, 16)
DAYS = pd.bdate_range(MON, FRI)


def stock(symbol, industry, daily, turnover=1e9, suspect=False, days=DAYS):
    return [(d, symbol, industry, 100.0, r, suspect, turnover) for d, r in zip(days, daily)]


def frame(rows):
    return pd.DataFrame(rows, columns=["trade_date", "symbol", "industry", "close", "daily_return",
                                       "return_suspect", "turnover"])


FLOWS = pd.DataFrame({
    "trade_date": DAYS, "fii_buy": [10000] * 5, "fii_sell": [12000, 11000, 9000, 13000, 12000],
    "fii_net": [-2000, -1000, 1000, -3000, -2000], "dii_buy": [9000] * 5, "dii_sell": [7000] * 5,
    "dii_net": [2000] * 5, "nifty_close": [22000, 22100, 22200, 22000, 22440]})


def test_week_bounds():
    assert wr.week_bounds(date(2026, 10, 14)) == (MON, FRI)


def test_weekly_return_compounds_daily_returns():
    s = wr.summarize(frame(stock("UP", "IT", [0.1] * 5)), FLOWS, 22000, MON, FRI)
    assert round(s["gainers"].iloc[0]["week_pct"], 2) == round((1.1 ** 5 - 1) * 100, 2)


def test_filters_unclassified_suspect_illiquid_and_partial_weeks():
    rows = (stock("GOOD", "IT", [0.01] * 5) + stock("JUNK", "Unclassified", [0.2] * 5)
            + stock("SPLIT", "IT", [0.01] * 5, suspect=True) + stock("TINY", "IT", [0.2] * 5, turnover=1e6)
            + stock("NEW", "IT", [0.2] * 3, days=DAYS[2:]))
    s = wr.summarize(frame(rows), FLOWS, 22000, MON, FRI)
    assert list(s["gainers"]["symbol"]) == ["GOOD"]


def test_losers_sorted_worst_first():
    rows = stock("A", "IT", [-0.01] * 5) + stock("B", "IT", [-0.03] * 5) + stock("C", "IT", [0.02] * 5)
    s = wr.summarize(frame(rows), FLOWS, 22000, MON, FRI)
    assert list(s["losers"]["symbol"])[:2] == ["B", "A"]


def test_flows_and_nifty_week():
    s = wr.summarize(frame(stock("A", "IT", [0.0] * 5)), FLOWS, 22000, MON, FRI)
    f = s["flow"]
    assert f["fii_net"] == -7000 and f["fii_sell_days"] == 4 and f["dii_net"] == 10000
    assert round(s["nifty_pct"], 2) == 2.0


def test_crore_formatting():
    assert wr.crore(123456) == "₹1.23 lakh cr" and wr.crore(-7000) == "−₹7,000 cr"
