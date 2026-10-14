"""Tests for daily_explainer.py on small synthetic days (no lake or database needed)."""

import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import daily_explainer as de  # noqa: E402

DAY = date(2026, 10, 12)


def stocks(rows):
    return pd.DataFrame(rows, columns=["symbol", "industry", "daily_return", "weight_turnover_20d"])


def sectors(rows):
    return pd.DataFrame(rows, columns=["industry", "stocks", "tw_return", "pct_advancing"])


FLOWS_DOWN = {"nifty_close": 22363, "nifty_pct": -0.7, "fii": -1708, "dii": 3045, "fii_side": "sell", "streak": 12}
SECTORS = sectors([("Auto", 40, 0.015, 0.8), ("FMCG", 28, -0.011, 0.39), ("Telecom", 9, -0.015, 0.44),
                   ("Media", 4, 0.037, 0.75), ("Unclassified", 900, -0.002, 0.4)])


def test_drags_are_index_stocks_ranked_by_size_times_move():
    s = stocks([("BIG", "Fin", -0.01, 1000), ("SMALL", "Fin", -0.05, 100), ("UP", "Auto", 0.02, 800),
                ("NOTINDEX", "Unclassified", -0.05, 5000)])
    out = de.build_summary(s, SECTORS, FLOWS_DOWN, {"BIG", "SMALL", "UP"}, DAY)
    assert list(out["movers"]["symbol"]) == ["BIG", "SMALL"]      # -10 impact beats -5; risers and non-index excluded


def test_without_index_list_unclassified_is_excluded():
    s = stocks([("A", "Fin", -0.02, 100), ("JUNK", "Unclassified", -0.09, 9000)])
    out = de.build_summary(s, SECTORS, FLOWS_DOWN, None, DAY)
    assert list(out["movers"]["symbol"]) == ["A"]


def test_tiny_and_unclassified_sectors_hidden():
    s = stocks([("A", "Fin", -0.02, 100)])
    shown = set(de.build_summary(s, SECTORS, FLOWS_DOWN, None, DAY)["sectors"]["industry"])
    assert shown == {"Auto", "FMCG", "Telecom"}


def test_headline_contrast_when_some_sectors_rose():
    s = stocks([("A", "x", -0.01, 1), ("B", "x", -0.01, 1), ("C", "x", 0.01, 1)])     # 33% up
    assert de.build_summary(s, SECTORS, FLOWS_DOWN, None, DAY)["headline"] == "Nifty fell 0.7%. Not every sector did."


def test_headline_most_stocks_rose_on_down_day():
    s = stocks([("A", "x", 0.01, 1), ("B", "x", 0.01, 1), ("C", "x", -0.01, 1)])
    assert de.build_summary(s, SECTORS, FLOWS_DOWN, None, DAY)["headline"] == "Nifty fell 0.7%. Most stocks rose."


def test_headline_broad_selloff_when_nothing_rose():
    secs = sectors([("FMCG", 28, -0.011, 0.3), ("Telecom", 9, -0.015, 0.2)])
    s = stocks([("A", "x", -0.01, 1)] * 9 + [("B", "x", 0.01, 1)])
    assert de.build_summary(s, secs, FLOWS_DOWN, None, DAY)["headline"] == "Nifty fell 0.7%. A broad sell-off: 90% of stocks fell."


def test_caption_mentions_drags_and_flows():
    s = stocks([("ITC", "FMCG", -0.03, 500), ("A", "x", 0.01, 1)])
    cap = de.build_caption(de.build_summary(s, SECTORS, FLOWS_DOWN, {"ITC"}, DAY))
    assert "Dragged down by: ITC" in cap and "But: Auto +1.5%" in cap and "FII -1,708 cr" in cap


# ---- Points attribution ------------------------------------------------------------------------

def index_day():
    # 4 index stocks + 2 non-index peers. Equal float shares, prices make weights 40/30/20/10.
    rows = [("BIGBANK", "Fin", 0.02, 1, 400.0, 1.0), ("ITCO", "FMCG", -0.03, 1, 300.0, 2.8),
            ("OILCO", "Oil", 0.01, 1, 200.0, 1.0), ("ITSVC", "IT", 0.05, 1, 100.0, 1.2),
            ("SMALLBANK", "Fin", 0.018, 1, 50.0, 1.0), ("SMALLFMCG", "FMCG", -0.002, 1, 50.0, 1.0)]
    return pd.DataFrame(rows, columns=["symbol", "industry", "daily_return", "weight_turnover_20d",
                                       "prev_close", "volume_vs_20d"])


FLOATS = {"BIGBANK": 1e6, "ITCO": 1e6, "OILCO": 1e6, "ITSVC": 1e6}
# Index move = 0.4*2% + 0.3*-3% + 0.2*1% + 0.1*5% = +0.6%
NIFTY_PREV = 20000.0


def test_points_reconcile_to_index_move():
    idx, rc = de.attribute(index_day(), FLOATS, NIFTY_PREV * 1.006, 0.6)
    assert abs(rc["sum"] - rc["actual"]) < 0.01 and rc["ok"]
    assert round(idx.set_index("symbol").loc["BIGBANK", "points"], 1) == 160.0     # 0.4 * 2% * 20000


def test_leave_one_out_labels():
    idx, _ = de.attribute(index_day(), FLOATS, NIFTY_PREV * 1.006, 0.6)
    lab = idx.set_index("symbol")["label"]
    assert lab["ITCO"] == "own move, 2.8x volume"        # -3% vs peers -0.2%
    assert lab["BIGBANK"] == "moved with sector"         # +2% vs other banks +1.8%


def test_summary_uses_points_when_float_data_present():
    flows = {"nifty_close": NIFTY_PREV * 1.006, "nifty_pct": 0.6, "fii": 1, "dii": 1, "fii_side": "buy", "streak": 1}
    s = de.build_summary(index_day(), SECTORS, flows, set(FLOATS), DAY, FLOATS)
    assert list(s["movers"]["symbol"]) == ["BIGBANK", "ITSVC", "OILCO"]       # biggest point lifters first
    assert "Nifty 50 stocks explain +120 of +120 pts" in de.build_caption(s)
