"""Tests for analyze_sectors.py using synthetic data (no Parquet or DuckDB needed).

Run:  pytest -q
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import analyze_sectors as an  # noqa: E402


def fake_data(days=300, seed=0) -> pd.DataFrame:
    """'Banks' fall hard when FIIs sell; 'FMCG' doesn't care; plus an Unclassified bucket."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2025-01-01", periods=days)
    fii = rng.normal(-1000, 3000, days)
    rows = []
    for d, f in zip(dates, fii):
        rows.append((d, "Banks", 20, 0.001 * f / 1000 + rng.normal(0, 0.003), f))
        rows.append((d, "FMCG", 15, rng.normal(0, 0.008), f))
        rows.append((d, "Unclassified", 900, rng.normal(0, 0.008), f))
    df = pd.DataFrame(rows, columns=["trade_date", "industry", "stocks", "tw_return", "fii_net"])
    df["ew_return"] = df["tw_return"]
    df["pct_advancing"] = (df["tw_return"] > 0).astype(float)
    df["dii_net"] = -df["fii_net"]
    return df


def test_heavy_sell_is_bottom_quintile_of_days():
    df = an.add_features(fake_data())
    share = df.drop_duplicates("trade_date")["heavy_sell"].mean()
    assert 0.18 <= share <= 0.22


def test_next_day_return_stays_within_sector():
    df = an.add_features(fake_data(days=5))
    banks = df[df["industry"] == "Banks"].reset_index(drop=True)
    assert banks.loc[0, "next_tw_return"] == banks.loc[1, "tw_return"]
    assert pd.isna(banks["next_tw_return"].iloc[-1])


def test_sensitive_sector_detected_and_insensitive_one_not():
    t = an.sector_table(an.add_features(fake_data())).set_index("industry")
    assert t.loc["Banks", "significant_same_day"]
    assert t.loc["Banks", "corr_with_fii_net"] > 0.5
    assert not t.loc["FMCG", "significant_same_day"]
    assert not t.loc["Banks", "significant_next_day"]          # no look-ahead leak


def test_unclassified_excluded_from_correction():
    t = an.sector_table(an.add_features(fake_data())).set_index("industry")
    assert pd.isna(t.loc["Unclassified", "q_same_day"])


def test_thin_sector_days_dropped():
    df = fake_data(days=10)
    df.loc[df["industry"] == "FMCG", "stocks"] = 1
    assert "FMCG" not in set(an.add_features(df)["industry"])
