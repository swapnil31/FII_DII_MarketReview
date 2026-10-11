"""Step 4: Which sectors take the hit when FIIs sell?

Joins the Spark gold table (sector_daily_returns, Parquet) with FII flows from
warehouse.duckdb and asks, for each sector:

  1. How does its return move with FII net flow on the same day? (correlation)
  2. Average return on HEAVY FII-selling days (bottom 20% of fii_net) vs all other days
     -> Welch t-test, with Benjamini-Hochberg correction because we test ~20 sectors at once
  3. Same comparison for the NEXT day (honesty check: FII data is published after the
     close, so only the next day is something you could act on)

Outputs (reports/sectors/):
  sector_vs_fii.csv          one row per sector, all numbers
  01_sell_day_returns.png    avg return on heavy-sell days vs other days, per sector
  02_correlation.png         same-day correlation of sector return with FII net flow

Usage:  python analyze_sectors.py
"""

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

BASE_DIR = Path(__file__).parent
SECTOR_GLOB = str(BASE_DIR / "lake" / "gold" / "sector_daily_returns" / "**" / "*.parquet")
WAREHOUSE = BASE_DIR / "warehouse.duckdb"
OUT_DIR = BASE_DIR / "reports" / "sectors"

HEAVY_QUANTILE = 0.20      # bottom 20% of fii_net days = "heavy selling"
MIN_STOCKS = 3             # ignore sector-days built from too few stocks
ALPHA = 0.05


# ---------------------------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------------------------

def load() -> pd.DataFrame:
    """One row per (trade_date, industry) with the sector return and that day's FII flow."""
    import duckdb

    con = duckdb.connect()
    con.execute(f"ATTACH '{WAREHOUSE.as_posix()}' AS wh (READ_ONLY)")
    df = con.execute(f"""
        SELECT s.trade_date::DATE AS trade_date, s.industry, s.stocks,
               s.tw_return, s.ew_return, s.pct_advancing,
               f.fii_net, f.dii_net
        FROM read_parquet('{Path(SECTOR_GLOB).as_posix()}', hive_partitioning = true) s
        JOIN wh.silver_daily f ON f.trade_date = s.trade_date::DATE
        WHERE f.fii_net IS NOT NULL
        ORDER BY s.trade_date, s.industry
    """).df()
    con.close()
    return df


# ---------------------------------------------------------------------------------------------
# Analysis (pure pandas, so it can be unit-tested with fake data)
# ---------------------------------------------------------------------------------------------

def add_features(df: pd.DataFrame, heavy_quantile: float = HEAVY_QUANTILE) -> pd.DataFrame:
    df = df[df["stocks"] >= MIN_STOCKS].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values(["industry", "trade_date"])

    # Threshold computed over DAYS, not sector-rows, so every day counts once.
    daily_fii = df.drop_duplicates("trade_date").set_index("trade_date")["fii_net"]
    threshold = daily_fii.quantile(heavy_quantile)
    df["heavy_sell"] = df["fii_net"] <= threshold
    df.attrs["heavy_sell_threshold"] = float(threshold)

    # Next trading day's return for the same sector.
    df["next_tw_return"] = df.groupby("industry")["tw_return"].shift(-1)
    return df


def _welch_p(a: pd.Series, b: pd.Series) -> float:
    a, b = a.dropna(), b.dropna()
    if len(a) < 3 or len(b) < 3:
        return np.nan
    return float(stats.ttest_ind(a, b, equal_var=False).pvalue)


def _bh(pvalues: pd.Series) -> pd.Series:
    """Benjamini-Hochberg adjusted p-values (q-values); NaNs stay NaN."""
    out = pd.Series(np.nan, index=pvalues.index)
    ok = pvalues.notna()
    if ok.any():
        out[ok] = stats.false_discovery_control(pvalues[ok].to_numpy(), method="bh")
    return out


def sector_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for industry, g in df.groupby("industry"):
        sell, other = g[g["heavy_sell"]], g[~g["heavy_sell"]]
        rows.append({
            "industry": industry,
            "days": len(g),
            "heavy_sell_days": len(sell),
            "avg_stocks": g["stocks"].mean(),
            "corr_with_fii_net": g["tw_return"].corr(g["fii_net"]),
            "sell_day_return_pct": sell["tw_return"].mean() * 100,
            "other_day_return_pct": other["tw_return"].mean() * 100,
            "sell_day_pct_advancing": sell["pct_advancing"].mean() * 100,
            "p_same_day": _welch_p(sell["tw_return"], other["tw_return"]),
            "next_day_after_sell_pct": sell["next_tw_return"].mean() * 100,
            "next_day_after_other_pct": other["next_tw_return"].mean() * 100,
            "p_next_day": _welch_p(sell["next_tw_return"], other["next_tw_return"]),
        })
    t = pd.DataFrame(rows)
    t["gap_same_day_pct"] = t["sell_day_return_pct"] - t["other_day_return_pct"]
    t["gap_next_day_pct"] = t["next_day_after_sell_pct"] - t["next_day_after_other_pct"]

    # Correct for testing many sectors at once; 'Unclassified' is not a real sector, keep it out.
    real = t["industry"] != "Unclassified"
    for col in ("p_same_day", "p_next_day"):
        q = col.replace("p_", "q_")
        t[q] = np.nan
        t.loc[real, q] = _bh(t.loc[real, col])
    t["significant_same_day"] = t["q_same_day"] < ALPHA
    t["significant_next_day"] = t["q_next_day"] < ALPHA
    return t.sort_values("sell_day_return_pct").reset_index(drop=True)


# ---------------------------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------------------------

def save_charts(t: pd.DataFrame, threshold: float, period: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    t = t[t["industry"] != "Unclassified"]

    # 1) Sell-day vs other-day returns
    t1 = t.sort_values("sell_day_return_pct", ascending=False)
    y = np.arange(len(t1))
    fig, ax = plt.subplots(figsize=(10, 0.42 * len(t1) + 2))
    ax.barh(y + 0.2, t1["sell_day_return_pct"], height=0.4, color="#d62728",
            label=f"Heavy FII-selling days (FIIs net sold ₹{-threshold:,.0f} cr or more)")
    ax.barh(y - 0.2, t1["other_day_return_pct"], height=0.4, color="#9e9e9e", label="All other days")
    for yi, (_, r) in zip(y, t1.iterrows()):
        if r["significant_same_day"]:
            ax.text(min(r["sell_day_return_pct"], 0) - 0.02, yi + 0.2, "*", ha="right", va="center",
                    fontsize=13, color="#d62728")
    ax.set_yticks(y, t1["industry"])
    ax.axvline(0, color="black", lw=0.8)
    ax.set_xlabel("Average sector return that day (%)")
    ax.set_title(f"Which sectors fall most when FIIs sell hard?  ({period})\n"
                 "* = difference significant after correcting for 20+ tests (BH, q < 0.05)", fontsize=11)
    fig.legend(*ax.get_legend_handles_labels(), loc="lower center", ncol=2, fontsize=9, frameon=False)
    fig.tight_layout(rect=(0, 0.6 / fig.get_figheight(), 1, 1))
    fig.savefig(OUT_DIR / "01_sell_day_returns.png", dpi=150)
    plt.close(fig)

    # 2) Correlation with FII net flow
    t2 = t.sort_values("corr_with_fii_net")
    fig, ax = plt.subplots(figsize=(9, 0.38 * len(t2) + 2))
    colors = ["#1f77b4" if c >= 0 else "#d62728" for c in t2["corr_with_fii_net"]]
    ax.barh(t2["industry"], t2["corr_with_fii_net"], color=colors)
    ax.axvline(0, color="black", lw=0.8)
    ax.set_xlabel("Correlation of daily sector return with FII net flow (same day)")
    ax.set_title(f"How closely each sector moves with FII flows  ({period})", fontsize=11)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "02_correlation.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------------

def main() -> None:
    raw = load()
    df = add_features(raw)
    threshold = df.attrs["heavy_sell_threshold"]
    first, last = df["trade_date"].min().date(), df["trade_date"].max().date()
    n_days, n_sell = df["trade_date"].nunique(), df.loc[df["heavy_sell"], "trade_date"].nunique()
    period = f"{first} to {last}, {n_days} days"

    t = sector_table(df)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    t.round(4).to_csv(OUT_DIR / "sector_vs_fii.csv", index=False)
    save_charts(t, threshold, period)

    pd.set_option("display.width", 160)
    print(f"Period: {period}")
    print(f"Heavy FII-selling days: {n_sell} (FIIs net sold ₹{-threshold:,.0f} cr or more, "
          f"bottom {HEAVY_QUANTILE:.0%} of days)\n")
    show = t[t["industry"] != "Unclassified"][[
        "industry", "avg_stocks", "corr_with_fii_net", "sell_day_return_pct", "other_day_return_pct",
        "q_same_day", "next_day_after_sell_pct", "q_next_day"]]
    print(show.to_string(index=False, float_format=lambda v: f"{v:.3f}"))

    hit = t[(t["industry"] != "Unclassified") & t["significant_same_day"]]
    nxt = t[(t["industry"] != "Unclassified") & t["significant_next_day"]]
    print(f"\nSame day: {len(hit)} sector(s) fall significantly more on heavy-sell days"
          + (f" -> {', '.join(hit['industry'])}" if len(hit) else ""))
    print(f"Next day: {len(nxt)} sector(s) show a significant follow-through"
          + (f" -> {', '.join(nxt['industry'])}" if len(nxt) else " (nothing tradeable)"))
    print(f"\nSaved CSV and 2 charts to {OUT_DIR}")


if __name__ == "__main__":
    main()