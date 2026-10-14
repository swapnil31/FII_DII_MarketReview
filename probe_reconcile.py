"""Test the attribution idea on a real past day, before building it into the pipeline.

weights = Yahoo float shares x NSE previous close (start-of-day float market cap)
points  = weight x stock return x Nifty previous close
Check:  sum of points  vs  Nifty's actual change that day

Usage:  python probe_reconcile.py 2026-10-13
"""

import sys
import time
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import yfinance as yf

BASE = Path(__file__).parent
day = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2026, 10, 13)
members = pd.read_csv(BASE / "reference" / "nifty50.csv")["symbol"].str.strip().tolist()
print(f"{len(members)} Nifty 50 members")

con = duckdb.connect()
glob = (BASE / "lake" / "gold" / "stock_daily_returns" / "**" / "*.parquet").as_posix()
stocks = con.execute(f"""
    SELECT symbol, prev_close, close, daily_return
    FROM read_parquet('{glob}', hive_partitioning = true)
    WHERE trade_date::DATE = ? AND symbol IN (SELECT UNNEST(?::VARCHAR[]))
""", [day, members]).df()
print(f"Lake rows for {day}: {len(stocks)}")

with duckdb.connect(str(BASE / "warehouse.duckdb"), read_only=True) as wh:
    n = wh.execute("""
        SELECT nifty_close, LAG(nifty_close) OVER (ORDER BY trade_date) AS prev
        FROM silver_daily WHERE nifty_close IS NOT NULL QUALIFY trade_date = ?
    """, [day]).fetchone()
nifty_close, nifty_prev = float(n[0]), float(n[1])
actual_pts = nifty_close - nifty_prev
print(f"Nifty {day}: {nifty_prev:,.2f} -> {nifty_close:,.2f} ({actual_pts:+.1f} pts, {actual_pts / nifty_prev:+.2%})")

floats = {}
for s in stocks["symbol"]:
    try:
        floats[s] = yf.Ticker(f"{s}.NS").info.get("floatShares")
    except Exception as exc:
        print(f"  {s}: {exc}")
    time.sleep(0.3)
stocks["float_shares"] = stocks["symbol"].map(floats)
missing = stocks[stocks["float_shares"].isna()]["symbol"].tolist()
print(f"Float shares found for {stocks['float_shares'].notna().sum()} of {len(stocks)}; missing: {missing or 'none'}")

s = stocks.dropna(subset=["float_shares"]).copy()
s["cap"] = s["float_shares"] * s["prev_close"]
s["weight"] = s["cap"] / s["cap"].sum()
s["points"] = s["weight"] * s["daily_return"] * nifty_prev
total = s["points"].sum()

print("\nTop weights:", ", ".join(f"{r.symbol} {r.weight:.1%}" for r in s.nlargest(8, "weight").itertuples()))
print("Biggest contributors:")
for r in s.reindex(s["points"].abs().sort_values(ascending=False).index).head(6).itertuples():
    print(f"  {r.symbol:<12} {r.daily_return:+.2%}  weight {r.weight:.1%}  ->  {r.points:+.1f} pts")
print(f"\nRECONCILIATION: sum of stock points {total:+.1f}  vs  actual {actual_pts:+.1f}  "
      f"(gap {total - actual_pts:+.1f} pts, {abs(total - actual_pts) / abs(actual_pts):.1%} of the move)")
