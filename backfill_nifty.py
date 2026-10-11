"""One-time history load: daily Nifty 50 open/high/low/close for a date range.

Writes the same files as the daily pipeline (everyday/dt=YYYY-MM-DD/nifty.json),
so load_warehouse.py picks them up with no changes. Safe to rerun: each day's
file is simply overwritten.

Usage:
    python backfill_nifty.py                      # from 2026-07-01 to today
    python backfill_nifty.py 2026-04-01           # from a custom start date
"""

import sys
from datetime import date, timedelta

import yfinance as yf

from extract_nifty import TICKER, save_raw


DEFAULT_START = "2024-09-01"


def fetch_range(start: str) -> list[dict]:
    end = (date.today() + timedelta(days=1)).isoformat()  # yfinance's end date is exclusive
    history = yf.Ticker(TICKER).history(start=start, end=end, interval="1d", auto_adjust=False)
    if history.empty:
        raise RuntimeError(f"No Nifty data returned for {start} to {end}")

    rows = []
    for timestamp, bar in history.iterrows():
        rows.append(
            {
                "date": timestamp.date().isoformat(),
                "open": round(float(bar["Open"]), 2),
                "high": round(float(bar["High"]), 2),
                "low": round(float(bar["Low"]), 2),
                "close": round(float(bar["Close"]), 2),
            }
        )
    return rows


def main() -> None:
    start = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_START
    rows = fetch_range(start)
    save_raw(rows)

    print(f"Saved {len(rows)} trading days of Nifty prices: {rows[0]['date']} to {rows[-1]['date']}")
    print(f"First: open {rows[0]['open']:,.2f}  close {rows[0]['close']:,.2f}")
    print(f"Last:  open {rows[-1]['open']:,.2f}  close {rows[-1]['close']:,.2f}")
    print("\nNext: python load_warehouse.py")


if __name__ == "__main__":
    main()