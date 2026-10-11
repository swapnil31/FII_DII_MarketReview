"""Fetch recent Nifty 50 daily prices from Yahoo Finance and save them raw.

Saves one file per trading day: everyday/dt=YYYY-MM-DD/nifty.json
Fetches the last few days on every run, so a missed evening is filled in
automatically. Reruns overwrite the same files, so nothing is duplicated.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf


TICKER = "^NSEI"          # Nifty 50 on Yahoo Finance
LOOKBACK = "3mo"           # last 3 months

# Same folder as the FII/DII files, next to this script.
RAW_DIR = Path(__file__).parent / "everyday"


def fetch_nifty() -> list[dict]:
    history = yf.Ticker(TICKER).history(period=LOOKBACK, interval="1d", auto_adjust=False)

    # Data quality check: Yahoo occasionally returns nothing on a bad day.
    if history.empty:
        raise RuntimeError(f"No data returned for {TICKER}")

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


def save_raw(rows: list[dict]) -> list[Path]:
    """Save each trading day into its own dt= folder."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    saved = []

    for row in rows:
        folder = RAW_DIR / f"dt={row['date']}"
        folder.mkdir(parents=True, exist_ok=True)

        payload = {
            "fetched_at": fetched_at,
            "source": f"yfinance {TICKER}",
            "trade_date": row["date"],
            "data": row,
        }

        path = folder / "nifty.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        saved.append(path)

    return saved


def main() -> None:
    rows = fetch_nifty()
    paths = save_raw(rows)
    for row, path in zip(rows, paths):
        print(f"{row['date']}  close {row['close']:>10,.2f}  -> {path}")


if __name__ == "__main__":
    main()