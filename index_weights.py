"""Float shares for the Nifty 50 members, used to weight each stock's contribution to the index.

    weight = float shares x previous close / sum over all 50 members

NSE's index-constituent API is not available to scripts, so float shares come from Yahoo Finance.
They change rarely (buybacks, stake sales), so the file is refreshed at most once every 7 days.
Tested on 13 Oct 2026: the 50 stocks' points summed to +364.9 vs Nifty's actual +358.6 (1.7% gap).

Writes reference/nifty50_float.csv  (symbol, float_shares, as_of)

Usage:  python index_weights.py          (refresh if older than 7 days)
        python index_weights.py --force  (refresh now)
"""

import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent
MEMBERS_CSV = BASE_DIR / "reference" / "nifty50.csv"
FLOAT_CSV = BASE_DIR / "reference" / "nifty50_float.csv"
MAX_AGE_DAYS = 7
MIN_FOUND = 45          # below this, keep the old file rather than publish shaky weights


def load_float() -> dict[str, float] | None:
    if not FLOAT_CSV.exists():
        return None
    df = pd.read_csv(FLOAT_CSV).dropna(subset=["float_shares"])
    return dict(zip(df["symbol"].str.strip(), df["float_shares"].astype(float))) or None


def needs_refresh(members: set[str], today: date) -> str | None:
    """Reason to refresh, or None if the file is fresh and matches the current members."""
    if not FLOAT_CSV.exists():
        return "no file yet"
    df = pd.read_csv(FLOAT_CSV)
    if set(df["symbol"].str.strip()) != members:
        return "index members changed"
    age = (today - date.fromisoformat(str(df["as_of"].max()))).days
    return f"file is {age} days old" if age >= MAX_AGE_DAYS else None


def refresh(force: bool = False, today: date | None = None) -> str:
    """Never raises: a failed refresh just keeps the previous file (the explainer still works)."""
    today = today or date.today()
    try:
        members = set(pd.read_csv(MEMBERS_CSV)["symbol"].str.strip())
        reason = "forced" if force else needs_refresh(members, today)
        if reason is None:
            return "float shares are fresh; nothing to do"

        import yfinance as yf

        old = load_float() or {}
        rows, missing = [], []
        for symbol in sorted(members):
            shares = None
            try:
                shares = yf.Ticker(f"{symbol}.NS").info.get("floatShares")
            except Exception as exc:
                print(f"{symbol}: {type(exc).__name__}: {exc}")
            if not shares and symbol in old:
                shares = old[symbol]                    # keep last known value
            if not shares:
                missing.append(symbol)
            rows.append({"symbol": symbol, "float_shares": shares, "as_of": today.isoformat()})
            time.sleep(0.3)

        found = len(members) - len(missing)
        if found < MIN_FOUND:
            return f"only {found} of {len(members)} found; kept the previous file"
        FLOAT_CSV.parent.mkdir(exist_ok=True)
        pd.DataFrame(rows).to_csv(FLOAT_CSV, index=False)
        return f"refreshed ({reason}): {found} of {len(members)} found" + (f"; missing {missing}" if missing else "")
    except Exception as exc:
        return f"refresh failed ({type(exc).__name__}: {exc}); kept the previous file"


if __name__ == "__main__":
    print(refresh(force="--force" in sys.argv))
