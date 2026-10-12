"""Download NSE index constituent lists.

  reference/nifty500.csv   industry/sector for ~500 stocks (used by the Spark sector tables)
  reference/nifty50.csv    the 50 index members (used by the daily explainer)

Both have: symbol, company, industry, isin, as_of.
Run occasionally (index membership changes a few times a year).

Usage:  python extract_sectors.py
"""

import csv
import io
from datetime import date
from pathlib import Path

from curl_cffi import requests


BASE = "https://nsearchives.nseindia.com/content/indices"
REF_DIR = Path(__file__).parent / "reference"
EXPECTED = {"Company Name", "Industry", "Symbol", "ISIN Code"}

# file on NSE -> (local file, allowed row count)
LISTS = {
    "ind_nifty500list.csv": ("nifty500.csv", range(450, 521)),
    "ind_nifty50list.csv": ("nifty50.csv", range(50, 52)),
}


def download(session, remote: str, local: str, allowed: range) -> list[dict]:
    response = session.get(f"{BASE}/{remote}", timeout=60)
    response.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(response.text)))
    if not rows or not EXPECTED <= set(rows[0]):
        raise RuntimeError(f"{remote}: unexpected layout {list(rows[0]) if rows else 'empty'}")
    if len(rows) not in allowed:
        raise RuntimeError(f"{remote}: expected {allowed.start}-{allowed.stop - 1} rows, got {len(rows)}")

    REF_DIR.mkdir(exist_ok=True)
    with (REF_DIR / local).open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["symbol", "company", "industry", "isin", "as_of"])
        for r in rows:
            writer.writerow([r["Symbol"].strip(), r["Company Name"].strip(),
                             r["Industry"].strip(), r["ISIN Code"].strip(), date.today().isoformat()])
    return rows


def main() -> None:
    session = requests.Session(impersonate="chrome")
    session.get("https://www.nseindia.com", timeout=30)
    for remote, (local, allowed) in LISTS.items():
        rows = download(session, remote, local, allowed)
        industries = sorted({r["Industry"].strip() for r in rows})
        print(f"Saved {len(rows)} stocks across {len(industries)} industries to {REF_DIR / local}")


if __name__ == "__main__":
    main()