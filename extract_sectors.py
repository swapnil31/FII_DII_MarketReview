"""Download the Nifty 500 constituent list, which gives an industry/sector for each stock.

Saves reference/nifty500.csv with: symbol, company, industry, isin.
Run occasionally (index membership changes a few times a year).

Usage:  python extract_sectors.py
"""

import csv
import io
from datetime import date
from pathlib import Path

from curl_cffi import requests


URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
OUT_PATH = Path(__file__).parent / "reference" / "nifty500.csv"
EXPECTED = {"Company Name", "Industry", "Symbol", "ISIN Code"}


def main() -> None:
    session = requests.Session(impersonate="chrome")
    session.get("https://www.nseindia.com", timeout=30)
    response = session.get(URL, timeout=60)
    response.raise_for_status()

    rows = list(csv.DictReader(io.StringIO(response.text)))
    if not rows or not EXPECTED <= set(rows[0]):
        raise RuntimeError(f"Unexpected file layout: {list(rows[0]) if rows else 'empty'}")
    if not 450 <= len(rows) <= 520:
        raise RuntimeError(f"Expected about 500 stocks, got {len(rows)}")

    OUT_PATH.parent.mkdir(exist_ok=True)
    with OUT_PATH.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["symbol", "company", "industry", "isin", "as_of"])
        for r in rows:
            writer.writerow([r["Symbol"].strip(), r["Company Name"].strip(),
                             r["Industry"].strip(), r["ISIN Code"].strip(), date.today().isoformat()])

    industries = sorted({r["Industry"].strip() for r in rows})
    print(f"Saved {len(rows)} stocks across {len(industries)} industries to {OUT_PATH}")
    for name in industries:
        count = sum(1 for r in rows if r["Industry"].strip() == name)
        print(f"  {count:>3}  {name}")


if __name__ == "__main__":
    main()