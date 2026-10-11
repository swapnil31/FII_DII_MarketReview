"""One-time history load: daily FII/DII cash-market flows from Trendlyne.

NSE only publishes the latest day, so history comes from Trendlyne's monthly
pages, which embed the same provisional NSE figures as JSON. Writes
backfill/fii_dii_backfill.csv, which load_warehouse.py already reads.

Usage:
    python backfill_fii_dii.py              # last 25 months (about 2 years)
    python backfill_fii_dii.py 12           # last 12 months
"""

import csv
import json
import sys
import time
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path

from curl_cffi import requests


PAGE_URL = "https://trendlyne.com/macro-data/fii-dii/latest/cash-pastmonth/{day}/"
TABLE_ID = "cash-table-main-pastmonth"
OUT_PATH = Path(__file__).parent / "backfill" / "fii_dii_backfill.csv"
MIN_GROSS_CR = 3000   # skip tiny special sessions (Muhurat trading, weekend budget sessions)


class TableJsonFinder(HTMLParser):
    """Finds the data-jsondata attribute of the cash FII/DII table."""

    def __init__(self) -> None:
        super().__init__()
        self.payload = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if attrs.get("id") == TABLE_ID and "data-jsondata" in attrs:
            self.payload = json.loads(attrs["data-jsondata"])


def month_ends(months: int) -> list[str]:
    """The 9th of each month going back, e.g. 09-Oct-2026, 09-Sep-2026, ..."""
    today = date.today()
    y, m = today.year, today.month
    out = []
    for _ in range(months):
        out.append(date(y, m, 9).strftime("%d-%b-%Y"))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


def fetch_month(session, day: str) -> list[list]:
    response = session.get(PAGE_URL.format(day=day), timeout=30)
    response.raise_for_status()
    finder = TableJsonFinder()
    finder.feed(response.text)
    if finder.payload is None:
        raise RuntimeError(f"Cash table not found on page for {day}")
    # Rows look like: ["2026-10-09", fii_buy, fii_sell, fii_net, dii_net, dii_sell, dii_buy, "caret"]
    return [row for row in finder.payload["data"] if len(str(row[0])) == 10 and str(row[0])[4] == "-"]


def main() -> None:
    months = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    session = requests.Session(impersonate="chrome")

    by_date: dict[str, list] = {}
    for day in month_ends(months):
        rows = fetch_month(session, day)
        for row in rows:
            by_date[row[0]] = row
        print(f"{day}: {len(rows)} days")
        time.sleep(1.5)  # be polite to the site

    kept, skipped = [], []
    for d in sorted(by_date):
        _, fii_buy, fii_sell, fii_net, dii_net, dii_sell, dii_buy = by_date[d][:7]
        weekday = datetime.strptime(d, "%Y-%m-%d").weekday()
        if weekday >= 5 or (fii_buy + fii_sell) < MIN_GROSS_CR:
            skipped.append(d)
            continue
        kept.append([d, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net, "trendlyne"])

    OUT_PATH.parent.mkdir(exist_ok=True)
    with OUT_PATH.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["trade_date", "fii_buy", "fii_sell", "fii_net",
                         "dii_buy", "dii_sell", "dii_net", "source"])
        writer.writerows(kept)

    print(f"\nWrote {len(kept)} trading days ({kept[0][0]} to {kept[-1][0]}) to {OUT_PATH}")
    print(f"Skipped {len(skipped)} special sessions: {', '.join(skipped)}")
    print("\nNext: python backfill_nifty.py 2024-09-01   then   python load_warehouse.py")


if __name__ == "__main__":
    main()