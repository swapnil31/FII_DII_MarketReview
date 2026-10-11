"""Fetch the latest provisional FII/DII cash data from NSE and save it raw.

Step 1: extract from NSE's hidden API (curl_cffi impersonates Chrome's TLS
        fingerprint, which NSE's bot protection checks).
Step 2: land the untouched response in everyday/dt=YYYY-MM-DD/fii_dii.json.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from curl_cffi import requests


BASE_URL = "https://www.nseindia.com"
REPORT_PAGE = f"{BASE_URL}/reports/fii-dii"
API_URL = f"{BASE_URL}/api/fiidiiTradeReact"  # combined NSE + BSE + MSEI

# Raw files live next to this script, regardless of where it is run from.
RAW_DIR = Path(__file__).parent / "everyday"

EXPECTED_CATEGORIES = {"FII/FPI", "DII"}

# Headers only for the API call. Page visits use Chrome's own default headers.
API_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": REPORT_PAGE,
    "X-Requested-With": "XMLHttpRequest",
}


def fetch_fii_dii() -> list[dict]:
    # impersonate="chrome" makes the TLS handshake and default headers match real Chrome.
    session = requests.Session(impersonate="chrome")

    # 1. Visit pages like a browser so NSE sets its bot-protection cookies.
    session.get(BASE_URL, timeout=30).raise_for_status()
    session.get(REPORT_PAGE, timeout=30).raise_for_status()

    # 2. Call the API with the same session (cookies are sent automatically).
    response = session.get(API_URL, headers=API_HEADERS, timeout=30)
    response.raise_for_status()

    # 3. If blocked, NSE can return an HTML page with status 200 instead of JSON.
    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type:
        raise RuntimeError(
            f"NSE returned non-JSON ({content_type}): {response.text[:200]}"
        )

    rows = response.json()

    # 4. Data quality checks: exactly one FII/FPI row and one DII row, same date.
    if not isinstance(rows, list):
        raise ValueError(f"Expected a JSON array, got {type(rows).__name__}")

    categories = {row.get("category") for row in rows}
    if categories != EXPECTED_CATEGORIES:
        raise ValueError(f"Unexpected categories: {categories}")

    dates = {row.get("date") for row in rows}
    if len(dates) != 1:
        raise ValueError(f"Rows have different dates: {dates}")

    return rows


def save_raw(rows: list[dict]) -> Path:
    """Save the untouched API response, partitioned by the data's trading date."""
    # 1. Use the date from the data ("09-Oct-2026"), not today's date.
    trade_date = datetime.strptime(rows[0]["date"], "%d-%b-%Y").date()

    # 2. One folder per trading day: everyday/dt=2026-10-09
    folder = RAW_DIR / f"dt={trade_date.isoformat()}"
    folder.mkdir(parents=True, exist_ok=True)

    # 3. Wrap the data with metadata so every number is traceable to its source.
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source_url": API_URL,
        "trade_date": trade_date.isoformat(),
        "data": rows,
    }

    # 4. Overwrite on rerun, so the same day never gets duplicated.
    path = folder / "fii_dii.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return path


def main() -> None:
    rows = fetch_fii_dii()
    path = save_raw(rows)
    print(f"Saved {len(rows)} rows for {rows[0]['date']} to {path}")


if __name__ == "__main__":
    main()