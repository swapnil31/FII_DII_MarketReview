"""Download NSE equity bhavcopy files (daily prices for every listed stock).

NSE changed the file format in July 2024:
  - classic (up to Jul 2024):  .../content/historical/EQUITIES/2024/JUN/cm28JUN2024bhav.csv.zip
  - UDiFF   (from Jul 2024):   .../content/cm/BhavCopy_NSE_CM_0_0_0_20261009_F_0000.csv.zip

Files are saved untouched to bhavcopy/dt=YYYY-MM-DD/<original name>.zip so
parsing can be re-run later without downloading again. Re-running skips days
already downloaded, so an interrupted backfill simply resumes.

Usage:
    python extract_bhavcopy.py                                   # latest trading day (last 7 days, missing only)
    python extract_bhavcopy.py --start 2024-09-01                # backfill up to today
    python extract_bhavcopy.py --start 2016-01-01 --end 2024-08-31
"""

import argparse
import csv
import io
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

from curl_cffi import requests


BASE_URL = "https://www.nseindia.com"
ARCHIVE_URL = "https://nsearchives.nseindia.com"
RAW_DIR = Path(__file__).parent / "bhavcopy"

UDIFF_START = date(2024, 7, 1)   # around here NSE switched; we try both formats near the boundary
MIN_ROWS = 500                   # a real bhavcopy has thousands of rows; fewer means a bad file
PAUSE_SECONDS = 0.7              # be polite to NSE's servers

CLASSIC_COLUMNS = {"SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "TOTTRDQTY", "TIMESTAMP"}
UDIFF_COLUMNS = {"TradDt", "TckrSymb", "SctySrs", "OpnPric", "HghPric", "LwPric", "ClsPric", "TtlTradgVol"}


# ---------- URLs ---------------------------------------------------------------------------

def classic_url(day: date) -> str:
    mon = day.strftime("%b").upper()
    return f"{ARCHIVE_URL}/content/historical/EQUITIES/{day.year}/{mon}/cm{day:%d}{mon}{day.year}bhav.csv.zip"


def udiff_url(day: date) -> str:
    return f"{ARCHIVE_URL}/content/cm/BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv.zip"


def candidate_urls(day: date) -> list[str]:
    """Most likely format first; near the switch-over both are tried."""
    if day >= UDIFF_START:
        return [udiff_url(day), classic_url(day)]
    return [classic_url(day), udiff_url(day)]


# ---------- Validation ---------------------------------------------------------------------

def detect_format(content: bytes) -> tuple[str, int]:
    """Open the zip, check the header, count rows. Returns (format, row_count) or raises."""
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise ValueError("zip contains no CSV")
        text = zf.read(names[0]).decode("utf-8", errors="replace")

    reader = csv.reader(io.StringIO(text))
    header = {h.strip() for h in next(reader)}
    rows = sum(1 for row in reader if row)

    if CLASSIC_COLUMNS <= header:
        fmt = "classic"
    elif UDIFF_COLUMNS <= header:
        fmt = "udiff"
    else:
        raise ValueError(f"unknown header: {sorted(header)[:10]}")
    if rows < MIN_ROWS:
        raise ValueError(f"only {rows} rows")
    return fmt, rows


# ---------- Download -----------------------------------------------------------------------

def day_folder(day: date) -> Path:
    return RAW_DIR / f"dt={day.isoformat()}"


def already_downloaded(day: date) -> bool:
    folder = day_folder(day)
    return folder.exists() and any(folder.glob("*.zip"))


def new_session() -> requests.Session:
    session = requests.Session(impersonate="chrome")
    session.get(BASE_URL, timeout=30)  # pick up cookies; archives sometimes want them
    return session


def fetch_day(session: requests.Session, day: date) -> tuple[str, str, int] | None:
    """Returns (url, format, rows) when saved, None when no file exists (holiday)."""
    for url in candidate_urls(day):
        for attempt in range(3):
            try:
                response = session.get(url, timeout=60)
            except Exception:
                time.sleep(2 * (attempt + 1))
                continue
            if response.status_code == 404:
                break                      # this format doesn't exist for this day; try the next
            if response.status_code in (401, 403):
                session = new_session()    # cookies expired or blocked; refresh and retry
                time.sleep(3)
                continue
            if response.status_code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            response.raise_for_status()

            fmt, rows = detect_format(response.content)
            folder = day_folder(day)
            folder.mkdir(parents=True, exist_ok=True)
            (folder / url.rsplit("/", 1)[-1]).write_bytes(response.content)
            return url, fmt, rows
    return None


def trading_weekdays(start: date, end: date) -> list[date]:
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", help="first date, YYYY-MM-DD (default: 7 days ago)")
    parser.add_argument("--end", help="last date, YYYY-MM-DD (default: today)")
    parser.add_argument("--force", action="store_true", help="re-download days that already exist")
    args = parser.parse_args()

    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else date.today()
    start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else end - timedelta(days=7)

    days = trading_weekdays(start, end)
    print(f"Checking {len(days)} weekdays from {start} to {end}\n")

    session = new_session()
    saved, skipped, missing, failed = 0, 0, [], []
    for i, day in enumerate(days, 1):
        if not args.force and already_downloaded(day):
            skipped += 1
            continue
        try:
            result = fetch_day(session, day)
        except Exception as exc:
            failed.append(f"{day} ({exc})")
            print(f"[{i}/{len(days)}] {day}  FAILED: {exc}")
            continue
        if result is None:
            missing.append(str(day))
        else:
            _, fmt, rows = result
            saved += 1
            if saved % 20 == 1 or i == len(days):
                print(f"[{i}/{len(days)}] {day}  {fmt:<7} {rows:>5} rows")
        time.sleep(PAUSE_SECONDS)

    print(f"\nSaved {saved}, already had {skipped}, no file (holiday) {len(missing)}, failed {len(failed)}")
    if missing:
        print(f"No file for: {', '.join(missing[:15])}{' ...' if len(missing) > 15 else ''}")
    if failed:
        print("Failed (re-run the same command to retry):")
        for line in failed:
            print(f"  {line}")


if __name__ == "__main__":
    main()