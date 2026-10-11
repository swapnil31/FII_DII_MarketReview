"""Is a given date an NSE trading day? Uses NSE's own trading-holiday list."""

from datetime import date, datetime

from curl_cffi import requests


BASE_URL = "https://www.nseindia.com"
HOLIDAY_API = f"{BASE_URL}/api/holiday-master?type=trading"


def fetch_cm_holidays() -> set[date]:
    """Capital-market (CM) trading holidays for the current year, from NSE."""
    session = requests.Session(impersonate="chrome")
    session.get(BASE_URL, timeout=30).raise_for_status()
    response = session.get(
        HOLIDAY_API,
        headers={"Accept": "application/json", "Referer": f"{BASE_URL}/resources/exchange-communication-holidays"},
        timeout=30,
    )
    response.raise_for_status()
    return {
        datetime.strptime(item["tradingDate"], "%d-%b-%Y").date()
        for item in response.json().get("CM", [])
    }


def is_trading_day(day: date) -> bool:
    if day.weekday() >= 5:  # Saturday or Sunday
        return False
    try:
        return day not in fetch_cm_holidays()
    except Exception as exc:  # NSE unreachable: assume open, the freshness check still protects us
        print(f"Could not fetch NSE holidays ({exc}); assuming {day} is a trading day.")
        return True


if __name__ == "__main__":
    today = date.today()
    print(f"{today}: {'trading day' if is_trading_day(today) else 'market closed'}")