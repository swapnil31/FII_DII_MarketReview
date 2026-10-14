"""Diagnostic: can we get Nifty 50 weights (free-float market cap) from NSE? If not, from Yahoo?

Also runs the reconciliation test on today's live data:
    sum(weight x stock % change)  vs  Nifty's own % change

Usage:  python probe_weights.py
"""

import json

from curl_cffi import requests

URL = "https://www.nseindia.com/api/equity-stockIndices"
session = requests.Session(impersonate="chrome")
print(f"Homepage: HTTP {session.get('https://www.nseindia.com', timeout=30).status_code}")
page = "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%2050"
print(f"Market page: HTTP {session.get(page, timeout=30).status_code}")

r = session.get(URL, params={"index": "NIFTY 50"},
                headers={"Accept": "application/json", "Referer": page}, timeout=30)
print(f"\n[equity-stockIndices] HTTP {r.status_code}, {r.headers.get('content-type')}, {len(r.content)} bytes")

rows = []
try:
    data = r.json()
    rows = data.get("data", [])
    print("Top-level keys:", list(data)[:12])
    print(f"Rows: {len(rows)}")
    if rows:
        print("Fields in a row:", sorted(rows[-1].keys()))
except Exception:
    print("Not JSON. First 200 chars:", r.text[:200].replace("\n", " "))

if rows:
    index_row = next((x for x in rows if x.get("symbol") == "NIFTY 50"), None)
    stocks = [x for x in rows if x.get("symbol") != "NIFTY 50"]
    have_ffmc = [x for x in stocks if x.get("ffmc")]
    print(f"\nStocks: {len(stocks)}, with ffmc: {len(have_ffmc)}")
    if have_ffmc:
        # Start-of-day cap = today's cap / (1 + today's return)
        start_caps = {x["symbol"]: float(x["ffmc"]) / (1 + float(x["pChange"]) / 100) for x in have_ffmc}
        total = sum(start_caps.values())
        weights = {s: c / total for s, c in start_caps.items()}
        top = sorted(weights.items(), key=lambda kv: -kv[1])[:8]
        print("Top weights:", ", ".join(f"{s} {w:.1%}" for s, w in top))

        implied = sum(weights[x["symbol"]] * float(x["pChange"]) for x in have_ffmc)
        actual = float(index_row["pChange"]) if index_row else None
        print(f"\nReconciliation: weighted sum of stock moves = {implied:+.3f}%  |  "
              f"Nifty's own move = {actual:+.3f}%" if actual is not None else f"implied {implied:+.3f}% (no index row)")
    else:
        print("No ffmc field. Sample row:", json.dumps(stocks[0], default=str)[:600] if stocks else "none")

# Fallback source: Yahoo's float shares x price for one stock
try:
    import yfinance as yf
    info = yf.Ticker("ITC.NS").info
    print(f"\n[yfinance ITC] marketCap={info.get('marketCap')} floatShares={info.get('floatShares')} "
          f"price={info.get('currentPrice')}")
except Exception as exc:
    print(f"\n[yfinance] failed: {type(exc).__name__}: {exc}")
