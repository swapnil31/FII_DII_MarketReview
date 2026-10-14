"""Diagnostic: what does NSE's quote API return for P/E? (and does yfinance have it?)

Usage:  python probe_pe.py ITC
"""

import json
import sys

from curl_cffi import requests

symbol = sys.argv[1] if len(sys.argv) > 1 else "ITC"


def find_keys(obj, path=""):
    """Every key anywhere in the JSON whose name mentions 'pe' (case-insensitive), with its value."""
    hits = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else k
            if "pe" in k.lower() and not isinstance(v, (dict, list)):
                hits.append((p, v))
            hits += find_keys(v, p)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:3]):
            hits += find_keys(v, f"{path}[{i}]")
    return hits


session = requests.Session(impersonate="chrome")
home = session.get("https://www.nseindia.com", timeout=30)
print(f"Homepage: HTTP {home.status_code}")
page = session.get(f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}", timeout=30)
print(f"Quote page: HTTP {page.status_code}")

for name, params in [("quote-equity", {"symbol": symbol}),
                     ("quote-equity trade_info", {"symbol": symbol, "section": "trade_info"})]:
    r = session.get("https://www.nseindia.com/api/quote-equity", params=params,
                    headers={"Accept": "application/json",
                             "Referer": f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"},
                    timeout=30)
    print(f"\n[{name}] HTTP {r.status_code}, content-type {r.headers.get('content-type')}, {len(r.content)} bytes")
    try:
        data = r.json()
    except Exception:
        print("  Not JSON. First 200 chars:", r.text[:200].replace("\n", " "))
        continue
    print("  Top-level keys:", list(data)[:15] if isinstance(data, dict) else type(data).__name__)
    if isinstance(data, dict) and isinstance(data.get("metadata"), dict):
        print("  metadata keys:", list(data["metadata"])[:25])
    hits = find_keys(data)
    print("  Keys mentioning 'pe':", json.dumps(hits[:15]) if hits else "none")

try:
    import yfinance as yf
    info = yf.Ticker(f"{symbol}.NS").info
    print(f"\n[yfinance] trailingPE={info.get('trailingPE')} forwardPE={info.get('forwardPE')}")
except Exception as exc:
    print(f"\n[yfinance] failed: {type(exc).__name__}: {exc}")
