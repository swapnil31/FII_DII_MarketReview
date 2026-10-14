"""Weekly market report: runs every Friday evening.

For the trading week (Mon-Fri) it reports:
  - Nifty 50 weekly change and NSE cash-market turnover (EQ series)
  - FII and DII: gross bought, gross sold, net, and how many days each was a net seller
  - Top 5 losers and top 5 gainers of the week (Nifty 500 stocks), with trailing and forward P/E

Writes posts/weekly/<week end>/weekly.png and weekly.txt

Usage:  python weekly_report.py               (this week)
        python weekly_report.py 2026-10-09    (the week containing that date)
"""

import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent
STOCK_GLOB = (BASE_DIR / "lake" / "gold" / "stock_daily_returns" / "**" / "*.parquet").as_posix()
WAREHOUSE = BASE_DIR / "warehouse.duckdb"
OUT_DIR = BASE_DIR / "posts" / "weekly"

TOP_N = 5
MIN_AVG_TURNOVER_CR = 5     # ignore stocks trading less than Rs 5 cr a day (illiquid, noisy moves)


def week_bounds(day: date) -> tuple[date, date]:
    monday = day - timedelta(days=day.weekday())
    return monday, monday + timedelta(days=4)


# ---------------------------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------------------------

def load(monday: date, friday: date) -> tuple[pd.DataFrame, pd.DataFrame, float | None]:
    import duckdb

    con = duckdb.connect()
    stocks = con.execute(f"""
        SELECT trade_date::DATE AS trade_date, symbol, industry, close, daily_return,
               return_suspect, turnover
        FROM read_parquet('{STOCK_GLOB}', hive_partitioning = true)
        WHERE trade_date::DATE BETWEEN ? AND ?
    """, [monday, friday]).df()
    con.close()

    with duckdb.connect(str(WAREHOUSE), read_only=True) as wh:
        flows = wh.execute("""
            SELECT trade_date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net, nifty_close
            FROM silver_daily WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date
        """, [monday, friday]).df()
        prev = wh.execute("""
            SELECT nifty_close FROM silver_daily
            WHERE trade_date < ? AND nifty_close IS NOT NULL ORDER BY trade_date DESC LIMIT 1
        """, [monday]).fetchone()
    return stocks, flows, (float(prev[0]) if prev else None)


def fetch_pe(symbols: list[str]) -> dict[str, tuple[float | None, float | None]]:
    """symbol -> (trailing P/E, forward P/E) from Yahoo Finance. Missing values stay None.

    NSE's own quote API blocks scripted requests (HTTP 403), so P/E comes from Yahoo Finance,
    the same library the pipeline already uses for Nifty. Values are as of report time.
    A loss-making company has no meaningful P/E, so it stays None (shown as n/a).
    """
    import yfinance as yf

    def num(value):
        try:
            v = float(value)
            return v if 0 < v < 1000 else None      # ignore negatives and absurd values
        except (TypeError, ValueError):
            return None

    out: dict[str, tuple[float | None, float | None]] = {}
    for symbol in symbols:
        try:
            info = yf.Ticker(f"{symbol}.NS").info
            out[symbol] = (num(info.get("trailingPE")), num(info.get("forwardPE")))
        except Exception as exc:
            print(f"P/E for {symbol} failed: {type(exc).__name__}: {exc}")
            out[symbol] = (None, None)
        time.sleep(0.5)
    found = sum(1 for v in out.values() if v[0] is not None)
    print(f"P/E found for {found} of {len(symbols)} stocks")
    return out


# ---------------------------------------------------------------------------------------------
# Analysis (pure pandas, unit-tested)
# ---------------------------------------------------------------------------------------------

def summarize(stocks: pd.DataFrame, flows: pd.DataFrame, prev_nifty: float | None,
              monday: date, friday: date) -> dict:
    if stocks.empty:
        raise RuntimeError(f"No stock data in the lake for {monday} to {friday}")
    stocks = stocks.assign(trade_date=pd.to_datetime(stocks["trade_date"]).dt.date).sort_values("trade_date")
    days = sorted(stocks["trade_date"].unique())
    n_days = len(days)

    # Market turnover: every stock, every day of the week (rupees -> crore)
    turnover_cr = stocks["turnover"].sum() / 1e7

    # Weekly return per stock: compound the daily returns (they use NSE's adjusted prev_close,
    # so splits and bonuses don't show up as crashes). Keep only clean, liquid Nifty 500 stocks
    # that traded every day of the week.
    s = stocks[stocks["industry"] != "Unclassified"]
    per = (s.groupby(["symbol", "industry"])
             .agg(days=("trade_date", "nunique"),
                  suspect=("return_suspect", "max"),
                  growth=("daily_return", lambda r: (1 + r).prod()),
                  avg_turnover_cr=("turnover", lambda t: t.mean() / 1e7),
                  close=("close", "last"))
             .reset_index())
    per = per[(per["days"] == n_days) & (~per["suspect"].astype(bool))
              & (per["avg_turnover_cr"] >= MIN_AVG_TURNOVER_CR)].copy()
    per["week_pct"] = (per["growth"] - 1) * 100

    losers = per.nsmallest(TOP_N, "week_pct")
    gainers = per.nlargest(TOP_N, "week_pct")

    f = flows.copy()
    for c in ("fii_buy", "fii_sell", "fii_net", "dii_buy", "dii_sell", "dii_net", "nifty_close"):
        f[c] = pd.to_numeric(f[c], errors="coerce")
    fl = f.dropna(subset=["fii_net"])
    flow = {
        "days": len(fl),
        "fii_buy": fl["fii_buy"].sum(), "fii_sell": fl["fii_sell"].sum(), "fii_net": fl["fii_net"].sum(),
        "dii_buy": fl["dii_buy"].sum(), "dii_sell": fl["dii_sell"].sum(), "dii_net": fl["dii_net"].sum(),
        "fii_sell_days": int((fl["fii_net"] < 0).sum()), "dii_sell_days": int((fl["dii_net"] < 0).sum()),
    }

    closes = f["nifty_close"].dropna()
    nifty_close = float(closes.iloc[-1]) if not closes.empty else None
    nifty_pct = (nifty_close / prev_nifty - 1) * 100 if nifty_close and prev_nifty else None

    return {
        "monday": monday, "friday": friday, "first_day": days[0], "last_day": days[-1], "n_days": n_days,
        "turnover_cr": turnover_cr, "flow": flow, "nifty_close": nifty_close, "nifty_pct": nifty_pct,
        "losers": losers, "gainers": gainers,
    }


def crore(value: float) -> str:
    """Rs crore with Indian-style scaling: 1,23,456 cr -> 1.23 lakh cr."""
    sign = "−" if value < 0 else ""
    v = abs(value)
    return f"{sign}₹{v / 1e5:.2f} lakh cr" if v >= 1e5 else f"{sign}₹{v:,.0f} cr"


def signed(value: float) -> str:
    return ("+" if value >= 0 else "") + crore(value)


def pe_text(pe: float | None, forward_pe: float | None) -> str:
    if pe is None:
        return "P/E n/a"
    return f"P/E {pe:.1f}" + (f" (fwd {forward_pe:.1f})" if forward_pe else "")


def build_caption(s: dict, pe: dict) -> str:
    f = s["flow"]
    week = f"{s['first_day']:%d %b}–{s['last_day']:%d %b %Y}"
    lines = [f"📅 Week in markets: {week}", ""]
    if s["nifty_pct"] is not None:
        lines.append(f"Nifty 50: {s['nifty_close']:,.0f} ({s['nifty_pct']:+.2f}% for the week)")
    lines.append(f"NSE cash turnover: {crore(s['turnover_cr'])}")
    lines += ["",
              f"FII: bought {crore(f['fii_buy'])}, sold {crore(f['fii_sell'])} → net {signed(f['fii_net'])}",
              f"DII: bought {crore(f['dii_buy'])}, sold {crore(f['dii_sell'])} → net {signed(f['dii_net'])}",
              "", "Weakest:"]
    for _, r in s["losers"].iterrows():
        lines.append(f"  {r['symbol']} {r['week_pct']:+.1f}% · {pe_text(*pe.get(r['symbol'], (None, None)))}")
    lines.append("Strongest:")
    for _, r in s["gainers"].iterrows():
        lines.append(f"  {r['symbol']} {r['week_pct']:+.1f}% · {pe_text(*pe.get(r['symbol'], (None, None)))}")
    lines += ["", "Auto-generated by my data pipeline. Not investment advice."]
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------
# Image
# ---------------------------------------------------------------------------------------------

INK, MUTED, RED, GREEN, BG, LINE, SOFT = "#14213d", "#5c6577", "#c8102e", "#1a7f4b", "#fbfaf7", "#d9d5cc", "#8a8f99"


def render(s: dict, pe: dict, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig = plt.figure(figsize=(12, 15), dpi=100, facecolor=BG)
    T = fig.text
    week = f"{s['first_day']:%d %b} – {s['last_day']:%d %b %Y}"
    T(0.06, 0.958, "The week in Indian markets", fontsize=30, weight="bold", color=INK, va="top")
    T(0.06, 0.915, f"{week}  ·  {s['n_days']} trading days  ·  NSE data", fontsize=15, color=MUTED, va="top")

    # Headline numbers
    nifty = f"{s['nifty_pct']:+.2f}%" if s["nifty_pct"] is not None else "n/a"
    cells = [("Nifty 50 (week)", nifty, GREEN if (s["nifty_pct"] or 0) >= 0 else RED),
             ("Nifty close", f"{s['nifty_close']:,.0f}" if s["nifty_close"] else "n/a", INK),
             ("NSE cash turnover", crore(s["turnover_cr"]), INK)]
    for i, (label, value, color) in enumerate(cells):
        x = 0.06 + i * 0.3
        T(x, 0.867, label, fontsize=13, color=MUTED, va="center")
        T(x, 0.839, value, fontsize=22, weight="bold", color=color, va="center")
    fig.add_artist(plt.Line2D([0.06, 0.94], [0.812, 0.812], color=LINE, lw=1))

    # Flows table
    f = s["flow"]
    T(0.06, 0.785, "Institutional flows this week", fontsize=18, weight="bold", color=INK, va="center")
    cols = [0.30, 0.48, 0.66, 0.84]
    for x, h in zip(cols, ["Bought", "Sold", "Net", "Net-sell days"]):
        T(x, 0.748, h, fontsize=13, color=MUTED, ha="right", va="center")
    for y, who in ((0.712, "fii"), (0.676, "dii")):
        T(0.06, y, "FII / FPI" if who == "fii" else "DII", fontsize=16, weight="bold", color=INK, va="center")
        net = f[f"{who}_net"]
        vals = [(crore(f[f"{who}_buy"]), INK), (crore(f[f"{who}_sell"]), INK),
                (signed(net), GREEN if net >= 0 else RED),
                (f"{f[f'{who}_sell_days']} of {f['days']}", INK)]
        for x, (v, c) in zip(cols, vals):
            T(x, y, v, fontsize=15, color=c, ha="right", va="center", weight="bold" if c != INK else "normal")
    fig.add_artist(plt.Line2D([0.06, 0.94], [0.645, 0.645], color=LINE, lw=1))

    # Losers and gainers tables
    def table(y0: float, title: str, df: pd.DataFrame, color: str) -> None:
        T(0.06, y0, title, fontsize=18, weight="bold", color=INK, va="center")
        T(0.06, y0 - 0.034, "Stock", fontsize=12, color=MUTED, va="center")
        T(0.30, y0 - 0.034, "Sector", fontsize=12, color=MUTED, va="center")
        T(0.70, y0 - 0.034, "Week", fontsize=12, color=MUTED, ha="right", va="center")
        T(0.80, y0 - 0.034, "P/E", fontsize=12, color=MUTED, ha="right", va="center")
        T(0.94, y0 - 0.034, "Fwd P/E", fontsize=12, color=MUTED, ha="right", va="center")
        for i, (_, r) in enumerate(df.iterrows()):
            y = y0 - 0.07 - i * 0.034
            p, sp = pe.get(r["symbol"], (None, None))
            T(0.06, y, r["symbol"], fontsize=15, weight="bold", color=INK, va="center")
            T(0.30, y, str(r["industry"])[:30], fontsize=13, color=MUTED, va="center")
            T(0.70, y, f"{r['week_pct']:+.1f}%", fontsize=15, weight="bold", color=color, ha="right", va="center")
            T(0.80, y, f"{p:.1f}" if p else "n/a", fontsize=14, color=INK, ha="right", va="center")
            T(0.94, y, f"{sp:.1f}" if sp else "n/a", fontsize=14, color=MUTED, ha="right", va="center")

    table(0.605, "Weakest stocks of the week", s["losers"], RED)
    table(0.355, "Strongest stocks of the week", s["gainers"], GREEN)

    fig.add_artist(plt.Line2D([0.06, 0.94], [0.105, 0.105], color=LINE, lw=1))
    T(0.06, 0.075, "Auto-generated by my data pipeline · Airflow · PySpark · DuckDB", fontsize=12, color=MUTED, va="center")
    T(0.94, 0.075, "Data analysis, not investment advice", fontsize=12, color=MUTED, va="center", ha="right", style="italic")
    T(0.06, 0.045, f"Stocks: Nifty 500 members trading ≥ ₹{MIN_AVG_TURNOVER_CR} cr/day on every day of the week. "
      "P/E from Yahoo Finance at report time; n/a = loss-making or unavailable.", fontsize=10, color=SOFT, va="center")
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=BG)
    plt.close(fig)


def main(day: date | None = None) -> Path:
    monday, friday = week_bounds(day or date.today())
    stocks, flows, prev_nifty = load(monday, friday)
    s = summarize(stocks, flows, prev_nifty, monday, friday)
    symbols = list(s["losers"]["symbol"]) + list(s["gainers"]["symbol"])
    pe = fetch_pe(symbols)

    out = OUT_DIR / s["last_day"].isoformat()
    render(s, pe, out / "weekly.png")
    caption = build_caption(s, pe)
    (out / "weekly.txt").write_text(caption, encoding="utf-8")
    print(caption)
    print(f"\nSaved {out / 'weekly.png'}")
    return out


if __name__ == "__main__":
    main(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None)
