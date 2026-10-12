"""Daily "why did the market move?" explainer: an image plus a short caption, built every evening.

Answers three questions for the latest trading day, using only data the pipeline already has:
  1. Which index stocks did the most damage (or lifting)?   gold/stock_daily_returns
  2. Which sectors moved most, in each direction?            gold/sector_daily_returns
  3. How broad was the move, and what did FIIs/DIIs do?      breadth + warehouse gold_daily_metrics

Writes posts/<date>/explainer.png and posts/<date>/explainer.txt

Usage:  python daily_explainer.py              (latest day in the lake)
        python daily_explainer.py 2026-10-12   (a specific day)
"""

import sys
from datetime import date
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent
STOCK_GLOB = (BASE_DIR / "lake" / "gold" / "stock_daily_returns" / "**" / "*.parquet").as_posix()
SECTOR_GLOB = (BASE_DIR / "lake" / "gold" / "sector_daily_returns" / "**" / "*.parquet").as_posix()
WAREHOUSE = BASE_DIR / "warehouse.duckdb"
NIFTY50_CSV = BASE_DIR / "reference" / "nifty50.csv"
POSTS_DIR = BASE_DIR / "posts"

TOP_STOCKS = 6        # stocks shown in panel 1
TOP_SECTORS = 5       # sectors shown per direction in panel 2
MIN_SECTOR_STOCKS = 8 # smaller sectors are too noisy to show

SHORT_NAMES = {        # NSE industry names are long; shorten for the image and caption
    "Automobile and Auto Components": "Auto",
    "Fast Moving Consumer Goods": "FMCG",
    "Oil Gas & Consumable Fuels": "Oil & Gas",
    "Construction Materials": "Cement & Materials",
    "Telecommunication": "Telecom",
    "Information Technology": "IT",
    "Media Entertainment & Publication": "Media",
    "Metals & Mining": "Metals",
}


# ---------------------------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------------------------

def load(day: date | None = None) -> tuple[pd.DataFrame, pd.DataFrame, dict | None, set[str] | None]:
    import duckdb

    con = duckdb.connect()
    if day is None:
        day = con.execute(f"SELECT MAX(trade_date)::DATE FROM read_parquet('{SECTOR_GLOB}', hive_partitioning = true)").fetchone()[0]

    stocks = con.execute(f"""
        SELECT symbol, industry, daily_return, weight_turnover_20d
        FROM read_parquet('{STOCK_GLOB}', hive_partitioning = true)
        WHERE trade_date::DATE = ? AND NOT return_suspect AND daily_return IS NOT NULL
    """, [day]).df()
    sectors = con.execute(f"""
        SELECT industry, stocks, tw_return, pct_advancing
        FROM read_parquet('{SECTOR_GLOB}', hive_partitioning = true)
        WHERE trade_date::DATE = ?
    """, [day]).df()
    con.close()

    flows = None
    if WAREHOUSE.exists():
        # Read-only: never blocks other readers. Runs after load_warehouse, so no writer is active.
        with duckdb.connect(str(WAREHOUSE), read_only=True) as wh:
            row = wh.execute("""
                SELECT nifty_close, nifty_change_pct, fii_net, dii_net, fii_side, fii_streak_days
                FROM gold_daily_metrics WHERE trade_date = ?
            """, [day]).fetchone()
        if row:
            flows = dict(zip(["nifty_close", "nifty_pct", "fii", "dii", "fii_side", "streak"],
                             [float(row[0]) if row[0] is not None else None,
                              float(row[1]) if row[1] is not None else None,
                              float(row[2]), float(row[3]), row[4], int(row[5])]))

    nifty50 = None
    if NIFTY50_CSV.exists():
        nifty50 = set(pd.read_csv(NIFTY50_CSV)["symbol"].str.strip())

    stocks.attrs["day"] = day
    return stocks, sectors, flows, nifty50


# ---------------------------------------------------------------------------------------------
# Analysis (pure pandas, unit-tested)
# ---------------------------------------------------------------------------------------------

def build_summary(stocks: pd.DataFrame, sectors: pd.DataFrame, flows: dict | None,
                  nifty50: set[str] | None, day: date) -> dict:
    if stocks.empty or sectors.empty:
        raise RuntimeError(f"No lake data for {day}; did spark_bhavcopy run?")

    nifty_pct = flows["nifty_pct"] if flows and flows.get("nifty_pct") is not None else None
    # Direction of the day: Nifty if we have it, otherwise the broad market.
    down = (nifty_pct < 0) if nifty_pct is not None else (stocks["daily_return"].mean() < 0)

    # 1) Breadth across all NSE main-board stocks
    advances = int((stocks["daily_return"] > 0).sum())
    declines = int((stocks["daily_return"] < 0).sum())
    pct_up = 100 * advances / max(advances + declines, 1)

    # 2) Stocks that pushed the index the same way it moved. Size proxy = 20-day avg traded value.
    pool = stocks[stocks["symbol"].isin(nifty50)] if nifty50 else stocks[stocks["industry"] != "Unclassified"]
    pool = pool.dropna(subset=["weight_turnover_20d"]).copy()
    pool["impact"] = pool["daily_return"] * pool["weight_turnover_20d"]
    pool = pool[pool["daily_return"] < 0] if down else pool[pool["daily_return"] > 0]
    movers = pool.sort_values("impact", ascending=down).head(TOP_STOCKS)

    # 3) Sectors, both directions, ignoring tiny ones
    sec = sectors[(sectors["industry"] != "Unclassified") & (sectors["stocks"] >= MIN_SECTOR_STOCKS)]
    sec_down = sec[sec["tw_return"] < 0].nsmallest(TOP_SECTORS, "tw_return")
    sec_up = sec[sec["tw_return"] > 0].nlargest(TOP_SECTORS, "tw_return")

    # Headline: what happened, then the most informative contrast
    verb = "fell" if down else "rose"
    first = f"Nifty {verb} {abs(nifty_pct):.1f}%." if nifty_pct is not None else f"Market {verb} today."
    against = sec_up if down else sec_down
    if (down and pct_up >= 50) or (not down and pct_up < 50):
        second = f"Most stocks {'rose' if down else 'fell'}."
    elif not against.empty:
        second = "Not every sector did."
    else:
        share = 100 - pct_up if down else pct_up
        second = f"A broad {'sell-off' if down else 'rally'}: {share:.0f}% of stocks {verb}."

    return {
        "day": day, "down": down, "headline": f"{first} {second}", "nifty_pct": nifty_pct,
        "flows": flows, "advances": advances, "declines": declines, "pct_up": pct_up,
        "movers": movers[["symbol", "daily_return"]].assign(pct=lambda d: d["daily_return"] * 100),
        "index_label": "Nifty 50" if nifty50 else "large",
        "sectors": pd.concat([sec_up, sec_down]).sort_values("tw_return", ascending=False)
                     .assign(pct=lambda d: d["tw_return"] * 100,
                             industry=lambda d: d["industry"].replace(SHORT_NAMES)),
    }


def build_caption(s: dict) -> str:
    lines = [s["headline"], ""]
    if not s["movers"].empty:
        names = ", ".join(s["movers"]["symbol"].head(4))
        lines.append(f"{'Dragged down' if s['down'] else 'Lifted'} by: {names}")
    against = s["sectors"][s["sectors"]["pct"] > 0] if s["down"] else s["sectors"][s["sectors"]["pct"] < 0]
    if not against.empty:
        top = against.iloc[0] if s["down"] else against.iloc[-1]
        lines.append(f"{'But' if s['down'] else 'Lagging'}: {top['industry']} {top['pct']:+.1f}%")
    lines.append(f"Breadth: {s['advances']:,} up / {s['declines']:,} down")
    f = s["flows"]
    if f:
        lines.append(f"FII {f['fii']:+,.0f} cr | DII {f['dii']:+,.0f} cr")
    lines += ["", "Auto-generated by my data pipeline. Not investment advice."]
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------
# Image
# ---------------------------------------------------------------------------------------------

INK, MUTED, RED, GREEN, BG, LINE = "#14213d", "#5c6577", "#c8102e", "#1a7f4b", "#fbfaf7", "#d9d5cc"


def _style(ax, plt):
    import matplotlib.ticker as mt
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color("#cfcfcf")
    ax.grid(axis="x", color="#e6e3dc", lw=0.8)
    ax.set_axisbelow(True)
    ax.set_facecolor(BG)
    ax.tick_params(axis="y", labelsize=14, colors=INK, length=0)
    ax.tick_params(axis="x", labelsize=11, colors=MUTED)
    ax.xaxis.set_major_locator(mt.MaxNLocator(nbins=5, steps=[1, 2, 5, 10]))
    ax.xaxis.set_major_formatter(mt.FuncFormatter(lambda x, _: f"{x:+.0f}%" if x else "0"))


def render(s: dict, path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig = plt.figure(figsize=(12, 15), dpi=100, facecolor=BG)
    f = s["flows"]
    sub = s["day"].strftime("%d %b %Y")
    if f and f.get("nifty_close"):
        sub += f"  ·  Nifty 50 closed at {f['nifty_close']:,.0f}"
    sub += f"  ·  Breadth: {s['advances']:,} up / {s['declines']:,} down"
    fig.text(0.06, 0.958, s["headline"], fontsize=30, weight="bold", color=INK, va="top")
    fig.text(0.06, 0.915, sub, fontsize=14.5, color=MUTED, va="top")

    if f:
        streak = f"{f['streak']} {'selling' if f['fii_side'] == 'sell' else 'buying'}"
        cells = [("FII net", f"{'−' if f['fii'] < 0 else '+'}₹{abs(f['fii']):,.0f} cr", RED if f["fii"] < 0 else GREEN),
                 ("DII net", f"{'−' if f['dii'] < 0 else '+'}₹{abs(f['dii']):,.0f} cr", RED if f["dii"] < 0 else GREEN),
                 ("FII streak", f"{streak} sessions", INK)]
        for i, (label, value, color) in enumerate(cells):
            x = 0.06 + i * 0.3
            fig.text(x, 0.867, label, fontsize=13, color=MUTED, va="center")
            fig.text(x, 0.839, value, fontsize=22, weight="bold", color=color, va="center")
    fig.add_artist(plt.Line2D([0.06, 0.94], [0.815, 0.815], color=LINE, lw=1))

    # Panel 1: index movers
    movers = s["movers"].iloc[::-1]
    word = "pulled Nifty down" if s["down"] else "lifted Nifty"
    fig.text(0.06, 0.785, f"1  Biggest {s['index_label']} stocks that {word}", fontsize=18, weight="bold", color=INK, va="center")
    ax = fig.add_axes([0.25, 0.565, 0.66, 0.195])
    _style(ax, plt)
    color = RED if s["down"] else GREEN
    if movers.empty:
        ax.text(0.5, 0.5, "No index stocks moved with the market", ha="center", transform=ax.transAxes, color=MUTED)
    else:
        ax.barh(movers["symbol"], movers["pct"], color=color, height=0.62)
        lim = max(abs(movers["pct"]).max() * 1.25, 1)
        ax.set_xlim(-lim, 0) if s["down"] else ax.set_xlim(0, lim)
        for i, v in enumerate(movers["pct"]):
            ax.text(v + (-0.04 if v < 0 else 0.04) * lim, i, f"{v:+.2f}%", ha="right" if v < 0 else "left",
                    va="center", fontsize=12.5, weight="bold", color=INK)

    # Panel 2: sectors
    sec = s["sectors"].iloc[::-1]
    fig.text(0.06, 0.51, "2  Sectors that moved most today", fontsize=18, weight="bold", color=INK, va="center")
    ax2 = fig.add_axes([0.25, 0.165, 0.66, 0.32])
    _style(ax2, plt)
    ax2.barh(sec["industry"], sec["pct"], color=[RED if v < 0 else GREEN for v in sec["pct"]], height=0.62)
    span = max(abs(sec["pct"]).max() * 1.3, 1) if not sec.empty else 1
    ax2.set_xlim(-span if (sec["pct"] < 0).any() else -0.2 * span, span if (sec["pct"] > 0).any() else 0.2 * span)
    ax2.axvline(0, color=INK, lw=0.8)
    for i, v in enumerate(sec["pct"]):
        ax2.text(v + (0.03 if v > 0 else -0.03) * span, i, f"{v:+.2f}%", ha="left" if v > 0 else "right",
                 va="center", fontsize=12.5, weight="bold", color=INK)
    ax2.set_xlabel("Sector return today (Nifty 500 stocks, liquidity-weighted)", fontsize=12, color=MUTED)

    fig.add_artist(plt.Line2D([0.06, 0.94], [0.105, 0.105], color=LINE, lw=1))
    fig.text(0.06, 0.075, "Auto-generated by my data pipeline · Airflow · PySpark · DuckDB", fontsize=12, color=MUTED, va="center")
    fig.text(0.94, 0.075, "Data analysis, not investment advice", fontsize=12, color=MUTED, va="center", ha="right", style="italic")
    fig.text(0.06, 0.045, f"Source: NSE bhavcopy and provisional FII/DII data. Stocks ranked by return × 20-day traded value. "
             f"Sectors with < {MIN_SECTOR_STOCKS} stocks excluded.", fontsize=10, color="#8a8f99", va="center")
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=BG)
    plt.close(fig)


def main(day: date | None = None) -> Path:
    stocks, sectors, flows, nifty50 = load(day)
    day = stocks.attrs["day"]
    summary = build_summary(stocks, sectors, flows, nifty50, day)
    out = POSTS_DIR / day.isoformat()
    render(summary, out / "explainer.png")
    caption = build_caption(summary)
    (out / "explainer.txt").write_text(caption, encoding="utf-8")
    print(caption)
    print(f"\nSaved {out / 'explainer.png'}")
    return out


if __name__ == "__main__":
    main(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None)