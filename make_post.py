"""Step 5a: the daily post kit.

Reads gold_daily_metrics from DuckDB and writes, for the latest trading day:
    posts/YYYY-MM-DD/chart.png    -> image to attach on X / LinkedIn
    posts/YYYY-MM-DD/caption.txt  -> ready-to-paste text

Chart design: two stacked panels sharing the date axis (never a dual y-axis):
    top    -> Nifty 50 close (line)
    bottom -> FII vs DII net flows in Rs crore (grouped bars)
"""

from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")  # render to file, no window needed
import matplotlib.pyplot as plt
import matplotlib.ticker
import pandas as pd


BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "warehouse.duckdb"
POSTS_DIR = BASE_DIR / "posts"
LOOKBACK_DAYS = 20          # trading days shown on the chart

# Colors: two categorical slots (blue = FII, orange = DII), text in neutral inks.
FII_COLOR = "#2a78d6"
DII_COLOR = "#eb6834"
NIFTY_COLOR = "#0b0b0b"
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"


def load_metrics() -> pd.DataFrame:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        df = con.execute(
            """
            SELECT trade_date, fii_net, dii_net, nifty_close, nifty_change_pct,
                   fii_side, fii_streak_days, fii_net_5d, dii_absorption_ratio
            FROM gold_daily_metrics
            ORDER BY trade_date
            """
        ).df()
    if df.empty:
        raise RuntimeError("gold_daily_metrics is empty - run load_warehouse.py first")
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    for col in ("fii_net", "dii_net", "nifty_close", "nifty_change_pct",
                "fii_net_5d", "dii_absorption_ratio"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.tail(LOOKBACK_DAYS).reset_index(drop=True)


def crore(value: float) -> str:
    """Format a crore value with sign and Indian-style thousands separator."""
    sign = "+" if value >= 0 else "-"
    return f"{sign}Rs {abs(value):,.0f} Cr"


def build_caption(df: pd.DataFrame) -> str:
    latest = df.iloc[-1]
    day = latest["trade_date"].strftime("%d %b %Y")

    lines = [f"FII/DII flows - {day} (provisional, NSE)", ""]
    lines.append(f"FII: {crore(latest['fii_net'])}")
    lines.append(f"DII: {crore(latest['dii_net'])}")

    if pd.notna(latest["nifty_close"]):
        change = latest["nifty_change_pct"]
        change_txt = f" ({change:+.2f}%)" if pd.notna(change) else ""
        lines.append(f"Nifty 50: {latest['nifty_close']:,.2f}{change_txt}")

    lines.append("")
    side = "selling" if latest["fii_side"] == "sell" else "buying"
    lines.append(f"FIIs have been net {side} for {int(latest['fii_streak_days'])} session(s) in a row.")

    ratio = latest["dii_absorption_ratio"]
    if pd.notna(ratio):
        if ratio >= 1:
            lines.append(f"DIIs absorbed all of it ({ratio:.2f}x the FII outflow).")
        else:
            lines.append(f"DIIs absorbed only {ratio:.0%} of the FII outflow.")

    lines.append("")
    lines.append("Data: NSE provisional figures. Not investment advice.")
    lines.append("#FII #DII #Nifty #StockMarketIndia")
    return "\n".join(lines)


def build_chart(df: pd.DataFrame, path: Path) -> None:
    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "axes.edgecolor": GRID,
        "axes.labelcolor": TEXT_SECONDARY,
        "xtick.color": TEXT_SECONDARY,
        "ytick.color": TEXT_SECONDARY,
    })

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(12, 6.75), dpi=150, sharex=True,
        gridspec_kw={"height_ratios": [1, 1.4], "hspace": 0.12},
    )
    fig.patch.set_facecolor(SURFACE)

    latest = df.iloc[-1]
    day = latest["trade_date"].strftime("%d %b %Y")
    fig.suptitle(f"FII vs DII net flows and Nifty 50 - as of {day}",
                 x=0.06, ha="left", fontsize=16, color=TEXT_PRIMARY, fontweight="bold")
    fig.text(0.06, 0.905, "Provisional cash-market data from NSE, Rs crore",
             ha="left", fontsize=11, color=TEXT_SECONDARY)

    # Trading days are plotted evenly (0, 1, 2...) so weekends and holidays leave no gaps.
    x = list(range(len(df)))
    last = x[-1]

    # Top panel: Nifty close
    has_nifty = df["nifty_close"].notna()
    nifty_x = [i for i, ok in zip(x, has_nifty) if ok]
    nifty_y = df.loc[has_nifty, "nifty_close"].tolist()
    ax_top.plot(nifty_x, nifty_y, color=NIFTY_COLOR, linewidth=2)
    if nifty_x:
        ax_top.scatter(nifty_x[-1:], nifty_y[-1:], color=NIFTY_COLOR, s=40, zorder=3,
                       edgecolor=SURFACE, linewidth=2)
        ax_top.annotate(f"{nifty_y[-1]:,.0f}", (nifty_x[-1], nifty_y[-1]),
                        xytext=(10, 0), textcoords="offset points", va="center",
                        color=TEXT_PRIMARY, fontsize=11, fontweight="bold")
    ax_top.set_ylabel("Nifty 50 close")
    ax_top.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))

    # Bottom panel: grouped bars, FII left of the day, DII right of it, small gap between
    width = 0.38
    fii_x = [i - 0.2 for i in x]
    dii_x = [i + 0.2 for i in x]
    ax_bot.bar(fii_x, df["fii_net"], width=width, color=FII_COLOR, label="FII net")
    ax_bot.bar(dii_x, df["dii_net"], width=width, color=DII_COLOR, label="DII net")
    ax_bot.axhline(0, color=TEXT_SECONDARY, linewidth=0.8)
    ax_bot.set_ylabel("Net flow (Rs crore)")
    ax_bot.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax_bot.legend(loc="lower left", bbox_to_anchor=(0, 1.0), frameon=False, ncol=2,
                  labelcolor=TEXT_PRIMARY, borderaxespad=0.2)

    # Direct labels on the latest day only
    for value, bar_x in ((latest["fii_net"], fii_x[-1]), (latest["dii_net"], dii_x[-1])):
        ax_bot.annotate(f"{value:,.0f}", (bar_x, value),
                        xytext=(0, 6 if value >= 0 else -14), textcoords="offset points",
                        ha="center", fontsize=10, color=TEXT_PRIMARY, fontweight="bold")

    # Pad the y-range so the direct labels never clip
    low, high = ax_bot.get_ylim()
    ax_bot.set_ylim(low * 1.12, high * 1.12)

    for ax in (ax_top, ax_bot):
        ax.set_facecolor(SURFACE)
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.tick_params(length=0)

    step = max(1, len(df) // 8)
    ticks = x[::-1][::step][::-1]  # always include the latest day
    ax_bot.set_xticks(ticks)
    ax_bot.set_xticklabels([df["trade_date"].iloc[i].strftime("%d %b") for i in ticks])
    ax_bot.set_xlim(-0.7, last + 0.9)

    fig.text(0.06, 0.015, "Source: NSE (FII/DII), Yahoo Finance (Nifty). Not investment advice.",
             fontsize=9, color=TEXT_SECONDARY)
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)


def main() -> None:
    df = load_metrics()
    latest_day = df.iloc[-1]["trade_date"].date().isoformat()

    out_dir = POSTS_DIR / latest_day
    out_dir.mkdir(parents=True, exist_ok=True)

    chart_path = out_dir / "chart.png"
    build_chart(df, chart_path)

    caption = build_caption(df)
    (out_dir / "caption.txt").write_text(caption, encoding="utf-8")

    print(f"Chart:   {chart_path}")
    print(f"Caption: {out_dir / 'caption.txt'}\n")
    print(caption)


if __name__ == "__main__":
    main()