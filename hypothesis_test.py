"""Hypothesis test: does weak DII absorption predict a falling Nifty the next day?

H0 (null):        after weak-absorption days, the next day behaves like any other day.
H1 (alternative): after weak-absorption days, the next day is more often red / falls more.

Weak absorption = FIIs net sellers AND DII net buying < 80% of the FII selling.

Tests run:
  1. Fisher's exact test       -> is the share of red next days higher?
  2. Welch's t-test            -> is the average next-day move lower?
  3. Mann-Whitney U test       -> same question, robust to outliers
  4. Permutation test          -> same question, no distribution assumptions
  5. Out-of-sample check       -> does the pattern found in year 1 hold in year 2?
  6. Tradeability check        -> how much of the move happens overnight (gap) vs after the open?

Run after load_warehouse.py:   python hypothesis_test.py
"""

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

DB_PATH = Path(__file__).parent / "warehouse.duckdb"
REPORT_DIR = Path(__file__).parent / "reports" / "hypothesis"

# Chart styling: two categorical colors, neutral text inks.
WEAK_COLOR = "#eb6834"      # weak-absorption days
OTHER_COLOR = "#2a78d6"     # all other days
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"
WEAK_THRESHOLD = 0.8
ALPHA = 0.05
RNG = np.random.default_rng(42)


def load() -> pd.DataFrame:
    import duckdb

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        df = con.execute(
            """
            SELECT trade_date, fii_net, dii_net, nifty_open, nifty_close
            FROM silver_daily
            WHERE nifty_close IS NOT NULL
            ORDER BY trade_date
            """
        ).df()
    for col in ("fii_net", "dii_net", "nifty_open", "nifty_close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    next_open = df["nifty_open"].shift(-1)
    next_close = df["nifty_close"].shift(-1)
    df["next_1d_pct"] = 100 * (next_close / df["nifty_close"] - 1)          # close -> next close
    df["next_gap_pct"] = 100 * (next_open / df["nifty_close"] - 1)          # close -> next open (overnight)
    df["next_open_to_close_pct"] = 100 * (next_close / next_open - 1)       # what a 9:15 AM entry gets
    df["next_red"] = df["next_1d_pct"] < 0

    selling = df["fii_net"] < 0
    df["absorption"] = np.where(selling, df["dii_net"] / df["fii_net"].abs(), np.nan)
    df["weak"] = selling & (df["absorption"] < WEAK_THRESHOLD)
    return df.dropna(subset=["next_1d_pct", "fii_net"])


def permutation_test(a: np.ndarray, b: np.ndarray, n: int = 20000) -> float:
    """One-sided p-value for mean(a) < mean(b) by shuffling group labels."""
    observed = a.mean() - b.mean()
    pooled = np.concatenate([a, b])
    count = 0
    for _ in range(n):
        RNG.shuffle(pooled)
        if pooled[: len(a)].mean() - pooled[len(a):].mean() <= observed:
            count += 1
    return (count + 1) / (n + 1)


def run_tests(df: pd.DataFrame, label: str) -> dict:
    weak, rest = df[df["weak"]], df[~df["weak"]]
    a, b = weak["next_1d_pct"].to_numpy(), rest["next_1d_pct"].to_numpy()

    table = [[int(weak["next_red"].sum()), int((~weak["next_red"]).sum())],
             [int(rest["next_red"].sum()), int((~rest["next_red"]).sum())]]
    _, p_fisher = stats.fisher_exact(table, alternative="greater")
    _, p_welch = stats.ttest_ind(a, b, equal_var=False, alternative="less")
    _, p_mwu = stats.mannwhitneyu(a, b, alternative="less")
    p_perm = permutation_test(a, b)

    return {
        "sample": label,
        "weak_days": len(weak),
        "other_days": len(rest),
        "pct_red_weak": round(100 * weak["next_red"].mean(), 1),
        "pct_red_other": round(100 * rest["next_red"].mean(), 1),
        "avg_next_weak": round(a.mean(), 2),
        "avg_next_other": round(b.mean(), 2),
        "p_fisher": round(p_fisher, 4),
        "p_welch_t": round(p_welch, 4),
        "p_mann_whitney": round(p_mwu, 4),
        "p_permutation": round(p_perm, 4),
    }


def verdict(r: dict) -> str:
    ps = [r["p_fisher"], r["p_welch_t"], r["p_mann_whitney"], r["p_permutation"]]
    if all(p < ALPHA for p in ps):
        return "REJECT H0: weak absorption is followed by worse next days (all tests p < 0.05)"
    if any(p < ALPHA for p in ps):
        return "MIXED: some tests significant, others not; treat as weak evidence"
    return "CANNOT REJECT H0: no reliable difference from normal days"


def mean_ci(values: np.ndarray) -> tuple[float, float]:
    """Mean and the half-width of its 95% confidence interval."""
    values = values[~np.isnan(values)]
    if len(values) < 2:
        return float(np.mean(values)) if len(values) else 0.0, 0.0
    sem = stats.sem(values)
    return float(values.mean()), float(sem * stats.t.ppf(0.975, len(values) - 1))


def style_axes(ax) -> None:
    ax.set_facecolor(SURFACE)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(length=0, colors=TEXT_SECONDARY)


def save_charts(df: pd.DataFrame, results: list[dict]) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11})
    paths = []
    first_day, last_day = df["trade_date"].min(), df["trade_date"].max()
    span = f"{first_day:%b %Y} to {last_day:%b %Y}"

    # Chart 1: short window vs full history, average next-day move with 95% CI.
    recent = df[df["trade_date"] >= last_day - pd.Timedelta(days=92)]
    windows = [("Last 3 months", recent), (f"Full history ({span})", df)]
    fig, ax = plt.subplots(figsize=(11, 6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    width, x = 0.36, np.arange(len(windows))
    for offset, (label, mask_weak, color) in enumerate(
        (("After weak-absorption days", True, WEAK_COLOR), ("After all other days", False, OTHER_COLOR))
    ):
        means, errs, counts = [], [], []
        for _, part in windows:
            vals = part.loc[part["weak"] == mask_weak, "next_1d_pct"].to_numpy()
            m, e = mean_ci(vals)
            means.append(m), errs.append(e), counts.append(len(vals))
        pos = x + (offset - 0.5) * (width + 0.04)
        ax.bar(pos, means, width, yerr=errs, color=color, label=label,
               error_kw={"ecolor": TEXT_SECONDARY, "elinewidth": 1.2, "capsize": 5})
        for p, m, e, n in zip(pos, means, errs, counts):
            y = m - e - 0.06 if m < 0 else m + e + 0.03
            ax.text(p, y, f"{m:+.2f}%\n(n={n})", ha="center", va="top" if m < 0 else "bottom",
                    fontsize=10, color=TEXT_PRIMARY)
    ax.axhline(0, color=TEXT_SECONDARY, linewidth=0.8)
    ax.set_xticks(x, [w[0] for w in windows], color=TEXT_PRIMARY)
    ax.set_ylabel("Average next-day Nifty move (%)", color=TEXT_SECONDARY)
    style_axes(ax)
    low, high = ax.get_ylim()
    ax.set_ylim(low * 1.35, max(high, 0.2) * 1.35)
    ax.legend(loc="upper right", ncol=1, frameon=False, labelcolor=TEXT_PRIMARY)
    fig.suptitle("Does weak DII absorption predict a falling Nifty the next day?",
                 x=0.07, ha="left", fontsize=15, fontweight="bold", color=TEXT_PRIMARY)
    fig.text(0.07, 0.905, "Weak absorption: FIIs sell and DIIs buy back less than 80% of it. "
             "Error bars: 95% confidence interval.", fontsize=10, color=TEXT_SECONDARY)
    fig.text(0.07, 0.01, "Data: NSE provisional FII/DII, Nifty 50 (Yahoo Finance). Not investment advice.",
             fontsize=9, color=TEXT_SECONDARY)
    path = REPORT_DIR / "01_short_vs_long_window.png"
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    paths.append(path)

    # Chart 2: distribution of next-day moves, weak days vs other days.
    fig, ax = plt.subplots(figsize=(11, 6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    bins = np.linspace(-3, 3, 31)
    for mask_weak, color, label in ((False, OTHER_COLOR, "After all other days"),
                                    (True, WEAK_COLOR, "After weak-absorption days")):
        vals = df.loc[df["weak"] == mask_weak, "next_1d_pct"].clip(-3, 3)
        ax.hist(vals, bins=bins, density=True, color=color, alpha=0.55 if not mask_weak else 0.7,
                label=f"{label} (n={len(vals)})", edgecolor=SURFACE, linewidth=0.8)
        ax.axvline(vals.mean(), color=color, linewidth=2, linestyle="--")
    ax.axvline(0, color=TEXT_SECONDARY, linewidth=0.8)
    ax.set_xlabel("Next-day Nifty move (%)", color=TEXT_SECONDARY)
    ax.set_ylabel("Share of days (density)", color=TEXT_SECONDARY)
    style_axes(ax)
    ax.legend(loc="upper left", frameon=False, labelcolor=TEXT_PRIMARY)
    fig.suptitle("Next-day moves look the same after weak and normal days",
                 x=0.07, ha="left", fontsize=15, fontweight="bold", color=TEXT_PRIMARY)
    fig.text(0.07, 0.905, f"Full history, {span}. Dashed lines: averages.", fontsize=10, color=TEXT_SECONDARY)
    path = REPORT_DIR / "02_next_day_distribution.png"
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    paths.append(path)

    # Chart 3: p-values for every test and sample, against the 0.05 line.
    tests = [("p_fisher", "Fisher (share red)"), ("p_welch_t", "Welch t (avg)"),
             ("p_mann_whitney", "Mann-Whitney"), ("p_permutation", "Permutation")]
    fig, ax = plt.subplots(figsize=(11, 6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    sample_colors = ["#0b0b0b", "#6250d6", "#1baf7a"]
    width, x = 0.26, np.arange(len(tests))
    for i, r in enumerate(results):
        vals = [r[key] for key, _ in tests]
        ax.bar(x + (i - 1) * (width + 0.02), vals, width, color=sample_colors[i], label=r["sample"])
    ax.axhline(ALPHA, color=WEAK_COLOR, linewidth=1.5, linestyle="--",
               label="p = 0.05 (below = significant)")
    ax.set_xticks(x, [t[1] for t in tests], color=TEXT_PRIMARY)
    ax.set_ylim(0, 1.35)
    ax.set_yticks([0.05, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_ylabel("p-value", color=TEXT_SECONDARY)
    style_axes(ax)
    ax.legend(loc="upper left", ncol=2, frameon=False, labelcolor=TEXT_PRIMARY)
    fig.suptitle("Hypothesis tests: is the difference real or chance?",
                 x=0.07, ha="left", fontsize=15, fontweight="bold", color=TEXT_PRIMARY)
    path = REPORT_DIR / "03_p_values.png"
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    paths.append(path)

    # Exports for Tableau / Excel.
    cols = ["trade_date", "fii_net", "dii_net", "absorption", "weak", "nifty_close",
            "next_1d_pct", "next_gap_pct", "next_open_to_close_pct", "next_red"]
    df[cols].to_csv(REPORT_DIR / "daily_events.csv", index=False)
    pd.DataFrame(results).to_csv(REPORT_DIR / "test_results.csv", index=False)
    return paths


def analyze(df: pd.DataFrame) -> None:
    df = add_features(df)
    print(f"Data: {len(df)} trading days, {df['trade_date'].min():%d %b %Y} to {df['trade_date'].max():%d %b %Y}")
    print(f"Weak-absorption rule: FIIs sell and DIIs absorb < {WEAK_THRESHOLD:.0%}\n")

    # 1-4. Full sample
    full = run_tests(df, "Full period")

    # 5. Out-of-sample: split in half by date. Year 1 = discovery, Year 2 = validation.
    mid = df["trade_date"].iloc[len(df) // 2]
    first = run_tests(df[df["trade_date"] < mid], f"First half (to {mid:%b %Y})")
    second = run_tests(df[df["trade_date"] >= mid], f"Second half (from {mid:%b %Y})")

    results = pd.DataFrame([full, first, second]).set_index("sample").T
    print(results.to_string())
    print()
    for r in (full, first, second):
        print(f"{r['sample']:<28} {verdict(r)}")

    # 6. Tradeability: overnight gap vs move after the open, on the day after weak absorption.
    weak = df[df["weak"]]
    rest = df[~df["weak"]]
    print("\nWhere does the next-day move happen? (average %)")
    print(f"{'':<22}{'gap at open':>14}{'open to close':>16}{'total':>10}")
    for name, part in (("after weak days", weak), ("after other days", rest)):
        print(f"{name:<22}{part['next_gap_pct'].mean():>14.2f}{part['next_open_to_close_pct'].mean():>16.2f}"
              f"{part['next_1d_pct'].mean():>10.2f}")
    print("If most of the fall is in the gap, it happens before a trader can act at 9:15 AM.")

    paths = save_charts(df, [full, first, second])
    print(f"\nCharts and Tableau exports saved to {REPORT_DIR}:")
    for p in paths:
        print(f"  {p.name}")
    print("  daily_events.csv, test_results.csv")

    print("\nReading the p-values: p < 0.05 means a difference this large would appear by chance")
    print("less than 5% of the time if weak absorption truly made no difference.")
    print("This is analysis, not investment advice.")


if __name__ == "__main__":
    analyze(load())