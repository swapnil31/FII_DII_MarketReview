"""Pattern check: does weak DII absorption predict a red Nifty the next day?

Groups every FII/DII day by absorption bucket and shows what the Nifty did
on the FOLLOWING trading day.

Run after load_warehouse.py:   python analyze_patterns.py
"""

from pathlib import Path

import duckdb


DB_PATH = Path(__file__).parent / "warehouse.duckdb"

SUMMARY_SQL = """
SELECT
    absorption_bucket,
    COUNT(*)                                                    AS days,
    SUM(CASE WHEN nifty_next_day_change_pct < 0 THEN 1 ELSE 0 END)  AS next_day_red,
    ROUND(100.0 * AVG(CASE WHEN nifty_next_day_change_pct < 0 THEN 1 ELSE 0 END), 0)
                                                                AS pct_next_day_red,
    ROUND(AVG(nifty_next_day_change_pct), 2)                    AS avg_next_day_pct,
    ROUND(MIN(nifty_next_day_change_pct), 2)                    AS worst_next_day_pct
FROM gold_daily_metrics
WHERE nifty_next_day_change_pct IS NOT NULL
GROUP BY absorption_bucket
ORDER BY CASE absorption_bucket
    WHEN 'weak_absorption' THEN 1 WHEN 'mostly_absorbed' THEN 2
    WHEN 'fully_absorbed' THEN 3 ELSE 4 END
"""

BASELINE_SQL = """
SELECT
    COUNT(*) AS days,
    ROUND(100.0 * AVG(CASE WHEN nifty_next_day_change_pct < 0 THEN 1 ELSE 0 END), 0) AS pct_red
FROM gold_daily_metrics
WHERE nifty_next_day_change_pct IS NOT NULL
"""

WEAK_DAYS_SQL = """
SELECT trade_date, fii_net, dii_net, dii_absorption_ratio,
       nifty_change_pct AS same_day_pct, nifty_next_day_change_pct AS next_day_pct
FROM gold_daily_metrics
WHERE absorption_bucket = 'weak_absorption'
ORDER BY trade_date
"""


def main() -> None:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        days, pct_red = con.execute(BASELINE_SQL).fetchone()
        print(f"Baseline: across all {days} days, the next day was red {pct_red:.0f}% of the time.\n")

        print("Next-day Nifty move by DII absorption bucket:")
        con.sql(SUMMARY_SQL).show()

        print("Every weak-absorption day (DII bought < 80% of FII selling):")
        con.sql(WEAK_DAYS_SQL).show()

    print("Read with care: with only a few months of data, each bucket holds a handful of days.")
    print("A pattern here is a hypothesis to keep tracking, not a trading signal.")


if __name__ == "__main__":
    main()