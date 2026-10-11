"""Step 3: load the raw JSON files into DuckDB and build bronze, silver and gold tables.

Bronze  -> raw rows, exactly as stored, one row per source record
Silver  -> one clean, typed row per trading day (FII, DII and Nifty side by side)
Gold    -> metrics for analysis and posts: streaks, rolling flows, DII absorption

The raw files are the source of truth, so every run rebuilds all tables from
scratch. That keeps the warehouse idempotent: run it once or ten times, the
result is the same.
"""

from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "everyday"
DB_PATH = BASE_DIR / "warehouse.duckdb"

BACKFILL_CSV = BASE_DIR / "backfill" / "fii_dii_backfill.csv"

# DuckDB wants forward slashes in glob patterns, even on Windows.
FII_GLOB = (RAW_DIR / "*" / "fii_dii.json").as_posix()
NIFTY_GLOB = (RAW_DIR / "*" / "nifty.json").as_posix()


BACKFILL_SQL = f"""
-- Manually entered history, one row per day. Blank buy/sell values are allowed.
CREATE OR REPLACE TABLE bronze_fii_dii_backfill AS
SELECT
    CAST(trade_date AS DATE)            AS trade_date,
    CAST(fii_buy  AS DECIMAL(14, 2))    AS fii_buy,
    CAST(fii_sell AS DECIMAL(14, 2))    AS fii_sell,
    CAST(fii_net  AS DECIMAL(14, 2))    AS fii_net,
    CAST(dii_buy  AS DECIMAL(14, 2))    AS dii_buy,
    CAST(dii_sell AS DECIMAL(14, 2))    AS dii_sell,
    CAST(dii_net  AS DECIMAL(14, 2))    AS dii_net,
    source
FROM read_csv('{BACKFILL_CSV.as_posix()}', header = true, all_varchar = true);
"""

EMPTY_BACKFILL_SQL = """
CREATE OR REPLACE TABLE bronze_fii_dii_backfill (
    trade_date DATE,
    fii_buy DECIMAL(14, 2), fii_sell DECIMAL(14, 2), fii_net DECIMAL(14, 2),
    dii_buy DECIMAL(14, 2), dii_sell DECIMAL(14, 2), dii_net DECIMAL(14, 2),
    source VARCHAR
);
"""


BRONZE_SQL = f"""
-- One row per FII/DII category per day, values still as NSE sent them (strings).
CREATE OR REPLACE TABLE bronze_fii_dii AS
WITH src AS (
    -- Each file holds a list of rows under "data"; UNNEST turns it into one row each.
    SELECT trade_date, fetched_at, source_url, UNNEST(data) AS r
    FROM read_json('{FII_GLOB}', format = 'auto')
)
SELECT
    CAST(trade_date AS DATE)            AS trade_date,
    CAST(fetched_at AS TIMESTAMPTZ)     AS fetched_at,
    source_url,
    r.category                          AS category,
    CAST(r.date AS VARCHAR)             AS nse_date,
    CAST(r.buyValue AS VARCHAR)         AS buy_value,
    CAST(r.sellValue AS VARCHAR)        AS sell_value,
    CAST(r.netValue AS VARCHAR)         AS net_value
FROM src;

-- One row per day of Nifty prices.
CREATE OR REPLACE TABLE bronze_nifty AS
SELECT
    CAST(trade_date AS DATE)            AS trade_date,
    CAST(fetched_at AS TIMESTAMPTZ)     AS fetched_at,
    source,
    struct_extract(data, 'open')        AS open,
    struct_extract(data, 'high')        AS high,
    struct_extract(data, 'low')         AS low,
    struct_extract(data, 'close')       AS close
FROM read_json('{NIFTY_GLOB}', format = 'auto');
"""


SILVER_SQL = """
-- One typed row per trading day. Strings become decimals, categories become columns.
CREATE OR REPLACE TABLE silver_daily AS
WITH api_flows AS (
    SELECT
        trade_date,
        'nse_api' AS source,
        MAX(CASE WHEN category = 'FII/FPI' THEN CAST(buy_value  AS DECIMAL(14, 2)) END) AS fii_buy,
        MAX(CASE WHEN category = 'FII/FPI' THEN CAST(sell_value AS DECIMAL(14, 2)) END) AS fii_sell,
        MAX(CASE WHEN category = 'FII/FPI' THEN CAST(net_value  AS DECIMAL(14, 2)) END) AS fii_net,
        MAX(CASE WHEN category = 'DII'     THEN CAST(buy_value  AS DECIMAL(14, 2)) END) AS dii_buy,
        MAX(CASE WHEN category = 'DII'     THEN CAST(sell_value AS DECIMAL(14, 2)) END) AS dii_sell,
        MAX(CASE WHEN category = 'DII'     THEN CAST(net_value  AS DECIMAL(14, 2)) END) AS dii_net
    FROM bronze_fii_dii
    GROUP BY trade_date
),
flows AS (
    -- Pipeline data wins: backfill rows are used only for dates the API never covered.
    SELECT * FROM api_flows
    UNION ALL BY NAME
    SELECT
        trade_date,
        'manual_backfill:' || source AS source,
        fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net
    FROM bronze_fii_dii_backfill
    WHERE trade_date NOT IN (SELECT trade_date FROM api_flows)
),
nifty AS (
    SELECT
        trade_date,
        CAST(open  AS DECIMAL(10, 2)) AS nifty_open,
        CAST(high  AS DECIMAL(10, 2)) AS nifty_high,
        CAST(low   AS DECIMAL(10, 2)) AS nifty_low,
        CAST(close AS DECIMAL(10, 2)) AS nifty_close
    FROM bronze_nifty
)
SELECT
    COALESCE(f.trade_date, n.trade_date) AS trade_date,
    f.source                             AS flow_source,
    f.fii_buy, f.fii_sell, f.fii_net,
    f.dii_buy, f.dii_sell, f.dii_net,
    n.nifty_open, n.nifty_high, n.nifty_low, n.nifty_close
FROM flows AS f
FULL OUTER JOIN nifty AS n USING (trade_date)
ORDER BY trade_date;
"""


GOLD_SQL = """
-- Metrics people actually care about, one row per day with FII/DII data.
CREATE OR REPLACE TABLE gold_daily_metrics AS
WITH with_change AS (
    -- Day-on-day Nifty change, computed over every trading day (before filtering).
    SELECT
        *,
        nifty_close - LAG(nifty_close) OVER (ORDER BY trade_date) AS nifty_change,
        -- What the Nifty did on the NEXT trading day (close to close).
        -- NULL for the latest day, because tomorrow hasn't happened yet.
        LEAD(nifty_close) OVER (ORDER BY trade_date) - nifty_close AS nifty_next_change
    FROM silver_daily
    WHERE nifty_close IS NOT NULL
),
base AS (
    SELECT
        trade_date,
        fii_net,
        dii_net,
        fii_net + dii_net AS combined_net,
        nifty_close,
        nifty_change,
        nifty_next_change,
        CASE WHEN fii_net < 0 THEN 'sell' ELSE 'buy' END AS fii_side
    FROM with_change
    WHERE fii_net IS NOT NULL
),
islands AS (
    -- Gaps-and-islands: consecutive days on the same side share a group id.
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY trade_date)
          - ROW_NUMBER() OVER (PARTITION BY fii_side ORDER BY trade_date) AS grp
    FROM base
)
SELECT
    trade_date,
    fii_net,
    dii_net,
    combined_net,
    nifty_close,
    ROUND(nifty_change, 2)                                              AS nifty_change,
    ROUND(100 * nifty_change / (nifty_close - nifty_change), 2)         AS nifty_change_pct,
    fii_side,
    ROW_NUMBER() OVER (PARTITION BY fii_side, grp ORDER BY trade_date)  AS fii_streak_days,
    SUM(fii_net) OVER (ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)  AS fii_net_5d,
    SUM(fii_net) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS fii_net_20d,
    -- How much of the FII selling DIIs absorbed (1.0 = fully absorbed). Only meaningful on FII sell days.
    CASE WHEN fii_net < 0 THEN ROUND(dii_net / ABS(fii_net), 2) END     AS dii_absorption_ratio,
    CASE
        WHEN fii_net >= 0                       THEN 'fii_buying'
        WHEN dii_net / ABS(fii_net) >= 1.0      THEN 'fully_absorbed'
        WHEN dii_net / ABS(fii_net) >= 0.8      THEN 'mostly_absorbed'
        ELSE                                         'weak_absorption'
    END                                                                 AS absorption_bucket,
    ROUND(100 * nifty_next_change / nifty_close, 2)                     AS nifty_next_day_change_pct
FROM islands
ORDER BY trade_date;
"""


MART_SQL = """
-- One row per trading day: FII exit, how much DIIs absorbed, and what the Nifty did afterwards.
CREATE OR REPLACE TABLE mart_absorption_events AS
WITH prices AS (
    -- Forward-looking prices, computed over every trading day that has a Nifty close.
    SELECT
        trade_date,
        nifty_close,
        LEAD(nifty_close, 1) OVER w AS close_1d,
        LEAD(nifty_close, 3) OVER w AS close_3d,
        LEAD(nifty_close, 5) OVER w AS close_5d,
        -- Lowest close over the next 5 trading days: how far the market sank afterwards.
        MIN(nifty_close) OVER (ORDER BY trade_date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS low_next_5d,
        COUNT(nifty_close) OVER (ORDER BY trade_date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) AS days_ahead
    FROM silver_daily
    WHERE nifty_close IS NOT NULL
    WINDOW w AS (ORDER BY trade_date)
)
SELECT
    g.trade_date,
    g.fii_net,
    -ABS(LEAST(g.fii_net, 0))                                       AS fii_exit,       -- negative = money out
    g.dii_net,
    g.combined_net,
    g.dii_absorption_ratio,
    g.absorption_bucket,
    g.fii_streak_days,
    g.nifty_change_pct                                              AS same_day_pct,
    ROUND(100 * (p.close_1d / p.nifty_close - 1), 2)                AS next_1d_pct,
    ROUND(100 * (p.close_3d / p.nifty_close - 1), 2)                AS next_3d_pct,
    ROUND(100 * (p.close_5d / p.nifty_close - 1), 2)                AS next_5d_pct,
    -- Only meaningful once 5 trading days have passed; NULL until then.
    CASE WHEN p.days_ahead = 5
         THEN ROUND(100 * (p.low_next_5d / p.nifty_close - 1), 2) END AS max_fall_5d_pct
FROM gold_daily_metrics AS g
JOIN prices AS p USING (trade_date)
ORDER BY g.trade_date;

-- The answer in one table: for each absorption bucket, what usually happened next.
CREATE OR REPLACE TABLE mart_absorption_summary AS
SELECT
    absorption_bucket,
    COUNT(*)                                                            AS days,
    ROUND(AVG(dii_absorption_ratio), 2)                                 AS avg_absorption_ratio,
    ROUND(AVG(fii_exit), 0)                                             AS avg_fii_exit_cr,
    ROUND(100.0 * AVG(CASE WHEN next_1d_pct < 0 THEN 1 ELSE 0 END), 0)  AS pct_next_day_red,
    ROUND(AVG(next_1d_pct), 2)                                          AS avg_next_1d_pct,
    ROUND(AVG(next_3d_pct), 2)                                          AS avg_next_3d_pct,
    ROUND(AVG(next_5d_pct), 2)                                          AS avg_next_5d_pct,
    ROUND(AVG(max_fall_5d_pct), 2)                                      AS avg_max_fall_5d_pct,
    ROUND(MIN(max_fall_5d_pct), 2)                                      AS worst_fall_5d_pct
FROM mart_absorption_events
WHERE next_1d_pct IS NOT NULL
GROUP BY absorption_bucket
UNION ALL
-- Baseline row: every day, so each bucket can be compared against "normal".
SELECT
    'ALL DAYS (baseline)', COUNT(*), NULL, ROUND(AVG(fii_exit), 0),
    ROUND(100.0 * AVG(CASE WHEN next_1d_pct < 0 THEN 1 ELSE 0 END), 0),
    ROUND(AVG(next_1d_pct), 2), ROUND(AVG(next_3d_pct), 2), ROUND(AVG(next_5d_pct), 2),
    ROUND(AVG(max_fall_5d_pct), 2), ROUND(MIN(max_fall_5d_pct), 2)
FROM mart_absorption_events
WHERE next_1d_pct IS NOT NULL;
"""


# Each check returns rows only when something is wrong.
QUALITY_CHECKS = {
    "duplicate dates in backfill CSV": """
        SELECT trade_date, COUNT(*) FROM bronze_fii_dii_backfill GROUP BY trade_date HAVING COUNT(*) > 1
    """,
    "backfill row with missing date or net values": """
        SELECT * FROM bronze_fii_dii_backfill
        WHERE trade_date IS NULL OR fii_net IS NULL OR dii_net IS NULL
    """,
    "duplicate trading days in silver": """
        SELECT trade_date, COUNT(*) FROM silver_daily GROUP BY trade_date HAVING COUNT(*) > 1
    """,
    "FII net != buy - sell": """
        SELECT trade_date, fii_buy, fii_sell, fii_net FROM silver_daily
        WHERE fii_net IS NOT NULL AND ABS(fii_buy - fii_sell - fii_net) > 0.15
    """,
    "DII net != buy - sell": """
        SELECT trade_date, dii_buy, dii_sell, dii_net FROM silver_daily
        WHERE dii_net IS NOT NULL AND ABS(dii_buy - dii_sell - dii_net) > 0.15
    """,
    "FII/DII day missing one category": """
        SELECT trade_date FROM silver_daily
        WHERE (fii_net IS NULL) <> (dii_net IS NULL)
    """,
    "NSE date differs from folder date": """
        SELECT trade_date, nse_date FROM bronze_fii_dii
        WHERE COALESCE(TRY_STRPTIME(nse_date, '%d-%b-%Y')::DATE, TRY_CAST(nse_date AS DATE))
              IS DISTINCT FROM trade_date
    """,
}


def run_quality_checks(con: duckdb.DuckDBPyConnection) -> None:
    failures = []
    for name, sql in QUALITY_CHECKS.items():
        bad_rows = con.execute(sql).fetchall()
        if bad_rows:
            failures.append(f"{name}: {bad_rows[:5]}")
    if failures:
        raise RuntimeError("Quality checks failed:\n  " + "\n  ".join(failures))
    print(f"All {len(QUALITY_CHECKS)} quality checks passed.")


def main() -> None:
    if not list(RAW_DIR.glob("*/fii_dii.json")):
        raise FileNotFoundError(f"No fii_dii.json files found under {RAW_DIR}")
    if not list(RAW_DIR.glob("*/nifty.json")):
        raise FileNotFoundError(f"No nifty.json files found under {RAW_DIR}")

    with duckdb.connect(str(DB_PATH)) as con:
        con.execute(BRONZE_SQL)
        if BACKFILL_CSV.exists():
            con.execute(BACKFILL_SQL)
        else:
            print(f"No backfill file at {BACKFILL_CSV}, continuing without it.")
            con.execute(EMPTY_BACKFILL_SQL)
        con.execute(SILVER_SQL)
        con.execute(GOLD_SQL)
        con.execute(MART_SQL)
        run_quality_checks(con)

        for table in (
            "bronze_fii_dii",
            "bronze_fii_dii_backfill",
            "bronze_nifty",
            "silver_daily",
            "gold_daily_metrics",
            "mart_absorption_events",
            "mart_absorption_summary",
        ):
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table:<26} {count:>5} rows")

        print("\nLatest gold metrics:")
        con.sql("""
            SELECT trade_date, fii_net, dii_net, nifty_close, nifty_change_pct,
                   fii_side, fii_streak_days, dii_absorption_ratio
            FROM gold_daily_metrics
            ORDER BY trade_date DESC
            LIMIT 5
        """).show()


if __name__ == "__main__":
    main()