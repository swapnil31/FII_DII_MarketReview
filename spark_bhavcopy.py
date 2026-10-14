"""PySpark job. Raw bhavcopy zips -> silver equity prices -> gold returns and sector returns.

Layers (all Parquet, under lake/, partitioned by year/month):
  silver/equity_prices        one row per stock per day, ONE schema for both NSE formats
  gold/stock_daily_returns    daily return, turnover, 20-day averages, sector
  gold/sector_daily_returns   per sector per day: equal- and liquidity-weighted returns

INCREMENTAL by default: lake/_manifest.json remembers which raw files were processed. Each run
reprocesses only the MONTHS that have new or changed files, reading ~60 days of earlier silver
data so the 20-day rolling windows stay correct, and overwrites only those month partitions.
No new files -> exits in seconds without starting Spark.

Run inside the Airflow container (Spark needs Java, which the image has):
  docker compose exec airflow-scheduler python /opt/airflow/project/spark_bhavcopy.py          # incremental
  docker compose exec airflow-scheduler python /opt/airflow/project/spark_bhavcopy.py --full   # rebuild all
"""

import csv
import io
import json
import shutil
import sys
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "bhavcopy"
SECTORS_CSV = BASE_DIR / "reference" / "nifty500.csv"
LAKE = BASE_DIR / "lake"
SILVER_PATH = str(LAKE / "silver" / "equity_prices")
GOLD_STOCK_PATH = str(LAKE / "gold" / "stock_daily_returns")
GOLD_SECTOR_PATH = str(LAKE / "gold" / "sector_daily_returns")
MANIFEST = LAKE / "_manifest.json"
LOOKBACK_DAYS = 60            # calendar days of earlier data read for the 20-trading-day windows

SERIES_KEPT = {"EQ"}          # main-board equities only (skip bonds, ETFs' other series, SME etc.)
MAX_ABS_RETURN = 0.35         # NSE circuit limits are 20%; bigger moves mean a data/corporate-action issue

UNIFIED_FIELDS = ["trade_date", "symbol", "series", "isin", "open", "high", "low", "close",
                  "prev_close", "volume", "turnover", "trades", "source_format", "source_file"]


# ---------------------------------------------------------------------------------------------
# Pure-Python parsing (runs on Spark executors; also unit-testable without Spark)
# ---------------------------------------------------------------------------------------------

def _num(value: str) -> float | None:
    value = (value or "").strip()
    if value in ("", "-", "NA"):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_classic(row: dict) -> dict:
    return {
        "trade_date": datetime.strptime(row["TIMESTAMP"].strip(), "%d-%b-%Y").date().isoformat(),
        "symbol": row["SYMBOL"].strip(),
        "series": row["SERIES"].strip(),
        "isin": (row.get("ISIN") or "").strip(),
        "open": _num(row["OPEN"]), "high": _num(row["HIGH"]), "low": _num(row["LOW"]),
        "close": _num(row["CLOSE"]), "prev_close": _num(row.get("PREVCLOSE")),
        "volume": _num(row["TOTTRDQTY"]), "turnover": _num(row.get("TOTTRDVAL")),
        "trades": _num(row.get("TOTALTRADES")),
        "source_format": "classic",
    }


def parse_udiff(row: dict) -> dict:
    return {
        "trade_date": datetime.strptime(row["TradDt"].strip(), "%Y-%m-%d").date().isoformat(),
        "symbol": row["TckrSymb"].strip(),
        "series": row["SctySrs"].strip(),
        "isin": (row.get("ISIN") or "").strip(),
        "open": _num(row["OpnPric"]), "high": _num(row["HghPric"]), "low": _num(row["LwPric"]),
        "close": _num(row["ClsPric"]), "prev_close": _num(row.get("PrvsClsgPric")),
        "volume": _num(row["TtlTradgVol"]), "turnover": _num(row.get("TtlTrfVal")),
        "trades": _num(row.get("TtlNbOfTxsExctd")),
        "source_format": "udiff",
    }


def parse_zip(path: str, content: bytes) -> list[tuple]:
    """Unzip one bhavcopy, detect its format from the header, return unified rows.

    On a bad file, returns a single marker row with symbol '__ERROR__' so the
    driver can report it instead of the whole job crashing.
    """
    name = path.rsplit("/", 1)[-1]
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
            text = zf.read(csv_name).decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        reader.fieldnames = [f.strip() for f in reader.fieldnames]
        if "TIMESTAMP" in reader.fieldnames:
            parse = parse_classic
        elif "TradDt" in reader.fieldnames:
            parse = parse_udiff
        else:
            raise ValueError(f"unknown header {reader.fieldnames[:6]}")

        out = []
        for row in reader:
            if not row or not any(row.values()):
                continue
            series = (row.get("SERIES") or row.get("SctySrs") or "").strip()
            if series not in SERIES_KEPT:
                continue
            rec = parse(row)
            rec["source_file"] = name
            out.append(tuple(rec[f] for f in UNIFIED_FIELDS))
        return out
    except Exception as exc:  # report, don't crash
        return [(None, "__ERROR__", str(exc)[:200]) + (None,) * (len(UNIFIED_FIELDS) - 4) + (name,)]



# ---------------------------------------------------------------------------------------------
# Planning: which months need (re)processing? (pure Python, unit-tested)
# ---------------------------------------------------------------------------------------------

def raw_inventory(raw_dir: Path = RAW_DIR) -> dict[str, int]:
    """Relative path ('dt=2026-10-13/file.zip') -> size in bytes, for every raw zip."""
    return {f"{p.parent.name}/{p.name}": p.stat().st_size for p in sorted(raw_dir.glob("dt=*/*.zip"))}


def file_month(rel_path: str) -> tuple[int, int]:
    day = date.fromisoformat(rel_path.split("/")[0].removeprefix("dt="))
    return day.year, day.month


def plan_months(raw: dict[str, int], manifest: dict[str, int]) -> list[tuple[int, int]]:
    """Months containing a raw file that is new or whose size changed since the last run."""
    changed = [path for path, size in raw.items() if manifest.get(path) != size]
    return sorted({file_month(p) for p in changed})


def load_manifest() -> dict[str, int]:
    try:
        return json.loads(MANIFEST.read_text())
    except (FileNotFoundError, ValueError):
        return {}


# ---------------------------------------------------------------------------------------------
# Spark job
# ---------------------------------------------------------------------------------------------

def build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("bhavcopy")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")   # overwrite only touched months
        .config("spark.sql.shuffle.partitions", "16")      # small data: avoid 200 tiny tasks
        .getOrCreate()
    )


def main(full: bool = False) -> None:
    started = time.time()
    raw = raw_inventory()
    if not raw:
        print(f"No raw files in {RAW_DIR}; nothing to do.")
        return

    manifest = {} if full else load_manifest()
    if not manifest or not Path(SILVER_PATH).exists():
        full = True                                     # first run, or lake missing: rebuild everything
    months = sorted({file_month(p) for p in raw}) if full else plan_months(raw, manifest)
    if not months:
        print(f"Lake is up to date ({len(raw)} raw files already processed). Done in {time.time() - started:.0f}s")
        return
    label = "FULL rebuild" if full else "Incremental"
    print(f"{label}: {len(months)} month(s) to process: {', '.join(f'{y}-{m:02d}' for y, m in months)}")

    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    if full:
        # A full rebuild starts from empty folders, so no stale partitions survive.
        for path in (SILVER_PATH, GOLD_STOCK_PATH, GOLD_SECTOR_PATH):
            shutil.rmtree(path, ignore_errors=True)

    schema = T.StructType([
        T.StructField("trade_date", T.StringType()), T.StructField("symbol", T.StringType()),
        T.StructField("series", T.StringType()), T.StructField("isin", T.StringType()),
        *[T.StructField(c, T.DoubleType()) for c in
          ("open", "high", "low", "close", "prev_close", "volume", "turnover", "trades")],
        T.StructField("source_format", T.StringType()), T.StructField("source_file", T.StringType()),
    ])
    month_keys = [y * 100 + m for y, m in months]
    in_months = (F.col("year") * 100 + F.col("month")).isin(month_keys)

    # ---- Bronze -> Silver: only the affected months' zips ------------------------------------
    globs = ",".join(str(RAW_DIR / f"dt={y}-{m:02d}-*" / "*.zip") for y, m in months)
    files = spark.sparkContext.binaryFiles(globs)
    parsed = spark.createDataFrame(files.flatMap(lambda kv: parse_zip(kv[0], kv[1])), schema).cache()

    errors = parsed.filter(F.col("symbol") == "__ERROR__").select("series", "source_file").collect()
    if errors:
        print(f"WARNING: {len(errors)} file(s) could not be parsed:")
        for e in errors[:20]:
            print(f"  {e.source_file}: {e.series}")

    new_silver = (
        parsed.filter(F.col("symbol") != "__ERROR__")
        .withColumn("trade_date", F.to_date("trade_date"))
        .dropDuplicates(["trade_date", "symbol"])
        .withColumn("year", F.year("trade_date"))
        .withColumn("month", F.month("trade_date"))
    )
    new_silver.write.mode("overwrite").partitionBy("year", "month").parquet(SILVER_PATH)

    # ---- Quality checks on the months just written --------------------------------------------
    written = spark.read.parquet(SILVER_PATH).filter(in_months)
    checks = {
        "duplicate (date, symbol)": written.groupBy("trade_date", "symbol").count().filter("count > 1").count(),
        "close missing or <= 0": written.filter("close IS NULL OR close <= 0").count(),
        "high < low": written.filter("high < low").count(),
        "days with < 1000 stocks": written.groupBy("trade_date").count().filter("count < 1000").count(),
    }
    failed = {k: v for k, v in checks.items() if v}
    print(f"Quality check warnings: {failed}" if failed else f"All {len(checks)} silver quality checks passed.")

    # ---- Gold: per-stock daily returns ---------------------------------------------------------
    # Rolling windows need ~20 earlier trading days, so read LOOKBACK_DAYS before the first month,
    # compute, then keep only the affected months.
    first_y, first_m = months[0]
    start = date(first_y, first_m, 1) - timedelta(days=LOOKBACK_DAYS)
    silver = (spark.read.parquet(SILVER_PATH)
              .filter((F.col("year") * 100 + F.col("month")) >= start.year * 100 + start.month)
              .filter(F.col("trade_date") >= F.lit(start)))

    # Return uses NSE's own prev_close, which NSE adjusts on corporate-action ex-dates,
    # so splits and bonuses don't show up as fake -50% days.
    by_stock = Window.partitionBy("symbol").orderBy("trade_date")
    vol_20d = by_stock.rowsBetween(-19, 0)

    sectors = (
        spark.read.option("header", True).csv(str(SECTORS_CSV)).select("symbol", "industry")
        if SECTORS_CSV.exists()
        else spark.createDataFrame([], "symbol string, industry string")
    )

    stock = (
        silver.withColumn("daily_return", F.col("close") / F.col("prev_close") - 1)
        .withColumn("return_suspect", F.abs("daily_return") > MAX_ABS_RETURN)
        .withColumn("avg_volume_20d", F.avg("volume").over(vol_20d))
        .withColumn("volume_vs_20d", F.col("volume") / F.col("avg_volume_20d"))
        # Weight for sector averages: average turnover of the PREVIOUS 20 days, known before the day
        # starts. Weighting by same-day turnover over-weights stocks that spiked that day (biased).
        .withColumn("weight_turnover_20d", F.avg("turnover").over(by_stock.rowsBetween(-20, -1)))
        .join(F.broadcast(sectors), "symbol", "left")
        .withColumn("industry", F.coalesce("industry", F.lit("Unclassified")))
        .filter(in_months)
    )
    stock.write.mode("overwrite").partitionBy("year", "month").parquet(GOLD_STOCK_PATH)
    stock = spark.read.parquet(GOLD_STOCK_PATH).filter(in_months)

    # ---- Gold: per-sector daily returns (same months) ------------------------------------------
    clean = stock.filter("NOT return_suspect AND daily_return IS NOT NULL AND turnover > 0 "
                         "AND weight_turnover_20d > 0")
    sector = (
        clean.groupBy("trade_date", "industry")
        .agg(
            F.count("*").alias("stocks"),
            F.avg("daily_return").alias("ew_return"),                                   # equal weight
            (F.sum(F.col("daily_return") * F.col("weight_turnover_20d"))
             / F.sum("weight_turnover_20d")).alias("tw_return"),                       # liquidity weight
            F.sum("turnover").alias("turnover"),
            F.avg(F.when(F.col("daily_return") > 0, 1).otherwise(0)).alias("pct_advancing"),
        )
        .withColumn("year", F.year("trade_date"))
        .withColumn("month", F.month("trade_date"))
    )
    sector.write.mode("overwrite").partitionBy("year", "month").parquet(GOLD_SECTOR_PATH)

    # ---- Remember what was processed, then summarise -------------------------------------------
    MANIFEST.write_text(json.dumps({**manifest, **raw}, indent=0))

    summary = written.agg(
        F.count("*").alias("rows"), F.countDistinct("trade_date").alias("days"),
        F.countDistinct("symbol").alias("stocks"), F.min("trade_date").alias("first"),
        F.max("trade_date").alias("last"),
    ).first()
    suspect = stock.filter("return_suspect").count()
    print(f"\nWritten: {summary.rows:,} rows | {summary.days} days | {summary.stocks:,} stocks | "
          f"{summary.first} to {summary.last}")
    print(f"Gold: {suspect:,} suspect returns (>|{MAX_ABS_RETURN:.0%}|) excluded from sector averages")
    print(f"{label} done in {time.time() - started:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main(full="--full" in sys.argv)
