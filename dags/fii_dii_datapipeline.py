"""Daily FII/DII + Nifty pipeline.

Runs 8:00 PM IST, Monday to Friday:

    is_trading_day -> extract_fii_dii  --+
                   -> extract_nifty    --+--> load_warehouse --> make_post
                                                     |
                   -> extract_bhavcopy -> spark_bhavcopy --+--> analyze_sectors
                                                           +--> daily_explainer

- The bhavcopy branch is independent: if NSE's bhavcopy is late or Spark fails,
  the FII/DII warehouse and the daily post still go out.

- is_trading_day skips the whole run on NSE holidays (Dussehra, Diwali...).
- extract_fii_dii retries every 20 minutes until NSE has published today's
  numbers (NSE sometimes publishes late).
- Missed evenings are NOT caught up automatically: NSE's API only returns the
  latest day, so add a missed day to backfill/fii_dii_backfill.csv instead.
"""

import sys
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.timetables.trigger import CronTriggerTimetable

PROJECT_DIR = "/opt/airflow/project"   # your D:\fii-dii data folder, mounted by docker-compose
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

IST = "Asia/Kolkata"


def run_date_ist(context) -> "pendulum.Date":
    return context["logical_date"].in_timezone(IST).date()


@dag(
    dag_id="fii_dii_pipeline",
    description="Extract FII/DII + Nifty, build the DuckDB warehouse, create the daily post",
    # CronTriggerTimetable runs AT 20:00 (a plain cron schedule would run a day late).
    schedule=CronTriggerTimetable("0 20 * * 1-5", timezone=IST),
    start_date=pendulum.datetime(2026, 10, 12, tz=IST),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["fii-dii", "markets"],
)
def fii_dii_pipeline():

    @task.short_circuit
    def is_trading_day(**context) -> bool:
        from market_calendar import is_trading_day as check

        day = run_date_ist(context)
        open_ = check(day)
        print(f"{day}: {'trading day, continuing' if open_ else 'market closed, skipping run'}")
        return open_

    @task(retries=6, retry_delay=timedelta(minutes=20))
    def extract_fii_dii(**context) -> str:
        import extract_fii_dii as fii

        rows = fii.fetch_fii_dii()
        data_date = pendulum.from_format(rows[0]["date"], "DD-MMM-YYYY").date()
        expected = run_date_ist(context)
        if data_date != expected:
            # NSE still shows the previous day: fail so Airflow retries in 20 minutes.
            raise ValueError(f"NSE has not published {expected} yet (latest is {data_date}).")
        return str(fii.save_raw(rows))

    @task
    def extract_nifty() -> int:
        import extract_nifty as nifty

        rows = nifty.fetch_nifty()
        nifty.save_raw(rows)
        return len(rows)

    @task(retries=6, retry_delay=timedelta(minutes=20))
    def extract_bhavcopy(**context) -> str:
        import extract_bhavcopy as bhav

        day = run_date_ist(context)
        if bhav.already_downloaded(day):
            return f"{day}: already downloaded"
        result = bhav.fetch_day(bhav.new_session(), day)
        if result is None:
            # It is a trading day (the gate said so), so the file should exist: NSE is late.
            raise ValueError(f"Bhavcopy for {day} not published yet; retrying in 20 minutes.")
        url, fmt, rows = result
        return f"{day}: {fmt}, {rows} rows from {url}"

    # Spark runs in its own process (bash), so its JVM and memory are released when it ends
    # instead of living inside the Airflow worker.
    @task.bash(retries=1, execution_timeout=timedelta(minutes=45))
    def spark_bhavcopy() -> str:
        return f"python {PROJECT_DIR}/spark_bhavcopy.py"

    @task
    def analyze_sectors() -> None:
        import analyze_sectors as sectors

        sectors.main()

    @task
    def daily_explainer(**context) -> str:
        import daily_explainer as explainer

        return str(explainer.main(run_date_ist(context)))

    @task
    def load_warehouse() -> None:
        import load_warehouse as warehouse

        warehouse.main()

    @task
    def make_post() -> None:
        import make_post as post

        post.main()

    gate = is_trading_day()
    loaded = load_warehouse()
    spark = spark_bhavcopy()

    gate >> [extract_fii_dii(), extract_nifty()] >> loaded >> make_post()
    gate >> extract_bhavcopy() >> spark
    [loaded, spark] >> analyze_sectors()
    [loaded, spark] >> daily_explainer()


fii_dii_pipeline()