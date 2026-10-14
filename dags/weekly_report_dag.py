"""Weekly market report, Friday 9:30 PM IST.

    wait_for_week_data -> build_report -> email_report

- wait_for_week_data checks that the lake and the warehouse already contain the week's last
  trading day (the daily pipeline must have finished). If not, it retries every 20 minutes.
  If the whole week was a holiday, the run is skipped.
- build_report writes posts/weekly/<date>/weekly.png and weekly.txt (P/E fetched live from NSE).
- email_report sends the image and caption to your inbox.
"""

import sys
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.timetables.trigger import CronTriggerTimetable

PROJECT_DIR = "/opt/airflow/project"
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

IST = "Asia/Kolkata"


def run_date_ist(context) -> "pendulum.Date":
    return context["logical_date"].in_timezone(IST).date()


def alert_on_failure(context) -> None:
    import notify

    ti = context["task_instance"]
    notify.send_message(notify.failure_text(ti.dag_id, ti.task_id, str(run_date_ist(context)),
                                            ti.try_number, repr(context.get("exception") or ""), ti.log_url))


@dag(
    dag_id="weekly_report",
    description="Friday report: weekly flows, turnover, top losers/gainers with P/E",
    schedule=CronTriggerTimetable("30 21 * * 5", timezone=IST),
    start_date=pendulum.datetime(2026, 10, 14, tz=IST),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "on_failure_callback": alert_on_failure},
    tags=["fii-dii", "markets", "weekly"],
)
def weekly_report():

    @task.short_circuit
    def week_had_trading(**context) -> bool:
        from market_calendar import is_trading_day
        from weekly_report import week_bounds

        monday, friday = week_bounds(run_date_ist(context))
        days = [monday + timedelta(days=i) for i in range(5)]
        traded = [d for d in days if is_trading_day(d)]
        print(f"Trading days this week: {traded or 'none, skipping'}")
        return bool(traded)

    @task(retries=6, retry_delay=timedelta(minutes=20))
    def wait_for_week_data(**context) -> str:
        import duckdb

        from market_calendar import is_trading_day
        from weekly_report import STOCK_GLOB, WAREHOUSE, week_bounds

        monday, friday = week_bounds(run_date_ist(context))
        last = next(friday - timedelta(days=i) for i in range(5) if is_trading_day(friday - timedelta(days=i)))
        lake = duckdb.connect().execute(
            f"SELECT MAX(trade_date)::DATE FROM read_parquet('{STOCK_GLOB}', hive_partitioning = true)").fetchone()[0]
        with duckdb.connect(str(WAREHOUSE), read_only=True) as wh:
            flows = wh.execute("SELECT MAX(trade_date) FROM silver_daily WHERE fii_net IS NOT NULL").fetchone()[0]
        print(f"Week's last trading day {last}; lake has {lake}; warehouse has {flows}")
        if lake is None or lake < last or flows is None or flows < last:
            raise ValueError(f"Data for {last} not ready yet (daily pipeline still running?); retrying in 20 min")
        return str(last)

    @task
    def build_report(last_day: str) -> str:
        import weekly_report as wr

        return str(wr.main(pendulum.parse(last_day).date()))

    @task(retries=1)
    def email_report(out_dir: str) -> None:
        from pathlib import Path

        import notify

        out = Path(out_dir)
        caption = (out / "weekly.txt").read_text(encoding="utf-8")
        notify.send_photo(out / "weekly.png", caption)

    ready = wait_for_week_data()
    week_had_trading() >> ready
    email_report(build_report(ready))


weekly_report()
