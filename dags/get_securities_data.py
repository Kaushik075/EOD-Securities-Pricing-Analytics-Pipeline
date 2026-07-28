# dags/get_securities_data.py

from airflow import DAG
import pendulum
from airflow.models import Variable  # To fetch variables from Airflow UI or environment
from airflow.exceptions import AirflowFailException  # To raise exceptions in case of failures
from airflow.providers.standard.operators.python import PythonOperator  # To execute Python functions as tasks
import os
from airflow.sdk import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator

from lib.eod_data_downloader import download_polygon_eod_data_to_csv  # Custom function to download EOD data
from lib.slack_utils import slack_post, on_task_failure
import logging

# Initialize logger for logging events
log = logging.getLogger(__name__)

# Default arguments for the DAG
DEFAULT_ARGS = {
    "owner": "data-eng",             # Owner of the DAG
    "retries": 3,                    # Retry the task 3 times if it fails
    "retry_delay": pendulum.duration(minutes=5),  # Retry delay of 5 minutes
}

# Setup basic configurations for Polygon API
POLYGON_API_KEY = Variable.get("POLYGON_API_KEY")  # API Key for Polygon.io to access market data
POLYGON_MAX_LOOKBACK_DAYS = int(Variable.get("LOOKBACK_DAYS", default_var="10"))  # Max lookback days

# S3_BUCKET should be set (via Airflow Variable) to: kaushik-eodsecurities-data
S3_BUCKET = Variable.get("S3_BUCKET")

# SQL templates for this DAG live in the pipeline-stage folders one level up from dags/,
# rather than a dedicated dags/sql/ folder — Airflow's template_searchpath happily accepts
# multiple paths, so the SQL files stay organized by pipeline stage without duplication.
DAGS_DIR = os.path.dirname(__file__)
TEMPLATE_SEARCHPATH = [
    os.path.join(DAGS_DIR, "..", "2_load"),
    os.path.join(DAGS_DIR, "..", "3_transform"),
]


with DAG(
    dag_id="kaushik_eod_securities_pipeline",
    start_date=pendulum.datetime(2025, 10, 20),  # Start date for the DAG execution
    schedule='5 21 * * 1-5',  # Scheduled to run Mon-Fri at 21:05 UTC
    catchup=False,  # Don't backfill past missed runs
    max_active_runs=1,  # Only allow one active DAG run at a time
    default_args=DEFAULT_ARGS,  # Default arguments for task retries and failure handling
    tags=["securities", "batch", "polygon"],  # Tags for categorization in the Airflow UI
    on_failure_callback=on_task_failure,
    description="Polygon-only batch EOD: Download and process the latest available trading day.",
    template_searchpath=TEMPLATE_SEARCHPATH,
):
    # Step: Download the trading day's data to CSV (imported from lib)
    def download_trading_day_csv(**ctx):
        """
        This function downloads the Polygon EOD data
        and stores it as a CSV file in the specified location.
        """
        trading_date = download_polygon_eod_data_to_csv(POLYGON_API_KEY, POLYGON_MAX_LOOKBACK_DAYS)
        ctx["ti"].xcom_push(key="trading_date", value=trading_date)
        log.info(f"Downloaded EOD data for {trading_date}")

    download = PythonOperator(
        task_id="t01_download_to_csv",
        python_callable=download_trading_day_csv,
    )

    # Step: Verify local file
    def verify_file_exists(**ctx):
        """
        This function checks if the expected CSV file exists at the given local path.
        If not, it raises an AirflowFailException.
        """
        trading_date = ctx["ti"].xcom_pull(task_ids="t01_download_to_csv", key="trading_date")
        path = f"/tmp/eod_{trading_date}.csv"
        log.info("[verify] expecting file at: %s", path)

        if not os.path.exists(path):
            raise AirflowFailException(f"Expected file not found: {path}")

        log.info("[verify] file exists at %s (size=%s bytes)", path, os.path.getsize(path))
        log.info(f"Next step is to upload to this S3 bucket: {S3_BUCKET}")

    verify_file = PythonOperator(
        task_id="t02_verify_local_file",
        python_callable=verify_file_exists,
    )

    # Step: Upload to S3
    upload_file = LocalFilesystemToS3Operator(
        task_id="t03_upload_to_s3",
        filename="/tmp/eod_{{ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date')}}.csv",
        dest_bucket=S3_BUCKET,  # kaushik-eodsecurities-data
        dest_key=(
            "market/bronze/eod/"
            "eod_prices_{{ ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date') }}.csv"
        ),
        aws_conn_id="aws_default",
        replace=True,
    )

    # Step: Snowflake load
    with TaskGroup(group_id="t04_snowflake_load") as snowflake_load:
        params_common = {"trading_ds_task_id": "t01_download_to_csv"}

        copy_to_raw = SQLExecuteQueryOperator(
            task_id="s01_copy_to_raw",
            conn_id="snowflake_default",
            sql="1__copy_to_raw.sql",
            params=params_common,
        )

        check_loaded = SnowflakeCheckOperator(
            task_id='s02_check_eod_prices_exist',
            sql="2__check_loaded.sql",
            snowflake_conn_id='snowflake_default',
            params=params_common,
        )

        premerge_metrics = SQLExecuteQueryOperator(
            task_id="s03_compute_premerge_metrics",
            conn_id="snowflake_default",
            sql="3__premerge_metrics.sql",
            params=params_common,
        )

        merge_core = SQLExecuteQueryOperator(
            task_id="s04_merge_core_eod",
            conn_id="snowflake_default",
            sql="4__merge_core.sql",
            params=params_common,
        )

        merge_dim_security = SQLExecuteQueryOperator(
            task_id="s05_merge_dim_security",
            conn_id="snowflake_default",
            sql="5__merge_dim_security.sql",
            params=params_common,
        )

        merge_dim_date = SQLExecuteQueryOperator(
            task_id="s06_merge_dim_date",
            conn_id="snowflake_default",
            sql="6__dm_dim_date.sql",
            params=params_common,
        )

        merge_fact = SQLExecuteQueryOperator(
            task_id="s07_merge_fact_daily_price",
            conn_id="snowflake_default",
            sql="7__merge_fact_daily_price.sql",
            params=params_common,
        )

        postmerge = SQLExecuteQueryOperator(
            task_id="s08_compute_postmerge_metrics",
            conn_id="snowflake_default",
            sql="8__postmerge_metrics.sql",
            params=params_common,
        )

        copy_to_raw >> check_loaded >> premerge_metrics >> merge_core
        merge_core >> [merge_dim_security, merge_dim_date] >> merge_fact >> postmerge

    # Wiring for extract → verify → upload → Snowflake TG
    download >> verify_file >> upload_file >> snowflake_load

    # Step: Slack summary
    def notify_slack_summary(**ctx):
        """
        Sends a summary message to Slack at the end of the DAG.
        Pulls metrics from pre/post merge tasks and posts a compact summary.
        """
        trading_date = ctx["ti"].xcom_pull(task_ids="t01_download_to_csv", key="trading_date")
        pre = ctx["ti"].xcom_pull(task_ids="t04_snowflake_load.s03_compute_premerge_metrics") or []
        post = ctx["ti"].xcom_pull(task_ids="t04_snowflake_load.s08_compute_postmerge_metrics") or []

        raw_cnt = ins_est = upd_est = core_ds = fact_ds = 0

        # pre = [(raw_cnt, core_existing_cnt, core_inserts_est, core_updates_est)]
        if pre and len(pre[0]) >= 4:
            raw_cnt, core_existing_cnt, ins_est, upd_est = pre[0]

        # post = [(core_rows, fact_rows)]
        if post and len(post[0]) >= 2:
            core_ds, fact_ds = post[0]

        msg = (
            ":white_check_mark: *EOD Summary*\n"
            f"• Trading Date: `{trading_date}`\n"
            f"• RAW rows: `{int(raw_cnt):,}`\n"
            f"• Estimated CORE inserts: `{int(ins_est):,}`\n"
            f"• Estimated CORE updates: `{int(upd_est):,}`\n"
            f"• CORE rows after merge: `{int(core_ds):,}`\n"
            f"• FACT rows after merge: `{int(fact_ds):,}`"
        )
        slack_post(msg)

    slack_summary = PythonOperator(
        task_id="t05_notify_slack_summary",
        python_callable=notify_slack_summary,
        trigger_rule="all_done",  # ensure Slack fires even if an upstream task failed/skipped
    )

    snowflake_load >> slack_summary
