# dags/test_slack_conn_basehook.py
"""
Lightweight DAG to verify Airflow ↔ Slack connection using BaseHook.
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
# from airflow.hooks.base import BaseHook
from airflow.sdk.bases.hook import BaseHook
import requests
from datetime import datetime


def test_slack_conn():
    """Send a simple test message to Slack via BaseHook connection."""
    conn = BaseHook.get_connection("slack_default")
    webhook_url = f"{conn.schema}://{conn.host}/{conn.password}"
    response = requests.post(webhook_url, json={"text": "✅ Slack connection test successful!"})
    if response.status_code != 200:
        raise Exception(f"Slack test failed: {response.status_code}, {response.text}")


with DAG(
        dag_id="test_slack_conn",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        tags=["slack", "connection", "test"],
) as dag:
    test_connection = PythonOperator(
        task_id="verify_slack_connection",
        python_callable=test_slack_conn)

    test_connection