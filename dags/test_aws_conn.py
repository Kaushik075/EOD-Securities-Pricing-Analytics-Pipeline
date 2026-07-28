# dags/test_aws_conn.py
"""
Boilerplate DAG to test Airflow ↔ AWS (S3) connection.
Verifies that the Airflow 'aws_default' connection is configured correctly
by listing objects from the project's S3 bucket.
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
import logging
from airflow.providers.standard.operators.python import PythonOperator

# Create a logger instance (Airflow automatically routes this to task logs)
log = logging.getLogger(__name__)


with DAG(
    dag_id="test_aws_conn",
    description="Simple DAG to verify AWS S3 connection credentials",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aws", "s3", "connection", "test"],
) as dag:

    # Task 1: List objects from S3 bucket
    list_s3_objects = S3ListOperator(
        task_id="list_s3_objects",
        aws_conn_id="aws_default",
        bucket="kaushik-eodsecurities-data",
    )

    # Task 2: Log one-line success message
    def log_success(**context):
        log.info("✅ AWS connection successful")

    log_success_task = PythonOperator(
        task_id="log_success",
        python_callable=log_success,
    )

    list_s3_objects >> log_success_task
