# dags/test_snowflake_conn.py
from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="test_snowflake_conn",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake","test"],
) as dag:

    test_connection = SQLExecuteQueryOperator(
        task_id="check_conn",
        conn_id="snowflake_default",      # ← use your Connection ID
        sql="""
            SELECT 
                CURRENT_USER()       AS user_name,
                CURRENT_ROLE()       AS active_role,
                CURRENT_ACCOUNT()    AS account,
                CURRENT_WAREHOUSE()  AS warehouse,
                CURRENT_DATABASE()   AS database,
                CURRENT_SCHEMA()     AS schema;
        """,
    )

    test_connection