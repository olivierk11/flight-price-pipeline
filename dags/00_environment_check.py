"""
Phase 0 smoke test: confirms Airflow can reach MySQL (staging),
PostgreSQL (analytics), and the source CSV before we build the real pipeline.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_PATH = os.environ.get(
    "FLIGHT_CSV_PATH", "/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv"
)


def check_mysql():
    hook = MySqlHook(mysql_conn_id="mysql_staging")
    print("MySQL OK, version:", hook.get_first("SELECT VERSION()")[0])


def check_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_analytics")
    print("PostgreSQL OK, version:", hook.get_first("SELECT version()")[0])


def check_csv():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"CSV not found at {CSV_PATH}. Put it in the project's ./data folder."
        )
    with open(CSV_PATH, encoding="utf-8-sig") as f:
        header = f.readline().strip()
        row_count = sum(1 for _ in f)
    print("CSV header:", header)
    print("Data rows:", row_count)


with DAG(
    dag_id="00_environment_check",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup"],
) as dag:
    t_mysql = PythonOperator(task_id="check_mysql", python_callable=check_mysql)
    t_pg = PythonOperator(task_id="check_postgres", python_callable=check_postgres)
    t_csv = PythonOperator(task_id="check_csv", python_callable=check_csv)

    [t_mysql, t_pg, t_csv]
