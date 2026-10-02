"""
weather_elt_pipeline
---------------------
A small ELT pipeline:
  1. ensure_tables  -- creates raw/clean tables if they don't exist
  2. extract        -- pulls forecast data from Open-Meteo, writes raw JSON to disk
  3. load           -- loads raw JSON into the raw_weather staging table
  4. transform      -- unpacks raw JSONB into weather_hourly + weather_daily_summary

Runs hourly. Each task is a thin wrapper around a function in scripts/, so the
logic itself is plain, testable Python/SQL independent of Airflow.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

from scripts.extract import extract_all
from scripts.load import load_all
from scripts.transform import run_transform

SQL_DIR = "/opt/airflow/sql"
DB_URI = os.environ["WAREHOUSE_DB_URI"]


def ensure_tables():
    engine = create_engine(DB_URI)
    with open(os.path.join(SQL_DIR, "create_tables.sql")) as f:
        ddl = f.read()
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)


default_args = {
    "owner": "gursimran",
    "retries": 2,
}

with DAG(
    dag_id="weather_elt_pipeline",
    description="Hourly Open-Meteo ELT pipeline: extract -> load -> transform",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "weather", "portfolio"],
) as dag:

    ensure_tables_task = PythonOperator(
        task_id="ensure_tables",
        python_callable=ensure_tables,
    )

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_all,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_all,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    ensure_tables_task >> extract_task >> load_task >> transform_task
