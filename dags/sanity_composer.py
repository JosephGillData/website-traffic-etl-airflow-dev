from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def ping():
  print("PING: task executed")
  return "ok"

default_args = {
  "owner": "airflow",
  "retries": 0,
}

with DAG(
  dag_id="sanity_composer",
  start_date=datetime(2026, 1, 1),
  schedule=None,  # manual trigger only
  catchup=False,
  default_args=default_args
) as dag:
  PythonOperator(
    task_id="ping",
    python_callable=ping,
  )
