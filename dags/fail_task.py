"""
Minimal failure test DAG to verify email alerts work correctly.
Triggers immediately and fails, sending an email notification.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# FIX: Use 'or' to handle both None and empty string cases
ALERT_EMAIL = os.environ.get('ALERT_EMAIL') or 'joegilldata@gmail.com'

# FIX: Sanitize email list to ensure no None values
_email_list = [e for e in [ALERT_EMAIL] if e and isinstance(e, str)]

def fail_task():
  """Intentionally fail to test email alerts."""
  raise ValueError("Intentional failure to test email alerts")

default_args={
    'owner': 'airflow',
    'retries': 0,
    'email': _email_list if _email_list else ['joegilldata@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
  }

with DAG(
  dag_id="fail_task",
  default_args=default_args,
  start_date=datetime(2026, 1, 1),
  schedule='0 0 * * *',  # Run at midnight everyday
  catchup=False,
  tags=['test', 'email'],
) as dag:

  test_failure = PythonOperator(
    task_id="test_email_alert",
    python_callable=fail_task,
  )
