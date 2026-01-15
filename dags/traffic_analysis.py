# import packages

# standard packages
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import tempfile

# airflow packages
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.email import send_email
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# =============================================================================
# Configuration - uses env vars with robust None handling
# =============================================================================

# FIX: Use 'or' to handle empty string AND None cases
ALERT_EMAIL = 'joegilldata@gmail.com'

# Get the bucket from the environemnt to ensure that locally we use dev bucket
# and deployments use prod bucket
GCS_BUCKET = os.environ.get('GCS_BUCKET')
GCS_DATA_PREFIX = 'data'

# Temp directory for intermediate files
TEMP_DIR = '/tmp/airflow_traffic'

# =============================================================================
# Default args - sanitized email list to prevent None values
# =============================================================================

# FIX: Filter out any None/empty values from email list
_email_list = [e for e in [ALERT_EMAIL] if e and isinstance(e, str)]

default_args = {
  'owner': 'airflow',
  'retries': 0,
  'email': _email_list if _email_list else ['joegilldata@gmail.com'],  # Fallback
  'email_on_failure': True,
  'email_on_retry': False,
}

# =============================================================================
# DAG definition
# =============================================================================

dag = DAG(
  dag_id="traffic_analysis",
  default_args=default_args,
  start_date=datetime(2026, 1, 1),
  schedule='0 0 * * *',  # Run at midnight everyday
  catchup=False,
  tags=['traffic', 'etl'],
)

# =============================================================================
# Helper functions for GCS operations
# =============================================================================

def ensure_temp_dir():
  """Ensure temp directory exists."""
  os.makedirs(TEMP_DIR, exist_ok=True)

def download_from_gcs(bucket: str, object_name: str, local_path: str) -> str:
  """Download file from GCS to local path."""
  hook = GCSHook()
  hook.download(
    bucket_name=bucket,
    object_name=object_name,
    filename=local_path,
  )
  return local_path

def upload_to_gcs(local_path: str, bucket: str, object_name: str) -> str:
  """Upload file from local path to GCS."""
  hook = GCSHook()
  hook.upload(
    bucket_name=bucket,
    object_name=object_name,
    filename=local_path,
  )
  return f"gs://{bucket}/{object_name}"

def get_gcs_uri(bucket: str, object_name: str) -> str:
  """Build GCS URI."""
  return f"gs://{bucket}/{object_name}"

# =============================================================================
# Extract functions
# =============================================================================

def read_traffic_data(**kwargs):
  """Read traffic data from GCS and save processed data back to GCS."""
  ensure_temp_dir()

  ti = kwargs['ti']
  run_id = kwargs['run_id'].replace(':', '_').replace('+', '_')  # Sanitize for filename

  # Download source CSV from GCS
  source_object = f"{GCS_DATA_PREFIX}/traffic_data.csv"
  local_source = os.path.join(TEMP_DIR, 'traffic_data.csv')

  download_from_gcs(GCS_BUCKET, source_object, local_source)

  # Load and process data
  df = pd.read_csv(local_source)
  df['bf_date'] = pd.to_datetime(df['bf_date'])
  df['bf_time'] = pd.to_datetime(df['bf_time'], format='%H:%M:%S').dt.time
  df['hour'] = df['bf_time'].apply(lambda x: x.hour)
  df['is_am'] = df['hour'] < 12

  # Save processed data to GCS (avoid XCom for large DataFrames)
  local_output = os.path.join(TEMP_DIR, f'traffic_processed_{run_id}.parquet')
  df.to_parquet(local_output, index=False)

  output_object = f"{GCS_DATA_PREFIX}/intermediate/traffic_processed_{run_id}.parquet"
  gcs_path = upload_to_gcs(local_output, GCS_BUCKET, output_object)

  # Push only the GCS path to XCom (small string, not DataFrame)
  ti.xcom_push(key='processed_data_path', value=gcs_path)
  ti.xcom_push(key='processed_object', value=output_object)

  # Cleanup local file
  os.remove(local_source)
  os.remove(local_output)

# =============================================================================
# Transform functions
# =============================================================================

def filter_ips(**kwargs):
  """Filter low-traffic IPs and save to GCS."""
  ensure_temp_dir()

  ti = kwargs['ti']
  run_id = kwargs['run_id'].replace(':', '_').replace('+', '_')

  # Get path from upstream task
  processed_object = ti.xcom_pull(task_ids='read_traffic_data', key='processed_object')

  # Download processed data
  local_input = os.path.join(TEMP_DIR, 'traffic_processed.parquet')
  download_from_gcs(GCS_BUCKET, processed_object, local_input)

  df = pd.read_parquet(local_input)

  # Aggregate and filter
  ip_traffic = df.groupby('ip')['gbps'].sum().reset_index()
  threshold = ip_traffic['gbps'].quantile(0.20)
  high_ips = ip_traffic[ip_traffic['gbps'] > threshold]['ip'].tolist()
  df_filtered = df[df['ip'].isin(high_ips)]

  # Save filtered data to GCS
  local_output = os.path.join(TEMP_DIR, f'traffic_filtered_{run_id}.parquet')
  df_filtered.to_parquet(local_output, index=False)

  output_object = f"{GCS_DATA_PREFIX}/intermediate/traffic_filtered_{run_id}.parquet"
  upload_to_gcs(local_output, GCS_BUCKET, output_object)

  ti.xcom_push(key='filtered_object', value=output_object)

  # Cleanup
  os.remove(local_input)
  os.remove(local_output)

def filter_am(**kwargs):
  """Filter AM observations and save to GCS."""
  ensure_temp_dir()

  ti = kwargs['ti']
  run_id = kwargs['run_id'].replace(':', '_').replace('+', '_')

  filtered_object = ti.xcom_pull(task_ids='filter_ips', key='filtered_object')

  local_input = os.path.join(TEMP_DIR, f"traffic_filtered_{run_id}_am.parquet")
  download_from_gcs(GCS_BUCKET, filtered_object, local_input)

  df = pd.read_parquet(local_input)
  df_am = df[df['is_am']]

  local_output = os.path.join(TEMP_DIR, f'traffic_am_{run_id}.parquet')
  df_am.to_parquet(local_output, index=False)

  output_object = f"{GCS_DATA_PREFIX}/intermediate/traffic_am_{run_id}.parquet"
  upload_to_gcs(local_output, GCS_BUCKET, output_object)

  ti.xcom_push(key='am_object', value=output_object)

  os.remove(local_input)
  os.remove(local_output)

def filter_pm(**kwargs):
  """Filter PM observations and save to GCS."""
  ensure_temp_dir()

  ti = kwargs['ti']
  run_id = kwargs['run_id'].replace(':', '_').replace('+', '_')

  filtered_object = ti.xcom_pull(task_ids='filter_ips', key='filtered_object')

  local_input = os.path.join(TEMP_DIR, f"traffic_filtered_{run_id}_pm.parquet")
  download_from_gcs(GCS_BUCKET, filtered_object, local_input)

  df = pd.read_parquet(local_input)
  df_pm = df[~df['is_am']]

  local_output = os.path.join(TEMP_DIR, f'traffic_pm_{run_id}.parquet')
  df_pm.to_parquet(local_output, index=False)

  output_object = f"{GCS_DATA_PREFIX}/intermediate/traffic_pm_{run_id}.parquet"
  upload_to_gcs(local_output, GCS_BUCKET, output_object)

  ti.xcom_push(key='pm_object', value=output_object)

  os.remove(local_input)
  os.remove(local_output)

# =============================================================================
# Load / reporting functions
# =============================================================================

def check_day_of_week(**kwargs):
  """Branch based on weekday vs weekend."""
  today = datetime.now()
  if today.weekday() < 5:
    return "do_nothing_am"
  else:
    return "send_email_am"

def send_email_am_fn(**kwargs):
  """Send AM traffic report email."""
  ensure_temp_dir()

  ti = kwargs['ti']
  am_object = ti.xcom_pull(task_ids='filter_am', key='am_object')

  local_input = os.path.join(TEMP_DIR, 'traffic_am.parquet')
  download_from_gcs(GCS_BUCKET, am_object, local_input)

  df = pd.read_parquet(local_input)
  today = datetime.now().date()

  ip_traffic = df.groupby('ip')['gbps'].sum().reset_index()
  ip_traffic_sorted = ip_traffic.sort_values(by='gbps', ascending=False)
  top_ips = ip_traffic_sorted.head(3)['ip'].tolist()

  if len(top_ips) >= 3:
    content = f"Top 3 IPs with most AM traffic on {today}: {top_ips[0]}, {top_ips[1]}, {top_ips[2]}"
  else:
    content = f"Top IPs with most AM traffic on {today}: {', '.join(top_ips)}"

  send_email(
    to=ALERT_EMAIL,
    subject='Top 3 Traffic IPs (AM Branch)',
    html_content=content
  )

  os.remove(local_input)

def send_email_pm_fn(**kwargs):
  """Send PM traffic report email."""
  ensure_temp_dir()

  ti = kwargs['ti']
  pm_object = ti.xcom_pull(task_ids='filter_pm', key='pm_object')

  local_input = os.path.join(TEMP_DIR, 'traffic_pm.parquet')
  download_from_gcs(GCS_BUCKET, pm_object, local_input)

  df = pd.read_parquet(local_input)
  today = datetime.now().date()

  ip_traffic = df.groupby('ip')['gbps'].sum().reset_index()
  ip_traffic_sorted = ip_traffic.sort_values(by='gbps', ascending=False)
  top_ips = ip_traffic_sorted.head(3)['ip'].tolist()

  if len(top_ips) >= 3:
    content = f"Top 3 IPs with most PM traffic on {today}: {top_ips[0]}, {top_ips[1]}, {top_ips[2]}"
  else:
    content = f"Top IPs with most PM traffic on {today}: {', '.join(top_ips)}"

  send_email(
    to=ALERT_EMAIL,
    subject='Top 3 Traffic IPs (PM Branch)',
    html_content=content
  )

  os.remove(local_input)

# =============================================================================
# Task definitions
# =============================================================================

read_traffic_data_task = PythonOperator(
  task_id='read_traffic_data',
  python_callable=read_traffic_data,
  dag=dag,
)

filter_ips_task = PythonOperator(
  task_id='filter_ips',
  python_callable=filter_ips,
  dag=dag,
)

split_am_pm = EmptyOperator(
  task_id='split_am_pm',
  dag=dag,
)

filter_am_task = PythonOperator(
  task_id='filter_am',
  python_callable=filter_am,
  dag=dag,
)

filter_pm_task = PythonOperator(
  task_id='filter_pm',
  python_callable=filter_pm,
  dag=dag,
)

day_of_week_task = BranchPythonOperator(
  task_id='day_of_week',
  python_callable=check_day_of_week,
  dag=dag,
)

send_email_am_task = PythonOperator(
  task_id='send_email_am',
  python_callable=send_email_am_fn,
  dag=dag,
)

do_nothing_am = EmptyOperator(
  task_id='do_nothing_am',
  dag=dag,
)

send_email_pm_task = PythonOperator(
  task_id='send_email_pm',
  python_callable=send_email_pm_fn,
  dag=dag,
)

# =============================================================================
# Dependencies
# =============================================================================

read_traffic_data_task >> filter_ips_task >> split_am_pm >> [filter_am_task, filter_pm_task]
filter_pm_task >> send_email_pm_task
filter_am_task >> day_of_week_task
day_of_week_task >> [do_nothing_am, send_email_am_task]
