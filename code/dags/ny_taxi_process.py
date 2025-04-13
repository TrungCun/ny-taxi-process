from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import configparser


AWS_S3_CONN_ID = "aws_credentials"

dag = DAG(
  'ny_taxi_process_10',
  description = 'pipeline process data ny taxi',
  start_date = datetime(2025, 4, 12),
  schedule_interval = '@daily',
  tags = ['ny_taxi']
)

def start_ny_taxi_process():
    hook = S3Hook(aws_conn_id=AWS_S3_CONN_ID)
    bucket = 'ny-taxi-bucket-s3-1744490641188763700'
    prefix = 'unprocessed_reports'
    print(bucket, prefix)
    keys = hook.list_keys(bucket, prefix=prefix)
    print('list')
    print(keys)


start_ny_taxi_process_task = PythonOperator(
  task_id='start_ny_taxi_process',
  python_callable=start_ny_taxi_process,
  dag=dag
)

start_ny_taxi_process_task