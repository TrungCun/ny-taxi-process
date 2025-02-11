import logging
import sql_statements as sql_statements
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook




dag = DAG(
  'ny_taxi',
  description = 'pipeline process data ny taxi',
  start_date = datetime.datetime.now(),
  schedule_interval = '@weekly',
  tags = ['ny_taxi'],
)

def data_quality_checks(tables):
  tables = tables.split(',')
  redshift_hook = PostgresHook("redshift")
  for table in tables:
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
      raise ValueError(f"Data quality check failed. {table} returned no results")
    num_records = records[0][0]
    if num_records < 1:
      raise ValueError(f"Data quality check failed. {table} contained 0 rows")
    logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")

def cleaning_stagings():
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.drop_staging
    redshift_hook.run(sql_stmt)
    print(f"Staging tables dropped successfully.")

def loading_table(table):
  redshift_hook = PostgresHook("redshift")

  if table == 'dim_vendor':
    sql_stmt = sql_statements.load_dim_vendor
  elif table == 'dim_storefwd':
    sql_stmt = sql_statements.load_dim_storefwd
  elif table == 'dim_ratecode':
    sql_stmt = sql_statements.load_dim_ratecode
  elif table == 'dim_payment':
    sql_stmt = sql_statements.load_dim_payment
  elif table == 'dim_locations':
    sql_stmt = sql_statements.load_dim_locations
  else:
    sql_stmt = sql_statements.load_fact_rides

  redshift_hook.run(sql_stmt)
  print(f"Table {table} was loaded successfully.")


def create_table(table):
    redshift_hook = PostgresHook("redshift")

    if table == 'dim_vendor':
      sql_stmt = sql_statements.create_dim_vendor
    elif table == 'dim_storefwd':
      sql_stmt = sql_statements.create_dim_storefwd
    elif table == 'dim_ratecode':
      sql_stmt = sql_statements.create_dim_ratecode
    elif table == 'dim_payment':
      sql_stmt = sql_statements.create_dim_payment
    elif table == 'dim_locations':
      sql_stmt = sql_statements.create_dim_locations
    elif table == 'staging_rides':
      sql_stmt = sql_statements.create_staging_rides
    elif table == 'staging_locations':
      sql_stmt = sql_statements.create_staging_locations
    else:
      sql_stmt = sql_statements.create_fact_rides

    redshift_hook.run(sql_stmt)
    print(f"Table {table} was created successfully.")


def staging_rides_to_redshift(*args, **kwargs):
    aws_hook = AwsHook(aws_conn_id ="aws_credentials", client_type ='s3')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    bucket = str(Variable.get('s3_bucket'))
    sql_stmt = sql_statements.COPY_ALL_RIDES_SQL.format(
        bucket,
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)
    print(f"Table staging_rides was loaded successfully.")

def staging_locations_to_redshift(*args, **kwargs):
    aws_hook = AwsHook(aws_conn_id ="aws_credentials", client_type ='s3')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    bucket = str(Variable.get('s3_bucket'))
    sql_stmt = sql_statements.COPY_ALL_LOCATIONS_ITEMS_SQL.format(
        bucket,
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)
    print(f"Table staging_locations was loaded successfully.")

running_cleaning_task = PythonOperator(
  task_id='cleaning_staging',
  dag=dag,
  python_callable=cleaning_stagings,
)

data_quality_checks_task = PythonOperator(
  task_id='data_quality_checks',
  dag=dag,
  python_callable=data_quality_checks,
  op_kwargs={
    'tables': 'fact_locations,fact_rides',
  }
)

loading_fact_rides_task = PythonOperator(
    task_id='loading_fact_rides',
    dag=dag,
    op_kwargs={'table': 'fact_rides'},
    python_callable=loading_table,
)

loading_dim_vendor_task = PythonOperator(
  task_id = 'loading_dim_vendor',
  dag = dag,
  op_kwargs = {'table': 'dim_vendor'},
  python_callable = loading_table,
)

loading_dim_storefwd_task = PythonOperator(
  task_id = 'loading_dim_storefwd',
  dag = dag,
  op_kwargs = {'table': 'dim_storefwd'},
  python_callable = loading_table,
)

loading_dim_ratecode_task = PythonOperator(
  task_id = 'loading_dim_ratecode',
  dag = dag,
  op_kwargs = {'table': 'dim_ratecoded'},
  python_callable = loading_table,
)

loading_dim_payment_task = PythonOperator(
  task_id = 'loading_dim_payment',
  dag = dag,
  op_kwargs = {'table': 'dim_payment'},
  python_callable = loading_table,
)

loading_dim_locations_task = PythonOperator(
  task_id = 'loading_dim_locations',
  dag = dag,
  op_kwargs = {'table': 'dim_locations'},
  python_callable = loading_table,
)

creating_fact_rides_task = PythonOperator(
    task_id='creating_fact_rides',
    dag=dag,
    op_kwargs={'table': 'fact_rides'},
    python_callable=create_table,
)

creating_dim_vendor_task = PythonOperator(
  task_id = 'creating_dim_vendor',
  dag = dag,
  op_kwargs = {'table': 'dim_vendor'},
  python_callable = create_table,
)

creating_dim_storefwd_task = PythonOperator(
  task_id = 'creating_dim_storefwd',
  dag = dag,
  op_kwargs = {'table': 'dim_storefwd'},
  python_callable = create_table,
)

creating_dim_ratecode_task = PythonOperator(
  task_id = 'creating_dim_ratecode',
  dag = dag,
  op_kwargs = {'table': 'dim_ratecoded'},
  python_callable = create_table,
)

creating_dim_payment_task = PythonOperator(
  task_id = 'creaing_dim_payment',
  dag = dag,
  op_kwargs = {'table': 'dim_payment'},
  python_callable = create_table,
)

creating_dim_locations_task = PythonOperator(
  task_id = 'creating_dim_locations',
  dag = dag,
  op_kwargs = {'table': 'dim_locations'},
  python_callable = create_table,
)

creating_staging_locations_task = PythonOperator(
  task_id = 'creating_staging_locations',
  dag = dag,
  op_kwargs = {'table': 'staging_locations'},
  python_callable = create_table,
)

creating_staging_rides_task = PythonOperator(
  task_id = 'creating_staging_rides',
  dag = dag,
  op_kwargs = {'table': 'staging_rides'},
  python_callable = create_table,
)


fact_tables_ready_task = DummyOperator(
  task_id = 'fact_tables_ready'
)

dimensions_tables_ready_task = DummyOperator(
  task_id = 'dimension_tables_ready'
)

tables_created_redshift_task = DummyOperator(
  task_id = 'tables_created_in_redshift'
)

staging_redshift_ready_task = DummyOperator(
  task_id = 's3_to_redshift_ready'
)

s3_receipts_ready_task = DummyOperator(
  task_id = 's3_receipts_ready'
)