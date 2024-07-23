from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

"""dotenv_path = "../.env"
load_dotenv(dotenv_path)

DBT_DREMIO_DIR = os.getenv('DBT_DREMIO_DIR')"""
DBT_DREMIO_DIR = '/Users/frederic/Desktop/BI_MODELING/dbt/bi_modeling_dremio/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 20, 12, 40),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_source_freshness_check',
    default_args=default_args,
    description='A DAG to check the freshness of dbt sources tables',
    schedule_interval='40 * * * *'
)

check_freshness = BashOperator(
    task_id='check_sales_freshness',
    bash_command=f'dbt source freshness --profiles-dir {DBT_DREMIO_DIR} --project-dir {DBT_DREMIO_DIR}',
    dag=dag,
)

