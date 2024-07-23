from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

"""dotenv_path = "../.env"
load_dotenv(dotenv_path)
PROJECT_ROOT_DIR = os.getenv('PROJECT_ROOT_DIR')"""
PROJECT_ROOT_DIR = '/Users/frederic/Desktop/BI_MODELING'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 20, 12, 35),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_sales_loading_ingestion = DAG(
    'daily_loading_and_ingestion_sales',
    default_args=default_args,
    description='A dag that perform the loading of the data from the source API and then ingest it into nessie',
    schedule_interval='35 * * * *',
)

run_loading = BashOperator(
    task_id='run_loading',
    bash_command=f'python3 {PROJECT_ROOT_DIR}/load_sales_data_into_minio.py',
    dag=dag_sales_loading_ingestion,
)

run_ingestion = BashOperator(
    task_id='run_ingestion',
    bash_command=f'python3 {PROJECT_ROOT_DIR}/minio_to_nessie.py',
    dag=dag_sales_loading_ingestion,
)

run_loading >> run_ingestion
