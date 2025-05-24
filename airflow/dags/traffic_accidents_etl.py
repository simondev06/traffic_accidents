import sys
import os
import time
import pandas as pd
from dotenv import load_dotenv
load_dotenv(dotenv_path="/opt/airflow/.env")
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
utils_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'utils')
sys.path.append(scripts_path)
sys.path.append(utils_path)
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dataset_extract import extract_dataset
from dataset_transform import transform_dataset
from api_extract import extract_api_data
from api_transform import transform_vias_task
from merge import merge_datasets
from load import run_load_task
from confluent_kafka import Producer
from scripts.kafka_producer_task import kafka_producer_task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

dag = DAG(
    dag_id='traffic_accidents_etl',
    default_args=default_args,
    description='ETL completo con merge y carga en modelo dimensional',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
)

extract_dataset = PythonOperator(
    task_id='extract_dataset',
    python_callable=extract_dataset,
    dag=dag
)

transform_dataset = PythonOperator(
    task_id='transform_dataset',
    python_callable=transform_dataset,
    dag=dag
)

extract_api = PythonOperator(
    task_id='extract_api',
    python_callable=extract_api_data,
    dag=dag
)

transform_api = PythonOperator(
    task_id='transform_api',
    python_callable=transform_vias_task,
    dag=dag
)

merge_data = PythonOperator(
    task_id='merge_data',
    python_callable=merge_datasets,
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=run_load_task,
    dag=dag
)

kafka_producer = PythonOperator(
    task_id='kafka_producer',
    python_callable=kafka_producer_task,
    dag=dag,
)

extract_dataset >> transform_dataset
extract_api >> transform_api
[transform_dataset, transform_api] >> merge_data >> load_data >> kafka_producer
