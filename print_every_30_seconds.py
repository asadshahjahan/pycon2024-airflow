import logging
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 7),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}


def print_current_time():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.warning(f"Current time: {current_time}")


dag = DAG(
    'print_time_every_30_seconds',
    default_args=default_args,
    description='A DAG that prints current time every 30 seconds',
    schedule_interval=timedelta(seconds=30),
)

print_time_task = PythonOperator(
    task_id='print_current_time',
    python_callable=print_current_time,
    dag=dag,
)

print_time_task
