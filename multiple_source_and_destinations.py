from airflow import DAG

from airflow.models import Variable
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


def read_data():
    # Read connection details from Airflow Connection
    source_conn_id = Variable.get("connection_id_variable_for_pycon_24_1")

    # Establish connection
    source_hook = MySqlHook(mysql_conn_id=source_conn_id)

    source_table = "weather_data"

    # Read data from source
    source_data = source_hook.get_records(sql=f"SELECT * FROM {source_table}")

    return source_data


def process_data(data):
    # Process data (example)
    return [row.upper() for row in data]


def write_data(processed_data):
    # Write connection details from Airflow Connection
    destination_conn_id = Variable.get("connection_id_variable_for_pycon_24_2")

    # Establish connection
    destination_hook = MySqlHook(mysql_conn_id=destination_conn_id)

    destination_table = "weather_data"
    # Write processed data to destination
    destination_hook.insert_rows(table=destination_table, rows=processed_data)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 7),
}

dag = DAG(
    'separate_read_write',
    default_args=default_args,
    description='A DAG for reading and writing data separately',
    schedule_interval=timedelta(minutes=2),
)

read_task = PythonOperator(
    task_id='read_data',
    python_callable=read_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

write_task = PythonOperator(
    task_id='write_data',
    python_callable=write_data,
    provide_context=True,
    dag=dag,
)

read_task >> process_task >> write_task
