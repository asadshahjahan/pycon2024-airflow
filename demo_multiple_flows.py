from datetime import datetime
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo_multiple_flows", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    def print_current_time():
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.warning(f"Current time: {current_time}")

    current_time = PythonOperator(
        task_id='print_current_time',
        python_callable=print_current_time,
        dag=dag,
    )

    @task()
    def airflow():
        print("airflow")

    # Set dependencies between tasks
    hello >> current_time >> airflow()
