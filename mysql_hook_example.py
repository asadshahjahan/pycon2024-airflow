from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
}

# Instantiate the DAG
dag = DAG('example_hooks_dag', default_args=default_args, schedule_interval=None)


# Function to execute SQL query using MySqlHook
def execute_sql_query():
    # Instantiate the MySqlHook
    mysql_hook = MySqlHook(mysql_conn_id='local_mysql')
    # Assuming you have defined a connection named 'my_mysql_connection' in Airflow UI

    # Define the SQL query
    sql_query = "SELECT * FROM pycon_db_1.weather_data;"

    # Execute the SQL query
    result = mysql_hook.get_records(sql_query)
    print("Query Result:", result)


# Define the task
execute_sql_task = PythonOperator(
    task_id='execute_sql_task',
    python_callable=execute_sql_query,
    dag=dag,
)

# Define the task dependencies
execute_sql_task
