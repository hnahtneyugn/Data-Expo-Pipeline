from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello from Airflow!")

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2025, 8, 14),
    schedule=None,  # run manually for now
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world
    )