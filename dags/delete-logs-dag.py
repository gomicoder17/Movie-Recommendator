from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import recommend


def delete_logs():
    print("Deleting logs...")
    import os
    import shutil

    shutil.rmtree("logs/")
    if not os.path.exists("logs/"):
        os.makedirs("logs/")


with DAG(
    "delete-logs-dag",
    start_date=datetime(2021, 1, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    delete_logs = PythonOperator(
        task_id="delete_logs",
        python_callable=delete_logs,
    )
    delete_logs
