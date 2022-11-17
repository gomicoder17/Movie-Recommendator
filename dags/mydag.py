from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.bash import BashOperator
import recommend


def brach_if_no_input(ti):
    if ti.xcom_pull(task_ids="look_for_new_input")[0] == True:
        return "parse_input"
    else:
        return "end"


def end():
    print("----No new input found. Exiting.----")


def delete_logs():
    print("Deleting logs...")
    import os
    import shutil

    shutil.rmtree("logs/")
    if not os.path.exists("logs/"):
        os.makedirs("logs/")


with DAG(
    "my_dag",
    start_date=datetime(2021, 1, 1),
    schedule_interval=timedelta(minutes=1),
    catchup=False,
) as dag:

    look_for_new_input_task = PythonOperator(
        task_id="look_for_new_input",
        python_callable=recommend.look_for_new_input,
    )

    parse_input_task = PythonOperator(
        task_id="parse_input",
        python_callable=recommend.parse_input,
        provide_context=True,
    )

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=recommend.extract,
        trigger_rule="none_failed_or_skipped",
    )

    filter_by_type_task = PythonOperator(
        task_id="filter_by_type",
        python_callable=recommend.filter_by_type,
        provide_context=True,
    )
    search_title_id_task = PythonOperator(
        task_id="search_title_id",
        python_callable=recommend.search_title_id,
        provide_context=True,
    )

    get_recommendations_task = PythonOperator(
        task_id="get_recommendations",
        python_callable=recommend.get_recommendations,
        provide_context=True,
    )

    write_recs_to_file_task = PythonOperator(
        task_id="write_recs_to_file",
        python_callable=recommend.write_recs_to_file,
        provide_context=True,
    )

    brach_if_no_input = BranchPythonOperator(
        task_id="brach_if_no_input",
        python_callable=brach_if_no_input,
        provide_context=True,
    )

    end_task = ShortCircuitOperator(
        task_id="end",
        python_callable=end,
    )

    (
        look_for_new_input_task
        >> brach_if_no_input
        >> [end_task, parse_input_task]
        >> extract_task
        >> filter_by_type_task
        >> search_title_id_task
        >> get_recommendations_task
        >> write_recs_to_file_task,
    )

with DAG(
    "delete_logs",
    start_date=datetime(2021, 1, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
) as dag:
    delete_logs = PythonOperator(
        task_id="delete_logs",
        python_callable=delete_logs,
    )
    delete_logs
