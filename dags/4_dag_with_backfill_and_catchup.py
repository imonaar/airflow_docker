from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.taskinstance import TaskInstance
from airflow.decorators import dag, task

from datetime import datetime, timedelta

default_args = {"owner": "monari", "retries": 5, "retry_delay": timedelta(minutes=2)}


with DAG(
    dag_id="dag_with_catchup_and_backfill_v1",
    start_date=datetime(2024, 11, 1),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
) as dag:
    task1 = BashOperator(
        task_id="first_task", bash_command="echo Hello World this is the first task"
    )