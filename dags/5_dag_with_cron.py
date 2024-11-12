from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.taskinstance import TaskInstance
from airflow.decorators import dag, task

from datetime import datetime, timedelta

default_args = {"owner": "monari", "retries": 5, "retry_delay": timedelta(minutes=2)}


with DAG(
    dag_id="dag_with_cron_expression_v4",
    start_date=datetime(2024, 11, 1),
    schedule_interval="0 3 * * Tue,Fri",
    default_args=default_args,
) as dag:
    task1 = BashOperator(
        task_id="first_task", bash_command="echo Dag with cron expression"
    )
