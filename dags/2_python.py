from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance

from datetime import datetime, timedelta

default_args = {"owner": "monari", "retries": 5, "retry_delay": timedelta(minutes=2)}


def get_name(ti: TaskInstance) -> str:
    ti.xcom_push(key="first_name", value="Kevin")
    ti.xcom_push(key="last_name", value="Monari")


def get_age(ti: TaskInstance) -> str:
    ti.xcom_push(key="age", value=31)


def greet(ti: TaskInstance) -> None:
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")

    print(
        f"Hello World my name is {first_name} {last_name} and my age is {age} years old."
    )


with DAG(
    default_args=default_args,
    dag_id="python_dag_v6",
    description="Python DAG",
    start_date=datetime(2024, 11, 10),
    schedule_interval="@daily",
) as dag:

    task1 = PythonOperator(task_id="greet", python_callable=greet)

    task2 = PythonOperator(task_id="get_name", python_callable=get_name)
    task3 = PythonOperator(task_id="get_age", python_callable=get_age)

    [task2, task3] >> task1
