from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.decorators import dag, task

from datetime import datetime, timedelta

default_args = {"owner": "monari", "retries": 5, "retry_delay": timedelta(minutes=2)}


@dag(
    default_args=default_args,
    dag_id="DAG_with_task_flow_v2",
    start_date=datetime(2024, 11, 10),
    schedule_interval="@daily",
)
def hello():
    @task(multiple_outputs=True)
    def get_name() -> str:
        return {
            "first_name": "Kevin",
            "last_name": "Kevin",
        }

    @task()
    def get_age() -> int:
        return 19

    @task()
    def greet(first_name: str, last_name: str, age: int) -> None:
        print(
            f"Hello World, my name is {first_name} {last_name} and i am {age} years old."
        )

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict["first_name"], last_name=name_dict["last_name"], age=age)


greet_dag = hello()
