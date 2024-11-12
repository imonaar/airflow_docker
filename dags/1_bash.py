from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {"owner": "monari", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="first_v5",
    description="This is my first DAG",
    start_date=datetime(2024, 11, 10, 5),
    schedule_interval="@daily",
    default_args=default_args,
) as dag:
    task1 = BashOperator(
        task_id="first_task", bash_command="echo Hello World this is the first task"
    )

    task2 = BashOperator(
        task_id="second_task",
        bash_command="echo I am the second task and i will be running after task 1",
    )

    task3 = BashOperator(
        task_id="third_task",
        bash_command="echo I am the third task and i will be running after task 1 with task 2",
    )

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # task1 >> task2
    # task1 >> task3

    task1 >> [task2, task3]
