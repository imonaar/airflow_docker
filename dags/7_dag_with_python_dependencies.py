from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance

from datetime import datetime, timedelta

default_args = {"owner": "monari", "retries": 5, "retry_delay": timedelta(minutes=2)}


def get_sklearn():
    import sklearn

    print(f"sk-learn with version: {sklearn.__version__}")


with DAG(
    default_args=default_args,
    dag_id="dag_with_python_dependencies_v1",
    description="Python depenencies DAG",
    start_date=datetime(2024, 11, 11),
    schedule_interval="@daily",
) as dag:
    task = PythonOperator(task_id="get_sklearn", python_callable=get_sklearn)
