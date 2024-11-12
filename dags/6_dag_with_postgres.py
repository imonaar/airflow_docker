from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.models.taskinstance import TaskInstance
from airflow.decorators import dag, task

from datetime import datetime, timedelta

default_args = {"owner": "monari", "retries": 5, "retry_delay": timedelta(minutes=2)}


with DAG(
    dag_id="dag_with_postgres_operator_v6",
    start_date=datetime(2024, 11, 11),
    schedule_interval="0 0 * * *",
    default_args=default_args,
) as dag:
    task1 = PostgresOperator(
        task_id="first_postgres_task",
        postgres_conn_id="postgres_localhost",
        sql="""
        CREATE TABLE IF NOT EXISTS dag_runs (
            dt date,
            dag_id character varying,
            PRIMARY KEY (dt, dag_id)
        );
        """,
    )

    task2 = PostgresOperator(
        task_id="add_dag_id_to_postgress_database",
        postgres_conn_id="postgres_localhost",
        sql="""
            INSERT INTO dag_runs (dt, dag_id) values ('{{ds}}' , '{{dag.dag_id}}');
        """,
    )

    task3 = PostgresOperator(
        task_id="DELETE_dag_from_database",
        postgres_conn_id="postgres_localhost",
        sql="""
            DELETE FROM dag_runs WHERE dt='{{ds}}' and dag_id='{{dag.dag_id}}'
        """,
    )

    task1 >> task3 >> task2
