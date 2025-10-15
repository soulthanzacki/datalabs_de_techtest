from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
from scripts.data_subscriber import main

with DAG(
    dag_id="stream_consumer",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    subscribe_task = PythonOperator(
        task_id=f"subscribe",
        python_callable=main
    )

    start >> subscribe_task >> end