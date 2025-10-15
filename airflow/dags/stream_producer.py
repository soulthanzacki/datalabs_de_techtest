from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
from scripts.data_publisher import publish

with DAG(
    dag_id="stream_producer",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    publish_task = PythonOperator(
        task_id=f"publish",
        python_callable=publish
    )

    start >> publish_task >> end
