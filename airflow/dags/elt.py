from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import json
from scripts.extract import extract
from scripts.load import load
from scripts.transform import transform

with DAG(
    dag_id="elt-pipeline",
    schedule="@daily",
    start_date=datetime(2025, 9, 1),
    catchup=True,
    max_active_runs=1,
) as dag:

    with open("/opt/airflow/config/tables_to_extract.json") as tbls:
        config = json.load(tbls)
        tables = config["tables"]

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    cleanup_task = BashOperator(
        task_id=f"cleanup",
        bash_command="rm -rf /tmp/*.csv",
        trigger_rule="all_done"
    )

    with TaskGroup("extract_load_group", tooltip="Extract and Load Tasks") as extract_load_group:
        for t in tables:
            table = t["name"]
            date = t.get("date")

            if table == 'customers':
                query = f"SELECT * FROM {table} WHERE {date} = '{{{{ macros.ds_format(ds, '%Y-%m-%d', '%m/%d/%Y') }}}}'"
            
            else:
                if date:
                    query = f"SELECT * FROM {table} WHERE {date} = '{{{{ ds }}}}'"

                else:
                    query = f"SELECT * FROM {table}"

            extract_task = PythonOperator(
                task_id=f"extract_{table}",
                python_callable=extract,
                op_kwargs={
                    "table_name": table,
                    "query": query,
                },
            )

            load_task = PythonOperator(
                task_id=f"load_{table}",
                python_callable=load,
                op_kwargs={
                    "table_name": table,
                },
            )

            extract_task >> load_task

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    start >> extract_load_group >> cleanup_task
    cleanup_task >> transform_task >> end
