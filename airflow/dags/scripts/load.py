import logging

def load(**kwargs):
    import os
    from google.cloud import bigquery

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/config/credentials.json"
    table_name = kwargs["table_name"]

    client = bigquery.Client()

    load_job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.enums.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.enums.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1
    )

    table_id = f"gcpl-469311.datalabs_techtest.stg_{table_name}"
    with open(f"/tmp/{table_name}.csv", "rb") as source_file:
        job = client.load_table_from_file(
            file_obj=source_file,
            destination=table_id,
            job_config=load_job_config
        )

    job.result()
    table = client.get_table(table_id)
    logging.info("Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    ))