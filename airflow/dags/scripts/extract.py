import logging

def extract(**kwargs):
    import psycopg
    import csv
    from datetime import datetime

    table_name = kwargs["table_name"]
    query = kwargs["query"]

    with psycopg.connect(f"host=db_main port=5433 dbname=supermarket user=postgres password=postgres") as conn:
        with conn.cursor() as cur:
            cur.execute(query=query)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

    with open(f"/tmp/{table_name}.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(data)
    
    logging.info(f"Data written to {table_name}.csv")