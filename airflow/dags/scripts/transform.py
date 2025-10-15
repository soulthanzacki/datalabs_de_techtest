import logging

def fact_sales():
    return """
    INSERT INTO gcpl-469311.datalabs_techtest.fact_sales 
        (transaction_id, customer_id, product_id, quantity, total_price)
    SELECT 
        t.transaction_id,
        t.customer_id,
        ti.product_id,
        ti.quantity,
        ti.price
    FROM gcpl-469311.datalabs_techtest.stg_transactions t 
    JOIN gcpl-469311.datalabs_techtest.stg_transaction_items ti ON t.transaction_id = ti.transaction_id
    WHERE NOT EXISTS (
        SELECT 1 
        FROM gcpl-469311.datalabs_techtest.fact_sales AS target
        WHERE target.transaction_id = t.transaction_id
        AND target.customer_id = t.customer_id
        AND target.product_id = ti.product_id
        AND target.quantity = ti.quantity
        AND target.total_price = ti.price
    );
    """

def dim_customers():
    return """
    MERGE INTO gcpl-469311.datalabs_techtest.dim_customers AS target
    USING (
        SELECT
        c.customer_id ,
        c.name ,
        c.email ,
        c.city ,
        c.signup_date ,
    FROM gcpl-469311.datalabs_techtest.stg_customers c
    ) AS source
    ON target.customer_id = source.customer_id

    WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.email = source.email,
        target.city = source.city,
        target.signup_date = source.signup_date

    WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, signup_date)
    VALUES (source.customer_id, source.name, source.email, source.city, source.signup_date)
    ;
    """

def dim_products():
    return """
    MERGE INTO gcpl-469311.datalabs_techtest.dim_products AS target
    USING (
        SELECT
            p.product_id ,
            p.product_name ,
            p.category ,
            p.price
        FROM gcpl-469311.datalabs_techtest.stg_products p
    ) AS source
    ON target.product_id = source.product_id

    WHEN MATCHED THEN
    UPDATE SET
        target.product_name = source.product_name,
        target.category = source.category,
        target.price = source.price

    WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, price)
    VALUES (source.product_id, source.product_name, source.category, source.price)
    ;
    """

def dim_dates():
    return """
    MERGE INTO gcpl-469311.datalabs_techtest.dim_dates AS target
    USING (
        SELECT
            t.transaction_id ,
            t.transaction_date
        FROM gcpl-469311.datalabs_techtest.stg_transactions t
    ) AS source
    ON target.transaction_id = source.transaction_id

    WHEN MATCHED THEN
    UPDATE SET
        target.transaction_date = source.transaction_date

    WHEN NOT MATCHED THEN
    INSERT (transaction_id, transaction_date)
    VALUES (source.transaction_id, source.transaction_date);
    """

def dim_campaigns():
    return """
    MERGE INTO gcpl-469311.datalabs_techtest.dim_campaigns AS target
    USING (
        SELECT
            mc.campaign_id ,
            mc.campaign_name ,
            mc.start_date ,
            mc.end_date ,
            mc.channel
        FROM gcpl-469311.datalabs_techtest.stg_marketing_campaigns mc
    ) AS source

    ON target.campaign_id = source.campaign_id

    WHEN MATCHED THEN
    UPDATE SET
        target.campaign_name = source.campaign_name ,
        target.start_date = source.start_date ,
        target.end_date = source.end_date ,
        target.channel = source.channel

    WHEN NOT MATCHED THEN
    INSERT (campaign_id, campaign_name, start_date, end_date, channel)
    VALUES (source.campaign_id, source.campaign_name, source.start_date, source.end_date, source.channel);
    """

def transform(**kwargs):
    import os
    from google.cloud import bigquery

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/config/bq-credential.json"

    client = bigquery.Client()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    try:
        dwh_ddl = "/opt/airflow/config/dwh_ddl.sql"
        with open(dwh_ddl, "r") as f:
            ddl_query = f.read()

        logging.info(f"Executing dwh_ddl.sql query:\n{ddl_query}")

        job = client.query(query=ddl_query)

        job.result()
        if job.error_result:
            logging.error(f"Error in running query: {job.error_result}")
        else:
            logging.info(f"query executed successfully.")

        queries = {
            "fact_sales": fact_sales(),
            "dim_customers": dim_customers(),
            "dim_products": dim_products(),
            "dim_dates": dim_dates(),
            "dim_campaigns": dim_campaigns()
        }

        for name, query in queries.items():
                logging.info(f"Executing {name} query:\n{query}")
                job = client.query(query=query) 
                
                job.result() 
                
                if job.error_result:
                    logging.error(f"Error in {name} query: {job.error_result}")
                else:
                    logging.info(f"{name} query executed successfully.")

    except Exception as e:
        logging.error(f"General error occurred: {e}", exc_info=True)