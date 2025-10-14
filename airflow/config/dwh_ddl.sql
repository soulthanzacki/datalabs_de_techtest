CREATE TABLE IF NOT EXISTS gcpl-469311.datalabs_techtest.fact_sales (
        transaction_id INT64,
        customer_id INT64,
        product_id INT64,
        quantity INT64,
        price FLOAT64
    );

CREATE TABLE IF NOT EXISTS gcpl-469311.datalabs_techtest.dim_customers (
        customer_id INT64,
        name STRING,
        email STRING,
        city STRING,
        signup_date DATE
    );

CREATE TABLE IF NOT EXISTS gcpl-469311.datalabs_techtest.dim_products (
        product_id INT64,
        product_name STRING,
        category STRING,
        price FLOAT64,
    );

CREATE TABLE IF NOT EXISTS gcpl-469311.datalabs_techtest.dim_dates (
        transaction_id INT64,
        transaction_date DATE
    );

CREATE TABLE IF NOT EXISTS gcpl-469311.datalabs_techtest.dim_campaigns (
        campaign_id INT64,
        campaign_name STRING,
        start_date DATE,
        end_date DATE,
        channel STRING
    );