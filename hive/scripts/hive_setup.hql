CREATE DATABASE IF NOT EXISTS commerce_dw;
USE commerce_dw;

CREATE EXTERNAL TABLE IF NOT EXISTS dim_customer (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
)
STORED AS PARQUET
LOCATION '/user/hadoop/commerce_gold/dim_customer';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_seller (
    seller_id STRING,
    seller_zip_code_prefix STRING,
    seller_city STRING,
    seller_state STRING
)
STORED AS PARQUET
LOCATION '/user/hadoop/commerce_gold/dim_seller';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
    date_key INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    day INT,
    day_of_week INT,
    day_name STRING,
    is_weekend BOOLEAN
)
STORED AS PARQUET
LOCATION '/user/hadoop/commerce_gold/dim_date';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_logistics (
    order_id STRING,
    customer_id STRING,
    seller_id STRING,
    order_date_key INT,
    delivery_date_key INT,
    delivery_time_delta_days INT,
    is_late_delivery INT,
    order_to_delivery_days INT,
    approval_to_ship_days INT,
    shipping_cost DOUBLE,
    total_order_price DOUBLE,
    items_count INT
)
STORED AS PARQUET
LOCATION '/user/hadoop/commerce_gold/fact_logistics';
