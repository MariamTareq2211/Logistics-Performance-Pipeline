from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark Session (Required for independent execution)
spark = SparkSession.builder.appName("OlistProject_Gold").master("local[*]").getOrCreate()
sc = spark.sparkContext

silver_path = "/user/hadoop/commerce_silver"
gold_path = "/user/hadoop/commerce_gold"

# ==========================================
# DIMENSIONS MODELING
# ==========================================

# Dim Customer
customers_silver = spark.read.parquet(f"{silver_path}/customers")
dim_customer = customers_silver
dim_customer.write.mode("overwrite").parquet(f"{gold_path}/dim_customer")

# Dim Seller
sellers_silver = spark.read.parquet(f"{silver_path}/sellers")
dim_seller = sellers_silver
dim_seller.write.mode("overwrite").parquet(f"{gold_path}/dim_seller")

# Dim Date
orders_silver = spark.read.parquet(f"{silver_path}/orders")

dates = orders_silver.select(
    min(least(col("order_purchase_timestamp"), col("order_delivered_customer_date"))).alias("start_date"),
    max(greatest(col("order_purchase_timestamp"), col("order_delivered_customer_date"))).alias("end_date")
).first()

base_dates = spark.sql(f"""
    SELECT explode(
        sequence(to_date('{dates["start_date"]}'), to_date('{dates["end_date"]}'), interval 1 day)
    ) as full_date
""")

dim_date = base_dates.select(
    date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"), 
    col("full_date"),
    year("full_date").alias("year"),
    quarter("full_date").alias("quarter"),
    month("full_date").alias("month"),
    date_format("full_date", "MMMM").alias("month_name"),
    dayofmonth("full_date").alias("day"),
    dayofweek("full_date").alias("day_of_week"), 
    date_format("full_date", "EEEE").alias("day_name"),   
    expr("dayofweek(full_date) IN (1, 7)").alias("is_weekend")
)
dim_date.write.mode("overwrite").parquet(f"{gold_path}/dim_date")

# ==========================================
# FACT TABLE MODELING
# ==========================================

order_items_silver = spark.read.parquet(f"{silver_path}/order_items")

aggregated_orders = orders_silver.join(
    order_items_silver, on="order_id", how="inner"
).filter(
    (col("order_status") == "delivered") & col("order_delivered_customer_date").isNotNull()
).groupBy(
    "order_id", "customer_id", "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date",
    "delivery_days", "is_late"
).agg(
    sum("freight_value").alias("shipping_cost"), 
    sum("price").alias("total_order_price"),
    count("order_item_id").alias("items_count"),
    expr("max_by(seller_id, freight_value)").alias("seller_id")
)

fact_logistics = aggregated_orders.join(
    dim_customer, on="customer_id", how="inner"
).join(
    dim_seller, on="seller_id", how="inner"
).select(
    "order_id",
    col("customer_id"),
    col("seller_id"),
    date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("order_date_key"),
    date_format("order_delivered_customer_date", "yyyyMMdd").cast("int").alias("delivery_date_key"),
    datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")).alias("delivery_time_delta_days"),
    col("is_late").alias("is_late_delivery"),
    col("delivery_days").alias("order_to_delivery_days"),
    datediff(col("order_delivered_carrier_date"), col("order_approved_at")).alias("approval_to_ship_days"),
    "shipping_cost",
    "total_order_price",
    "items_count"
)

fact_logistics.write.mode("overwrite").parquet(f"{gold_path}/fact_logistics")

spark.stop()
