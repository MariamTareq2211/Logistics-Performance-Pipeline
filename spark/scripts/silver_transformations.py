from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder.appName("OlistProject").master("local[*]").getOrCreate()
sc = spark.sparkContext

raw_path = "/user/hadoop/commerce_storage"
silver_path = "/user/hadoop/commerce_silver"

# ==========================================
# ORDERS: EDA and Preprocessing
# ==========================================
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True)
])

orders = (
    spark.read
    .option("sep", ",")
    .schema(orders_schema)
    .csv(f"{raw_path}/orders")
)

orders_silver = orders
date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column_name in date_columns:
    orders_silver = orders_silver.withColumn(
        column_name,
        when(
            trim(col(column_name)).isin("null", "", "0000-00-00 00:00:00.0"),
            None
        ).otherwise(col(column_name))
    )

for column_name in date_columns:
    orders_silver = orders_silver.withColumn(
        column_name,
        to_timestamp(col(column_name), "yyyy-MM-dd HH:mm:ss.S")
    )

orders_silver = orders_silver.withColumn(
    "delivery_days",
    datediff(
        col("order_delivered_customer_date"),
        col("order_purchase_timestamp")
    )
)

orders_silver = orders_silver.withColumn(
    "is_late",
    when(
        col("order_delivered_customer_date").isNull(),
        lit(None).cast("integer")
    ).when(
        col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
        lit(1)
    ).otherwise(lit(0))
)


orders_silver.write.mode("overwrite").parquet(f"{silver_path}/orders")


# ==========================================
# CUSTOMERS: Preprocessing
# ==========================================
customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

customers = (
    spark.read
    .option("sep", ",")
    .schema(customers_schema)
    .csv(f"{raw_path}/customers")
)

customers_silver = (
    customers
    .dropDuplicates(["customer_id"])
    .withColumn("customer_city", lower(trim(col("customer_city"))))
    .withColumn("customer_state", upper(trim(col("customer_state"))))
    .withColumn(
        "customer_zip_code_prefix",
        trim(col("customer_zip_code_prefix"))
    )
)

customers_silver.write.mode("overwrite").parquet(f"{silver_path}/customers")


# ==========================================
# SELLERS: Preprocessing
# ==========================================
sellers_schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True)
])

sellers = (
    spark.read
    .option("sep", ",")
    .schema(sellers_schema)
    .csv(f"{raw_path}/sellers")
)

sellers_silver = (
    sellers
    .filter(
        col("seller_id").isNotNull() &
        trim(col("seller_id")).rlike("^[0-9a-fA-F]{32}$")
    )
    .dropDuplicates(["seller_id"])
    .withColumn("seller_city", lower(trim(col("seller_city"))))
    .withColumn("seller_state", upper(trim(col("seller_state"))))
    .withColumn(
        "seller_zip_code_prefix",
        trim(col("seller_zip_code_prefix"))
    )
)

sellers_silver.write.mode("overwrite").parquet(f"{silver_path}/sellers")


# ==========================================
# ORDER ITEMS: Preprocessing
# ==========================================
order_items_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True)
])

order_items = (
    spark.read
    .option("sep", ",")
    .schema(order_items_schema)
    .csv(f"{raw_path}/order_items")
)

order_items_silver = (
    order_items
    .dropDuplicates(["order_id", "order_item_id"])
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("seller_id", trim(col("seller_id")))
    .withColumn(
        "shipping_limit_date",
        to_timestamp(
            col("shipping_limit_date"),
            "yyyy-MM-dd HH:mm:ss.S"
        )
    )
    .withColumn(
        "total_item_value",
        col("price") + col("freight_value")
    )
)

order_items_silver = order_items_silver.withColumn(
    "total_item_value",
    round("total_item_value", 2)
)

order_items_silver.write.mode("overwrite").parquet(f"{silver_path}/order_items")

spark.stop()
