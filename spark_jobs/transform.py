from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
from pyspark.sql.functions import col, to_timestamp, sum


# Load env
load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
mongo_db = os.getenv("MONGO_DB")

# Combine into full URI
mongo_connection = f"{mongo_uri}{mongo_db}"

# Minio credentials
minio_endpoint  = os.getenv("MINIO_ENDPOINT") 
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")

spark = SparkSession.builder \
    .appName("Olist Transformation") \
    .config("spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367") \
    \
    .config("spark.mongodb.read.connection.uri", mongo_connection) \
    .config("spark.mongodb.write.connection.uri", mongo_connection) \
    \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# Define Schema
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
])

order_items_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
])

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
])

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

payments_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_value", DoubleType(), True),
])

# Read collections
orders = spark.read.format("mongodb") \
    .option("collection", "orders") \
    .schema(orders_schema) \
    .load()

order_items = spark.read.format("mongodb") \
    .option("collection", "order_items") \
    .schema(order_items_schema) \
    .load()

products = spark.read.format("mongodb") \
    .option("collection", "products") \
    .schema(products_schema) \
    .load()

customers = spark.read.format("mongodb") \
    .option("collection", "customers") \
    .schema(customers_schema) \
    .load()

payments = spark.read.format("mongodb") \
    .option("collection", "payments") \
    .schema(payments_schema) \
    .load()

# Select columns
orders = orders.select("order_id", "customer_id", "order_purchase_timestamp")
order_items = order_items.select("order_id", "product_id", "price")
products = products.select("product_id", "product_category_name")
customers = customers.select("customer_id", "customer_city", "customer_state")
payments = payments.select("order_id", "payment_value")

# Aggregate payment
payments_agg = payments.groupBy("order_id") \
    .agg(sum("payment_value").alias("payment_value"))

# Join tables
df = orders \
    .join(order_items, "order_id") \
    .join(products, "product_id") \
    .join(customers, "customer_id") \
    .join(payments_agg, "order_id")

# Transform
df_clean = orders \
    .join(order_items, "order_id") \
    .join(products, "product_id") \
    .join(customers, "customer_id") \
    .join(payments_agg, "order_id") \
    .withColumn("order_date", to_timestamp(col("order_purchase_timestamp"))) \
    .drop("order_purchase_timestamp")

# Save to MinIO (Silver Layer)
df_clean.write \
    .mode("overwrite") \
    .parquet("s3a://olist-data/silver/fact_orders")

print("Silver data saved to MinIO")