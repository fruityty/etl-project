from dotenv import load_dotenv
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, date_format, desc, sum
import os
import pandas as pd
import psycopg2


load_dotenv()

minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access = os.getenv("MINIO_ACCESS_KEY")
minio_secret = os.getenv("MINIO_SECRET_KEY")

spark = SparkSession.builder \
    .appName("Olist Aggregation") \
    .config("spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Airflow runs inside Docker, so Spark must use the MinIO service name.
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.s3a.endpoint", "http://minio:9000")
conf.set("fs.s3a.access.key", minio_access)
conf.set("fs.s3a.secret.key", minio_secret)
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.aws.credentials.provider",
         "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
conf.set("fs.s3a.multipart.purge.age", "86400")
conf.set("fs.s3a.retry.interval", "500")
conf.set("fs.s3a.retry.throttle.interval", "100")
conf.set("fs.s3a.connection.ttl", "300000")
conf.set("fs.s3a.assumed.role.session.duration", "1800")
conf.set("fs.s3a.connection.ssl.enabled", "false")
conf.set("fs.s3a.connection.timeout", "60000")
conf.set("fs.s3a.connection.establish.timeout", "5000")
conf.set("fs.s3a.connection.maximum", "100")
conf.set("fs.s3a.threads.keepalivetime", "60")


def python_value(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def write_gold_table(cursor, table_name, create_sql, spark_df, columns):
    cursor.execute(create_sql)
    cursor.execute(f"DELETE FROM {table_name}")

    pandas_df = spark_df.select(*columns).toPandas()
    rows = [
        tuple(python_value(row[column]) for column in columns)
        for _, row in pandas_df.iterrows()
    ]

    if not rows:
        print(f"No rows to load for {table_name}")
        return

    column_list = ", ".join(columns)
    execute_values(
        cursor,
        f"INSERT INTO {table_name} ({column_list}) VALUES %s",
        rows
    )
    print(f"Loaded {len(rows)} rows into {table_name}")


df = spark.read.parquet("s3a://olist-data/silver/fact_orders")

# One payment row per order and payment type.
order_payments = df.select(
    "order_id",
    "customer_id",
    "customer_state",
    "order_status",
    "order_date",
    "payment_type",
    "payment_value"
).dropDuplicates(["order_id", "payment_type"])

# One row per order item.
order_items = df.select(
    "order_id",
    "order_item_id",
    "product_id",
    "product_category_name",
    "price"
).dropDuplicates(["order_id", "order_item_id"])

gold_tables = [
    {
        "table_name": "revenue_by_state",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS revenue_by_state (
                customer_state TEXT,
                total_revenue DOUBLE PRECISION
            )
        """,
        "dataframe": order_payments.groupBy("customer_state")
        .agg(sum("payment_value").alias("total_revenue"))
        .orderBy(desc("total_revenue")),
        "columns": ["customer_state", "total_revenue"],
    },
    {
        "table_name": "monthly_revenue",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS monthly_revenue (
                order_month TEXT,
                total_revenue DOUBLE PRECISION,
                order_count BIGINT
            )
        """,
        "dataframe": order_payments.withColumn(
            "order_month", date_format(col("order_date"), "yyyy-MM")
        ).groupBy("order_month")
        .agg(
            sum("payment_value").alias("total_revenue"),
            countDistinct("order_id").alias("order_count")
        )
        .orderBy("order_month"),
        "columns": ["order_month", "total_revenue", "order_count"],
    },
    {
        "table_name": "top_product_categories",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS top_product_categories (
                product_category_name TEXT,
                total_item_revenue DOUBLE PRECISION,
                item_count BIGINT,
                order_count BIGINT
            )
        """,
        "dataframe": order_items.groupBy("product_category_name")
        .agg(
            sum("price").alias("total_item_revenue"),
            count("order_item_id").alias("item_count"),
            countDistinct("order_id").alias("order_count")
        )
        .orderBy(desc("total_item_revenue")),
        "columns": [
            "product_category_name",
            "total_item_revenue",
            "item_count",
            "order_count",
        ],
    },
    {
        "table_name": "orders_by_status",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS orders_by_status (
                order_status TEXT,
                order_count BIGINT,
                total_revenue DOUBLE PRECISION
            )
        """,
        "dataframe": order_payments.groupBy("order_status")
        .agg(
            countDistinct("order_id").alias("order_count"),
            sum("payment_value").alias("total_revenue")
        )
        .orderBy(desc("order_count")),
        "columns": ["order_status", "order_count", "total_revenue"],
    },
    {
        "table_name": "customer_state_summary",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS customer_state_summary (
                customer_state TEXT,
                customer_count BIGINT,
                order_count BIGINT,
                total_revenue DOUBLE PRECISION
            )
        """,
        "dataframe": order_payments.groupBy("customer_state")
        .agg(
            countDistinct("customer_id").alias("customer_count"),
            countDistinct("order_id").alias("order_count"),
            sum("payment_value").alias("total_revenue")
        )
        .orderBy(desc("total_revenue")),
        "columns": [
            "customer_state",
            "customer_count",
            "order_count",
            "total_revenue",
        ],
    },
    {
        "table_name": "payment_type_summary",
        "create_sql": """
            CREATE TABLE IF NOT EXISTS payment_type_summary (
                payment_type TEXT,
                order_count BIGINT,
                total_payment_value DOUBLE PRECISION
            )
        """,
        "dataframe": order_payments.groupBy("payment_type")
        .agg(
            countDistinct("order_id").alias("order_count"),
            sum("payment_value").alias("total_payment_value")
        )
        .orderBy(desc("total_payment_value")),
        "columns": ["payment_type", "order_count", "total_payment_value"],
    },
]

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    port=os.getenv("POSTGRES_PORT")
)

cursor = None

try:
    cursor = conn.cursor()
    for table in gold_tables:
        write_gold_table(
            cursor,
            table["table_name"],
            table["create_sql"],
            table["dataframe"],
            table["columns"]
        )
    conn.commit()
finally:
    if cursor is not None:
        cursor.close()
    conn.close()
    spark.stop()

print("Gold tables loaded into PostgreSQL")
