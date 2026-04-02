from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import sum
import os
import psycopg2

# -------------------------
# Load env
# -------------------------
load_dotenv()

minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access = os.getenv("MINIO_ACCESS_KEY")
minio_secret = os.getenv("MINIO_SECRET_KEY")

# -------------------------
# Spark session
# -------------------------
spark = SparkSession.builder \
    .appName("Olist Aggregation") \
    .config("spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367") \
    \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# fix airflow 60s error override bad values
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.s3a.endpoint", "http://minio:9000")  # use service name not localhost in Airflow/Docker
conf.set("fs.s3a.access.key", minio_access)
conf.set("fs.s3a.secret.key", minio_secret)
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# Fix invalid string format values
conf.set("fs.s3a.multipart.purge.age", "86400")   # 24h in seconds
conf.set("fs.s3a.retry.interval", "500")           # ms as number
conf.set("fs.s3a.retry.throttle.interval", "100")  # ms as number
conf.set("fs.s3a.connection.ttl", "300000")        # 5m in milliseconds
conf.set("fs.s3a.assumed.role.session.duration", "1800") # 30m in seconds

# Fix SSL - MinIO uses plain HTTP
conf.set("fs.s3a.connection.ssl.enabled", "false")
conf.set("fs.s3a.connection.timeout", "60000") 
conf.set("fs.s3a.connection.establish.timeout", "5000") 
conf.set("fs.s3a.connection.maximum", "100")
conf.set("fs.s3a.threads.keepalivetime", "60")


# -------------------------
# Read Silver from MinIO
# -------------------------
df = spark.read.parquet("s3a://olist-data/silver/fact_orders")

# -------------------------
# Create Gold dataset
# -------------------------
df_gold = df.groupBy("customer_state") \
    .agg(sum("payment_value").alias("total_revenue"))

df_gold.show()

# -------------------------
# Convert to Pandas
# -------------------------
df_pd = df_gold.toPandas()

# -------------------------
# Load PostgreSQL config from .env
# -------------------------
pg_host = os.getenv("POSTGRES_HOST")
pg_db = os.getenv("POSTGRES_DB")
pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")
pg_port = os.getenv("POSTGRES_PORT")


# -------------------------
# Connect to PostgreSQL
# -------------------------
conn = psycopg2.connect(
    host=pg_host,
    database=pg_db,
    user=pg_user,
    password=pg_password,
    port=pg_port
)

cursor = conn.cursor()

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS revenue_by_state (
    customer_state TEXT,
    total_revenue FLOAT
)
""")

# Clear old data
cursor.execute("DELETE FROM revenue_by_state")

# Insert new data
for _, row in df_pd.iterrows():
    cursor.execute("""
        INSERT INTO revenue_by_state (customer_state, total_revenue)
        VALUES (%s, %s)
    """, (row["customer_state"], row["total_revenue"]))

conn.commit()
cursor.close()
conn.close()

print("✅ Data loaded into PostgreSQL")