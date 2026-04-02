from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Create Spark session
spark = SparkSession.builder \
    .appName("Olist Transformation") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/olist") \
    .getOrCreate()

# Read collections
orders = spark.read.format("mongodb").option("collection", "orders").load()
order_items = spark.read.format("mongodb").option("collection", "order_items").load()
products = spark.read.format("mongodb").option("collection", "products").load()
customers = spark.read.format("mongodb").option("collection", "customers").load()
payments = spark.read.format("mongodb").option("collection", "payments").load()

# Select columns
orders = orders.select("order_id", "customer_id", "order_purchase_timestamp")
order_items = order_items.select("order_id", "product_id", "price")
products = products.select("product_id", "product_category_name")
customers = customers.select("customer_id", "customer_city", "customer_state")
payments = payments.select("order_id", "payment_value")

# Join tables
df = orders \
    .join(order_items, "order_id") \
    .join(products, "product_id") \
    .join(customers, "customer_id") \
    .join(payments, "order_id")

# Transform
df_clean = df \
    .withColumn("order_date", to_date(col("order_purchase_timestamp"))) \
    .drop("order_purchase_timestamp")

# Show result
df_clean.show(5)
df_clean.printSchema()