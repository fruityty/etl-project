from minio import Minio
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import os

# Load env
load_dotenv()

# MinIO config
minio_client = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

bucket_name = os.getenv("MINIO_BUCKET")

# MongoDB config
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["olist"]
collection = db["raw_data"]

# Files
collections = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv"
}

for name, file in collections.items():
    collection = db[name]

    # Clear old data 
    collection.delete_many({})

    object_name = f"bronze/{file}"
    local_file = f"/tmp/{file}"

    minio_client.fget_object(bucket_name, object_name, local_file)

    df = pd.read_csv(local_file)
    records = df.to_dict(orient="records")

    if records:
        collection.insert_many(records)

    print(f"Loaded {file} into MongoDB")