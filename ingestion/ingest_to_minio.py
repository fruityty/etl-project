from minio import Minio
from dotenv import load_dotenv
import os

# Load .env
load_dotenv()

# Read env variables
endpoint = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
bucket_name = os.getenv("MINIO_BUCKET")

# Connect to MinIO
client = Minio(
    endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)

# Create bucket if not exists
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)

# data_path = "../olist_dataset"
data_path = "/opt/airflow/olist_dataset" #for air flow

files = [
    "olist_orders_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_products_dataset.csv",
    "olist_order_payments_dataset.csv"
]

print(os.curdir)

for file in files:
    file_path = os.path.join(data_path, file)
    object_name = f"bronze/{file}"

    client.fput_object(
        bucket_name,
        object_name,
        file_path
    )

    print(f"Uploaded {file} to MinIO")