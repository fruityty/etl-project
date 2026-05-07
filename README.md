# etl-project
orders (order_id)

   ↓
   
order_items (order_id, product_id)

   ↓
   
products (product_id)

orders (customer_id)

   ↓
   
customers (customer_id)


orders (order_id)

   ↓
   
payments (order_id)


                +-------------------+
                |   Raw CSV Files   |
                +-------------------+
                          |
                          v
                +-------------------+
                |  MinIO (Bronze)   |
                |   Raw CSV Layer   |
                +-------------------+
                          |
                          v
                +-------------------+
                |     MongoDB       |
                |   Raw Data Store  |
                +-------------------+
                          |
                          v
                +-------------------+
                |      PySpark      |
                | Data Transformation|
                +-------------------+
                          |
                          v
                +-------------------+
                |  MinIO (Silver)   |
                |  Parquet Storage  |
                +-------------------+
                          |
                          v
                +-------------------+
                |      PySpark      |
                | Aggregation Layer |
                +-------------------+
                          |
                          v
                +-------------------+
                |   PostgreSQL      |
                |    Gold Layer     |
                +-------------------+
                          |
                          v
                +-------------------+
                | Apache Airflow    |
                | Workflow Orchestration |
                +-------------------+
