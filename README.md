# etl-project
![alt text](assets/overall_project.png)

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
