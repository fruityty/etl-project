# Olist ETL Data Pipeline

This repository contains an end-to-end ETL pipeline for the Brazilian Olist ecommerce dataset. The project uses Docker Compose to run the local data platform, Apache Airflow to orchestrate the workflow, MinIO as an S3-compatible data lake, MongoDB as the bronze/raw store, PySpark for transformation and aggregation, and PostgreSQL as the gold-layer warehouse.

![Medallion architecture](assets/medallion_architecture.png)

![Overall project architecture](assets/overall_project.png)

## Project Overview

The pipeline follows a medallion-style data architecture:

1. Raw CSV files are stored locally in `olist_dataset/`.
2. The CSV files are uploaded to MinIO under the bronze path.
3. The bronze data is loaded into MongoDB collections.
4. PySpark reads from MongoDB, joins and cleans the data, then writes the silver dataset back to MinIO as Parquet.
5. PySpark reads the silver Parquet data, aggregates revenue by customer state, and loads the result into PostgreSQL.
6. Apache Airflow orchestrates the pipeline from ingestion through gold-layer loading.

## Tech Stack

| Component | Purpose |
| --- | --- |
| Docker Compose | Runs the full local environment |
| Apache Airflow | Orchestrates ETL tasks |
| MinIO | S3-compatible data lake |
| MongoDB | Bronze/raw document store |
| PySpark | Data transformation and aggregation |
| PostgreSQL | Gold-layer data warehouse |
| Pandas | CSV and Spark-to-PostgreSQL helper processing |

## Repository Structure

```text
.
|-- dags/
|   `-- etl_pipeline.py          # Airflow DAG for the ETL workflow
|-- ingestion/
|   |-- ingest_to_minio.py       # Uploads CSV files to MinIO
|   `-- ingest_to_mongo.py       # Loads bronze CSV data from MinIO into MongoDB
|-- spark_jobs/
|   |-- transform.py             # Builds the silver Parquet dataset
|   `-- aggregate.py             # Builds and loads the gold PostgreSQL table
|-- olist_dataset/               # Source Olist CSV files and dataset notes
|-- notebooks/                   # Exploration and development notebooks
|-- assets/                      # Architecture and Airflow screenshots
|-- docker-compose.yml           # Local services
|-- dockerfile                   # Airflow image with Spark dependencies
|-- example.env                  # Environment variable template
`-- README.md                    # Original project README
```

## Dataset

The active pipeline currently uses these Olist CSV files:

| File | Purpose |
| --- | --- |
| `olist_orders_dataset.csv` | Main order table |
| `olist_customers_dataset.csv` | Customer location data |
| `olist_order_items_dataset.csv` | Order item and product links |
| `olist_products_dataset.csv` | Product category data |
| `olist_order_payments_dataset.csv` | Payment values |

Additional files are included for future analysis, such as reviews, sellers, geolocation, and product category translations.

## Pipeline Flow

```text
Local CSV files
    -> MinIO bronze path
    -> MongoDB bronze collections
    -> PySpark transformation
    -> MinIO silver Parquet dataset
    -> PySpark aggregation
    -> PostgreSQL gold table
```

The Airflow DAG is defined in `dags/etl_pipeline.py` and runs these tasks in order:

1. `ingest_datalake`
2. `ingest_bronze`
3. `transform_silver`
4. `aggregate_gold`

## Gold Output

The final table is written to PostgreSQL:

```sql
revenue_by_state (
    customer_state TEXT,
    total_revenue FLOAT
)
```

This table contains total payment revenue grouped by Brazilian customer state.

## Local Services

After the Docker environment is running, these services are available:

| Service | URL | Purpose |
| --- | --- | --- |
| Apache Airflow | `http://localhost:8080` | DAG orchestration UI |
| MinIO Console | `http://localhost:9001` | Data lake web UI |
| MinIO API | `http://localhost:9000` | S3-compatible endpoint |
| MongoDB | `mongodb://localhost:27017` | Bronze/raw data storage |
| PostgreSQL | `postgresql://localhost:5432` | Gold data warehouse |

## Setup

### 1. Create the environment file

Copy the example environment file:

```powershell
Copy-Item example.env .env
```

Or on macOS/Linux:

```bash
cp example.env .env
```

Then update credentials in `.env` as needed.

Required variables:

```env
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=your-access-key
MINIO_SECRET_KEY=your-secret-key
MINIO_BUCKET=olist-data

MONGO_URI=mongodb://mongodb:27017/
MONGO_DB=olist

POSTGRES_HOST=postgres
POSTGRES_DB=olist
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your-password
POSTGRES_PORT=5432

AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_EMAIL=admin@example.com
AIRFLOW_DB=sqlite:////opt/airflow/airflow.db
AIRFLOW_FERNET_KEY=your-fernet-key
```


### 2. Start the stack

```bash
docker compose up --build
```

If Docker cannot find the Airflow build file on a case-sensitive system, either rename `dockerfile` to `Dockerfile` or add this to the Airflow service in `docker-compose.yml`:

```yaml
build:
  context: .
  dockerfile: dockerfile
```

### 3. Open Airflow

Go to:

```text
http://localhost:8080
```

Log in with the Airflow username and password from `.env`.

### 4. Run the DAG

In the Airflow UI:

1. Find the DAG named `olist_etl_pipeline`.
2. Enable it if needed.
3. Trigger the DAG manually.
4. Watch the task graph run from ingestion to aggregation.

## Running Scripts Manually

The pipeline is designed to run inside the Docker/Airflow environment, but the task order is:

```bash
python ingestion/ingest_to_minio.py
python ingestion/ingest_to_mongo.py
python spark_jobs/transform.py
python spark_jobs/aggregate.py
```

When running outside Docker, update the service hostnames in `.env`. For example, use `localhost:9000`, `localhost`, or `127.0.0.1` instead of Docker service names such as `minio`, `mongodb`, and `postgres`.

## Development Notebooks

The `notebooks/` folder contains step-by-step exploration and development work:

| Notebook | Purpose |
| --- | --- |
| `01_eda.ipynb` | Initial exploratory data analysis |
| `02_ingest.ipynb` | Ingestion experiments |
| `03_transform.ipynb` | Transformation experiments |
| `04_aggregate.ipynb` | Aggregation experiments |
| `05_airflow.ipynb` | Airflow workflow notes |

## Troubleshooting

### Airflow task cannot connect to MinIO

Inside Docker, the MinIO endpoint should usually be:

```env
MINIO_ENDPOINT=minio:9000
```

The Spark jobs also explicitly set `fs.s3a.endpoint` to `http://minio:9000` for the Airflow/Docker environment.

### Local scripts cannot connect to services

If you run scripts directly from your host machine instead of inside Docker, use local hostnames:

```env
MINIO_ENDPOINT=localhost:9000
MONGO_URI=mongodb://localhost:27017/
POSTGRES_HOST=localhost
```

### PostgreSQL table is empty

Check that the previous tasks completed successfully:

1. CSV files were uploaded to MinIO under `bronze/`.
2. MongoDB collections were populated.
3. Silver Parquet data exists at `s3a://olist-data/silver/fact_orders`.
4. The `aggregate_gold` Airflow task completed without errors.

## Possible Future Improvements

- Add data quality checks before writing the silver and gold layers.
- Use Airflow connections instead of reading all credentials from `.env`.
- Add dbt models for the PostgreSQL gold layer.
- Add more gold tables for product, seller, review, and geolocation analytics.
- Add tests for ingestion, transformation, and aggregation logic.
- Add a dashboard layer on top of PostgreSQL.
