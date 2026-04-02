from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "you",
    "retries": 1,
}

with DAG(
    dag_id="olist_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # schedule_interval=None,
    catchup=False,
    tags=["etl", "spark"],
) as dag:

    ingest_minio = BashOperator(
    task_id="ingest_datalake",
        bash_command="python /opt/airflow/ingestion/ingest_to_minio.py"
    )

    ingest_mongodb = BashOperator(
        task_id="ingest_bronze",
        bash_command="python /opt/airflow/ingestion/ingest_to_mongo.py"
    )

    transform = BashOperator(
        task_id="transform_silver",
        bash_command="python /opt/airflow/spark_jobs/transform.py"
    )

    aggregate = BashOperator(
        task_id="aggregate_gold",
        bash_command="python /opt/airflow/spark_jobs/aggregate.py"
    )

    ingest_minio >> ingest_mongodb >> transform >> aggregate