FROM apache/airflow:2.9.3

USER root

# Install system deps (optional but safe)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && apt-get clean

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    pymongo \
    minio \
    psycopg2-binary \
    python-dotenv \
    pandas