docker exec -it airflow airflow users create \
    --username admin \
    --password admin \
    --firstname admin \
    --lastname user \
    --role Admin \
    --email admin@example.com