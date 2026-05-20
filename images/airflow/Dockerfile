ARG AIRFLOW_VERSION=3.2.0
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python3.12

USER airflow

# Postgres provider (slim image omits it; chart's bundled DB needs psycopg2)
# plus Iceberg/Parquet libs for the NYC taxi DAG. Pinned; bump deliberately.
RUN pip install --no-cache-dir --user \
        "apache-airflow-providers-postgres" \
        "apache-airflow-providers-fab" \
        "apache-airflow-providers-cncf-kubernetes" \
        "pyiceberg[s3fs,pyarrow]==0.7.1" \
        "pyarrow==17.0.0" \
        "httpx==0.27.2"

COPY --chown=airflow:0 dags/ /opt/airflow/dags/
