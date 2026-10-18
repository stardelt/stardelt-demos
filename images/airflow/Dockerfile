ARG AIRFLOW_VERSION=3.2.0
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python3.12

# Non-root, as upstream ships it (UID 50000); the numeric id is not needed here
# and pinning it would fork the base image's user database.
# hadolint ignore=DL3066
USER airflow

# Postgres provider (slim image omits it; chart's bundled DB needs psycopg2)
# plus Iceberg/Parquet libs for the NYC taxi DAG. Pinned; bump deliberately.
# The Airflow providers stay unpinned on purpose: this image tracks the pinned
# AIRFLOW_VERSION above and a provider pin would add a second, conflicting pin.
# hadolint ignore=DL3013
RUN pip install --no-cache-dir --user \
        "apache-airflow-providers-postgres" \
        "apache-airflow-providers-fab" \
        "apache-airflow-providers-cncf-kubernetes" \
        "pyiceberg[s3fs,pyarrow]==0.7.1" \
        "pyarrow==17.0.0" \
        "httpx==0.27.2"

COPY --chown=airflow:0 dags/ /opt/airflow/dags/
