ARG AIRFLOW_VERSION=3.2.0
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python3.12

# Non-root, as upstream ships it (UID 50000); the numeric id is not needed here
# and pinning it would fork the base image's user database.
# hadolint ignore=DL3066
USER airflow

# Postgres provider (the slim image omits it; the chart's bundled DB needs
# psycopg2), the FAB auth manager the chart configures (with its `oauth` extra —
# Flask-AppBuilder imports authlib for AUTH_TYPE=AUTH_OAUTH, and without it the
# api-server dies at startup with ModuleNotFoundError: authlib), the Kubernetes
# provider the chart's KubernetesExecutor needs, plus the Iceberg/Parquet clients
# the NYC-taxi DAG imports inside its tasks.
#
# Installed with the image's *own* virtualenv pip and anchored on the base
# image's Airflow version. Do not "simplify" this back to `pip install --user`:
# that runs the base interpreter's pip, which sees no Airflow installed and
# re-resolves from scratch — with pyiceberg 0.7.1 (s3fs<2024.1 → fsspec<2025.1)
# pinned that silently replaced Airflow 3.2.0 with 3.1.8, and the chart's
# database-migration hook then never produced a schema. The `apache-airflow==`
# pin makes any such conflict a build failure instead of a wrong image.
#
# AIRFLOW_VERSION must stay in step with the appVersion of the
# apache-airflow/airflow chart pinned in stardelt-platform's Makefile: that
# chart's migration and init containers run *this* image against that version's
# metadata schema.
#
# The library pins are the versions uv resolves with apache-airflow held at
# AIRFLOW_VERSION (uv pip compile --python-version 3.12 --python-platform
# linux); bump them deliberately. The Airflow providers stay unpinned, as they
# track AIRFLOW_VERSION, and a provider that needs a different Airflow fails
# this build (which is the point).
# hadolint ignore=DL3013
RUN "$VIRTUAL_ENV/bin/python" -m pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        "apache-airflow-providers-postgres" \
        "apache-airflow-providers-fab" \
        "apache-airflow-providers-cncf-kubernetes" \
        "authlib==1.9.1" \
        "pyiceberg[s3fs,pyarrow]==0.12.0" \
        "pyarrow==26.0.0" \
        "httpx==0.28.1" \
 # A silently wrong image is worse than a red build: fail if the install moved
 # Airflow off the base image's version (see above) or if the DAG's imports are
 # missing. `pip check` catches a half-resolved environment.
 && "$VIRTUAL_ENV/bin/python" -c 'import sys, importlib.metadata as md; v = md.version("apache-airflow"); print("airflow", v); sys.exit(f"airflow {v} != the base image version {sys.argv[1]}") if v != sys.argv[1] else None' "${AIRFLOW_VERSION}" \
 && "$VIRTUAL_ENV/bin/python" -c 'import importlib.metadata as md, authlib, pyiceberg, pyarrow, pyarrow.parquet, httpx, psycopg2; print("DAG imports ok: pyiceberg", md.version("pyiceberg"), "pyarrow", md.version("pyarrow"), "httpx", md.version("httpx"))' \
 && "$VIRTUAL_ENV/bin/python" -m pip check

COPY --chown=airflow:0 dags/ /opt/airflow/dags/
