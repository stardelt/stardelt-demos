# stardelt demos — sample pipelines and the Airflow image

The stardelt sample pipelines, the pipeline loader DAGs and the Airflow image they run in.

There is no install here. The platform is installed from
[`stardelt-platform`](https://github.com/stardelt/stardelt-platform) on a real node — a full
install needs about 8 vCPU and 16 GB. The single-node kind demo this repository used to carry was
dropped because the platform no longer fits a laptop (D36).

To run the platform, follow
[stardelt.io → Getting started → Hetzner k3s](https://stardelt.io/getting-started/hetzner-k3s);
it provisions the single-node k3s development cluster that the platform and these DAGs are
developed against.

## What is here

| Path | Purpose |
|---|---|
| `dags/nyc_taxi_load.py` | The NYC-taxi sample pipeline: downloads the 2023 TLC trip files and appends them to an Iceberg table through Trino |
| `dags/pipeline_loader.py` | One Airflow DAG per `Pipeline` resource of the namespace (scheduled jobs, phase J0) |
| `dags/stardelt_pipelines/` | The loader's pure parts — `plan`, `expand`, `principal`, `trino` — unit-tested without Airflow |
| `images/airflow/Dockerfile` | The image the platform's Airflow deployment runs: Airflow, the providers and clients the DAGs need, and the DAGs themselves |
| `tests/` | pytest for the loader's pure parts |
| `Makefile` | Helpers for a running cluster: `pf`, `logs`, `airflow-trigger`, `airflow-ui`, `superset-ui` |

## The Airflow image

`images/airflow/Dockerfile` builds from `apache/airflow:slim-*-python3.12` and adds the providers
the platform's chart needs (Postgres, FAB, Kubernetes, Amazon), the Iceberg/Parquet clients the
NYC-taxi DAG imports, and `dags/` baked in at `/opt/airflow/dags/`.

CI publishes the image as `ghcr.io/stardelt/airflow:demo` on every push to `main`; a pull request
builds it without pushing. `stardelt-platform` pins the digest it deploys in
`helm-values/airflow.yaml` — the tag moves on every merge here, the digest is what runs.

`AIRFLOW_VERSION` must match the `appVersion` of the apache-airflow/airflow chart pinned in
`stardelt-platform`'s Makefile: the chart's migration and init containers run this image against
that version's metadata schema. The build fails if the installed Airflow version drifts from the
base image, or if any import the DAGs need is missing.

## The pipeline loader DAGs

`dags/pipeline_loader.py` reads the `Pipeline` resources of its namespace (one LIST per parse,
through a read-only ServiceAccount) and builds one DAG per resource — the resource is the source
of truth, nobody edits a DAG by hand. A pipeline with an `ownerGroup` runs as that group's
`ServicePrincipal` (`runAs`): only that pipeline's task pods receive the principal's credentials.
A stopped run (the executor sends SIGTERM) cancels the statement at the engine, so no query keeps
running behind a failed task.

The pure parts live in `dags/stardelt_pipelines/` and are tested without Airflow:
`plan.py` validates a resource into a plan (mirroring the operator's `PipelineSpec::validate`),
`expand.py` substitutes the template placeholders, `principal.py` builds the pod override, and
`trino.py` runs and cancels a statement.

## The sample pipeline

`dags/nyc_taxi_load.py` downloads the twelve monthly NYC TLC parquet files for 2023 (~630 MB) and
appends each to the `nyc_taxi.trips` Iceberg table through Trino. It is part of the image, so the
Airflow deployment the platform installs picks it up. With cluster access, unpause and trigger it:

```bash
make airflow-trigger     # kubectl exec into the scheduler: unpause + trigger nyc_taxi_load
```

The DAG is the long pole of an integration run; it is not exercised by this repository's CI.

## Tests

```bash
python -m pip install pytest "httpx==0.28.1" "kubernetes>=35.0.0,!=36.0.0,<37.0.0"
python -m pytest -q tests
```

The `kubernetes` pin matches what the Airflow Kubernetes provider resolves in the image: the
principal's pod override is built with the client's models, so the tests use the same range.

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every pull request and every push to
`main`:

- **Lint** — `hadolint images/airflow/Dockerfile`.
- **Pipeline loader tests** — `python -m pytest -q tests`.
- **Airflow image** — builds `ghcr.io/stardelt/airflow:demo` without pushing on a pull request;
  pushes it on a push to `main`.

There is no cluster job: nothing in CI needs Kubernetes. The platform is verified on the Hetzner
k3s dev cluster.

## Helpers against a running cluster

The `Makefile` only talks to a cluster that already runs stardelt (see the getting-started guide);
it never creates one.

```bash
make pf              # port-forwards: Nova :8080, Trino :8081, Lakekeeper :8181, Superset :8089
make logs            # tail logs across the stardelt namespace
make airflow-ui      # Airflow API server on http://localhost:8088
make superset-ui     # Superset on http://localhost:8089
make deps            # check docker/kubectl/helm (stardelt-platform's script)
```

`NAMESPACE` (default `stardelt`) and `STARDELT_PLATFORM_DIR` (default `../stardelt-platform`) are
overridable.

## Documentation

Full docs at [stardelt.io](https://stardelt.io).
