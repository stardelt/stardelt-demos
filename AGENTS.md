# AGENTS.md — stardelt-demos

The stardelt sample pipelines (NYC taxi), the pipeline loader DAGs and the Airflow image they run
in. This repository owns no install logic: the platform is installed from `stardelt-platform` on a
real node (a full install needs about 8 vCPU / 16 GB). The single-node kind demo was dropped (D36
— the platform needs a real node; nothing here creates or destroys a cluster).

## Read first

- `../stardelt-docs/docs/design/master-spec.md` — canonical spec (published at
  https://stardelt.io); when documents disagree, it wins.
- `../stardelt-docs/docs/architecture/licenses.md` — license policy.
- https://stardelt.io/getting-started/hetzner-k3s — the single-node k3s development cluster
  (`stardelt-platform/dev/hetzner-k3s`), where the platform and these DAGs are run.

## Layout

- `dags/pipeline_loader.py` — one Airflow DAG per `Pipeline` resource of the namespace (scheduled
  jobs, phase J0). The resource is the source of truth: the loader lists it at parse time and
  builds the DAG; a pipeline with an `ownerGroup` runs as that group's `ServicePrincipal`.
- `dags/stardelt_pipelines/` — the loader's pure parts, tested without Airflow:
  `plan.py` (resource → plan, mirroring the operator's `PipelineSpec::validate`), `expand.py`
  (template placeholders), `principal.py` (pod override), `trino.py` (run + cancel a statement).
- `dags/nyc_taxi_load.py` — the NYC-taxi sample pipeline: 2023 TLC parquet files → Iceberg through
  Trino.
- `images/airflow/Dockerfile` — the image the platform's Airflow deployment runs: Airflow, the
  providers the chart needs, the DAG clients, and `dags/` baked in. The Dockerfile pins
  `AIRFLOW_VERSION`, which must match the appVersion of the airflow chart pinned in
  `stardelt-platform`'s Makefile.
- `tests/` — pytest for the pure loader modules.
- `Makefile` — helpers against a cluster that already runs stardelt (`pf`, `logs`,
  `airflow-trigger`, `airflow-ui`, `superset-ui`, `deps`).

## Build / check / run

```bash
python -m pip install pytest "httpx==0.28.1" "kubernetes>=35.0.0,!=36.0.0,<37.0.0"
python -m pytest -q tests

hadolint images/airflow/Dockerfile
docker build -f images/airflow/Dockerfile -t stardelt/airflow:dev .
```

CI is `.github/workflows/ci.yml`: hadolint and pytest on every pull request and push to `main`;
the image is built without pushing on a pull request and pushed as
`ghcr.io/stardelt/airflow:demo` on a push to `main`. There is no cluster job — integration runs on
the Hetzner k3s dev cluster, not in CI.

## Conventions

- **PR-only.** Never push to `main`; branch off `origin/main` and keep PRs small.
- **License policy: OSI-permissive only** (Apache-2.0, MIT, BSD). The single documented
  exception is OpenBao (MPL-2.0). See `../stardelt-docs/docs/architecture/licenses.md`.
- Read the master spec first. Never add install steps here: change them in `stardelt-platform` so
  the platform has one install path.
- Keep `plan.py` in step with the operator's `PipelineSpec::validate`: the API server's schema
  rules cannot see a dependency cycle or an unknown dependency, the plan can.

## Gotchas

- The image build asserts the installed Airflow version against the base image, the DAG imports,
  and `pip check` — a silently wrong image fails the build.
- `pipeline_loader.py` needs the namespace ServiceAccount and the API at parse time; the tests
  exercise only the pure modules, so a loader change needs a run on the dev cluster.
- `airflow-trigger`, `airflow-ui` and `superset-ui` assume the full install (Airflow and Superset
  are not in `install-core`).
- The `demo` tag moves on every merge to `main`; `stardelt-platform`'s `helm-values/airflow.yaml`
  pins a digest, and deploys only what that digest names.
