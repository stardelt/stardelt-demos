# stardelt demos — Try stardelt on a laptop

Run the full stardelt open lakehouse stack — Trino, Lakekeeper, SeaweedFS, Airflow, Superset, and Nova — on a single-node [kind](https://kind.sigs.k8s.io/) cluster. No cloud account required.

## Prerequisites

| Tool | Min version |
|------|-------------|
| Docker | 24+ |
| kind | 0.23+ |
| kubectl | 1.29+ |
| helm | 3.18+ |

You also need two sibling repos checked out next to this one:

```
git clone git@github.com:stardelt/stardelt-platform.git ../stardelt-platform
git clone git@github.com:stardelt/stardelt-nova.git     ../stardelt-nova
```

Expected directory layout:

```
parent/
  stardelt-demos/       ← this repo
  stardelt-platform/    ← helm values, manifests, scripts
  stardelt-nova/        ← Nova UI + backend
```

## Quickstart

```bash
make up          # ~10 min on first run (builds 3 images, spins up the stack)
make smoke       # verify Trino → Iceberg → SeaweedFS round-trip
```

## What's running

| Component | Purpose |
|-----------|---------|
| **SeaweedFS** | S3-compatible object store (lakehouse data) |
| **Lakekeeper** | Apache Iceberg REST catalog |
| **Trino** | Distributed SQL query engine |
| **Airflow** | Workflow orchestration (sample DAG: `nyc_taxi_load`) |
| **Superset** | BI dashboards over Trino |
| **Nova** | stardelt UI — pipelines, observability, lineage |

## Try it

```bash
# Load the sample NYC taxi dataset
make airflow-trigger

# Open port-forwards (Trino :8081, Lakekeeper :8181)
make pf

# Per-service UIs
make airflow-ui    # http://localhost:8088  (admin / admin)
make superset-ui   # http://localhost:8089  (admin / admin)
```

Services exposed via NodePort on the kind cluster:

| URL | Service |
|-----|---------|
| http://localhost:8080 | Nova |
| http://localhost:8081 | Trino |
| http://localhost:8181 | Lakekeeper |

Superset is available at http://localhost:8089 after running `make superset-ui`.

## Tear down

```bash
make down    # deletes the kind cluster
make clean   # down + docker image prune
```

## Custom paths

If your sibling repos are in non-default locations, set:

```bash
export STARDELT_PLATFORM_DIR=/path/to/stardelt-platform
export STARDELT_NOVA_DIR=/path/to/stardelt-nova
make up
```

## Documentation

Full docs at [docs.stardelt.io](https://docs.stardelt.io).

## k3s

A k3s-based demo (closer to production) is coming soon. See [`k3s/README.md`](k3s/README.md).
