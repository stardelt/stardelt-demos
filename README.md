# stardelt demos — Try stardelt on a laptop

A thin wrapper that stands up stardelt on a single-node [kind](https://kind.sigs.k8s.io/) cluster. No cloud account required.

The demo does **not** define its own install. After creating the cluster it delegates to the [`stardelt-platform`](https://github.com/stardelt/stardelt-platform) Makefile (`make install-core`), so the laptop path and the platform path cannot drift apart.

> **The primary development environment is the Hetzner k3s cluster** in [`stardelt-platform/dev/hetzner-k3s`](https://github.com/stardelt/stardelt-platform/tree/main/dev/hetzner-k3s). That is where the product is deployed and exercised day to day.
> This kind demo is the secondary/laptop path: same install, smaller footprint, fewer moving parts.

## Prerequisites

| Tool | Min version |
|------|-------------|
| Docker | 24+ |
| kind | 0.23+ |
| kubectl | 1.29+ |
| helm | 3.18+ |
| make | any |

You also need two sibling repos checked out next to this one:

```
git clone git@github.com:stardelt/stardelt-platform.git ../stardelt-platform
git clone git@github.com:stardelt/stardelt-nova.git     ../stardelt-nova
```

Expected directory layout:

```
parent/
  stardelt-demos/       ← this repo
  stardelt-platform/    ← helm values, manifests, Makefile, scripts
  stardelt-nova/        ← Nova UI + backend
```

## Quickstart

```bash
make up          # ~10 min on first run (builds 3 images, installs the core profile)
make smoke       # verify Trino → Iceberg → SeaweedFS round-trip
```

`make up` does four things, in order:

1. ensures the kind cluster (`kind/kind-config.yaml`),
2. creates the namespace,
3. builds and kind-loads the stardelt-owned images (`kind/build-images.sh`),
4. runs `make install-core` in `stardelt-platform`.

Everything from step 4 down is the platform's wiring — chart versions, helm values, manifests and the order they are applied all live in `stardelt-platform`. To change the install, change the platform.

## Profiles

Pick the profile with `APP_PROFILE`:

| `APP_PROFILE` | Platform target | Installs |
|---|---|---|
| `core` (default) | `make install-core` | SeaweedFS, CNPG + Lakekeeper, Trino, Nova |
| `full` | `make install` | core plus Airflow and Superset |

```bash
APP_PROFILE=full make up    # everything, incl. Airflow + Superset
SLIM=1 make up              # core with small-node overrides (forwarded to install-core)
```

## What's running

| Component | Purpose | core | full |
|-----------|---------|:----:|:----:|
| **SeaweedFS** | S3-compatible object store (lakehouse data) | ✓ | ✓ |
| **Lakekeeper** | Apache Iceberg REST catalog | ✓ | ✓ |
| **Trino** | Distributed SQL query engine | ✓ | ✓ |
| **Nova** | stardelt UI — pipelines, observability, lineage | ✓ | ✓ |
| **Airflow** | Workflow orchestration (sample DAG: `nyc_taxi_load`) | | ✓ |
| **Superset** | BI dashboards over Trino | | ✓ |

## Try it

```bash
# Full profile only: load the sample NYC taxi dataset
APP_PROFILE=full make up
make airflow-trigger

# Open port-forwards (Trino :8081, Lakekeeper :8181)
make pf

# Per-service UIs (full profile)
make airflow-ui    # http://localhost:8088  (admin / admin)
make superset-ui   # http://localhost:8089  (admin / admin)
```

Services exposed via NodePort on the kind cluster:

| URL | Service |
|-----|---------|
| http://localhost:8080 | Nova |
| http://localhost:8081 | Trino |
| http://localhost:8181 | Lakekeeper |

Superset is available at http://localhost:8089 after running `make superset-ui` (full profile).

## Tear down

```bash
make down    # deletes the kind cluster
make clean   # down + docker image prune
```

## Custom namespaces and paths

```bash
export NAMESPACE=my-namespace          # default: stardelt
export STARDELT_PLATFORM_DIR=/path/to/stardelt-platform
export STARDELT_NOVA_DIR=/path/to/stardelt-nova
make up
```

## Primary dev environment — Hetzner k3s

Development happens on the single-node Hetzner k3s cluster defined in [`stardelt-platform/dev/hetzner-k3s`](https://github.com/stardelt/stardelt-platform/tree/main/dev/hetzner-k3s):

```bash
cd ../stardelt-platform
set -a; . ~/.config/stardelt/hetzner.env; set +a
make dev-up                 # provision the cluster
export KUBECONFIG="$HOME/.kube/stardelt-dev.yaml"
make install-core SLIM=1    # core fits a cx33 (8 GB); full stack wants cx43 (16 GB)
```

Use kind (this repo) when you want a disposable laptop cluster; use the Hetzner k3s cluster when you want the real thing. Both run the same `stardelt-platform` install.

## Documentation

Full docs at [stardelt.io](https://stardelt.io).
