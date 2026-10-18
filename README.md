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

## Verification gate (CI)

[`.github/workflows/verify.yml`](.github/workflows/verify.yml) is the WP-02 gate: proof that the
platform installs and works on a clean cluster, headlessly, on a GitHub-hosted runner. The same
gate runs on a laptop as `bash kind/gate.sh` (needs Docker, kind, kubectl, helm and `jq`).

It runs on every pull request, on every push to `main`, and on demand:

```bash
gh workflow run verify.yml                              # install the platform's main
gh workflow run verify.yml -f platform_ref=my-branch    # ...or a stardelt-platform branch
gh run watch
```

**What it proves** — the end-to-end property, not a subset of it:

1. `kind create cluster` (one node) and then `make install-core SLIM=1` from `stardelt-platform` — the
   same install the laptop path and the Hetzner dev cluster use.
2. CloudNative-PG, SeaweedFS (a real S3 object store), Lakekeeper (a real Iceberg REST catalog) and
   Trino (a real query engine) are all up: every helm release is `deployed` and every workload has
   rolled out. No pod in the namespace may be crashlooping or unpullable.
3. `kind/smoke.sh` writes a row and reads it back through all four
   (`1,hello-iceberg-on-s3` out of `warehouse.smoke.t`).
4. That row is a committed **Iceberg snapshot** (`warehouse.smoke."t$snapshots"`), and it is still
   readable after the object store's pod is restarted — the regression detector for a data directory
   that is not a PVC.

**What it deliberately does not cover.** The gate's header says so, and it fails loudly rather than
skipping anything:

| Excluded | Why |
|---|---|
| Nova | the core profile applies it, but its image is built locally by `kind/build-images.sh`; CI builds no stardelt images, so the gate installs with `STARDELT_IMAGES=none` and drops the Deployment |
| Airflow, Superset | `APP_PROFILE=full`; needs a 16 GB node, the gate is `SLIM=1` |
| JupyterHub, ingress/TLS, OIDC login, the operator | other work packages; nothing in this data path depends on them |
| NYC-taxi DAG (`make airflow-trigger`) | full profile — the nightly/integration leg of WP-02 is still open |

Runs are bounded: a 30-minute budget inside the gate (`GATE_BUDGET_SECONDS`), inside a 45-minute job
timeout. Every start-up wait is a `kubectl wait`/`rollout status` with a timeout — there are no sleeps
to "give it a minute". On failure `kind/diagnostics.sh` dumps pods, events, describes, logs, helm
history, the Trino catalog config and the node state into the run log:

```bash
gh run view --log-failed        # the failing component, described and logged
```

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
export STARDELT_IMAGES=none            # skip the locally built images (then no Nova)
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
