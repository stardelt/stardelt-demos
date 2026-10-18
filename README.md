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
| Airflow, Superset | `APP_PROFILE=full`; needs a 16 GB node, the gate is `SLIM=1` — covered by the [nightly demo leg](#nightly-demo-leg-ci) |
| JupyterHub, ingress/TLS, OIDC login, the operator | other work packages; nothing in this data path depends on them |
| NYC-taxi DAG (`make airflow-trigger`) | the full profile, so it is the nightly demo leg's job (below), not the per-PR gate's |

Runs are bounded: a 30-minute budget inside the gate (`GATE_BUDGET_SECONDS`), inside a 45-minute job
timeout; a green run takes about five minutes. The gate pins the Kubernetes version it installs onto
(`KIND_NODE_IMAGE`, currently `kindest/node:v1.37.0`, overridable per run). Every start-up wait is a
`kubectl wait`/`rollout status` with a timeout — there are no sleeps to "give it a minute". On failure
`kind/diagnostics.sh` dumps pods, events, describes, logs, helm history, the Trino catalog config and
the node state into the run log:

```bash
gh run view --log-failed        # the failing component, described and logged
```

## Nightly demo leg (CI)

[`.github/workflows/nightly.yml`](.github/workflows/nightly.yml) is the integration tier of the same
gate: it installs the **full** profile and then proves the demo actually demos. It runs on a
schedule (03:10 UTC), on demand, and on a pull request that carries the `nightly-demo` label — never
as part of the per-PR gate. It needs a 16 GB node (the full profile does not fit the 8 GB dev node),
which is exactly why it is nightly.

```bash
bash kind/demo.sh                            # the whole leg, locally (make demo is the same)
gh workflow run nightly.yml                  # ...or in CI, installing the platform's main
gh workflow run nightly.yml -f platform_ref=my-branch
gh run watch
gh pr edit <nr> --add-label nightly-demo     # run it on a pull request
```

**What it proves** — the demo path end to end, not a subset of it:

1. the two stardelt-owned images the full profile runs are **built and kind-loaded**
   (`stardelt/airflow:dev` from this repo, `ghcr.io/stardelt/superset:dev` from `stardelt-platform`)
   — kind has no registry, and CI has no ghcr credentials;
2. `make install` (full profile) brings up core + Airflow + Superset + Nova: every helm release is
   `deployed`, every workload rolled out, the one-shot jobs (Lakekeeper bootstrap, Airflow DB
   migration + admin user, Superset init) complete, and no pod is crashlooping or unpullable;
3. `kind/smoke.sh` still round-trips a row through Trino → Iceberg → SeaweedFS in the same cluster;
4. `nyc_taxi_load` is parsed, unpaused and triggered with a known run id, and the run reaches
   `success` (bounded — the only thing in the leg whose duration is the product);
5. the Iceberg table the DAG wrote is asserted **through Trino**: 12 monthly partitions, a row
   count inside the expected band (30–45M for 2023, 2–4M per month), the canonical 20 columns, and
   at least one Iceberg snapshot per append — with the counts, the DAG run id and the query output
   printed as evidence on a green run.

**What it deliberately does not cover:**

| Excluded | Why |
|---|---|
| Nova | not built here (its image comes from the `stardelt-nova` checkout), so `kind/demo.sh` drops the Deployment; the full profile applies it, and nothing in the DAG path depends on it |
| Superset functionality | the full profile installs Superset, so the leg cannot ignore it — it is asserted as *ready* (release deployed, pods rolled out, init job complete). No dashboard, chart or SQL Lab query is exercised |
| JupyterHub, ingress/TLS, OIDC login, the operator | other work packages |
| Airflow task-log persistence | the kind values keep logs in `emptyDir` (`logs.persistence.enabled: false`), so a *successful* task pod's log is deleted with the pod. Failed pods are kept, and `kind/diagnostics.sh` dumps them |

Runs are bounded: a 75-minute in-script budget (`DEMO_BUDGET_SECONDS`, 40 of it reserved for the DAG
run) inside a 110-minute job timeout. Observed on the first green run (2026-10-18, run
[39114177938](https://github.com/stardelt/stardelt-demos/actions/runs/39114177938)): **12m28s** wall
clock — images 2m, install 9m, DAG run 2m20s, assertions 7s — with the DAG run
`nyc_taxi_load / nightly-20261018T090858Z` succeeding in 140s and Trino reading back
`38,310,226` rows in 12 monthly partitions (2.82M–3.52M each, summed from Iceberg's own partition
metadata) and 12 snapshots. If a run is red, the job's last step dumps `kind/diagnostics.sh` —
including the Airflow section (DAG runs, DAG import errors, every task pod's log) and the Superset
section — so `gh run view --log-failed` says which part failed.

The leg installs with `STARDELT_IMAGES="airflow superset"` (see `kind/up.sh`) and, for the full
profile, applies the platform's own `manifests/s3-credentials.dev.yaml` — the Secret `make install`
documents as a prerequisite but does not apply itself (`make install-core` does). Both are noted
where they happen, and the second one goes away when the platform's `install` target applies it.

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
