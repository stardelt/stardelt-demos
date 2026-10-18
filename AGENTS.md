# AGENTS.md — stardelt-demos

Laptop demo path for stardelt: stands up the platform on a single-node kind cluster.
It owns no install logic — it delegates to the `stardelt-platform` Makefile
(`make install-core` / `make install`), so the laptop path cannot drift from the
platform wiring.

## Read first

- `../stardelt-docs/docs/design/master-spec.md` — canonical spec (published at
  https://stardelt.io); when documents disagree, it wins.
- `../stardelt-docs/docs/architecture/licenses.md` — license policy.
- The **primary** development environment is the Hetzner k3s cluster
  (`alpha.dev.stardelt.io`) in `stardelt-platform/dev/hetzner-k3s`. This kind demo is
  the secondary/laptop path — same install, smaller footprint.

## Layout

- `Makefile` — wrapper targets: `up`, `smoke`, `demo`, `pf`, `down`, `clean` (+ full-profile
  `airflow-trigger`, `airflow-ui`, `superset-ui`).
- `kind/up.sh` — creates the kind cluster + namespace and delegates the install to
  `stardelt-platform` (`make install-core`, or `install` with `APP_PROFILE=full`).
- `kind/smoke.sh` — Stage-1 acceptance: Trino → Iceberg → SeaweedFS round-trip.
- `kind/gate.sh` + `kind/diagnostics.sh` — the WP-02 verification gate and its
  failure dump; driven by `.github/workflows/verify.yml` (PRs, pushes to `main`,
  `workflow_dispatch`). What it proves, and what it excludes, is in the README.
- `kind/demo.sh` — the WP-02 nightly/integration leg: full profile → Airflow
  `nyc_taxi_load` run → Trino assertions on the Iceberg table; driven by
  `.github/workflows/nightly.yml` (schedule, `workflow_dispatch`, and the
  `nightly-demo` label on a PR). Needs a 16 GB node; ~30–60 min.
- `kind/build-images.sh` — builds + `kind load`s the stardelt images (airflow, superset,
  nova; kind has no registry). Takes an optional image list.
- `dags/nyc_taxi_load.py` and `images/airflow/` — sample DAG + demo Airflow image.

Requires sibling checkouts of `stardelt-platform` and `stardelt-nova` next to this repo
(override with `STARDELT_PLATFORM_DIR` / `STARDELT_NOVA_DIR`).

## Build / check / run

```bash
make deps        # check CLI tools (calls platform's scripts/check-deps.sh)
make up          # kind cluster + install; ~10 min first run
APP_PROFILE=full make up     # add Airflow + Superset
SLIM=1 make up   # small-node overrides, forwarded to install-core
make smoke       # verify the Trino → Iceberg → SeaweedFS round-trip
make pf          # port-forwards: Trino :8081, Lakekeeper :8181
make down        # delete the kind cluster (make clean = down + docker image prune -f)
```

Cheap local checks: `bash -n kind/*.sh` (CI runs `shellcheck` on the same files).
`make up` needs Docker, kind, kubectl and helm (all checked by `make deps`).

The verification gate (same scope in CI and locally):

```bash
bash kind/gate.sh          # kind + install-core SLIM=1 + readiness + smoke + restart re-read
bash kind/diagnostics.sh   # dump pods/events/logs/helm after a red run
gh workflow run verify.yml -f platform_ref=my-branch   # the CI job, installer ref selectable
```

`kind/gate.sh` runs `STARDELT_IMAGES=none` (CI builds no stardelt images, so Nova is out of
scope) and fails loudly: no phase is skipped to keep a run green.

The nightly demo leg (full profile + NYC-taxi DAG, same script in CI and locally):

```bash
make demo                                            # bash kind/demo.sh
gh workflow run nightly.yml -f platform_ref=my-branch
gh pr edit <nr> --add-label nightly-demo             # run it on a PR
bash kind/diagnostics.sh                             # dump after a red run
```

`kind/demo.sh` builds the two images the full profile runs (`STARDELT_IMAGES="airflow superset"`),
installs with `APP_PROFILE=full`, triggers `nyc_taxi_load` with a known run id, waits (bounded) for
`success`, and asserts the resulting Iceberg table through Trino. It drops the Nova Deployment (no
image is built for it here) and asserts Superset only as ready. Budget: `DEMO_BUDGET_SECONDS`
(default 4500) — the budget is inside the workflow's 110-minute job timeout.

## Conventions

- **PR-only.** Never push to `main`; branch off `origin/main` and keep PRs small.
- **License policy: OSI-permissive only** (Apache-2.0, MIT, BSD). The single documented
  exception is OpenBao (MPL-2.0). See `../stardelt-docs/docs/architecture/licenses.md`.
- Read the master spec first. Never add install steps here: change them in
  `stardelt-platform` so both paths stay identical.

## Gotchas

- `kind/up.sh` **pins the kube-context to `kind-<cluster>`** before installing, so a
  local run cannot accidentally hit the Hetzner dev cluster. It then retargets the Nova
  deployment at the locally built `stardelt/nova:dev` image (kind has no registry).
- `make smoke` requires a finished `make up` — it execs into the Trino coordinator pod.
- Useful env vars: `APP_PROFILE` (`core` default, `full`), `SLIM=1`, `NAMESPACE`,
  `CLUSTER`, `STARDELT_IMAGES` (`build` default, `none`, or a list such as
  `"airflow superset"`), `KIND_NODE_IMAGE`, `STARDELT_PLATFORM_DIR`,
  `STARDELT_NOVA_DIR`. `kind/gate.sh` honours `GATE_BUDGET_SECONDS` (default 1800);
  `kind/demo.sh` honours `DEMO_BUDGET_SECONDS` (default 4500) and
  `DAG_WAIT_SECONDS` (default 2400).
- The full profile (`APP_PROFILE=full`) needs the `stardelt-s3-creds` Secret: the
  platform's `make install-core` applies `manifests/s3-credentials.dev.yaml` itself,
  `make install` does not — `kind/up.sh` applies it for the full profile (the
  manifest is pinned to `namespace: stardelt`).
