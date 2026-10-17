# AGENTS.md — stardelt-demos

Demo/onboarding path for stardelt: stands up the platform on a single-node kind
cluster for a laptop, with no cloud account. It owns no install logic — after creating
the cluster it delegates to the `stardelt-platform` Makefile (`make install-core` /
`make install`), so the laptop path cannot drift from the platform wiring.

## Read first

- `../stardelt-docs/docs/design/master-spec.md` — canonical spec (published at
  https://stardelt.io); when documents disagree, it wins.
- `../stardelt-docs/docs/architecture/licenses.md` — license policy.
- The **primary** development environment is the Hetzner k3s cluster
  (`alpha.dev.stardelt.io`) in `stardelt-platform/dev/hetzner-k3s`. This kind demo is
  the secondary/laptop path — same install, smaller footprint.

## Layout

- `Makefile` — thin wrapper: `up`, `smoke`, `pf`, `down`, `clean`, plus full-profile
  extras (`airflow-trigger`, `airflow-ui`, `superset-ui`).
- `kind/up.sh` — creates the kind cluster + namespace, builds/loads images, then runs
  `make install-core` (or `install` with `APP_PROFILE=full`) in `stardelt-platform`.
- `kind/smoke.sh` — Stage-1 acceptance: write + read round-trip through
  Trino → Iceberg → SeaweedFS.
- `kind/build-images.sh` — builds the stardelt-owned images (airflow, superset, nova)
  and `kind load`s them (kind has no registry).
- `dags/nyc_taxi_load.py` — sample pipeline, full profile only.
- `images/airflow/` — demo Airflow image; CI hadolints it.

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
make down        # delete the kind cluster
make clean       # make down + docker image prune -f
```

Cheap local checks: `bash -n kind/*.sh` (CI shellchecks the same files:
`shellcheck kind/up.sh kind/build-images.sh kind/smoke.sh`). `make up` needs Docker,
kind, kubectl and helm (all checked by `make deps`).

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
  `CLUSTER`, `STARDELT_PLATFORM_DIR`, `STARDELT_NOVA_DIR`.
