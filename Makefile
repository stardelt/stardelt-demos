SHELL   := /bin/bash
CLUSTER     ?= stardelt
NAMESPACE   ?= stardelt
# APP_PROFILE=core runs the platform's install-core; 'full' adds Airflow + Superset.
APP_PROFILE ?= core
SLIM        ?=

# Resolve sibling-repo paths (override with env vars if layout differs).
PLATFORM_DIR ?= $${STARDELT_PLATFORM_DIR:-../stardelt-platform}

.PHONY: help deps up down pf logs smoke build-images airflow-trigger airflow-ui superset-ui demo clean

help:
	@echo "stardelt demos — local kind cluster"
	@echo
	@echo "Targets:"
	@echo "  deps             — check required CLI tools are installed"
	@echo "  up               — create kind cluster + install stardelt (delegates to stardelt-platform)"
	@echo "  smoke            — run the Stage 1 smoke query against Trino"
	@echo "  demo             — full profile + NYC-taxi DAG → Trino assertion (the nightly leg)"
	@echo "  build-images     — build + kind-load the stardelt-owned container images"
	@echo "  airflow-trigger  — unpause and trigger the nyc_taxi_load DAG"
	@echo "  airflow-ui       — port-forward Airflow API server to localhost:8088"
	@echo "  superset-ui      — port-forward Superset to localhost:8089 (admin; password: kubectl -n stardelt get secret superset-admin -o jsonpath='{.data.password}' | base64 -d)"
	@echo "  pf               — open port-forwards for Trino (8081) and Lakekeeper (8181)"
	@echo "  logs             — tail logs across the stardelt namespace"
	@echo "  down             — delete the kind cluster"
	@echo "  clean            — down + remove cached docker images"
	@echo
	@echo "Env vars:"
	@echo "  STARDELT_PLATFORM_DIR  — path to stardelt-platform repo (default: ../stardelt-platform)"
	@echo "  STARDELT_NOVA_DIR      — path to stardelt-nova repo    (default: ../stardelt-nova)"
	@echo "  APP_PROFILE            — 'core' (default) = install-core, 'full' = install"
	@echo "  SLIM                   — SLIM=1 forwards small-node overrides to install-core"

deps:
	@$(PLATFORM_DIR)/scripts/check-deps.sh

up:
	@CLUSTER=$(CLUSTER) NAMESPACE=$(NAMESPACE) APP_PROFILE=$(APP_PROFILE) SLIM=$(SLIM) bash kind/up.sh

down:
	kind delete cluster --name $(CLUSTER)

pf:
	@bash $(PLATFORM_DIR)/scripts/port-forwards.sh

logs:
	kubectl -n $(NAMESPACE) logs -l app.kubernetes.io/part-of=stardelt --all-containers --tail=100 -f

smoke:
	@bash kind/smoke.sh

# The nightly demo leg: full profile + the NYC-taxi DAG + the Trino assertion.
# Long (it downloads and loads a year of taxi trips); needs a 16 GB node.
demo:
	@bash kind/demo.sh

build-images:
	@bash kind/build-images.sh

airflow-trigger:
	kubectl -n $(NAMESPACE) exec deploy/airflow-scheduler -c scheduler -- airflow dags unpause nyc_taxi_load
	kubectl -n $(NAMESPACE) exec deploy/airflow-scheduler -c scheduler -- airflow dags trigger nyc_taxi_load

airflow-ui:
	kubectl -n $(NAMESPACE) port-forward svc/airflow-api-server 8088:8080

superset-ui:
	@echo "Superset login: admin, password from: kubectl -n stardelt get secret superset-admin -o jsonpath='{.data.password}' | base64 -d  — http://localhost:8089"
	kubectl -n $(NAMESPACE) port-forward svc/superset 8089:8088

clean: down
	docker image prune -f
