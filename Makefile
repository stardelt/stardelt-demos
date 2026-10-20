SHELL       := /bin/bash
NAMESPACE   ?= stardelt

# Resolve sibling-repo paths (override with env vars if layout differs).
PLATFORM_DIR ?= $${STARDELT_PLATFORM_DIR:-../stardelt-platform}

.PHONY: help deps pf logs airflow-trigger airflow-ui superset-ui

help:
	@echo "stardelt demos — helpers for a running stardelt cluster"
	@echo
	@echo "The platform is installed from stardelt-platform on a real node;"
	@echo "see https://stardelt.io/getting-started/hetzner-k3s"
	@echo
	@echo "Targets:"
	@echo "  deps             — check required CLI tools are installed"
	@echo "  pf               — open port-forwards: Nova (8080), Trino (8081), Lakekeeper (8181), Superset (8089)"
	@echo "  logs             — tail logs across the stardelt namespace"
	@echo "  airflow-trigger  — unpause and trigger the nyc_taxi_load DAG"
	@echo "  airflow-ui       — port-forward Airflow API server to localhost:8088"
	@echo "  superset-ui      — port-forward Superset to localhost:8089"
	@echo
	@echo "Env vars:"
	@echo "  NAMESPACE              — namespace to target (default: stardelt)"
	@echo "  STARDELT_PLATFORM_DIR  — path to the stardelt-platform repo (default: ../stardelt-platform)"

deps:
	@$(PLATFORM_DIR)/scripts/check-deps.sh

pf:
	@bash $(PLATFORM_DIR)/scripts/port-forwards.sh

logs:
	kubectl -n $(NAMESPACE) logs -l app.kubernetes.io/part-of=stardelt --all-containers --tail=100 -f

airflow-trigger:
	kubectl -n $(NAMESPACE) exec deploy/airflow-scheduler -c scheduler -- airflow dags unpause nyc_taxi_load
	kubectl -n $(NAMESPACE) exec deploy/airflow-scheduler -c scheduler -- airflow dags trigger nyc_taxi_load

airflow-ui:
	kubectl -n $(NAMESPACE) port-forward svc/airflow-api-server 8088:8080

superset-ui:
	@echo "Superset login: admin, password from: kubectl -n stardelt get secret superset-admin -o jsonpath='{.data.password}' | base64 -d  — http://localhost:8089"
	kubectl -n $(NAMESPACE) port-forward svc/superset 8089:8088
