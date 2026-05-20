SHELL   := /bin/bash
CLUSTER   ?= stardelt
NAMESPACE ?= stardelt

# Resolve sibling-repo paths (override with env vars if layout differs).
PLATFORM_DIR ?= $${STARDELT_PLATFORM_DIR:-../stardelt-platform}

.PHONY: help deps up down pf logs smoke build-images airflow-trigger airflow-ui superset-ui clean

help:
	@echo "Stardelt demos — local kind cluster"
	@echo
	@echo "Targets:"
	@echo "  deps             — check required CLI tools are installed"
	@echo "  up               — create kind cluster + install the full Stardelt stack (~10 min)"
	@echo "  smoke            — run the Stage 1 smoke query against Trino"
	@echo "  build-images     — build + kind-load the stardelt-owned container images"
	@echo "  airflow-trigger  — unpause and trigger the nyc_taxi_load DAG"
	@echo "  airflow-ui       — port-forward Airflow API server to localhost:8088"
	@echo "  superset-ui      — port-forward Superset to localhost:8089 (admin/admin)"
	@echo "  pf               — open port-forwards for Trino (8081) and Lakekeeper (8181)"
	@echo "  logs             — tail logs across the stardelt namespace"
	@echo "  down             — delete the kind cluster"
	@echo "  clean            — down + remove cached docker images"
	@echo
	@echo "Env vars:"
	@echo "  STARDELT_PLATFORM_DIR  — path to stardelt-platform repo (default: ../stardelt-platform)"
	@echo "  STARDELT_NOVA_DIR      — path to stardelt-nova repo    (default: ../stardelt-nova)"

deps:
	@$(PLATFORM_DIR)/scripts/check-deps.sh

up:
	@bash kind/up.sh

down:
	kind delete cluster --name $(CLUSTER)

pf:
	@bash $(PLATFORM_DIR)/scripts/port-forwards.sh

logs:
	kubectl -n $(NAMESPACE) logs -l app.kubernetes.io/part-of=stardelt --all-containers --tail=100 -f

smoke:
	@bash kind/smoke.sh

build-images:
	@bash kind/build-images.sh

airflow-trigger:
	kubectl -n $(NAMESPACE) exec deploy/airflow-scheduler -c scheduler -- airflow dags unpause nyc_taxi_load
	kubectl -n $(NAMESPACE) exec deploy/airflow-scheduler -c scheduler -- airflow dags trigger nyc_taxi_load

airflow-ui:
	kubectl -n $(NAMESPACE) port-forward svc/airflow-api-server 8088:8080

superset-ui:
	@echo "Superset login: admin / admin  — http://localhost:8089"
	kubectl -n $(NAMESPACE) port-forward svc/superset 8089:8088

clean: down
	docker image prune -f
