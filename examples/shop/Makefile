# Targets for the shop example. Needs dbt-core and dbt-trino (see README.md).
DBT   ?= dbt
SCALE ?= 200000
FULL  ?=
ARGS   = --profiles-dir . --project-dir .

.PHONY: help check demo teardown

help:
	@echo "check     parse the project (no cluster needed)"
	@echo "demo      seeds, generated lines, silver views, gold marts and all tests (needs DBT_TRINO_HOST and DBT_TRINO_JWT)"
	@echo "teardown  drop everything the example created in bronze, silver and gold"
	@echo "SCALE=N   order lines to generate (multiple of 1000, default 200000)"
	@echo "FULL=1    rebuild the incremental gold table too; needed whenever SCALE differs from the earlier run"

check:
	$(DBT) parse $(ARGS)

demo:
	$(DBT) build $(ARGS) --vars '{scale: $(SCALE)}' $(if $(FULL),--full-refresh,)

teardown:
	$(DBT) run-operation teardown $(ARGS)
