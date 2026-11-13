"""The shop bundle (examples/shop-bundle) stays valid and stays the same shop as the dbt example (examples/shop).

No cluster and no stardelt package: the files are read and compared. `stardelt bundle validate` is the other half
of the check and runs in the README's loop.
"""

import csv
import pathlib
import re

import yaml

EXAMPLES = pathlib.Path(__file__).resolve().parent.parent / "examples"
BUNDLE = EXAMPLES / "shop-bundle"
DBT = EXAMPLES / "shop"

# The limits of a pipeline statement (Nova: one statement, at most 20 000 characters) and of a development name.
MAX_STATEMENT = 20_000
MAX_NAME = 13  # "dev-" + a sign-in name of up to 22 characters + "-" + "shop-1-bronze" stays within 40 characters

PARAM = re.compile(r"\$\{params\.([a-z][a-z0-9_]*)\}")
VAR = re.compile(r"\$\{var\.([A-Za-z0-9_]+)\}")


def pipelines():
    return [
        (path, yaml.safe_load(path.read_text(encoding="utf-8")))
        for path in sorted((BUNDLE / "resources").glob("*.yaml"))
    ]


def manifest():
    return yaml.safe_load((BUNDLE / "bundle.yaml").read_text(encoding="utf-8"))


# The statement file of every task, by the pipeline it belongs to (the numbers are the old file order).
TASK_FILES = {
    "shop-1-bronze": {"schema": "01_schema_bronze", "customers": "04_customers",
                      "products": "05_products", "order_lines": "06_order_lines"},
    "shop-2-silver": {"schema": "02_schema_silver", "stg_customers": "07_stg_customers",
                      "stg_products": "08_stg_products", "stg_order_lines": "09_stg_order_lines"},
    "shop-3-gold": {"schema": "03_schema_gold", "daily_revenue": "10_daily_revenue",
                    "customer_ltv": "11_customer_ltv", "product_mix": "12_product_mix"},
}


def tasks():
    return [(path, doc, task) for path, doc in pipelines() for task in doc["tasks"]]


def test_three_pipelines_one_per_layer():
    found = pipelines()
    assert [doc["name"] for _, doc in found] == ["shop-1-bronze", "shop-2-silver", "shop-3-gold"]
    for path, doc in found:
        assert "tasks" in doc, path.name
        for key in ("sql", "sqlFile", "notebook"):
            assert key not in doc, f"{path.name}: `tasks` cannot be combined with {key}"
        assert len(doc["tasks"]) == 4, path.name
        assert len(doc["name"]) <= MAX_NAME, f"{doc['name']} is {len(doc['name'])} characters"


def test_every_task_names_a_statement_file_that_exists():
    found = pipelines()
    names = {doc["name"]: [task["name"] for task in doc["tasks"]] for _, doc in found}
    for _, doc, task in tasks():
        expected = f"sql/{TASK_FILES[doc['name']][task['name']]}.sql"
        assert task["sqlFile"] == expected, task
        assert (BUNDLE / task["sqlFile"]).is_file(), task["sqlFile"]
    on_disk = sorted(p.name for p in (BUNDLE / "sql").glob("*.sql"))
    referenced = sorted(pathlib.Path(task["sqlFile"]).name for _, _, task in tasks())
    assert referenced == on_disk, "every statement file belongs to exactly one task"
    assert sorted(names) == sorted(TASK_FILES)


def test_dependencies_name_a_task_of_the_same_pipeline():
    for _, doc, task in tasks():
        rest = {other["name"] for other in doc["tasks"]} - {task["name"]}
        depends = task.get("dependsOn", [])
        assert len(depends) <= 8 and len(set(depends)) == len(depends), task
        assert set(depends) <= rest, f"{doc['name']}/{task['name']}: {set(depends) - rest}"
        if task["name"] == "schema":
            assert depends == [], "schema must run first"
        else:
            assert depends == ["schema"], f"{doc['name']}/{task['name']}"


def test_a_statement_uses_only_the_params_its_pipeline_sets():
    for _, doc in pipelines():
        used = set()
        for task in doc["tasks"]:
            sql = (BUNDLE / task["sqlFile"]).read_text(encoding="utf-8")
            task_used = set(PARAM.findall(sql))
            assert task_used <= set(doc.get("params", {})), (
                f"{doc['name']}/{task['name']} uses {task_used - set(doc.get('params', {}))} without setting it"
            )
            used |= task_used
        assert set(doc.get("params", {})) <= used, f"{doc['name']} sets a param no task uses"


def test_a_statement_is_one_statement_within_the_limit():
    for _, doc, task in tasks():
        sql = (BUNDLE / task["sqlFile"]).read_text(encoding="utf-8")
        code = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--")).strip().rstrip(";")
        assert ";" not in code, f"{doc['name']}/{task['name']} holds more than one statement"
        assert 0 < len(sql) <= MAX_STATEMENT, f"{doc['name']}/{task['name']}: {len(sql)} characters"
        assert "\0" not in sql


def test_the_manifest_declares_every_variable_and_every_target_sets_it():
    declared = manifest()["variables"]
    used = set()
    for path, _ in pipelines():
        used |= set(VAR.findall(path.read_text(encoding="utf-8")))
    assert used == set(declared), f"used {sorted(used)}, declared {sorted(declared)}"
    for name, target in manifest()["targets"].items():
        assert set(target["variables"]) == set(declared), name
    assert manifest()["targets"]["dev"]["variables"]["schema"] != manifest()["targets"]["prod"]["variables"]["schema"]
    assert manifest()["targets"]["prod"]["runAs"] == "pipeline-" + manifest()["bundle"]["ownerGroup"]


def test_the_seed_statements_hold_exactly_the_rows_of_the_dbt_seeds():
    for table, seed in (("04_customers", "customers"), ("05_products", "products")):
        with open(DBT / "seeds" / f"{seed}.csv", newline="") as handle:
            rows = list(csv.DictReader(handle))
        sql = (BUNDLE / "sql" / f"{table}.sql").read_text(encoding="utf-8")
        values = [line for line in sql.splitlines() if line.startswith("    (")]
        assert len(values) == len(rows), f"{table}: {len(values)} rows, the seed has {len(rows)}"
        first, last = rows[0], rows[-1]
        assert f"({first['id']}, '{first['name']}'" in values[0]
        assert f"({last['id']}, '{last['name']}'" in values[-1]


def test_the_generated_lines_use_the_same_hashes_as_the_dbt_model():
    def hashes(text):
        return sorted(line.strip() for line in text.splitlines() if "xxhash64" in line)

    model = (DBT / "models" / "bronze" / "order_lines.sql").read_text(encoding="utf-8")
    statement = (BUNDLE / "sql" / "06_order_lines.sql").read_text(encoding="utf-8")
    assert len(hashes(statement)) == 4
    assert hashes(statement) == hashes(model)


def test_the_layers_are_the_catalogs_of_the_dbt_example():
    targets = {
        "04_customers": "bronze", "05_products": "bronze", "06_order_lines": "bronze",
        "07_stg_customers": "silver", "08_stg_products": "silver", "09_stg_order_lines": "silver",
        "10_daily_revenue": "gold", "11_customer_ltv": "gold", "12_product_mix": "gold",
    }
    for stem, catalog in targets.items():
        sql = (BUNDLE / "sql" / f"{stem}.sql").read_text(encoding="utf-8")
        assert re.search(rf"create or replace (table|view) {catalog}\.\$\{{params\.schema\}}\.", sql), stem
