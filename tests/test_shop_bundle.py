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
MAX_NAME = 17  # "dev-" + a sign-in name of up to 18 characters + "-" + the name stays within 40 characters

PARAM = re.compile(r"\$\{params\.([a-z][a-z0-9_]*)\}")
VAR = re.compile(r"\$\{var\.([A-Za-z0-9_]+)\}")


def pipelines():
    return [
        (path, yaml.safe_load(path.read_text(encoding="utf-8")))
        for path in sorted((BUNDLE / "resources").glob("*.yaml"))
    ]


def manifest():
    return yaml.safe_load((BUNDLE / "bundle.yaml").read_text(encoding="utf-8"))


def test_every_pipeline_names_a_statement_file_that_exists():
    found = pipelines()
    assert len(found) == 12
    for path, doc in found:
        statement = BUNDLE / doc["sqlFile"]
        assert statement.is_file(), f"{path.name}: {doc['sqlFile']} is missing"
        assert statement.stem == path.stem, f"{path.name} and {statement.name} do not belong together"
    on_disk = sorted(p.name for p in (BUNDLE / "sql").glob("*.sql"))
    assert on_disk == sorted(f"{path.stem}.sql" for path, _ in found), "a statement file belongs to no pipeline"


def test_names_are_short_unique_and_run_in_file_order():
    names = [doc["name"] for _, doc in pipelines()]
    assert len(set(names)) == len(names)
    for (path, _), name in zip(pipelines(), names):
        assert re.fullmatch(r"shop-\d\d-[a-z-]+", name), name
        assert len(name) <= MAX_NAME, f"{name} is {len(name)} characters"
        assert name.split("-")[1] == path.stem.split("_")[0], f"{name} does not carry the number of {path.name}"
    assert names == sorted(names), "bundle run starts the pipelines in name order: the order of the files must be the same"


def test_a_statement_uses_only_the_params_its_pipeline_sets():
    for _, doc in pipelines():
        sql = (BUNDLE / doc["sqlFile"]).read_text(encoding="utf-8")
        used = set(PARAM.findall(sql))
        assert used <= set(doc.get("params", {})), f"{doc['name']} uses {used - set(doc.get('params', {}))} without setting it"
        assert set(doc.get("params", {})) <= used, f"{doc['name']} sets a param its statement never uses"


def test_a_statement_is_one_statement_within_the_limit():
    for _, doc in pipelines():
        sql = (BUNDLE / doc["sqlFile"]).read_text(encoding="utf-8")
        code = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--")).strip().rstrip(";")
        assert ";" not in code, f"{doc['name']} holds more than one statement"
        assert 0 < len(sql) <= MAX_STATEMENT, f"{doc['name']}: {len(sql)} characters"
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
