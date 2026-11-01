"""The shop example (examples/shop): its seeds, its generator and its layers agree with each other. No dbt, no cluster."""

import csv
import re
from pathlib import Path

import yaml

SHOP = Path(__file__).resolve().parents[1] / "examples" / "shop"


def rows(name):
    with open(SHOP / "seeds" / name, newline="") as handle:
        return list(csv.DictReader(handle))


def test_the_seeds_are_small_and_have_the_ids_the_generator_draws_from():
    customers, products = rows("customers.csv"), rows("products.csv")
    assert [int(r["id"]) for r in customers] == list(range(1, 201))
    assert [int(r["id"]) for r in products] == list(range(1, 31))
    assert all(re.fullmatch(r"\d+\.\d\d", r["unit_price"]) and float(r["unit_price"]) > 0 for r in products)
    assert {r["segment"] for r in customers} == {"retail", "business", "enterprise"}
    assert sum(p.stat().st_size for p in (SHOP / "seeds").glob("*.csv")) < 20_000


def test_the_generator_draws_customers_and_products_from_exactly_the_ids_that_exist():
    sql = (SHOP / "models" / "bronze" / "order_lines.sql").read_text()
    found = re.findall(r"mod\(mod\(from_big_endian_64\(xxhash64\(to_utf8\('(\w)'[^)]*\)\)\)\), (\d+)\) \+ (\d+), (\d+)\)", sql)
    by_letter = {letter: (int(a), int(b), int(c)) for letter, a, b, c in found}
    assert by_letter["c"] == (200, 200, 200) and by_letter["p"] == (30, 30, 30)
    assert by_letter["q"] == (5, 5, 5) and by_letter["d"] == (365, 365, 365)


def test_the_layers_are_the_catalogs_and_the_scale_is_a_multiple_of_a_thousand():
    project = yaml.safe_load((SHOP / "dbt_project.yml").read_text())
    models = project["models"]["shop"]
    assert {layer: models[layer]["+database"] for layer in ("bronze", "staging", "marts")} == {"bronze": "bronze", "staging": "silver", "marts": "gold"}
    assert project["seeds"]["shop"]["+database"] == "bronze"
    assert project["vars"]["scale"] % 1000 == 0 and project["vars"]["scale"] >= 1000


def test_every_model_is_described_and_tested():
    described = {m["name"]: m for m in yaml.safe_load((SHOP / "models" / "schema.yml").read_text())["models"]}
    on_disk = {p.stem for p in (SHOP / "models").rglob("*.sql")}
    assert set(described) == on_disk
    for name, model in described.items():
        assert model.get("description"), name
        assert any(column.get("data_tests") for column in model["columns"]), name


def test_the_profile_reads_the_token_from_the_environment_and_holds_no_secret():
    text = (SHOP / "profiles.yml").read_text()
    assert "env_var('DBT_TRINO_JWT'" in text
    assert "eyJ" not in text and "Bearer" not in text
