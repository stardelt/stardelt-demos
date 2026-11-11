"""A DAG is scoped to its owner group's Airflow role (platform#477)."""
from pathlib import Path

from stardelt_pipelines.access import (
    DEFAULT_OWNER,
    GROUP_PERMISSIONS,
    ROLE_PREFIX,
    dag_access,
    group_role,
)

LOADER = Path(__file__).resolve().parents[1] / "dags" / "pipeline_loader.py"


def test_the_role_of_a_group_is_the_one_the_platform_creates():
    assert ROLE_PREFIX == "stardelt-group-"
    assert group_role("mobility-analytics") == "stardelt-group-mobility-analytics"


def test_an_owner_group_gets_read_on_the_dag_and_on_its_runs():
    access = dag_access("mobility-analytics")
    assert access["access_control"] == {
        "stardelt-group-mobility-analytics": {
            "DAGs": {"can_read"},
            "DAG Runs": {"can_read"},
        }
    }
    assert access["owner"] == "mobility-analytics"
    assert access["tags"] == ["group:mobility-analytics"]


def test_a_group_never_gets_delete_or_a_global_permission():
    granted = {action for actions in GROUP_PERMISSIONS.values() for action in actions}
    assert "can_delete" not in granted
    assert set(GROUP_PERMISSIONS) == {"DAGs", "DAG Runs"}
    for resource in ("DAGs", "DAG Runs"):
        assert "can_edit" not in GROUP_PERMISSIONS[resource]
        assert "can_create" not in GROUP_PERMISSIONS[resource]


def test_a_pipeline_without_an_owner_group_names_no_role():
    for owner in (None, ""):
        access = dag_access(owner)
        assert access["access_control"] is None
        assert access["owner"] == DEFAULT_OWNER
        assert access["tags"] == []


def test_two_calls_do_not_share_their_permission_sets():
    first = dag_access("a")["access_control"]["stardelt-group-a"]["DAGs"]
    first.add("can_delete")
    assert "can_delete" not in dag_access("b")["access_control"]["stardelt-group-b"]["DAGs"]
    assert "can_delete" not in GROUP_PERMISSIONS["DAGs"]


def test_the_loader_gives_every_dag_the_access_control_and_the_owner_of_its_group():
    source = LOADER.read_text(encoding="utf-8")
    assert "from stardelt_pipelines.access import dag_access" in source
    assert "access = dag_access(p.owner_group)" in source
    assert "access_control=access[\"access_control\"]," in source
    assert "\"owner\": access[\"owner\"]," in source
    assert "*access[\"tags\"]" in source
