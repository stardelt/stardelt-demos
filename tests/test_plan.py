import pytest
from stardelt_pipelines.plan import PlanError, parse_duration, plan


def resource(**spec):
    return {"metadata": {"name": "daily"}, "spec": spec}


def sql(name, *deps):
    task = {"name": name, "kind": "sql", "sql": "SELECT 1"}
    if deps:
        task["dependsOn"] = list(deps)
    return task


def test_a_minimal_pipeline_gets_the_defaults():
    p = plan(resource(tasks=[sql("a")]), "team")
    assert p.dag_id == "team__daily"
    assert p.cron is None and p.timezone == "UTC"
    assert p.retries == 0 and p.retry_delay_s == 60
    assert p.timeout_s is None and p.max_active_runs == 1
    assert p.run_as is None and p.params == {}


def test_every_field_is_carried():
    p = plan(
        resource(
            tasks=[sql("a"), sql("b", "a")],
            schedule={"cron": "0 3 * * *", "timezone": "Europe/Berlin"},
            runAs="pipeline-analytics",
            params={"days": "1"},
            retries={"attempts": 2, "backoff": "5m"},
            timeout="2h",
            concurrency="Allow",
        ),
        "team",
    )
    assert (p.cron, p.timezone) == ("0 3 * * *", "Europe/Berlin")
    assert (p.retries, p.retry_delay_s, p.timeout_s) == (2, 300, 7200)
    assert p.max_active_runs == 16 and p.run_as == "pipeline-analytics"
    assert p.params == {"days": "1"}
    assert [t.depends_on for t in p.tasks] == [(), ("a",)]


def test_a_diamond_is_fine():
    p = plan(resource(tasks=[sql("a"), sql("b", "a"), sql("c", "a"), sql("d", "b", "c")]), "t")
    assert len(p.tasks) == 4


@pytest.mark.parametrize(
    "tasks, message",
    [
        ([], "no tasks"),
        ([sql("a"), sql("a")], "used twice"),
        ([sql("a", "ghost")], "ghost"),
        ([sql("a", "a")], "depends on `a`"),
        ([sql("a", "c"), sql("b", "a"), sql("c", "b")], "circle"),
        ([{"name": "a", "kind": "sql"}], "needs a sql statement"),
        ([{"kind": "sql", "sql": "x"}], "no name"),
    ],
)
def test_broken_shapes_are_plan_errors(tasks, message):
    with pytest.raises(PlanError, match=message):
        plan(resource(tasks=tasks), "t")


def test_a_resource_without_a_name_is_rejected():
    with pytest.raises(PlanError, match="no name"):
        plan({"spec": {"tasks": [sql("a")]}}, "t")


@pytest.mark.parametrize("text, seconds", [("30s", 30), ("5m", 300), ("2h", 7200), ("1d", 86400)])
def test_durations(text, seconds):
    assert parse_duration(text, None) == seconds


def test_duration_defaults_and_errors():
    assert parse_duration(None, 60) == 60 and parse_duration(None, None) is None
    for bad in ("5", "m5", "5 m", "1.5h", ""):
        with pytest.raises(PlanError):
            parse_duration(bad, None)
