"""Turn a `Pipeline` resource (stardelt.io/v1alpha1) into a plan the loader builds a DAG from.

Pure: no Airflow, no network, no clock. The rules mirror `PipelineSpec::validate` in the
operator (unique task names, known dependencies, no cycles, a `sql` task has a statement),
because the API server's schema rules cannot see a dependency cycle.

The same holds for the principal: a pipeline that names one is checked against the
`ServicePrincipal` resources of its namespace (the caller passes them in), so no pipeline can
run as another group's principal.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

_DURATION = re.compile(r"^(\d+)([smhd])$")
_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
# `Replace` has no Airflow equivalent; it is treated like `Forbid` (one run at a time).
_ACTIVE_RUNS = {"Allow": 16, "Forbid": 1, "Replace": 1}
DEFAULT_RETRY_DELAY_S = 60
# The platform's shared pipeline principal (phase J0). It belongs to no group, so it is never the
# principal a pipeline with an `ownerGroup` may name.
DEFAULT_PRINCIPAL = "pipeline-runner"


class PlanError(ValueError):
    """The resource cannot become a DAG; the message names the reason."""


@dataclass(frozen=True)
class PrincipalRef:
    """The `ServicePrincipal` a pipeline runs as: its name and the Secret holding its credentials."""

    name: str
    secret_name: str


@dataclass(frozen=True)
class TaskPlan:
    name: str
    kind: str
    depends_on: tuple[str, ...]
    sql: str | None
    warehouse: str | None


@dataclass(frozen=True)
class PipelinePlan:
    dag_id: str
    cron: str | None
    timezone: str
    retries: int
    retry_delay_s: int
    timeout_s: int | None
    max_active_runs: int
    params: dict[str, str]
    run_as: str | None
    owner_group: str | None
    principal: PrincipalRef | None
    tasks: tuple[TaskPlan, ...]


def parse_duration(text: str | None, default: int | None) -> int | None:
    """`5m` -> 300. `None` gives `default`. Anything else than digits plus s, m, h or d is an error."""
    if text is None:
        return default
    match = _DURATION.match(text)
    if not match:
        raise PlanError(f"`{text}` is not a duration (use for example 30s, 5m, 2h, 1d)")
    return int(match.group(1)) * _SECONDS[match.group(2)]


def dag_id(namespace: str, name: str) -> str:
    return f"{namespace}__{name}"


def plan(resource: dict, namespace: str, principals: dict[str, dict] | None = None) -> PipelinePlan:
    """Validate `resource` and return its plan, or raise `PlanError`.

    `principals` are the `ServicePrincipal` resources of the namespace by name; `None` means the
    namespace has none, so a pipeline that names one is refused.
    """
    name = resource.get("metadata", {}).get("name")
    if not name:
        raise PlanError("the resource has no name")
    spec = resource.get("spec") or {}

    raw_tasks = spec.get("tasks") or []
    if not raw_tasks:
        raise PlanError("the pipeline has no tasks")
    tasks: list[TaskPlan] = []
    seen: set[str] = set()
    for raw in raw_tasks:
        task_name = raw.get("name")
        if not task_name:
            raise PlanError("a task has no name")
        if task_name in seen:
            raise PlanError(f"task name `{task_name}` is used twice")
        seen.add(task_name)
        kind = raw.get("kind")
        if kind == "sql" and not raw.get("sql"):
            raise PlanError(f"sql task `{task_name}` needs a sql statement")
        tasks.append(
            TaskPlan(
                name=task_name,
                kind=kind,
                depends_on=tuple(raw.get("dependsOn") or ()),
                sql=raw.get("sql"),
                warehouse=raw.get("warehouse"),
            )
        )
    for task in tasks:
        for dep in task.depends_on:
            if dep == task.name or dep not in seen:
                raise PlanError(
                    f"task `{task.name}` depends on `{dep}`, which is not another task of this pipeline"
                )
    _reject_cycles(tasks)

    schedule = spec.get("schedule") or {}
    retries = spec.get("retries") or {}
    return PipelinePlan(
        dag_id=dag_id(namespace, name),
        cron=schedule.get("cron"),
        timezone=schedule.get("timezone") or "UTC",
        retries=int(retries.get("attempts", 0)),
        retry_delay_s=parse_duration(retries.get("backoff"), DEFAULT_RETRY_DELAY_S),
        timeout_s=parse_duration(spec.get("timeout"), None),
        max_active_runs=_ACTIVE_RUNS[spec.get("concurrency") or "Forbid"],
        params=dict(spec.get("params") or {}),
        run_as=spec.get("runAs"),
        owner_group=spec.get("ownerGroup"),
        principal=_principal(spec, principals or {}),
        tasks=tuple(tasks),
    )


def _principal(spec: dict, principals: dict[str, dict]) -> PrincipalRef | None:
    """Which principal the tasks run as; `None` is the platform's default.

    Every mixture that would let one group run as another group's principal is refused: the
    principal must be a `ServicePrincipal` of the namespace, owned by the pipeline's group and
    ready, and its credentials Secret must exist.
    """
    run_as = spec.get("runAs")
    owner_group = spec.get("ownerGroup")
    if run_as is None and owner_group is None:
        return None
    if run_as is None:
        raise PlanError(
            "a pipeline with an owner needs runAs: the principal of its group "
            f"(pipeline-{owner_group})"
        )
    if owner_group is None:
        raise PlanError("runAs needs ownerGroup: say which group owns this pipeline")
    # The shared default principal belongs to no group, so naming it can never make it one's own
    # principal — even if a resource of that name existed.
    resource = None if run_as == DEFAULT_PRINCIPAL else principals.get(run_as)
    if resource is None:
        raise PlanError(f"runAs {run_as} is not a ServicePrincipal of this namespace")
    principal_group = (resource.get("spec") or {}).get("ownerGroup")
    if principal_group != owner_group:
        raise PlanError(
            f"runAs {run_as} belongs to the group {principal_group}, "
            f"this pipeline is owned by {owner_group}"
        )
    status = resource.get("status") or {}
    if status.get("ready") is not True:
        raise PlanError(f"runAs {run_as} is not ready: {_ready_message(status)}")
    secret_name = status.get("secretName")
    if not secret_name:
        raise PlanError(f"runAs {run_as} has no credentials Secret yet")
    return PrincipalRef(run_as, secret_name)


def _ready_message(status: dict) -> str:
    """The message of the principal's `Ready` condition, or the sentence for an empty status."""
    for condition in status.get("conditions") or ():
        if condition.get("type") == "Ready" and condition.get("message"):
            return condition["message"]
    return "no status yet"


def _reject_cycles(tasks: list[TaskPlan]) -> None:
    """Kahn's algorithm: whatever cannot be ordered lies on or behind a cycle."""
    waiting = {t.name: len(t.depends_on) for t in tasks}
    ready = [n for n, count in waiting.items() if count == 0]
    done = 0
    while ready:
        current = ready.pop()
        done += 1
        for t in tasks:
            if current in t.depends_on:
                waiting[t.name] -= 1
                if waiting[t.name] == 0:
                    ready.append(t.name)
    if done != len(tasks):
        raise PlanError("the tasks depend on each other in a circle")
