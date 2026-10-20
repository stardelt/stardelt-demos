"""One Airflow DAG per `Pipeline` resource in this namespace (scheduled jobs, phase J0).

The resource is the source of truth; this file only reads it (one LIST per parse, through a
read-only ServiceAccount) and builds DAGs from it, so nobody edits DAGs by hand. The pure
parts live in `stardelt_pipelines` and are unit-tested without Airflow.

A pipeline with an `ownerGroup` runs as that group's `ServicePrincipal` (`runAs`): the plan checks
it against the namespace's `ServicePrincipal` resources, and each of the pipeline's task pods gets
the principal's credentials through a `secretKeyRef` (see `stardelt_pipelines.principal`). The
loader itself never reads a Secret.

Environment of the dag-processor and the task pods:
  PIPELINE_TRINO_URL        base URL of the query engine, e.g. https://trino:8443
  PIPELINE_TRINO_CA_FILE    CA bundle that signed the engine's certificate (optional)
  PIPELINE_TOKEN_URL        token endpoint of the identity provider
  PIPELINE_CLIENT_ID        the default principal, `pipeline-runner` (a pipeline with a group
                            principal overrides it in its own task pods)
  PIPELINE_CLIENT_SECRET    its secret (from a Kubernetes Secret, never in a file)
  PIPELINE_API_GROUP / PIPELINE_API_PLURAL   defaults `stardelt.io` / `pipelines` (tests override)
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import httpx
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.timetables.trigger import CronTriggerTimetable

# The support package sits next to this file. Airflow does not put the DAGs folder on the
# import path in every process (dag-processor, task pod, tests), so do it here.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from stardelt_pipelines import trino  # noqa: E402
from stardelt_pipelines.expand import expand  # noqa: E402
from stardelt_pipelines.plan import DEFAULT_PRINCIPAL, PlanError, plan  # noqa: E402
from stardelt_pipelines.principal import principal_pod_override  # noqa: E402

log = logging.getLogger(__name__)

API_GROUP = os.environ.get("PIPELINE_API_GROUP", "stardelt.io")
API_PLURAL = os.environ.get("PIPELINE_API_PLURAL", "pipelines")
API_VERSION = "v1alpha1"
PRINCIPAL_PLURAL = "serviceprincipals"
NAMESPACE_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


def _namespace() -> str:
    with open(NAMESPACE_FILE) as handle:
        return handle.read().strip()


def _list_resources(namespace: str) -> tuple[list[dict], dict[str, dict]]:
    """Every `Pipeline` of the namespace, plus its `ServicePrincipal` resources by name."""
    from kubernetes import client, config

    config.load_incluster_config()
    api = client.CustomObjectsApi()
    pipelines = api.list_namespaced_custom_object(
        API_GROUP, API_VERSION, namespace, API_PLURAL
    ).get("items", [])
    principals = api.list_namespaced_custom_object(
        API_GROUP, API_VERSION, namespace, PRINCIPAL_PLURAL
    ).get("items", [])
    return pipelines, {
        resource["metadata"]["name"]: resource
        for resource in principals
        if resource.get("metadata", {}).get("name")
    }


def _run_task(task: dict, params: dict, **context) -> None:
    """Executes inside the task pod. `sql` tasks run as the service principal, never logging it."""
    if task["kind"] != "sql":
        # A silent no-op for a kind that has no runtime yet would hide that nothing ran.
        raise NotImplementedError(f"task kind `{task['kind']}` has no runtime yet")
    dag_run = context["dag_run"]
    run_date = (context.get("logical_date") or dag_run.run_after).date().isoformat()
    sql = expand(task["sql"], run_date=run_date, run_id=dag_run.run_id, params=params)

    ca_file = os.environ.get("PIPELINE_TRINO_CA_FILE")
    with httpx.Client(verify=ca_file or True, timeout=60.0) as http:
        token = trino.client_credentials_token(
            os.environ["PIPELINE_TOKEN_URL"],
            os.environ.get("PIPELINE_CLIENT_ID", DEFAULT_PRINCIPAL),
            os.environ["PIPELINE_CLIENT_SECRET"],
            client=http,
        )
        outcome = trino.run_statement(
            sql, base_url=os.environ["PIPELINE_TRINO_URL"], token=token, client=http
        )
    log.info(
        "task %s finished: query %s, %s rows, update count %s",
        task["name"],
        outcome.query_id,
        outcome.rows,
        outcome.update_count,
    )


def _build_dag(p) -> DAG:
    dag = DAG(
        dag_id=p.dag_id,
        schedule=CronTriggerTimetable(p.cron, timezone=p.timezone) if p.cron else None,
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        is_paused_upon_creation=False,
        max_active_runs=p.max_active_runs,
        dagrun_timeout=timedelta(seconds=p.timeout_s) if p.timeout_s else None,
        default_args={"retries": p.retries, "retry_delay": timedelta(seconds=p.retry_delay_s)},
        tags=["stardelt", "pipeline"],
    )
    # The credentials of a group's principal reach the pods of that pipeline only: every other
    # DAG keeps the default principal the deployment sets.
    executor_config = (
        principal_pod_override(p.principal.secret_name) if p.principal is not None else None
    )
    operators = {}
    for t in p.tasks:
        operators[t.name] = PythonOperator(
            task_id=t.name,
            python_callable=_run_task,
            op_kwargs={
                "task": {"name": t.name, "kind": t.kind, "sql": t.sql},
                "params": p.params,
            },
            executor_config=executor_config,
            dag=dag,
        )
    for t in p.tasks:
        for dep in t.depends_on:
            operators[dep] >> operators[t.name]
    return dag


namespace = _namespace()
pipelines, principals = _list_resources(namespace)
for resource in pipelines:
    name = resource.get("metadata", {}).get("name", "?")
    try:
        pipeline_plan = plan(resource, namespace, principals)
        globals()[pipeline_plan.dag_id] = _build_dag(pipeline_plan)
    except PlanError as problem:
        log.warning("pipeline %s/%s is not loaded: %s", namespace, name, problem)
