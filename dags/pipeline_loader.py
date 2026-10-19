"""One Airflow DAG per `Pipeline` resource in this namespace (scheduled jobs, phase J0).

The resource is the source of truth; this file only reads it (one LIST per parse, through a
read-only ServiceAccount) and builds DAGs from it, so nobody edits DAGs by hand. The pure
parts live in `stardelt_pipelines` and are unit-tested without Airflow.

Environment of the dag-processor and the task pods:
  PIPELINE_TRINO_URL        base URL of the query engine, e.g. https://trino:8443
  PIPELINE_TRINO_CA_FILE    CA bundle that signed the engine's certificate (optional)
  PIPELINE_TOKEN_URL        token endpoint of the identity provider
  PIPELINE_CLIENT_ID        the service principal, default `pipeline-runner`
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
from stardelt_pipelines.plan import PlanError, plan  # noqa: E402

log = logging.getLogger(__name__)

API_GROUP = os.environ.get("PIPELINE_API_GROUP", "stardelt.io")
API_PLURAL = os.environ.get("PIPELINE_API_PLURAL", "pipelines")
API_VERSION = "v1alpha1"
NAMESPACE_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
DEFAULT_PRINCIPAL = "pipeline-runner"


def _namespace() -> str:
    with open(NAMESPACE_FILE) as handle:
        return handle.read().strip()


def _list_resources(namespace: str) -> list[dict]:
    from kubernetes import client, config

    config.load_incluster_config()
    api = client.CustomObjectsApi()
    return api.list_namespaced_custom_object(API_GROUP, API_VERSION, namespace, API_PLURAL).get(
        "items", []
    )


def _run_task(task: dict, params: dict, **context) -> None:
    """Executes inside the task pod. `sql` tasks run as the service principal."""
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
    operators = {}
    for t in p.tasks:
        operators[t.name] = PythonOperator(
            task_id=t.name,
            python_callable=_run_task,
            op_kwargs={
                "task": {"name": t.name, "kind": t.kind, "sql": t.sql},
                "params": p.params,
            },
            dag=dag,
        )
    for t in p.tasks:
        for dep in t.depends_on:
            operators[dep] >> operators[t.name]
    return dag


namespace = _namespace()
principal = os.environ.get("PIPELINE_CLIENT_ID", DEFAULT_PRINCIPAL)
for resource in _list_resources(namespace):
    name = resource.get("metadata", {}).get("name", "?")
    try:
        pipeline_plan = plan(resource, namespace)
        if pipeline_plan.run_as and pipeline_plan.run_as != principal:
            # One principal exists in phase J0; running as someone else would be a guess.
            raise PlanError(f"runAs `{pipeline_plan.run_as}` is not available (only `{principal}`)")
        globals()[pipeline_plan.dag_id] = _build_dag(pipeline_plan)
    except PlanError as problem:
        log.warning("pipeline %s/%s is not loaded: %s", namespace, name, problem)
