"""Run a `notebook` task as a pod of its own (scheduled jobs, phase J2).

The notebook image has no Airflow, so an image override of the Airflow task pod can never work
(`airflow tasks run` exits 127 there — the first attempt of this ticket proved it). A `notebook`
task is therefore a `KubernetesPodOperator`: its pod runs one process,
`python3 -m stardelt.notebook_run`, from the pinned notebook image (platform#315), and the
operator only orchestrates — it runs in the ordinary worker pod.

The notebook pod holds no storage credential. The worker, which has the object-store connection,
pre-signs one GET of the published object version and one PUT of the executed copy and hands the
pod only those two URLs (`NOTEBOOK_GET_URL`, `NOTEBOOK_PUT_URL`); the principal's client
credentials arrive through a `secretKeyRef`, exactly like the `sql` tasks' pod override.

The pure parts (`source_key`, `output_key`, `merged_params`, `pod_env`) import no Airflow, so the
tests that cover them run without the loader image.
"""
from __future__ import annotations

import json
import logging
import os
import string
from collections.abc import Mapping

from kubernetes.client import models as k8s

try:  # Airflow is in the loader image and on the cluster, not in the unit-test environment.
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
except ImportError:  # pragma: no cover - the operator tests importorskip the provider
    KubernetesPodOperator = object  # type: ignore[assignment,misc]

log = logging.getLogger(__name__)

TRINO_CA_MOUNT = "/etc/stardelt/trino-ca"
TRINO_CA_FILE = f"{TRINO_CA_MOUNT}/ca.crt"
QUERY_IDS_FILE = "/tmp/stardelt-query-ids"
SIGNED_URL_TTL_SECONDS = 7200
DEFAULT_S3_CONN = "stardelt_s3"
IMAGE_ENV = "PIPELINE_NOTEBOOK_IMAGE"
BUCKET_ENV = "PIPELINE_NOTEBOOK_BUCKET"
S3_CONN_ENV = "PIPELINE_NOTEBOOK_S3_CONN"
TRINO_CA_SECRET_ENV = "PIPELINE_NOTEBOOK_TRINO_CA_SECRET"
# What a run id may keep: everything else becomes `_`, so a key stays one path segment.
_RUN_ID_CHARS = frozenset(string.ascii_letters + string.digits + "_.:+-")


def source_key(owner_group: str, notebook: str) -> str:
    """`<ownerGroup>/<notebook>`: the published notebook a group's pipeline runs.

    Both parts are one path segment. A name that could climb out of the group's prefix (`..`, a
    slash, a backslash, a leading dot or a NUL) is refused here rather than normalized: the key
    is built from the pipeline's own `ownerGroup`, so no task can read another group's notebook.
    """
    return f"{_segment(owner_group, 'owner group')}/{_segment(notebook, 'notebook name')}"


def _segment(value: str, what: str) -> str:
    """`value` as one S3 key segment, or a `ValueError` that names the rule it breaks."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"the {what} is empty")
    for bad, rule in (
        ("\x00", "contains a NUL byte"),
        ("..", "contains `..`"),
        ("/", "contains a slash"),
        ("\\", "contains a backslash"),
    ):
        if bad in value:
            raise ValueError(f"the {what} {rule}")
    if value.startswith("."):
        raise ValueError(f"the {what} starts with a dot")
    return value


def output_key(owner_group: str, pipeline: str, run_id: str, task: str) -> str:
    """`_runs/<ownerGroup>/<pipeline>/<run_id>/<task>.ipynb`: this run's executed copy.

    A group name starts with a letter or a digit, so `_runs` is never a group's prefix. Airflow run
    ids carry characters an object key should not (`+`, `:`), so everything outside
    `[A-Za-z0-9_.:+-]` becomes `_`.
    """
    safe_run_id = "".join(char if char in _RUN_ID_CHARS else "_" for char in run_id)
    return f"_runs/{_segment(owner_group, 'owner group')}/{pipeline}/{safe_run_id}/{task}.ipynb"


def merged_params(
    pipeline_params: Mapping[str, str], task_params: Mapping[str, str]
) -> dict[str, str]:
    """The pipeline's parameters with the task's on top (a task's value wins)."""
    merged = dict(pipeline_params)
    merged.update(task_params)
    return merged


def pod_env(
    *,
    run_date: str,
    run_id: str,
    params: Mapping[str, str],
    principal_secret: str,
) -> list[k8s.V1EnvVar]:
    """The environment of the notebook pod: the wrapper's contract, and no storage credential.

    `NOTEBOOK_PARAMS` is the merged parameter object (sorted, so the pod spec is stable);
    the principal's `client-id` and `client-secret` come from its Secret through `secretKeyRef`
    (nothing secret passes through Python). The engine and identity endpoints come from the
    loader's own environment.
    """
    return [
        k8s.V1EnvVar(name="NOTEBOOK_PARAMS", value=json.dumps(dict(params), sort_keys=True)),
        k8s.V1EnvVar(name="NOTEBOOK_RUN_DATE", value=run_date),
        k8s.V1EnvVar(name="NOTEBOOK_RUN_ID", value=run_id),
        k8s.V1EnvVar(name="TRINO_URL", value=_required("PIPELINE_TRINO_URL")),
        k8s.V1EnvVar(name="REQUESTS_CA_BUNDLE", value=TRINO_CA_FILE),
        k8s.V1EnvVar(name="PIPELINE_TOKEN_URL", value=_required("PIPELINE_TOKEN_URL")),
        _secret_env("PIPELINE_CLIENT_ID", principal_secret, "client-id"),
        _secret_env("PIPELINE_CLIENT_SECRET", principal_secret, "client-secret"),
        k8s.V1EnvVar(name="STARDELT_QUERY_IDS", value=QUERY_IDS_FILE),
    ]


def _secret_env(name: str, secret: str, key: str) -> k8s.V1EnvVar:
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name=secret, key=key)
        ),
    )


def _required(name: str) -> str:
    """A non-empty variable from the loader's environment, or a one-line error naming it."""
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is not set")
    return value


def _s3_client(conn_id: str):
    """The worker's S3 client: the only component here that holds the object-store credentials.

    A client whose pre-signed URLs use SigV4, not the hook's default client: with this connection
    (`us-east-1`, custom endpoint) the default signs with the legacy SigV2 scheme, and the object
    store rejects a SigV2 **PUT** with `403` — a GET happens to work, so only the upload of the
    executed notebook failed. The connection's own config (path-style) is kept.
    """
    from botocore.config import Config
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=conn_id)
    return hook.get_client_type(config=hook.config.merge(Config(signature_version="s3v4")))


def _presigned(s3, bucket: str, get_key: str, version: str, put_key: str) -> tuple[str, str]:
    """One GET of the pinned version and one PUT of the output key, both short-lived.

    The URLs are credentials; the caller hands them to the pod and never logs them.
    """
    get_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": get_key, "VersionId": version},
        ExpiresIn=SIGNED_URL_TTL_SECONDS,
    )
    put_url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": put_key},
        ExpiresIn=SIGNED_URL_TTL_SECONDS,
    )
    return get_url, put_url


class NotebookPodOperator(KubernetesPodOperator):
    """One `notebook` task, run in a pod of the pinned notebook image.

    The pod is built at execution time because the image, the bucket and the CA Secret come from
    the loader's environment (`PIPELINE_NOTEBOOK_*`), so a DAG still parses on an install that has
    no notebook stack — and the task fails with a sentence instead of a missing image.
    """

    def __init__(
        self,
        *,
        pipeline: str,
        owner_group: str,
        notebook: str,
        object_version: str,
        task_params: Mapping[str, str],
        pipeline_params: Mapping[str, str],
        principal_secret: str,
        namespace: str,
        **kwargs,
    ) -> None:
        self.pipeline = pipeline
        self.owner_group = owner_group
        self.notebook = notebook
        self.object_version = object_version
        self.task_params = dict(task_params)
        self.pipeline_params = dict(pipeline_params)
        self.principal_secret = principal_secret
        super().__init__(
            namespace=namespace,
            image=None,  # set in execute from PIPELINE_NOTEBOOK_IMAGE
            cmds=["python3", "-m", "stardelt.notebook_run"],
            labels={
                "app.kubernetes.io/name": "pipeline-notebook",
                "stardelt.io/pipeline": pipeline,
            },
            security_context=k8s.V1PodSecurityContext(
                run_as_non_root=True,
                run_as_user=1000,
                run_as_group=100,
                fs_group=100,
                seccomp_profile=k8s.V1SeccompProfile(type="RuntimeDefault"),
            ),
            container_security_context=k8s.V1SecurityContext(
                allow_privilege_escalation=False,
                capabilities=k8s.V1Capabilities(drop=["ALL"]),
            ),
            automount_service_account_token=False,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "250m", "memory": "512Mi"},
                limits={"cpu": "2", "memory": "2Gi"},
            ),
            termination_grace_period=30,
            get_logs=True,
            on_finish_action="delete_pod",
            # The pod spec carries the two pre-signed URLs; a failure must not dump it into the
            # task log (they are credentials, and the log outlives the run).
            log_pod_spec_on_failure=False,
            startup_timeout_seconds=300,
            reattach_on_restart=False,
            in_cluster=True,
            random_name_suffix=True,
            do_xcom_push=False,
            **kwargs,
        )

    def build_pod_request_obj(self, context=None, **kwargs):
        """The operator's pod, with service links off (platform#315: they leak every Service)."""
        pod = super().build_pod_request_obj(context, **kwargs)
        pod.spec.enable_service_links = False
        return pod

    def execute(self, context):
        """Hand the pod its two pre-signed URLs and the principal's environment, then run it."""
        self.image = _required(IMAGE_ENV)
        bucket = _required(BUCKET_ENV)
        ca_secret = _required(TRINO_CA_SECRET_ENV)
        self.volumes = [
            k8s.V1Volume(
                name="trino-ca",
                secret=k8s.V1SecretVolumeSource(
                    secret_name=ca_secret,
                    items=[k8s.V1KeyToPath(key="ca.crt", path="ca.crt")],
                ),
            )
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(name="trino-ca", mount_path=TRINO_CA_MOUNT, read_only=True)
        ]

        dag_run = context["dag_run"]
        run_date = (context.get("logical_date") or dag_run.run_after).date().isoformat()
        run_id = dag_run.run_id
        get_key = source_key(self.owner_group, self.notebook)
        put_key = output_key(self.owner_group, self.pipeline, run_id, self.task_id)

        s3 = _s3_client(os.environ.get(S3_CONN_ENV, DEFAULT_S3_CONN))
        get_url, put_url = _presigned(s3, bucket, get_key, self.object_version, put_key)
        # The output key and the version are not credentials; the URLs are, and never appear here.
        log.info(
            "notebook `%s` at object version %s; this run writes %s",
            self.notebook,
            self.object_version,
            put_key,
        )

        self.env_vars = list(self.env_vars) + pod_env(
            run_date=run_date,
            run_id=run_id,
            params=merged_params(self.pipeline_params, self.task_params),
            principal_secret=self.principal_secret,
        )
        self.env_vars += [
            k8s.V1EnvVar(name="NOTEBOOK_GET_URL", value=get_url),
            k8s.V1EnvVar(name="NOTEBOOK_PUT_URL", value=put_url),
        ]
        return super().execute(context)
