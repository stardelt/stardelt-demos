"""The pure parts of a `notebook` task, plus the operator's pre-signing and env wiring.

The operator tests need Airflow's Kubernetes provider (the loader image and the dev cluster have
it; the plain pytest job does not), so each of them skips when the provider is absent.
"""
import logging
from datetime import datetime, timezone

import pytest
from stardelt_pipelines import notebook
from stardelt_pipelines.notebook import (
    merged_params,
    output_key,
    pod_env,
    source_key,
)

PROVIDER = "airflow.providers.cncf.kubernetes"

RUN_ID = "manual__2026-10-21T03:00:00+00:00"
RUN_AFTER = datetime(2026, 10, 21, 3, 0, tzinfo=timezone.utc)

GET_KEY = "analytics/Report - daily.ipynb"
PUT_KEY = f"_runs/analytics/nbtest/{RUN_ID}/report.ipynb"


class DagRun:
    run_id = RUN_ID
    run_after = RUN_AFTER


def context():
    return {"dag_run": DagRun(), "logical_date": None}


class FakeS3:
    """The boto3 client a presign call needs, and a record of every call."""

    def __init__(self):
        self.calls = []

    def generate_presigned_url(self, operation, Params, ExpiresIn):
        self.calls.append((operation, dict(Params), ExpiresIn))
        return f"https://s3.example/{Params['Key']}?X-Amz-Signature=canary"


def fake_client(client):
    """The seam `_s3_client` provides: a pre-signing client, no hook machinery."""
    return lambda conn_id: client


def operator(**overrides):
    """A `NotebookPodOperator` with the pod settings the loader would give it."""
    from stardelt_pipelines.notebook import NotebookPodOperator

    kwargs = {
        "task_id": "report",
        "pipeline": "nbtest",
        "owner_group": "analytics",
        "notebook": "Report - daily.ipynb",
        "object_version": "version-1",
        "task_params": {"days": "3"},
        "pipeline_params": {"region": "eu", "days": "1"},
        "principal_secret": "sp-pipeline-analytics",
        "namespace": "stardelt",
    }
    kwargs.update(overrides)
    return NotebookPodOperator(**kwargs)


def set_contract(monkeypatch):
    monkeypatch.setenv("PIPELINE_NOTEBOOK_IMAGE", "ghcr.io/stardelt/jupyter-singleuser@sha256:aaa")
    monkeypatch.setenv("PIPELINE_NOTEBOOK_BUCKET", "notebooks")
    monkeypatch.setenv("PIPELINE_NOTEBOOK_TRINO_CA_SECRET", "trino-ca")
    monkeypatch.setenv("PIPELINE_TRINO_URL", "https://trino:8443")
    monkeypatch.setenv("PIPELINE_TOKEN_URL", "https://id.example/realms/stardelt/token")


# --- keys ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "owner_group, name",
    [
        ("analytics", "../sales/report.ipynb"),
        ("analytics", "sales/report.ipynb"),
        ("analytics", "sales\\report.ipynb"),
        ("analytics", ".hidden.ipynb"),
        ("analytics", "report\x00.ipynb"),
        ("..", "report.ipynb"),
        ("a/b", "report.ipynb"),
        ("a\\b", "report.ipynb"),
        (".analytics", "report.ipynb"),
        ("analytics", ""),
    ],
)
def test_source_key_refuses_traversal(owner_group, name):
    with pytest.raises(ValueError):
        source_key(owner_group, name)


def test_output_key_is_per_run_and_safe():
    key = output_key("analytics", "nbtest", RUN_ID, "report")
    assert key == PUT_KEY
    # Every run writes under its own prefix, and the key stays one path segment per part.
    assert output_key("analytics", "nbtest", "manual__2026-10-21 04:00:00#2", "report") == (
        "_runs/analytics/nbtest/manual__2026-10-21_04:00:00_2/report.ipynb"
    )
    assert "sales" not in output_key("analytics", "nbtest", RUN_ID, "report")


# --- parameters ---------------------------------------------------------------------------------


def test_merged_params_task_wins():
    merged = merged_params({"days": "1", "region": "eu"}, {"days": "3"})
    assert merged == {"days": "3", "region": "eu"}


# --- the pod's environment ----------------------------------------------------------------------


def test_pod_env_has_no_s3_names_and_no_secret_values(monkeypatch):
    set_contract(monkeypatch)
    canary = "canary-client-secret-value"
    monkeypatch.setenv("PIPELINE_CLIENT_SECRET", canary)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA-CANARY")

    env = pod_env(
        run_date="2026-10-21",
        run_id=RUN_ID,
        params={"days": "3"},
        principal_secret="sp-pipeline-analytics",
    )
    names = [item.name for item in env]
    # The notebook pod must not be able to reach the object store: no storage variable at all.
    assert [name for name in names if "S3" in name.upper() or "AWS" in name.upper()] == []
    assert "PIPELINE_NOTEBOOK_S3_CONN" not in names

    by_name = {item.name: item for item in env}
    assert by_name["NOTEBOOK_RUN_DATE"].value == "2026-10-21"
    assert by_name["NOTEBOOK_RUN_ID"].value == RUN_ID
    assert by_name["NOTEBOOK_PARAMS"].value == '{"days": "3"}'
    assert by_name["TRINO_URL"].value == "https://trino:8443"
    assert by_name["REQUESTS_CA_BUNDLE"].value == "/etc/stardelt/trino-ca/ca.crt"
    assert by_name["STARDELT_QUERY_IDS"].value == "/tmp/stardelt-query-ids"

    # The credentials are references: the kubelet reads the Secret, Python never holds a value.
    for name in ("PIPELINE_CLIENT_ID", "PIPELINE_CLIENT_SECRET"):
        assert by_name[name].value is None
        assert by_name[name].value_from.secret_key_ref.name == "sp-pipeline-analytics"
    values = [item.value for item in env if item.value]
    assert canary not in values and "AKIA-CANARY" not in values


# --- the operator -------------------------------------------------------------------------------


def test_presign_called_with_version_and_expiry(monkeypatch):
    pytest.importorskip(PROVIDER)
    set_contract(monkeypatch)
    s3 = FakeS3()
    monkeypatch.setattr(notebook, "_s3_client", fake_client(s3))
    monkeypatch.setattr(notebook.KubernetesPodOperator, "execute", lambda self, context: None)

    operator().execute(context())

    assert s3.calls == [
        ("get_object", {"Bucket": "notebooks", "Key": GET_KEY, "VersionId": "version-1"}, 7200),
        ("put_object", {"Bucket": "notebooks", "Key": PUT_KEY}, 7200),
    ]


def test_execute_adds_both_urls_and_never_logs_them(monkeypatch, caplog):
    pytest.importorskip(PROVIDER)
    set_contract(monkeypatch)
    s3 = FakeS3()
    monkeypatch.setattr(notebook, "_s3_client", fake_client(s3))
    ran = {}

    def fake_execute(self, context):
        ran["pod"] = list(self.env_vars)

    monkeypatch.setattr(notebook.KubernetesPodOperator, "execute", fake_execute)

    caplog.set_level(logging.DEBUG)
    operator().execute(context())

    names = [item.name for item in ran["pod"]]
    assert "NOTEBOOK_GET_URL" in names and "NOTEBOOK_PUT_URL" in names
    assert {item.name: item.value for item in ran["pod"]}["NOTEBOOK_GET_URL"].startswith(
        "https://s3.example/"
    )
    # A pre-signed URL is a credential; neither it nor its signature may reach the log.
    assert "X-Amz" not in caplog.text
    assert "s3.example" not in caplog.text
    # What the log is for: the version and the output key.
    assert PUT_KEY in caplog.text
    assert "version-1" in caplog.text


def test_missing_image_env_fails_clearly(monkeypatch):
    pytest.importorskip(PROVIDER)
    monkeypatch.delenv("PIPELINE_NOTEBOOK_IMAGE", raising=False)
    with pytest.raises(ValueError, match="^PIPELINE_NOTEBOOK_IMAGE is not set$"):
        operator().execute(context())
