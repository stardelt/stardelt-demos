"""The Pod override that gives a pipeline its group principal's credentials — shape only.

Nothing here talks to a cluster: the point is that the override *references* the Secret (the
kubelet resolves it, so the worker's ServiceAccount needs no permission on Secrets) and never
carries a value itself. `kubernetes` is not installed everywhere the tests run, so skip cleanly.
"""
import pytest

pytest.importorskip("kubernetes")

from stardelt_pipelines.principal import principal_pod_override  # noqa: E402


def test_principal_pod_override_references_the_secret_and_holds_no_value():
    override = principal_pod_override("sp-pipeline-analytics")

    containers = override["pod_override"].spec.containers
    assert [container.name for container in containers] == ["base"]

    env = {var.name: var for var in containers[0].env}
    assert set(env) == {"PIPELINE_CLIENT_ID", "PIPELINE_CLIENT_SECRET"}
    for name, key in (
        ("PIPELINE_CLIENT_ID", "client-id"),
        ("PIPELINE_CLIENT_SECRET", "client-secret"),
    ):
        reference = env[name].value_from.secret_key_ref
        assert reference.name == "sp-pipeline-analytics"
        assert reference.key == key

    for var in env.values():
        assert var.value is None  # the value comes from the reference, not from this file
        assert var.value_from is not None
