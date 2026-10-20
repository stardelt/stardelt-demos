"""The Pod override that gives a pipeline's tasks the credentials of its group's principal.

A pipeline with an `ownerGroup` runs as that group's `ServicePrincipal`, so its task pods need that
principal's client credentials — and only theirs. The override below sets exactly two environment
variables, from the principal's Secret. The values are `secretKeyRef`s: the kubelet resolves them,
so the worker's ServiceAccount needs no permission to read Secrets, and the credentials never pass
through Python (nothing here can log them).
"""
from __future__ import annotations

from kubernetes.client import models as k8s


def principal_pod_override(secret_name: str) -> dict:
    """The `executor_config` of a task of a pipeline that runs as its group's principal.

    `secret_name` is the principal's Secret (`status.secretName`); it holds `client-id` and
    `client-secret`. Everything else in the pod stays as the KubernetesExecutor built it.
    """
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        env=[
                            k8s.V1EnvVar(
                                name="PIPELINE_CLIENT_ID",
                                value_from=k8s.V1EnvVarSource(
                                    secret_key_ref=k8s.V1SecretKeySelector(
                                        name=secret_name, key="client-id"
                                    )
                                ),
                            ),
                            k8s.V1EnvVar(
                                name="PIPELINE_CLIENT_SECRET",
                                value_from=k8s.V1EnvVarSource(
                                    secret_key_ref=k8s.V1SecretKeySelector(
                                        name=secret_name, key="client-secret"
                                    )
                                ),
                            ),
                        ],
                    )
                ]
            )
        )
    }
