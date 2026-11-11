"""Who may see a pipeline's DAG in Airflow (platform#477, design `airflow-access`).

A person signs in to Airflow as `stardelt-member`, which holds no read on any DAG. The members of an
owner group get the role `stardelt-group-<ownerGroup>` (created by the platform's sync Job and granted
at sign-in from the Keycloak group), and a DAG names that role in its `access_control`: Airflow then
gives the role read on this DAG and on its runs, and nothing else: people run, pause and stop a
pipeline in Nova. A pipeline without an `ownerGroup` names no role: only administrators see its DAG.

A role that does not exist yet makes Airflow refuse to write the DAG at all (the dag-processor logs
"Failed to write serialized DAG"); it is written by the parse after the sync Job has created the role.
"""
from __future__ import annotations

ROLE_PREFIX = "stardelt-group-"
DEFAULT_OWNER = "stardelt"
GROUP_PERMISSIONS = {
    "DAGs": {"can_read"},
    "DAG Runs": {"can_read"},
}


def group_role(owner_group: str) -> str:
    """The Airflow role of an owner group: `stardelt-group-<ownerGroup>`."""
    return f"{ROLE_PREFIX}{owner_group}"


def dag_access(owner_group: str | None) -> dict:
    """The DAG arguments that scope a DAG to its owner group: `access_control`, `owner` and `tags`."""
    if not owner_group:
        return {"access_control": None, "owner": DEFAULT_OWNER, "tags": []}
    return {
        "access_control": {group_role(owner_group): {k: set(v) for k, v in GROUP_PERMISSIONS.items()}},
        "owner": owner_group,
        "tags": [f"group:{owner_group}"],
    }
