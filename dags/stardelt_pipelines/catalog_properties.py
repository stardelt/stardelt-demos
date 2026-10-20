"""The properties the demo DAG's PyIceberg REST catalog is built with.

Kept free of Airflow and PyIceberg imports: `nyc_taxi_load` builds the catalog from these
properties and the unit tests exercise them without either dependency.

`CATALOG_CLIENT_ID`, `CATALOG_CLIENT_SECRET` and `CATALOG_TOKEN_URL` arrive together on every
Airflow pod (dag-processor and KubernetesExecutor task pods) from Secret `svc-catalog-airflow`.
When all three are non-empty the catalog signs in to the Lakekeeper catalog as
`svc-catalog-airflow` through the client-credentials grant; when any is missing it stays
anonymous, exactly as before sign-in existed. All-or-none: the platform wires one Secret, so a
partial trio is not a state it produces and is treated as absent.

The scope is `openid`, not the catalog's name and not Iceberg's default `catalog`: the realm
rejects both of those for the client-credentials grant with 400 `invalid_scope`, while `openid`
is accepted and the audience mapper still puts the catalog in the token's `aud`.
"""
from __future__ import annotations

from collections.abc import Mapping

CREDENTIAL_VARIABLES = ("CATALOG_CLIENT_ID", "CATALOG_CLIENT_SECRET", "CATALOG_TOKEN_URL")


def catalog_properties(environ: Mapping[str, str]) -> dict[str, str]:
    """The properties for `RestCatalog`, with sign-in only when the credentials are complete.

    A missing required variable raises `KeyError` naming the variable alone (never a value),
    which is how the DAG already failed; the secret and any token stay out of messages.
    """
    properties: dict[str, str] = {
        "uri": environ["CATALOG_URI"],
        "warehouse": environ.get("CATALOG_WAREHOUSE", "warehouse"),
        "s3.endpoint": environ["S3_ENDPOINT"],
        "s3.access-key-id": environ["S3_ACCESS_KEY"],
        "s3.secret-access-key": environ["S3_SECRET_KEY"],
        "s3.region": environ.get("S3_REGION", "us-east-1"),
        "s3.path-style-access": "true",
    }
    credentials = [environ.get(name, "") for name in CREDENTIAL_VARIABLES]
    if all(credentials):
        client_id, client_secret, token_url = credentials
        properties["credential"] = f"{client_id}:{client_secret}"
        properties["oauth2-server-uri"] = token_url
        properties["scope"] = "openid"
    return properties
