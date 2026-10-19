"""Run one SQL statement on the query engine as a service principal (Keycloak client credentials).

Uses the engine's REST protocol over an injected `httpx.Client`, so the image needs no extra
client library and the tests need no network. Nothing here logs a token or a secret.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

import httpx


class TrinoError(RuntimeError):
    """The engine rejected or failed the statement."""


@dataclass(frozen=True)
class Outcome:
    query_id: str
    rows: int
    update_count: int | None


def client_credentials_token(
    token_url: str, client_id: str, client_secret: str, *, client: httpx.Client
) -> str:
    """Fetch an access token for the service principal."""
    response = client.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    response.raise_for_status()
    return response.json()["access_token"]


def run_statement(
    sql: str,
    *,
    base_url: str,
    token: str,
    client: httpx.Client,
    source: str = "stardelt-pipeline",
    deadline_s: float | None = None,
    poll_s: float = 0.2,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Outcome:
    """Submit `sql`, follow the result pages to the end and return what happened."""
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Trino-Source": source,
        "X-Trino-Client-Tags": "stardelt-pipeline",
    }
    started = clock()
    response = client.post(
        f"{base_url.rstrip('/')}/v1/statement",
        content=sql.encode("utf-8"),
        headers={**headers, "Content-Type": "text/plain; charset=utf-8"},
    )
    response.raise_for_status()
    body = response.json()
    query_id = body["id"]
    rows = 0
    update_count: int | None = None
    while True:
        if "error" in body:
            error = body["error"]
            raise TrinoError(f"{error.get('errorName', 'ERROR')}: {error.get('message', '')}")
        rows += len(body.get("data") or [])
        if body.get("updateCount") is not None:
            update_count = body["updateCount"]
        next_uri = body.get("nextUri")
        if not next_uri:
            return Outcome(query_id=query_id, rows=rows, update_count=update_count)
        if deadline_s is not None and clock() - started > deadline_s:
            client.delete(next_uri, headers=headers)
            raise TrinoError(f"timed out after {deadline_s:.0f}s (query {query_id} cancelled)")
        sleep(poll_s)
        response = client.get(next_uri, headers=headers)
        response.raise_for_status()
        body = response.json()
