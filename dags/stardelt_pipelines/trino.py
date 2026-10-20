"""Run one SQL statement on the query engine as a service principal (Keycloak client credentials).

Uses the engine's REST protocol over an injected `httpx.Client`, so the image needs no extra
client library and the tests need no network. Nothing here logs a token or a secret.
"""
from __future__ import annotations

import logging
import signal
import time
from contextlib import contextmanager
from dataclasses import dataclass
from types import FrameType
from typing import Callable, Iterator

import httpx

log = logging.getLogger(__name__)


class TrinoError(RuntimeError):
    """The engine rejected or failed the statement."""


@dataclass(frozen=True)
class Outcome:
    query_id: str
    rows: int
    update_count: int | None


def statement_headers(token: str, source: str = "stardelt-pipeline") -> dict[str, str]:
    """The headers every statement call carries: the submit, the polls and both cancels."""
    return {
        "Authorization": f"Bearer {token}",
        "X-Trino-Source": source,
        "X-Trino-Client-Tags": "stardelt-pipeline",
    }


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
    on_started: Callable[[str, str | None], None] | None = None,
) -> Outcome:
    """Submit `sql`, follow the result pages to the end and return what happened.

    `on_started` gets `(query_id, next_uri)` as soon as the engine has answered the submit and
    again whenever `next_uri` changes, so a caller can cancel the statement while it runs.
    """
    headers = statement_headers(token, source)
    started = clock()
    response = client.post(
        f"{base_url.rstrip('/')}/v1/statement",
        content=sql.encode("utf-8"),
        headers={**headers, "Content-Type": "text/plain; charset=utf-8"},
    )
    response.raise_for_status()
    body = response.json()
    query_id = body["id"]
    reported_uri = body.get("nextUri")
    if on_started is not None:
        on_started(query_id, reported_uri)
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
        if on_started is not None and next_uri != reported_uri:
            reported_uri = next_uri
            on_started(query_id, next_uri)
        if deadline_s is not None and clock() - started > deadline_s:
            client.delete(next_uri, headers=headers)
            raise TrinoError(f"timed out after {deadline_s:.0f}s (query {query_id} cancelled)")
        sleep(poll_s)
        response = client.get(next_uri, headers=headers)
        response.raise_for_status()
        body = response.json()


def cancel_handler(
    client: httpx.Client,
    *,
    token: str,
    current: dict[str, str | None],
    source: str = "stardelt-pipeline",
) -> Callable[[int, FrameType | None], None]:
    """Build the SIGTERM handler that cancels the statement `current` points at.

    The handler asks the engine to cancel and never raises: the poll that follows reads
    `USER_CANCELED`, so `run_statement` raises `TrinoError` and the task fails with the
    engine's own error instead of dying silently. Nothing here logs the token or the SQL.
    """

    def _cancel(signum: int, frame: FrameType | None) -> None:
        log.info("cancel requested; cancelling query %s", current["query_id"])
        uri = current["next_uri"]
        if uri:
            try:
                client.delete(uri, headers=statement_headers(token, source))
            except Exception:  # noqa: BLE001 - the next poll reports the outcome
                log.warning("could not cancel query %s", current["query_id"])

    return _cancel


@contextmanager
def cancel_on_termination(handler: Callable[[int, FrameType | None], None]) -> Iterator[None]:
    """Install `handler` for SIGTERM for the duration of the block, then restore the previous."""
    previous = signal.signal(signal.SIGTERM, handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)
