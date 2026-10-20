import logging
import os
import signal

import httpx
import pytest
from stardelt_pipelines.trino import (
    TrinoError,
    cancel_handler,
    cancel_on_termination,
    client_credentials_token,
    run_statement,
    statement_headers,
)

BASE = "https://trino.test:8443"


def client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_token_comes_from_the_client_credentials_grant():
    seen = {}

    def handler(request):
        seen["body"] = request.content.decode()
        return httpx.Response(200, json={"access_token": "tok"})

    with client(handler) as http:
        assert client_credentials_token("https://id.test/token", "pipeline-runner", "s3cret", client=http) == "tok"
    assert "grant_type=client_credentials" in seen["body"] and "client_id=pipeline-runner" in seen["body"]


def test_a_statement_is_followed_to_the_end_and_counts_rows():
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path, request.headers["authorization"]))
        if request.method == "POST":
            assert request.content == b"INSERT INTO t SELECT 1"
            assert request.headers["x-trino-source"] == "stardelt-pipeline"
            return httpx.Response(200, json={"id": "q1", "nextUri": f"{BASE}/v1/statement/q1/1"})
        if request.url.path.endswith("/1"):
            return httpx.Response(200, json={"id": "q1", "data": [[1], [2]], "nextUri": f"{BASE}/v1/statement/q1/2"})
        return httpx.Response(200, json={"id": "q1", "data": [[3]], "updateCount": 3})

    with client(handler) as http:
        out = run_statement("INSERT INTO t SELECT 1", base_url=BASE, token="tok", client=http, sleep=lambda s: None)
    assert (out.query_id, out.rows, out.update_count) == ("q1", 3, 3)
    assert all(auth == "Bearer tok" for _, _, auth in calls) and len(calls) == 3


def test_an_engine_error_becomes_a_trino_error_with_the_name_and_message():
    def handler(request):
        if request.method == "POST":
            return httpx.Response(200, json={"id": "q2", "nextUri": f"{BASE}/v1/statement/q2/1"})
        return httpx.Response(
            200, json={"id": "q2", "error": {"errorName": "TABLE_NOT_FOUND", "message": "Table 't' does not exist"}}
        )

    with client(handler) as http, pytest.raises(TrinoError, match="TABLE_NOT_FOUND: Table 't' does not exist"):
        run_statement("SELECT * FROM t", base_url=BASE, token="tok", client=http, sleep=lambda s: None)


def test_a_deadline_cancels_the_query():
    deleted = []
    now = iter([0.0, 1.0, 100.0, 200.0])

    def handler(request):
        if request.method == "DELETE":
            deleted.append(str(request.url))
            return httpx.Response(204)
        return httpx.Response(200, json={"id": "q3", "nextUri": f"{BASE}/v1/statement/q3/next"})

    with client(handler) as http, pytest.raises(TrinoError, match="timed out after 5s"):
        run_statement(
            "SELECT 1", base_url=BASE, token="tok", client=http, deadline_s=5,
            clock=lambda: next(now), sleep=lambda s: None,
        )
    assert deleted == [f"{BASE}/v1/statement/q3/next"]


def test_an_http_failure_raises():
    with client(lambda request: httpx.Response(401)) as http, pytest.raises(httpx.HTTPStatusError):
        run_statement("SELECT 1", base_url=BASE, token="bad", client=http)


def test_on_started_receives_the_id_and_the_next_uri_each_page():
    started = []

    def handler(request):
        if request.method == "POST":
            return httpx.Response(200, json={"id": "q1", "nextUri": f"{BASE}/v1/statement/q1/1"})
        if request.url.path.endswith("/1"):
            return httpx.Response(200, json={"id": "q1", "nextUri": f"{BASE}/v1/statement/q1/2"})
        return httpx.Response(200, json={"id": "q1", "data": [[1]], "updateCount": 1})

    with client(handler) as http:
        run_statement(
            "SELECT 1", base_url=BASE, token="tok", client=http, sleep=lambda s: None,
            on_started=lambda query_id, next_uri: started.append((query_id, next_uri)),
        )
    assert started == [
        ("q1", f"{BASE}/v1/statement/q1/1"),
        ("q1", f"{BASE}/v1/statement/q1/2"),
    ]


def test_statement_headers_are_shared_with_the_polls():
    seen = []

    def handler(request):
        seen.append(request.headers)
        if request.method == "POST":
            return httpx.Response(200, json={"id": "q1", "nextUri": f"{BASE}/v1/statement/q1/1"})
        return httpx.Response(200, json={"id": "q1", "data": [], "updateCount": 0})

    with client(handler) as http:
        run_statement(
            "SELECT 1", base_url=BASE, token="tok", client=http, source="my-source", sleep=lambda s: None
        )
    expected = statement_headers("tok", "my-source")
    assert len(seen) == 2
    for headers in seen:
        assert headers["authorization"] == expected["Authorization"]
        assert headers["x-trino-source"] == expected["X-Trino-Source"]
        assert headers["x-trino-client-tags"] == expected["X-Trino-Client-Tags"]


def test_the_cancel_handler_deletes_the_current_next_uri():
    deleted = []

    def handler(request):
        deleted.append(request)
        return httpx.Response(204)

    with client(handler) as http:
        cancel = cancel_handler(
            http,
            token="tok",
            current={"query_id": "q7", "next_uri": f"{BASE}/v1/statement/q7/9"},
            source="my-source",
        )
        cancel(signal.SIGTERM, None)

    assert [(request.method, str(request.url)) for request in deleted] == [
        ("DELETE", f"{BASE}/v1/statement/q7/9")
    ]
    assert deleted[0].headers["authorization"] == "Bearer tok"
    assert deleted[0].headers["x-trino-source"] == "my-source"


def test_the_cancel_handler_without_a_next_uri_does_nothing_and_does_not_raise():
    called = []

    with client(lambda request: called.append(request) or httpx.Response(204)) as http:
        for current in ({"query_id": None, "next_uri": None}, {"query_id": "q7", "next_uri": None}):
            cancel_handler(http, token="tok", current=current)(signal.SIGTERM, None)
    assert called == []


def test_the_cancel_handler_swallows_a_failing_delete(caplog):
    def handler(request):
        raise httpx.ConnectError("boom", request=request)

    with client(handler) as http, caplog.at_level(logging.INFO):
        cancel_handler(
            http, token="tok", current={"query_id": "q9", "next_uri": f"{BASE}/v1/statement/q9/1"}
        )(signal.SIGTERM, None)

    assert "cancel requested; cancelling query q9" in caplog.text
    assert "could not cancel query q9" in caplog.text


def test_no_token_in_any_log_line(caplog):
    token = "tok-canary-7f3a"

    def handler(request):
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(200, json={"id": "q1", "data": [[1]], "updateCount": 1})

    with client(handler) as http, caplog.at_level(logging.DEBUG):
        current = {"query_id": None, "next_uri": None}

        def started(query_id, next_uri):
            current.update(query_id=query_id, next_uri=next_uri)
            logging.getLogger("pipeline_loader").info("query %s submitted", query_id)

        run_statement("SELECT 1", base_url=BASE, token=token, client=http, on_started=started)
        cancel_handler(
            http, token=token, current={"query_id": "q1", "next_uri": f"{BASE}/v1/statement/q1/1"}
        )(signal.SIGTERM, None)

    assert "query q1 submitted" in caplog.text
    assert "cancel requested; cancelling query q1" in caplog.text
    assert token not in caplog.text


def test_the_previous_signal_handler_is_restored():
    previous = signal.getsignal(signal.SIGTERM)
    current = {"query_id": None, "next_uri": None}

    def stub_statement_runner():
        return "finished"

    with client(lambda request: httpx.Response(204)) as http:
        handler = cancel_handler(http, token="tok", current=current)
        with cancel_on_termination(handler):
            assert signal.getsignal(signal.SIGTERM) is handler
            assert stub_statement_runner() == "finished"
    assert signal.getsignal(signal.SIGTERM) is previous


def test_the_sigterm_path_cancels_the_running_statement():
    calls = []
    delivered = []

    def handler(request):
        calls.append((request.method, str(request.url)))
        if request.method == "DELETE":
            return httpx.Response(204)
        if request.method == "POST":
            return httpx.Response(200, json={"id": "q1", "nextUri": f"{BASE}/v1/statement/q1/1"})
        return httpx.Response(
            200, json={"id": "q1", "error": {"errorName": "USER_CANCELED", "message": "Query was canceled"}}
        )

    def executor_sigterm(_seconds):
        # What the executor does on a stopped run: one SIGTERM while the statement waits for
        # its next page (the signal reaches the main thread inside this sleep).
        if not delivered:
            delivered.append(True)
            os.kill(os.getpid(), signal.SIGTERM)

    with client(handler) as http:
        current = {"query_id": None, "next_uri": None}
        with cancel_on_termination(cancel_handler(http, token="tok", current=current)):
            with pytest.raises(TrinoError, match="USER_CANCELED"):
                run_statement(
                    "SELECT 1", base_url=BASE, token="tok", client=http, sleep=executor_sigterm,
                    on_started=lambda query_id, next_uri: current.update(
                        query_id=query_id, next_uri=next_uri
                    ),
                )

    cancelled = ("DELETE", f"{BASE}/v1/statement/q1/1")
    assert cancelled in calls
    assert calls.index(cancelled) < calls.index(("GET", f"{BASE}/v1/statement/q1/1"))
