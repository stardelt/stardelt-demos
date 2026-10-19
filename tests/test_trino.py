import httpx
import pytest
from stardelt_pipelines.trino import TrinoError, client_credentials_token, run_statement

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
