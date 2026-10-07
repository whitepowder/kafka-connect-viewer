import re
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app.connect import ConnectClient, ConnectError
from app.errors import ERROR_CODES, KCVError, error_body
from app.main import app

ROOT = Path(__file__).resolve().parent.parent
USER_FACING = [
    "app/auth.py",
    "app/connect.py",
    "app/errors.py",
    "app/main.py",
    "app/graph/build.py",
    "app/graph/diagnostics.py",
    "app/graph/facts.py",
    "app/graph/ids.py",
]
CODE_PATTERNS = [
    re.compile(r'code="([a-z_]+)"'),
    re.compile(r'KCVError\(\d{3}, "([a-z_]+)"'),
    re.compile(r'error_body\("([a-z_]+)"'),
]


def _source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_every_code_used_by_the_backend_is_registered_and_used():
    used = {code for path in USER_FACING + ["app/graph/service.py"] for pattern in CODE_PATTERNS for code in pattern.findall(_source(path))}
    assert used <= ERROR_CODES, used - ERROR_CODES
    assert ERROR_CODES <= used, f"unused codes: {sorted(ERROR_CODES - used)}"


def test_user_facing_backend_code_has_no_hardcoded_messages():
    for path in USER_FACING:
        source = _source(path)
        assert not re.search(r"[А-Яа-яЁё]", source), f"{path} contains Russian text"
        assert "HTTPException(" not in source, f"{path} raises HTTPException with a text detail"
        assert '"message": "' not in source and "\"message\": f\"" not in source, f"{path} builds a message string"


def test_unknown_codes_are_rejected():
    with pytest.raises(ValueError):
        error_body("made_up")
    with pytest.raises(ValueError):
        KCVError(400, "made_up")
    with pytest.raises(ValueError):
        ConnectError(400)


UPSTREAM_RU = "Коннектор alpha уже существует"


def _handler(request: httpx.Request) -> httpx.Response:
    method, path = request.method, request.url.path
    if (method, path) == ("GET", "/"):
        return httpx.Response(502, text="<html>Bad Gateway from proxy</html>")
    if (method, path) == ("GET", "/connectors"):
        return httpx.Response(200, json=["detail", "secret"])
    if (method, path) == ("GET", "/connectors/detail/config"):
        return httpx.Response(500, json={"message": UPSTREAM_RU})
    if (method, path) == ("GET", "/connectors/secret/config"):
        return httpx.Response(200, json=["not", "an", "object"])
    if (method, path) == ("PUT", "/connectors/missing/pause"):
        return httpx.Response(404, json={"message": "Connector missing not found."})
    if (method, path) == ("POST", "/connectors"):
        return httpx.Response(409, json={"message": "Connector alpha already exists"})
    if (method, path) == ("GET", "/connector-plugins"):
        return httpx.Response(200, json={"not": "a list"})
    if (method, path) == ("GET", "/connectors/detail"):
        return httpx.Response(200, json=["not", "an", "object"])
    if (method, path) == ("GET", "/connectors/detail/status"):
        return httpx.Response(200, json={"name": "detail", "connector": {"state": "RUNNING"}, "tasks": []})
    if (method, path) == ("GET", "/connectors/upstream"):
        return httpx.Response(200, json={"name": "upstream", "config": {"connector.class": "X"}, "type": "sink"})
    if (method, path) == ("GET", "/connectors/upstream/status"):
        return httpx.Response(500, json={"message": UPSTREAM_RU})
    if (method, path) == ("GET", "/connectors/secret"):
        return httpx.Response(200, json={"config": {"connector.class": "Demo", "password": "*****"}})
    if (method, path) == ("POST", "/connectors/slow/restart"):
        raise httpx.ReadTimeout("slow", request=request)
    if (method, path) == ("PUT", "/connectors/down/pause"):
        raise httpx.ConnectError("refused", request=request)
    if (method, path) == ("PUT", "/connectors/nojson/resume"):
        return httpx.Response(200, text="definitely not json")
    if (method, path) == ("DELETE", "/connectors/bare"):
        return httpx.Response(500, json={})
    if method in {"POST", "PUT"} and path.startswith("/connectors") and path != "/connectors/secret/config":
        pytest.fail(f"unexpected upstream call: {method} {path}")
    return httpx.Response(404, json={"message": "not found"})


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("CLUSTERS_FILE", str(tmp_path / "missing.json"))
    monkeypatch.setenv("CONNECT_CLUSTERS", "lab=http://connect.test")
    monkeypatch.delenv("AUTH_ENABLED", raising=False)
    app.state.client_factory = lambda cluster: ConnectClient(cluster, transport=httpx.MockTransport(_handler))
    with TestClient(app) as test_client:
        yield test_client
    app.state.client_factory = None


def _kcv(code, **params):
    return {"code": code, "params": params}


@pytest.mark.parametrize(
    ("method", "path", "kwargs", "status", "body"),
    [
        ("GET", "/api/clusters/nope/connectors", {}, 404, _kcv("cluster_not_found")),
        ("POST", "/api/clusters/lab/connectors", {"json": {"name": "a/b", "config": {"connector.class": "X"}}}, 400, _kcv("invalid_connector_name")),
        ("POST", "/api/clusters/lab/connectors", {"json": {"name": "a", "config": {"tasks.max": "1"}}}, 400, _kcv("connector_class_missing")),
        ("POST", "/api/clusters/lab/connectors", {"json": {"name": "a", "config": {}}}, 400, _kcv("config_empty")),
        ("POST", "/api/clusters/lab/connectors", {"json": {"name": "a", "config": {"connector.class": "X", "nested": {"a": 1}}}}, 400, _kcv("config_value_not_string", key="nested")),
        ("POST", "/api/clusters/lab/connectors", {"json": {"name": "a", "config": {"connector.class": "X", " ": "1"}}}, 400, _kcv("config_key_invalid")),
        ("PUT", "/api/clusters/lab/plugins/validate", {"json": {"config": {"tasks.max": "1"}}}, 400, _kcv("connector_class_missing")),
        ("PUT", "/api/clusters/lab/connectors/secret/config", {"json": {"config": {"connector.class": "Demo", "password": "*****"}}}, 400, _kcv("secret_masked", key="password")),
        ("POST", "/api/clusters/lab/connectors/alpha/tasks/-1/restart", {}, 400, _kcv("task_id_negative")),
        ("POST", "/api/clusters/lab/connectors/a/pause", {"headers": {"content-length": str(2 * 1024 * 1024)}}, 413, _kcv("request_too_large")),
        ("POST", "/api/clusters/lab/connectors/a/pause", {"headers": {"content-length": "lots"}}, 400, _kcv("invalid_content_length")),
        ("POST", "/api/clusters/lab/connectors/a/pause", {"headers": {"sec-fetch-site": "cross-site"}}, 403, _kcv("cross_origin_write")),
        ("POST", "/api/clusters/lab/connectors/a/pause", {"headers": {"origin": "https://evil.example"}}, 403, _kcv("cross_origin_write")),
        ("GET", "/api/clusters/lab/plugins", {}, 502, _kcv("upstream_invalid_plugin_list")),
        ("POST", "/api/clusters/lab/connectors/slow/restart", {}, 504, _kcv("upstream_timeout")),
        ("POST", "/api/clusters/lab/connectors/down/pause", {}, 502, _kcv("upstream_unreachable", error="ConnectError")),
        ("POST", "/api/clusters/lab/connectors/nojson/resume", {}, 502, _kcv("upstream_not_json")),
        ("DELETE", "/api/clusters/lab/connectors/bare", {}, 500, _kcv("upstream_status", status=500)),
    ],
)
def test_kcv_errors_are_codes_with_unchanged_status(client, method, path, kwargs, status, body):
    response = client.request(method, path, **kwargs)
    assert response.status_code == status
    assert response.json() == body


@pytest.mark.parametrize(
    ("method", "path", "kwargs", "status", "message"),
    [
        ("POST", "/api/clusters/lab/connectors", {"json": {"name": "alpha", "config": {"connector.class": "X"}}}, 409, "Connector alpha already exists"),
        ("GET", "/api/clusters/lab", {}, 502, "<html>Bad Gateway from proxy</html>"),
        ("POST", "/api/clusters/lab/connectors/missing/pause", {}, 404, "Connector missing not found."),
    ],
)
def test_kafka_connect_errors_pass_through_unchanged(client, method, path, kwargs, status, message):
    response = client.request(method, path, **kwargs)
    assert response.status_code == status
    assert response.json() == {"message": message}


def test_connector_detail_keeps_upstream_and_kcv_errors_apart(client):
    kcv = client.get("/api/clusters/lab/connectors/detail")
    assert kcv.status_code == 200
    assert kcv.json()["info_error"] == _kcv("upstream_empty_connector_info")
    assert kcv.json()["status_error"] is None

    upstream = client.get("/api/clusters/lab/connectors/upstream")
    assert upstream.status_code == 200
    assert upstream.json()["status_error"] == {"message": UPSTREAM_RU}
    assert upstream.json()["info_error"] is None

    both = client.get("/api/clusters/lab/connectors/missing")
    assert both.status_code == 404
    assert both.json() == {"message": "not found"}


def test_graph_errors_use_codes_and_keep_upstream_messages(client):
    graph = client.get("/api/clusters/lab/graph").json()
    assert graph["partial"] is True
    assert graph["errors"] == [
        {"connector": "detail", "message": UPSTREAM_RU},
        {"connector": "secret", **_kcv("upstream_empty_connector_config")},
    ]
    unreadable = {item["connectors"][0]: item["error"] for item in graph["diagnostics"] if item["code"] == "config_unreadable"}
    assert unreadable == {"detail": {"message": UPSTREAM_RU}, "secret": _kcv("upstream_empty_connector_config")}
    assert all("message" not in item for item in graph["diagnostics"])

    limited = client.get("/api/clusters/lab/graph?refresh=true")
    assert limited.status_code == 200
    limited = client.get("/api/clusters/lab/graph?refresh=true")
    assert limited.status_code == 429
    assert limited.headers["Retry-After"]
    assert limited.json() == _kcv("graph_refresh_limited", seconds=10)
