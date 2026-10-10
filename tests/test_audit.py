from __future__ import annotations

import json
import logging
import subprocess
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from xml.etree.ElementTree import Element, SubElement, tostring

import httpx
import pytest
from fastapi.testclient import TestClient

from app.audit.events import (
    AuditStorageError,
    actor_payload,
    build_event,
    classify_request,
    sanitize_event,
)
from app.audit.local import LocalJsonlStore
from app.audit.s3 import S3AuditStore
from app.audit.service import AuditService, result_for_status
from app.audit.settings import AuditSettings, AuditSettingsError
from app.connect import ConnectClient
from app.main import AUDIT_WARNING_HEADER, AUDIT_WARNING_WRITE_FAILED, app, _mark_audit_write_failed


def _event(**overrides):
    payload = build_event(
        event_id="11111111-1111-1111-1111-111111111111",
        ts=datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc),
        action="CREATE",
        actor={"name": "alice", "role": "admin", "sub": "user-1"},
        cluster={"id": "lab", "name": "lab"},
        connector="orders-sink",
        task_id=None,
        result="success",
        status=201,
        request_id="req-1",
    )
    payload.update(overrides)
    return payload


class MemoryS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.calls: list[tuple[str, str, dict[str, str]]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        parsed = urlparse(str(request.url))
        parts = [part for part in parsed.path.split("/") if part]
        bucket = parts[0] if parts else ""
        key = "/".join(parts[1:])
        self.calls.append((request.method, key, {k.lower(): v for k, v in request.headers.items()}))
        if request.method == "PUT":
            if request.headers.get("if-none-match") == "*" and key in self.objects:
                return httpx.Response(412)
            self.objects[key] = request.content
            return httpx.Response(200)
        if request.method == "GET" and parse_qs(parsed.query).get("list-type") == ["2"]:
            prefix = parse_qs(parsed.query).get("prefix", [""])[0]
            root = Element("ListBucketResult")
            for name in sorted(self.objects):
                if name.startswith(prefix):
                    contents = SubElement(root, "Contents")
                    SubElement(contents, "Key").text = name
            SubElement(root, "IsTruncated").text = "false"
            return httpx.Response(200, content=tostring(root))
        if request.method == "GET":
            if key not in self.objects:
                return httpx.Response(404)
            return httpx.Response(200, content=self.objects[key])
        if request.method == "DELETE":
            self.objects.pop(key, None)
            return httpx.Response(204)
        return httpx.Response(404)


def _s3(memory: MemoryS3, retention_days: int = 90) -> S3AuditStore:
    return S3AuditStore(
        bucket="audit-bucket",
        prefix="kcv-audit",
        region="us-east-1",
        access_key="AKIATEST",
        secret_key="secret",
        endpoint="https://s3.test",
        transport=httpx.MockTransport(memory.handler),
        retention_days=retention_days,
    )


def test_audit_settings_defaults_to_local():
    settings = AuditSettings.from_env({"AUDIT_DIR": "/tmp/kcv-audit-test"})
    assert settings.enabled is True
    assert settings.storage == "local"
    assert settings.retention_days == 90


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"AUDIT_STORAGE": "sql"}, "AUDIT_STORAGE must be local or s3"),
        ({"AUDIT_ENABLED": "maybe"}, "AUDIT_ENABLED must be true or false"),
        ({"AUDIT_RETENTION_DAYS": "week"}, "AUDIT_RETENTION_DAYS must be a number of days"),
        ({"AUDIT_RETENTION_DAYS": "-1"}, "AUDIT_RETENTION_DAYS must not be negative"),
        ({"AUDIT_STORAGE": "s3"}, "AUDIT_STORAGE=s3 but missing AUDIT_S3_BUCKET, AUDIT_S3_ACCESS_KEY, AUDIT_S3_SECRET_KEY"),
        ({"AUDIT_STORAGE": "s3", "AUDIT_S3_BUCKET": "b", "AUDIT_S3_ACCESS_KEY": "a", "AUDIT_S3_SECRET_KEY": "s", "AUDIT_S3_ENDPOINT": "s3.local"}, "AUDIT_S3_ENDPOINT must be http(s)://host"),
    ],
)
def test_audit_settings_errors_are_plain_english(env, message):
    with pytest.raises(AuditSettingsError) as raised:
        AuditSettings.from_env(env)
    assert str(raised.value) == message


def test_s3_secret_from_environment_variable(monkeypatch):
    monkeypatch.setenv("KCV_TEST_AUDIT_SECRET", "from-env")
    settings = AuditSettings.from_env({
        "AUDIT_STORAGE": "s3",
        "AUDIT_S3_BUCKET": "bucket",
        "AUDIT_S3_ACCESS_KEY": "key",
        "AUDIT_S3_SECRET_KEY": "env:KCV_TEST_AUDIT_SECRET",
    })
    assert settings.s3_secret_key == "from-env"


@pytest.mark.parametrize(
    ("method", "path", "action", "cluster", "connector", "task_id"),
    [
        ("POST", "/api/clusters/lab/connectors", "CREATE", "lab", None, None),
        ("PUT", "/api/clusters/lab/connectors/orders/config", "UPDATE", "lab", "orders", None),
        ("DELETE", "/api/clusters/lab/connectors/orders", "DELETE", "lab", "orders", None),
        ("POST", "/api/clusters/lab/connectors/orders/pause", "PAUSE", "lab", "orders", None),
        ("POST", "/api/clusters/lab/connectors/orders/resume", "RESUME", "lab", "orders", None),
        ("POST", "/api/clusters/lab/connectors/orders/restart", "RESTART", "lab", "orders", None),
        ("POST", "/api/clusters/lab/connectors/orders/tasks/3/restart", "TASK_RESTART", "lab", "orders", 3),
        ("PUT", "/api/clusters/lab/plugins/validate", "VALIDATE", "lab", None, None),
    ],
)
def test_classify_auditable_paths(method, path, action, cluster, connector, task_id):
    target = classify_request(method, path)
    assert target is not None
    assert (target.action, target.cluster_id, target.connector, target.task_id) == (action, cluster, connector, task_id)


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/clusters/lab/connectors"),
        ("GET", "/api/audit"),
        ("POST", "/api/clusters/lab/connectors/orders"),
        ("PUT", "/api/clusters/lab/plugins"),
    ],
)
def test_classify_ignores_reads_and_unknown_writes(method, path):
    assert classify_request(method, path) is None


def test_events_reject_secrets_and_unknown_fields():
    with pytest.raises(ValueError):
        sanitize_event({**_event(), "config": {"password": "s3cr3t"}})
    with pytest.raises(ValueError):
        sanitize_event({**_event(), "token": "abc"})
    actor = actor_payload({"name": "alice", "role": "admin", "sub": "user-1", "id_token": "SHOULD_NOT_APPEAR"}, auth_enabled=True)
    assert actor == {"name": "alice", "role": "admin", "sub": "user-1"}
    assert actor_payload(None, auth_enabled=False) == {"name": "anonymous", "role": "admin", "sub": None}


def test_local_jsonl_round_trip_filter_and_pagination(tmp_path):
    store = LocalJsonlStore(str(tmp_path / "audit"), retention_days=30)
    first = _event(id="a", ts="2026-10-10T12:00:00.000Z", action="CREATE")
    second = _event(id="b", ts="2026-10-10T12:01:00.000Z", action="DELETE", result="failure", status=409)
    third = _event(id="c", ts="2026-10-10T12:02:00.000Z", action="PAUSE", actor={"name": "bob", "role": "operator", "sub": "2"})
    for event in (first, second, third):
        store.append(event)
    lines = (tmp_path / "audit" / "2026-10-10.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    for line in lines:
        payload = json.loads(line)
        assert set(payload) == {"id", "ts", "action", "actor", "cluster", "connector", "task_id", "result", "status", "request_id"}
        assert "password" not in line and "config" not in line
    page, cursor = store.query(limit=2)
    assert [item["id"] for item in page] == ["c", "b"]
    assert cursor
    rest, done = store.query(cursor=cursor, limit=2)
    assert [item["id"] for item in rest] == ["a"]
    assert done is None
    only_create, _ = store.query(action="CREATE")
    assert [item["id"] for item in only_create] == ["a"]
    only_bob, _ = store.query(actor="bob")
    assert [item["id"] for item in only_bob] == ["c"]


def test_local_retention_deletes_old_files(tmp_path):
    directory = tmp_path / "audit"
    directory.mkdir()
    (directory / "2020-01-01.jsonl").write_text("{}\n", encoding="utf-8")
    store = LocalJsonlStore(str(directory), retention_days=7)
    store.append(_event())
    assert not (directory / "2020-01-01.jsonl").exists()
    assert (directory / "2026-10-10.jsonl").exists()


def test_s3_writes_immutable_per_event_objects():
    memory = MemoryS3()
    store = _s3(memory)
    first = _event(id="a")
    second = _event(id="b", ts="2026-10-10T12:01:00.000Z", action="UPDATE")
    store.append(first)
    store.append(second)
    assert len(memory.objects) == 2
    assert all(key.startswith("kcv-audit/2026/10/10/") for key in memory.objects)
    assert all(call[2].get("if-none-match") == "*" for call in memory.calls if call[0] == "PUT")
    page, _ = store.query()
    assert [item["id"] for item in page] == ["b", "a"]
    replica = _s3(memory)
    replica.append(_event(id="c", ts="2026-10-10T12:02:00.000Z"))
    both, _ = store.query()
    assert {item["id"] for item in both} == {"a", "b", "c"}


def test_s3_put_conflict_is_a_storage_error():
    memory = MemoryS3()
    store = _s3(memory)
    event = _event()
    store.append(event)
    with pytest.raises(AuditStorageError):
        store.append(event)


def test_disabled_audit_does_not_write(tmp_path):
    service = AuditService.from_env({"AUDIT_ENABLED": "false", "AUDIT_DIR": str(tmp_path / "audit")})
    service.record(
        action="CREATE",
        actor={"name": "anonymous", "role": "admin", "sub": None},
        cluster={"id": "lab", "name": "lab"},
        connector="orders",
        task_id=None,
        result="success",
        status=201,
        request_id="r",
    )
    assert list((tmp_path / "audit").glob("*")) == []


def _connect_handler(request: httpx.Request) -> httpx.Response:
    method, path = request.method, request.url.path
    if method == "GET" and path == "/connectors":
        return httpx.Response(200, json=["orders"])
    if method == "POST" and path == "/connectors":
        return httpx.Response(201, json={"name": "orders"})
    if method == "PUT" and path == "/connectors/orders/config":
        return httpx.Response(200, json={"connector.class": "X"})
    if method == "DELETE" and path == "/connectors/orders":
        return httpx.Response(204)
    if method == "PUT" and path.endswith("/pause"):
        return httpx.Response(204)
    if method == "PUT" and path.endswith("/resume"):
        return httpx.Response(409, json={"message": "Connector is already running"})
    if method == "POST" and path.endswith("/restart"):
        return httpx.Response(204)
    if method == "PUT" and path == "/connector-plugins/X/config/validate":
        return httpx.Response(200, json={"name": "X", "error_count": 0, "configs": []})
    return httpx.Response(404, json={"message": "not found"})


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("CLUSTERS_FILE", str(tmp_path / "missing.json"))
    monkeypatch.setenv("CONNECT_CLUSTERS", "lab=http://connect.test")
    monkeypatch.delenv("AUTH_ENABLED", raising=False)
    app.state.client_factory = lambda cluster: ConnectClient(cluster, transport=httpx.MockTransport(_connect_handler))
    with TestClient(app) as test_client:
        yield test_client
    app.state.client_factory = None


def _events(client: TestClient, **params):
    response = client.get("/api/audit", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_api_records_success_failure_and_hides_config(client, tmp_path):
    created = client.post("/api/clusters/lab/connectors", json={"name": "orders", "config": {"connector.class": "X", "password": "s3cr3t"}})
    assert created.status_code == 201
    assert created.headers["X-Request-ID"]
    failed = client.post("/api/clusters/lab/connectors/orders/resume")
    assert failed.status_code == 409
    client.put("/api/clusters/lab/connectors/orders/config", json={"config": {"connector.class": "X"}})
    client.delete("/api/clusters/lab/connectors/orders")
    client.post("/api/clusters/lab/connectors/orders/pause")
    client.post("/api/clusters/lab/connectors/orders/restart")
    client.post("/api/clusters/lab/connectors/orders/tasks/0/restart")
    client.put("/api/clusters/lab/plugins/validate", json={"config": {"connector.class": "X", "password": "s3cr3t"}})
    payload = _events(client)
    actions = [item["action"] for item in payload["events"]]
    assert Counter(actions) == Counter({
        "CREATE": 1, "UPDATE": 1, "DELETE": 1, "PAUSE": 1,
        "RESUME": 1, "RESTART": 1, "TASK_RESTART": 1, "VALIDATE": 1,
    })
    create = next(item for item in payload["events"] if item["action"] == "CREATE")
    assert create["result"] == "success"
    assert create["actor"] == {"name": "anonymous", "role": "admin", "sub": None}
    assert create["cluster"] == {"id": "lab", "name": "lab"}
    assert create["connector"] == "orders"
    resume = next(item for item in payload["events"] if item["action"] == "RESUME")
    assert resume["result"] == "failure"
    assert resume["status"] == 409
    dumped = json.dumps(payload)
    assert "s3cr3t" not in dumped
    assert "connector.class" not in dumped
    assert "http://connect.test" not in dumped
    text = "".join(path.read_text(encoding="utf-8") for path in Path(tmp_path / "kcv-audit").glob("*.jsonl"))
    assert "s3cr3t" not in text
    assert "password" not in text


def test_api_filters_and_cursor(client):
    client.post("/api/clusters/lab/connectors", json={"name": "orders", "config": {"connector.class": "X"}})
    client.post("/api/clusters/lab/connectors/orders/pause")
    first = _events(client, limit=1)
    assert len(first["events"]) == 1
    assert first["next_cursor"]
    second = _events(client, cursor=first["next_cursor"], limit=1)
    assert second["events"][0]["id"] != first["events"][0]["id"]
    paused = _events(client, action="PAUSE")
    assert {item["action"] for item in paused["events"]} == {"PAUSE"}
    assert client.get("/api/audit", params={"action": "EXPLODE"}).status_code == 400
    assert client.get("/api/audit", params={"cursor": "bad"}).json()["code"] == "audit_invalid_cursor"


class BoomStore:
    def __init__(self, query_error=False):
        self.events = []
        self.query_error = query_error

    def prepare(self):
        return None

    def append(self, event):
        raise AuditStorageError("disk full")

    def query(self, **_kwargs):
        if self.query_error:
            raise AuditStorageError("disk full")
        return [], None

    def close(self):
        return None


def test_successful_create_keeps_status_when_audit_write_fails(client, caplog):
    original = app.state.audit.store
    app.state.audit.store = BoomStore()
    try:
        with caplog.at_level(logging.ERROR, logger="kafka-connect-viewer"):
            response = client.post(
                "/api/clusters/lab/connectors",
                json={"name": "orders", "config": {"connector.class": "X", "password": "s3cr3t"}},
            )
        assert response.status_code == 201
        assert response.json() == {"name": "orders"}
        assert response.headers[AUDIT_WARNING_HEADER] == AUDIT_WARNING_WRITE_FAILED
        assert "audit_write_failed" not in response.text
        assert "audit write failed" in caplog.text
        assert "s3cr3t" not in caplog.text
        assert "password" not in caplog.text
        assert "http://connect.test" not in caplog.text
        assert "connector.class" not in caplog.text
        second = client.post("/api/clusters/lab/connectors", json={"name": "orders", "config": {"connector.class": "X"}})
        assert second.status_code == 201
        assert second.headers[AUDIT_WARNING_HEADER] == AUDIT_WARNING_WRITE_FAILED
    finally:
        app.state.audit.store = original


def test_successful_json_action_keeps_status_when_audit_write_fails(client):
    original = app.state.audit.store
    app.state.audit.store = BoomStore()
    try:
        response = client.post("/api/clusters/lab/connectors/orders/pause")
        assert response.status_code == 200
        assert response.json() == {"ok": True}
        assert response.headers[AUDIT_WARNING_HEADER] == AUDIT_WARNING_WRITE_FAILED
    finally:
        app.state.audit.store = original


def test_successful_204_keeps_empty_body_when_audit_write_fails(client):
    import asyncio

    from starlette.requests import Request
    from starlette.responses import Response

    from app.main import audit_middleware

    assert result_for_status(204, None) == "success"
    marked = Response(status_code=204)
    _mark_audit_write_failed(marked)
    assert marked.status_code == 204
    assert marked.body == b""
    assert marked.headers[AUDIT_WARNING_HEADER] == AUDIT_WARNING_WRITE_FAILED

    original = app.state.audit.store
    app.state.audit.store = BoomStore()

    async def call_next(_request):
        return Response(status_code=204)

    async def run():
        request = Request({
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "DELETE",
            "scheme": "http",
            "path": "/api/clusters/lab/connectors/orders",
            "raw_path": b"/api/clusters/lab/connectors/orders",
            "query_string": b"",
            "headers": [],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
            "app": app,
        })
        return await audit_middleware(request, call_next)

    try:
        response = asyncio.run(run())
        assert response.status_code == 204
        assert response.body == b""
        assert response.headers[AUDIT_WARNING_HEADER] == AUDIT_WARNING_WRITE_FAILED
    finally:
        app.state.audit.store = original


def test_failed_operation_keeps_upstream_status_when_audit_write_fails(client):
    original = app.state.audit.store
    app.state.audit.store = BoomStore()
    try:
        response = client.post("/api/clusters/lab/connectors/orders/resume")
        assert response.status_code == 409
        assert response.json() == {"message": "Connector is already running"}
        assert response.headers.get("X-KCV-Audit") is None
    finally:
        app.state.audit.store = original


def test_audit_query_still_returns_503_when_storage_cannot_be_read(client):
    original = app.state.audit.store
    app.state.audit.store = BoomStore(query_error=True)
    try:
        response = client.get("/api/audit")
        assert response.status_code == 503
        assert response.json() == {"code": "audit_write_failed", "params": {}}
    finally:
        app.state.audit.store = original


def test_default_audit_directory_is_ignored_by_git():
    repo = Path(__file__).resolve().parents[1]
    ignored = subprocess.run(
        ["git", "-C", str(repo), "check-ignore", "-v", "--", "data/audit/placeholder.jsonl"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert ignored.returncode == 0, ignored.stderr
    assert "/data/audit/" in ignored.stdout
    tracked = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "--", "data/audit"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert tracked.stdout.strip() == ""


def test_disabled_audit_api(monkeypatch, tmp_path):
    monkeypatch.setenv("AUDIT_ENABLED", "false")
    monkeypatch.setenv("CLUSTERS_FILE", str(tmp_path / "missing.json"))
    monkeypatch.setenv("CONNECT_CLUSTERS", "lab=http://connect.test")
    monkeypatch.delenv("AUTH_ENABLED", raising=False)
    app.state.client_factory = lambda cluster: ConnectClient(cluster, transport=httpx.MockTransport(_connect_handler))
    try:
        with TestClient(app) as client:
            created = client.post("/api/clusters/lab/connectors", json={"name": "orders", "config": {"connector.class": "X"}})
            assert created.status_code == 201
            response = client.get("/api/audit")
            assert response.status_code == 404
            assert response.json() == {"code": "audit_disabled", "params": {}}
    finally:
        app.state.client_factory = None
