from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.audit import (
    AuditService,
    AuditSettingsError,
    AuditStorageError,
    actor_for_request,
    classify_request,
    cluster_for,
    name_from_body,
    request_id_from,
    result_for_status,
)
from app.auth import AuthSettings, OIDCAuth, require_role
from app.connect import ClientPool, ConnectClient, ConnectError
from app.errors import KCVError, error_body
from app.graph import GraphRateLimited, GraphService, GraphSettingsError
from app.settings import Cluster, Settings, SettingsError, load_settings

logger = logging.getLogger("kafka-connect-viewer")
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


class ConnectorBody(BaseModel):
    name: str = Field(min_length=1, max_length=512)
    config: dict[str, Any]


class ConfigBody(BaseModel):
    config: dict[str, Any]


class ValidateBody(BaseModel):
    config: dict[str, Any]


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        settings = load_settings()
        graph = GraphService.from_env(os.environ)
        audit = AuditService.from_env(os.environ)
        if getattr(app.state, "audit_store", None) is not None:
            audit.store = app.state.audit_store
        audit.prepare()
    except (SettingsError, GraphSettingsError, AuditSettingsError) as exc:
        raise RuntimeError(str(exc)) from exc
    app.state.settings = settings
    app.state.graph = graph
    app.state.audit = audit
    app.state.auth = OIDCAuth(AuthSettings.from_env(), getattr(app.state, "oidc_transport", None))
    factory = getattr(app.state, "client_factory", None)
    app.state.pool = ClientPool(factory)
    logger.info(
        "ready clusters=%s; connector list uses GET /connectors without expand",
        ",".join(cluster.name for cluster in settings.clusters),
    )
    try:
        yield
    finally:
        await app.state.pool.aclose()
        await app.state.auth.close()
        app.state.audit.close()


app = FastAPI(title="Kafka Connect Viewer", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


MAX_BODY_BYTES = 1024 * 1024
MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}




def _required_api_role(request: Request) -> str | None:
    path = request.url.path
    if not path.startswith("/api/") or path in {"/api/health", "/api/me"}:
        return None
    if path == "/api/audit":
        return "admin"
    if request.method == "GET":
        return "viewer"
    if path.endswith("/pause") or path.endswith("/resume") or path.endswith("/restart") or "/tasks/" in path:
        return "operator"
    return "admin"


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    role = _required_api_role(request)
    if role:
        try:
            require_role(request, role)
        except KCVError as exc:
            return JSONResponse(status_code=exc.status, content=exc.body)
    return await call_next(request)

@app.middleware("http")
async def security_middleware(request: Request, call_next):
    # Reject unexpectedly large API payloads before parsing JSON. Connector configs
    # are tiny in practice; 1 MiB leaves ample headroom while limiting memory abuse.
    if request.url.path.startswith("/api/") and request.method in MUTATING_METHODS:
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > MAX_BODY_BYTES:
                    return JSONResponse(status_code=413, content=error_body("request_too_large"))
            except ValueError:
                return JSONResponse(status_code=400, content=error_body("invalid_content_length"))

        # The UI is same-origin. Block cross-site browser writes (CSRF) while still
        # allowing non-browser API clients that do not send Origin.
        fetch_site = request.headers.get("sec-fetch-site", "").lower()
        if fetch_site == "cross-site":
            return JSONResponse(status_code=403, content=error_body("cross_origin_write"))
        origin = request.headers.get("origin")
        if origin:
            origin_host = urlparse(origin).netloc.lower()
            request_host = request.headers.get("host", "").lower()
            if not origin_host or origin_host != request_host:
                return JSONResponse(status_code=403, content=error_body("cross_origin_write"))

    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; "
        "form-action 'self'; object-src 'none'; img-src 'self' data:; "
        "style-src 'self'; script-src 'self'; connect-src 'self'"
    )
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response


@app.middleware("http")
async def audit_middleware(request: Request, call_next):
    request_id = request_id_from(request.headers.get("x-request-id"))
    request.state.request_id = request_id
    target = classify_request(request.method, request.url.path)
    audit: AuditService | None = getattr(request.app.state, "audit", None)
    raw_body = b""
    if target is not None and target.action in {"CREATE", "VALIDATE"}:
        raw_body = await request.body()
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    if target is None or audit is None or not audit.settings.enabled:
        return response
    connector = target.connector or name_from_body(raw_body)
    code = _response_code(response)
    result = result_for_status(response.status_code, code)
    try:
        audit.record(
            action=target.action,
            actor=actor_for_request(request),
            cluster=cluster_for(request, target.cluster_id),
            connector=connector,
            task_id=target.task_id,
            result=result,
            status=response.status_code,
            request_id=request_id,
        )
    except AuditStorageError:
        if result == "success":
            _mark_audit_write_failed(response)
    return response


@app.exception_handler(ConnectError)
async def connect_error(_request: Request, exc: ConnectError):
    status = exc.status if 400 <= exc.status <= 599 else 502
    return JSONResponse(status_code=status, content=exc.body)


@app.exception_handler(KCVError)
async def kcv_error(_request: Request, exc: KCVError):
    return JSONResponse(status_code=exc.status, content=exc.body)


@app.get("/")
async def index(request: Request):
    auth: OIDCAuth = request.app.state.auth
    if auth.settings.enabled and not auth.session(request):
        return await auth.begin_login("/")
    return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-store, max-age=0"})


@app.get("/auth/login", include_in_schema=False)
async def login(request: Request, next: str = "/"):
    auth: OIDCAuth = request.app.state.auth
    if not auth.settings.enabled:
        return RedirectResponse("/", status_code=302)
    return await auth.begin_login(next)


@app.get("/auth/callback", include_in_schema=False)
async def callback(request: Request, code: str, state: str):
    auth: OIDCAuth = request.app.state.auth
    if not auth.settings.enabled:
        return RedirectResponse("/", status_code=302)
    return await auth.finish_login(request, code, state)


@app.get("/auth/logout", include_in_schema=False)
async def logout(request: Request):
    auth: OIDCAuth = request.app.state.auth
    if not auth.settings.enabled:
        return RedirectResponse("/", status_code=302)
    return await auth.logout(request)


@app.get("/api/me")
def me(request: Request):
    user = require_role(request, "viewer")
    return {"auth_enabled": request.app.state.auth.settings.enabled, "name": user.get("name", "local"), "role": user.get("role", "admin")}


@app.get("/health", include_in_schema=False)
@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/ready", include_in_schema=False)
def ready(request: Request):
    settings: Settings = request.app.state.settings
    return {"ok": True, "clusters": len(settings.clusters)}


@app.get("/api/audit")
def list_audit(request: Request):
    audit: AuditService = request.app.state.audit
    return audit.query(request.query_params)


@app.get("/api/clusters")
def list_clusters(request: Request):
    settings: Settings = request.app.state.settings
    return {"clusters": [cluster.public() for cluster in settings.clusters]}


@app.get("/api/clusters/{cluster_id}")
async def cluster_info(cluster_id: str, request: Request):
    cluster = _cluster(request, cluster_id)
    payload = await _client(request, cluster).info()
    return {
        "id": cluster.id,
        "name": cluster.name,
        "version": payload.get("version"),
        "commit": payload.get("commit"),
        "kafka_cluster_id": payload.get("kafka_cluster_id"),
    }


@app.get("/api/clusters/{cluster_id}/connectors")
async def list_connectors(cluster_id: str, request: Request):
    cluster = _cluster(request, cluster_id)
    names = await _client(request, cluster).list_names()
    return {"count": len(names), "connectors": names}


@app.get("/api/clusters/{cluster_id}/connectors/{name}")
async def connector_detail(cluster_id: str, name: str, request: Request):
    cluster = _cluster(request, cluster_id)
    client = _client(request, cluster)
    info_result, status_result = await asyncio.gather(
        client.connector_info(name),
        client.connector_status(name),
        return_exceptions=True,
    )
    info, info_error = _split(info_result)
    status, status_error = _split(status_result)
    if info is None and status is None:
        raise info_error or status_error or ConnectError(502, code="connector_read_failed")
    status_body = status or {}
    connector_state = status_body.get("connector") if isinstance(status_body.get("connector"), dict) else None
    tasks = status_body.get("tasks") if isinstance(status_body.get("tasks"), list) else []
    config = {}
    connector_type = None
    if isinstance(info, dict):
        raw_config = info.get("config")
        if isinstance(raw_config, dict):
            config = {str(key): "" if value is None else str(value) for key, value in raw_config.items()}
        connector_type = info.get("type") or status_body.get("type")
    elif isinstance(status_body.get("type"), str):
        connector_type = status_body.get("type")
    return {
        "name": name,
        "type": connector_type,
        "config": config,
        "connector": connector_state,
        "tasks": tasks,
        "info_error": info_error.body if info_error else None,
        "status_error": status_error.body if status_error else None,
    }


@app.post("/api/clusters/{cluster_id}/connectors", status_code=201)
async def create_connector(cluster_id: str, body: ConnectorBody, request: Request):
    cluster = _cluster(request, cluster_id)
    try:
        created = await _client(request, cluster).create(body.name, body.config)
    finally:
        request.app.state.graph.invalidate(cluster.id, body.name)
    return created


@app.delete("/api/clusters/{cluster_id}/connectors/{name}")
async def delete_connector(cluster_id: str, name: str, request: Request):
    cluster = _cluster(request, cluster_id)
    try:
        await _client(request, cluster).delete(name)
    finally:
        request.app.state.graph.invalidate(cluster.id, name)
    return {"ok": True}


@app.put("/api/clusters/{cluster_id}/connectors/{name}/config")
async def update_config(cluster_id: str, name: str, body: ConfigBody, request: Request):
    cluster = _cluster(request, cluster_id)
    try:
        config = await _client(request, cluster).update_config(name, body.config)
    finally:
        request.app.state.graph.invalidate(cluster.id, name)
    return {"config": config}


@app.get("/api/clusters/{cluster_id}/graph")
async def connector_graph(cluster_id: str, request: Request, refresh: bool = Query(False)):
    cluster = _cluster(request, cluster_id)
    service: GraphService = request.app.state.graph
    try:
        return await service.graph(cluster.id, _client(request, cluster), refresh=refresh)
    except GraphRateLimited as exc:
        seconds = max(1, math.ceil(exc.retry_after))
        return JSONResponse(
            status_code=429,
            content=error_body("graph_refresh_limited", {"seconds": math.ceil(service.refresh_interval)}),
            headers={"Retry-After": str(seconds)},
        )


@app.post("/api/clusters/{cluster_id}/connectors/{name}/pause")
async def pause_connector(cluster_id: str, name: str, request: Request):
    cluster = _cluster(request, cluster_id)
    await _client(request, cluster).pause(name)
    return {"ok": True}


@app.post("/api/clusters/{cluster_id}/connectors/{name}/resume")
async def resume_connector(cluster_id: str, name: str, request: Request):
    cluster = _cluster(request, cluster_id)
    await _client(request, cluster).resume(name)
    return {"ok": True}


@app.post("/api/clusters/{cluster_id}/connectors/{name}/restart")
async def restart_connector(
    cluster_id: str,
    name: str,
    request: Request,
    include_tasks: bool = Query(False),
    only_failed: bool = Query(False),
):
    cluster = _cluster(request, cluster_id)
    await _client(request, cluster).restart(
        name,
        include_tasks=include_tasks,
        only_failed=only_failed,
    )
    return {"ok": True}


@app.post("/api/clusters/{cluster_id}/connectors/{name}/tasks/{task_id}/restart")
async def restart_task(cluster_id: str, name: str, task_id: int, request: Request):
    cluster = _cluster(request, cluster_id)
    await _client(request, cluster).restart_task(name, task_id)
    return {"ok": True}


@app.get("/api/clusters/{cluster_id}/plugins")
async def list_plugins(cluster_id: str, request: Request):
    cluster = _cluster(request, cluster_id)
    plugins = await _client(request, cluster).plugins()
    compact = []
    for plugin in plugins:
        class_name = plugin.get("class")
        if not isinstance(class_name, str):
            continue
        compact.append(
            {
                "class": class_name,
                "type": plugin.get("type"),
                "version": plugin.get("version"),
            }
        )
    compact.sort(key=lambda item: item["class"].casefold())
    return {"plugins": compact}


@app.put("/api/clusters/{cluster_id}/plugins/validate")
async def validate_plugin(cluster_id: str, body: ValidateBody, request: Request):
    cluster = _cluster(request, cluster_id)
    connector_class = body.config.get("connector.class")
    if not isinstance(connector_class, str) or not connector_class.strip():
        raise KCVError(400, "connector_class_missing")
    return await _client(request, cluster).validate(connector_class, body.config)


def _cluster(request: Request, cluster_id: str) -> Cluster:
    settings: Settings = request.app.state.settings
    for cluster in settings.clusters:
        if cluster.id == cluster_id:
            return cluster
    raise KCVError(404, "cluster_not_found")


def _client(request: Request, cluster: Cluster) -> ConnectClient:
    return request.app.state.pool.client(cluster)


AUDIT_WARNING_HEADER = "X-KCV-Audit"
AUDIT_WARNING_WRITE_FAILED = "write_failed"


def _mark_audit_write_failed(response) -> None:
    response.headers[AUDIT_WARNING_HEADER] = AUDIT_WARNING_WRITE_FAILED


def _response_code(response) -> str | None:
    body = getattr(response, "body", b"") or b""
    try:
        payload = json.loads(body)
    except (ValueError, TypeError):
        return None
    if isinstance(payload, dict) and isinstance(payload.get("code"), str):
        return payload["code"]
    return None


def _split(result: Any) -> tuple[dict | None, ConnectError | None]:
    if isinstance(result, ConnectError):
        return None, result
    if isinstance(result, Exception):
        return None, ConnectError(502, code="connector_read_failed")
    if isinstance(result, dict):
        return result, None
    return None, ConnectError(502, code="upstream_empty_response")
