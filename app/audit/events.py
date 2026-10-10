from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote

ACTIONS = (
    "CREATE",
    "UPDATE",
    "DELETE",
    "PAUSE",
    "RESUME",
    "RESTART",
    "TASK_RESTART",
    "VALIDATE",
)
RESULTS = ("success", "failure", "denied")
ACTOR_KEYS = ("name", "role", "sub")
CLUSTER_KEYS = ("id", "name")
EVENT_KEYS = ("id", "ts", "action", "actor", "cluster", "connector", "task_id", "result", "status", "request_id")

_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_CREATE = re.compile(r"^/api/clusters/([^/]+)/connectors$")
_VALIDATE = re.compile(r"^/api/clusters/([^/]+)/plugins/validate$")
_TASK = re.compile(r"^/api/clusters/([^/]+)/connectors/([^/]+)/tasks/([^/]+)/restart$")
_NAMED = re.compile(r"^/api/clusters/([^/]+)/connectors/([^/]+)/(pause|resume|restart|config)$")
_DELETE = re.compile(r"^/api/clusters/([^/]+)/connectors/([^/]+)$")


class AuditError(Exception):
    pass


class AuditStorageError(AuditError):
    pass


@dataclass(frozen=True)
class AuditTarget:
    action: str
    cluster_id: str
    connector: str | None
    task_id: int | None


def classify_request(method: str, path: str) -> AuditTarget | None:
    if method == "POST":
        matched = _CREATE.fullmatch(path)
        if matched:
            return AuditTarget("CREATE", _part(matched.group(1)), None, None)
        matched = _TASK.fullmatch(path)
        if matched:
            return AuditTarget("TASK_RESTART", _part(matched.group(1)), _part(matched.group(2)), _task_id(matched.group(3)))
        matched = _NAMED.fullmatch(path)
        if matched and matched.group(3) != "config":
            return AuditTarget(matched.group(3).upper(), _part(matched.group(1)), _part(matched.group(2)), None)
    elif method == "PUT":
        if _VALIDATE.fullmatch(path):
            matched = _VALIDATE.fullmatch(path)
            return AuditTarget("VALIDATE", _part(matched.group(1)), None, None)
        matched = _NAMED.fullmatch(path)
        if matched and matched.group(3) == "config":
            return AuditTarget("UPDATE", _part(matched.group(1)), _part(matched.group(2)), None)
    elif method == "DELETE":
        matched = _DELETE.fullmatch(path)
        if matched:
            return AuditTarget("DELETE", _part(matched.group(1)), _part(matched.group(2)), None)
    return None


def new_event_id() -> str:
    return str(uuid.uuid4())


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_ts(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    moment = moment.astimezone(timezone.utc)
    text = moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return text


def parse_ts(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("invalid timestamp") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def request_id_from(raw: str | None) -> str:
    if raw and _REQUEST_ID.fullmatch(raw.strip()):
        return raw.strip()
    return new_event_id()


def actor_payload(session: dict[str, Any] | None, *, auth_enabled: bool) -> dict[str, Any]:
    if not auth_enabled:
        return {"name": "anonymous", "role": "admin", "sub": None}
    if not session:
        return {"name": "anonymous", "role": None, "sub": None}
    name = session.get("name")
    role = session.get("role")
    sub = session.get("sub")
    return {
        "name": str(name).strip() if isinstance(name, str) and name.strip() else "anonymous",
        "role": str(role) if isinstance(role, str) and role in {"viewer", "operator", "admin"} else None,
        "sub": str(sub) if isinstance(sub, str) and sub.strip() else None,
    }


def cluster_payload(cluster_id: str, name: str | None) -> dict[str, str]:
    return {"id": cluster_id, "name": name or cluster_id}


def connector_name(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    name = value.strip()
    if not name or "/" in name or "\\" in name or len(name) > 512:
        return None
    return name


def build_event(
    *,
    event_id: str,
    ts: datetime,
    action: str,
    actor: dict[str, Any],
    cluster: dict[str, str] | None,
    connector: str | None,
    task_id: int | None,
    result: str,
    status: int,
    request_id: str,
) -> dict[str, Any]:
    if action not in ACTIONS:
        raise ValueError("invalid action")
    if result not in RESULTS:
        raise ValueError("invalid result")
    if not 100 <= int(status) <= 599:
        raise ValueError("invalid status")
    event = {
        "id": str(event_id),
        "ts": format_ts(ts),
        "action": action,
        "actor": _only(actor, ACTOR_KEYS),
        "cluster": _only(cluster, CLUSTER_KEYS) if cluster else None,
        "connector": connector_name(connector),
        "task_id": int(task_id) if task_id is not None else None,
        "result": result,
        "status": int(status),
        "request_id": str(request_id),
    }
    return sanitize_event(event)


def sanitize_event(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("audit event must be an object")
    unknown = set(raw) - set(EVENT_KEYS)
    if unknown:
        raise ValueError("audit event has forbidden fields")
    action = raw.get("action")
    result = raw.get("result")
    if action not in ACTIONS or result not in RESULTS:
        raise ValueError("invalid audit event")
    actor = raw.get("actor")
    if not isinstance(actor, dict):
        raise ValueError("invalid actor")
    cluster = raw.get("cluster")
    if cluster is not None and not isinstance(cluster, dict):
        raise ValueError("invalid cluster")
    status = raw.get("status")
    if not isinstance(status, int) or isinstance(status, bool) or not 100 <= status <= 599:
        raise ValueError("invalid status")
    task_id = raw.get("task_id")
    if task_id is not None and (not isinstance(task_id, int) or isinstance(task_id, bool) or task_id < 0):
        raise ValueError("invalid task_id")
    return {
        "id": str(raw.get("id") or ""),
        "ts": str(raw.get("ts") or ""),
        "action": action,
        "actor": _only(actor, ACTOR_KEYS),
        "cluster": _only(cluster, CLUSTER_KEYS) if cluster else None,
        "connector": connector_name(raw.get("connector")),
        "task_id": task_id,
        "result": result,
        "status": status,
        "request_id": str(raw.get("request_id") or ""),
    }


def encode_cursor(ts: str, event_id: str) -> str:
    return f"{ts}|{event_id}"


def decode_cursor(raw: str) -> tuple[str, str]:
    ts, separator, event_id = raw.partition("|")
    if not separator or not ts or not event_id:
        raise ValueError("invalid cursor")
    parse_ts(ts)
    return ts, event_id


def after_cursor(event: dict[str, Any], cursor: tuple[str, str] | None) -> bool:
    if cursor is None:
        return True
    key = (event["ts"], event["id"])
    return key < cursor


def _only(payload: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: (None if payload.get(key) is None else str(payload.get(key))) for key in keys}


def _part(value: str) -> str:
    return unquote(value)


def _task_id(raw: str) -> int | None:
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value >= 0 else None
