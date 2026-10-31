from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable, Mapping

from app.audit.events import (
    ACTIONS,
    RESULTS,
    AuditStorageError,
    actor_payload,
    build_event,
    classify_request,
    cluster_payload,
    connector_name,
    decode_cursor,
    new_event_id,
    parse_ts,
    request_id_from,
    utc_now,
)
from app.audit.local import LocalJsonlStore
from app.audit.s3 import S3AuditStore
from app.audit.settings import STORAGE_S3, AuditSettings, AuditSettingsError
from app.errors import KCVError

logger = logging.getLogger("kafka-connect-viewer")
DEFAULT_LIMIT = 50
MAX_LIMIT = 200


class AuditService:
    def __init__(
        self,
        settings: AuditSettings,
        store: Any | None = None,
        clock: Callable[[], datetime] | None = None,
        purge_clock: Callable[[], float] | None = None,
    ):
        self.settings = settings
        self.clock = clock or utc_now
        self.store = store if store is not None else _build_store(settings)
        self._purge_clock = purge_clock or time.monotonic
        self._purge_lock = threading.Lock()
        self._last_purge_at: float | None = None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, store: Any | None = None) -> "AuditService":
        return cls(AuditSettings.from_env(env), store=store)

    def close(self) -> None:
        closer = getattr(self.store, "close", None)
        if callable(closer):
            closer()

    def prepare(self) -> None:
        if not self.settings.enabled:
            return
        try:
            self.store.prepare()
        except AuditStorageError as exc:
            raise AuditSettingsError(str(exc)) from exc

    def record(
        self,
        *,
        action: str,
        actor: dict[str, Any],
        cluster: dict[str, str] | None,
        connector: str | None,
        task_id: int | None,
        result: str,
        status: int,
        request_id: str,
    ) -> dict[str, Any]:
        event = build_event(
            event_id=new_event_id(),
            ts=self.clock(),
            action=action,
            actor=actor,
            cluster=cluster,
            connector=connector,
            task_id=task_id,
            result=result,
            status=status,
            request_id=request_id,
        )
        if not self.settings.enabled:
            return event
        try:
            self.store.append(event)
        except AuditStorageError:
            logger.error("audit write failed action=%s result=%s request_id=%s", action, result, request_id)
            raise
        self._maybe_purge()
        return event

    async def record_async(self, **kwargs: Any) -> dict[str, Any]:
        """Record an event without blocking the event loop.

        The synchronous store (file I/O with flock/fsync, or S3 HTTP through a
        sync client) runs in a worker thread. The caller still awaits the
        result, so audit semantics and error handling are unchanged: a failed
        write raises AuditStorageError to the caller. If the awaiting task is
        cancelled, CancelledError propagates to the caller, but the already
        started thread is not interrupted and the write still completes; the
        event is never silently discarded.
        """
        return await asyncio.to_thread(self.record, **kwargs)

    def _maybe_purge(self) -> None:
        """Run S3 retention cleanup at most once per configured interval.

        Runs in the caller's (worker) thread after a successful append, never
        on the event loop. It is per-process: several replicas may each purge
        about once per interval, and deletes of the same expired objects are
        idempotent. A non-blocking lock keeps concurrent writers from running
        overlapping cleanups in this process. Cleanup failures are logged and
        never fail the audit write that triggered them; the attempt timestamp
        is recorded either way so a failing cleanup is not retried on every
        write.
        """
        if self.settings.storage != STORAGE_S3:
            return
        if self.settings.retention_days <= 0 or self.settings.purge_interval_seconds <= 0:
            return
        if not self._purge_lock.acquire(blocking=False):
            return
        try:
            now = self._purge_clock()
            if self._last_purge_at is not None and now - self._last_purge_at < self.settings.purge_interval_seconds:
                return
            self._last_purge_at = now
            try:
                self.store.purge()
            except Exception:
                logger.error("audit retention cleanup failed")
        finally:
            self._purge_lock.release()

    def query(self, raw: Mapping[str, str]) -> dict[str, Any]:
        if not self.settings.enabled:
            raise KCVError(404, "audit_disabled")
        action = _optional(raw.get("action"))
        result = _optional(raw.get("result"))
        if action and action not in ACTIONS:
            raise KCVError(400, "audit_invalid_filter")
        if result and result not in RESULTS:
            raise KCVError(400, "audit_invalid_filter")
        cursor = _optional(raw.get("cursor"))
        if cursor:
            try:
                decode_cursor(cursor)
            except ValueError as exc:
                raise KCVError(400, "audit_invalid_cursor") from exc
        try:
            since = parse_ts(raw["since"]) if _optional(raw.get("since")) else None
            until = parse_ts(raw["until"]) if _optional(raw.get("until")) else None
        except ValueError as exc:
            raise KCVError(400, "audit_invalid_filter") from exc
        try:
            events, next_cursor = self.store.query(
                cluster=_optional(raw.get("cluster")),
                connector=connector_name(raw.get("connector")) if raw.get("connector") else None,
                action=action,
                result=result,
                actor=_optional(raw.get("actor")),
                since=since,
                until=until,
                cursor=cursor,
                limit=_limit(raw.get("limit")),
            )
        except AuditStorageError as exc:
            logger.error("audit query failed")
            raise KCVError(503, "audit_write_failed") from exc
        return {"events": events, "next_cursor": next_cursor}


def actor_for_request(request: Any) -> dict[str, Any]:
    auth = request.app.state.auth
    session = auth.session(request) if auth.settings.enabled else None
    return actor_payload(session, auth_enabled=auth.settings.enabled)


def cluster_for(request: Any, cluster_id: str) -> dict[str, str]:
    settings = request.app.state.settings
    for cluster in settings.clusters:
        if cluster.id == cluster_id:
            return cluster_payload(cluster.id, cluster.name)
    return cluster_payload(cluster_id, cluster_id)


def name_from_body(raw: bytes) -> str | None:
    if not raw:
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return connector_name(payload.get("name"))


def result_for_status(status: int, code: str | None) -> str:
    if status in {401, 403} or code in {"auth_required", "role_required"}:
        return "denied"
    if 200 <= status < 300:
        return "success"
    return "failure"


def _build_store(settings: AuditSettings):
    if settings.storage == STORAGE_S3:
        return S3AuditStore(
            bucket=settings.s3_bucket or "",
            prefix=settings.s3_prefix,
            region=settings.s3_region,
            access_key=settings.s3_access_key or "",
            secret_key=settings.s3_secret_key or "",
            endpoint=settings.s3_endpoint,
            retention_days=settings.retention_days,
        )
    return LocalJsonlStore(settings.directory, settings.retention_days)


def _optional(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def _limit(raw: str | None) -> int:
    if raw is None or not str(raw).strip():
        return DEFAULT_LIMIT
    try:
        value = int(str(raw).strip())
    except ValueError as exc:
        raise KCVError(400, "audit_invalid_filter") from exc
    if value < 1 or value > MAX_LIMIT:
        raise KCVError(400, "audit_invalid_filter")
    return value
