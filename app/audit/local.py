from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.audit.events import (
    AuditStorageError,
    after_cursor,
    decode_cursor,
    parse_ts,
    sanitize_event,
)

try:
    import fcntl
except ImportError:  # pragma: no cover - Unix lock is used in supported deployments
    fcntl = None


class LocalJsonlStore:
    def __init__(self, directory: str, retention_days: int = 90):
        self.directory = Path(directory)
        self.retention_days = retention_days

    def prepare(self) -> None:
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise AuditStorageError(f"cannot create audit directory: {exc}") from exc

    def append(self, event: dict) -> None:
        self.prepare()
        path = self.directory / f"{event['ts'][:10]}.jsonl"
        line = json.dumps(sanitize_event(event), ensure_ascii=True, separators=(",", ":"))
        try:
            with path.open("a", encoding="utf-8") as handle:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                handle.write(line + "\n")
                handle.flush()
                try:
                    os.fsync(handle.fileno())
                except OSError:
                    pass
        except OSError as exc:
            raise AuditStorageError(f"cannot write audit event: {exc}") from exc
        self.purge()

    def query(
        self,
        *,
        cluster: str | None = None,
        connector: str | None = None,
        action: str | None = None,
        result: str | None = None,
        actor: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[dict], str | None]:
        self.prepare()
        marker = decode_cursor(cursor) if cursor else None
        cutoff = self._cutoff()
        events: list[dict] = []
        for path in self._files(since, until, cutoff):
            try:
                text = path.read_text(encoding="utf-8")
            except OSError as exc:
                raise AuditStorageError(f"cannot read audit log: {exc}") from exc
            for raw in text.splitlines():
                if not raw.strip():
                    continue
                try:
                    event = sanitize_event(json.loads(raw))
                except (ValueError, json.JSONDecodeError):
                    continue
                if self._keep(event, cluster, connector, action, result, actor, since, until, cutoff) and after_cursor(event, marker):
                    events.append(event)
        events.sort(key=lambda item: (item["ts"], item["id"]), reverse=True)
        page = events[:limit]
        nxt = f'{page[-1]["ts"]}|{page[-1]["id"]}' if len(events) > limit else None
        return page, nxt

    def purge(self) -> None:
        cutoff = self._cutoff()
        if cutoff is None:
            return
        for path in self.directory.glob("*.jsonl"):
            try:
                day = datetime.strptime(path.stem, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if day < cutoff:
                try:
                    path.unlink()
                except OSError:
                    continue

    def _cutoff(self) -> datetime | None:
        if self.retention_days <= 0:
            return None
        return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=self.retention_days)

    def _files(self, since: datetime | None, until: datetime | None, cutoff: datetime | None) -> list[Path]:
        start = since or cutoff
        paths = []
        for path in self.directory.glob("*.jsonl"):
            try:
                day = datetime.strptime(path.stem, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if start and day < start.replace(hour=0, minute=0, second=0, microsecond=0):
                continue
            if until and day > until.replace(hour=0, minute=0, second=0, microsecond=0):
                continue
            paths.append(path)
        paths.sort(reverse=True)
        return paths

    @staticmethod
    def _keep(
        event: dict,
        cluster: str | None,
        connector: str | None,
        action: str | None,
        result: str | None,
        actor: str | None,
        since: datetime | None,
        until: datetime | None,
        cutoff: datetime | None,
    ) -> bool:
        try:
            ts = parse_ts(event["ts"])
        except ValueError:
            return False
        if cutoff and ts < cutoff:
            return False
        if since and ts < since:
            return False
        if until and ts > until:
            return False
        if action and event["action"] != action:
            return False
        if result and event["result"] != result:
            return False
        if connector and event.get("connector") != connector:
            return False
        if cluster and (not event.get("cluster") or event["cluster"].get("id") != cluster):
            return False
        if actor and str(event.get("actor", {}).get("name") or "") != actor:
            return False
        return True
