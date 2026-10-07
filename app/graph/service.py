from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from app.connect import ConnectClient, ConnectError
from app.graph.build import build_graph
from app.graph.facts import ConnectorFacts, parse_connector

MAX_CONCURRENT_CONFIG_REQUESTS = 8


class GraphRateLimited(Exception):
    def __init__(self, retry_after: float):
        super().__init__("graph refresh rate limited")
        self.retry_after = retry_after


class GraphSettingsError(ValueError):
    pass


@dataclass
class _ClusterCache:
    facts: dict[str, tuple[float, ConnectorFacts]] = field(default_factory=dict)
    plugin_types: tuple[float, dict[str, str]] | None = None
    graph: tuple[float, dict[str, Any]] | None = None
    last_refresh: float | None = None
    inflight: asyncio.Task | None = None
    generation: int = 0


class GraphService:
    """Builds graphs from per-connector configs, cached as facts only."""

    def __init__(
        self,
        *,
        facts_ttl: float = 60.0,
        graph_ttl: float = 30.0,
        refresh_interval: float = 10.0,
        concurrency: int = MAX_CONCURRENT_CONFIG_REQUESTS,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.facts_ttl = facts_ttl
        self.graph_ttl = graph_ttl
        self.refresh_interval = refresh_interval
        self.concurrency = min(concurrency, MAX_CONCURRENT_CONFIG_REQUESTS)
        self._clock = clock
        self._caches: dict[str, _ClusterCache] = {}

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> GraphService:
        return cls(
            facts_ttl=_seconds(env, "GRAPH_FACTS_TTL", 60.0),
            graph_ttl=_seconds(env, "GRAPH_TTL", 30.0),
        )

    async def graph(self, cluster_id: str, client: ConnectClient, *, refresh: bool = False) -> dict[str, Any]:
        cache = self._caches.setdefault(cluster_id, _ClusterCache())
        now = self._clock()
        if refresh:
            if cache.last_refresh is not None and now - cache.last_refresh < self.refresh_interval:
                raise GraphRateLimited(self.refresh_interval - (now - cache.last_refresh))
        elif cache.graph is not None and now - cache.graph[0] < self.graph_ttl:
            return cache.graph[1]

        if cache.inflight is None:
            if refresh:
                cache.last_refresh = now
            task = asyncio.ensure_future(self._build(client, cache, refresh))
            cache.inflight = task

            def release(done: asyncio.Task) -> None:
                if cache.inflight is done:
                    cache.inflight = None

            task.add_done_callback(release)
        return await asyncio.shield(cache.inflight)

    def invalidate(self, cluster_id: str, name: str | None = None) -> None:
        cache = self._caches.get(cluster_id)
        if cache is None:
            return
        cache.generation += 1
        cache.graph = None
        if name is None:
            cache.facts.clear()
        else:
            cache.facts.pop(name, None)

    async def _build(self, client: ConnectClient, cache: _ClusterCache, refresh: bool) -> dict[str, Any]:
        generation = cache.generation
        names = await client.list_names()
        plugin_types = await self._plugin_types(client, cache, refresh)
        started = self._clock()
        for stale in set(cache.facts) - set(names):
            del cache.facts[stale]

        semaphore = asyncio.Semaphore(self.concurrency)
        errors: list[dict[str, str]] = []

        async def load(name: str) -> ConnectorFacts | None:
            cached = cache.facts.get(name)
            if cached is not None and not refresh and started - cached[0] < self.facts_ttl:
                return cached[1]
            async with semaphore:
                try:
                    config = await client.connector_config(name)
                except ConnectError as exc:
                    cache.facts.pop(name, None)
                    if exc.status != 404:
                        errors.append({"connector": name, "message": exc.message})
                    return None
                except Exception:
                    cache.facts.pop(name, None)
                    errors.append({"connector": name, "message": "config not readable"})
                    return None
            facts = parse_connector(name, config, plugin_types)
            if cache.generation == generation:
                cache.facts[name] = (self._clock(), facts)
            return facts

        results = await asyncio.gather(*(load(name) for name in names))
        payload = build_graph([item for item in results if item is not None], errors)
        payload["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if cache.generation == generation:
            cache.graph = (self._clock(), payload)
        return payload

    async def _plugin_types(self, client: ConnectClient, cache: _ClusterCache, refresh: bool) -> dict[str, str]:
        now = self._clock()
        if cache.plugin_types is not None and not refresh and now - cache.plugin_types[0] < self.facts_ttl:
            return cache.plugin_types[1]
        try:
            plugins = await client.plugins()
        except ConnectError:
            return cache.plugin_types[1] if cache.plugin_types else {}
        types = {
            str(plugin["class"]): str(plugin["type"]).lower()
            for plugin in plugins
            if isinstance(plugin.get("class"), str) and isinstance(plugin.get("type"), str)
        }
        cache.plugin_types = (now, types)
        return types


def _seconds(env: Mapping[str, str], key: str, default: float) -> float:
    raw = env.get(key, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise GraphSettingsError(f"{key} должен быть числом секунд") from exc
    if value < 0:
        raise GraphSettingsError(f"{key} не может быть отрицательным")
    return value
