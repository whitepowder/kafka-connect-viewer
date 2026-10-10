"""Measured graph benchmarks against a mocked Kafka Connect API.

Timings are recorded, not asserted. Structural limits (request count,
concurrency, cache, secret redaction) are asserted.
"""

from __future__ import annotations

import asyncio
import json
import time
import tracemalloc
from collections import Counter

import httpx
import pytest

from app.connect import ConnectClient
from app.graph.service import GraphRateLimited, GraphService
from app.settings import Cluster
from tests.graph_workload import plugin_catalog, synthetic_configs

COUNTS = (10, 50, 100, 250, 500, 1000)


class RecordingConnect:
    def __init__(self, configs: dict[str, dict[str, str]], *, delay: float = 0.0, fail: set[str] | None = None):
        self.configs = configs
        self.delay = delay
        self.fail = fail or set()
        self.calls: list[str] = []
        self.inflight = 0
        self.max_inflight = 0
        self._lock = asyncio.Lock()

    def client(self) -> ConnectClient:
        cluster = Cluster(id="bench", name="bench", url="http://connect.bench", username=None, password=None, verify_ssl=True, headers={})
        return ConnectClient(cluster, transport=httpx.MockTransport(self.handler))

    async def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        async with self._lock:
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
            self.calls.append(path)
        try:
            if self.delay and path.endswith("/config"):
                await asyncio.sleep(self.delay)
            if path == "/":
                return httpx.Response(200, json={"version": "bench", "commit": "bench", "kafka_cluster_id": "bench"})
            if path == "/connectors":
                return httpx.Response(200, json=sorted(self.configs))
            if path == "/connector-plugins":
                return httpx.Response(200, json=plugin_catalog())
            if path.endswith("/config"):
                name = path.split("/")[2]
                if name in self.fail:
                    return httpx.Response(500, json={"message": "worker exploded"})
                config = self.configs.get(name)
                if config is None:
                    return httpx.Response(404, json={"message": "not found"})
                return httpx.Response(200, json=config)
            return httpx.Response(404, json={"message": "not found"})
        finally:
            async with self._lock:
                self.inflight -= 1


def _summarize(payload: dict) -> dict:
    raw = json.dumps(payload).encode()
    return {
        "nodes": len(payload["nodes"]),
        "edges": len(payload["edges"]),
        "topics": payload["stats"]["topics"],
        "diagnostics": len(payload["diagnostics"]),
        "bytes": len(raw),
    }


async def _once(count: int) -> dict:
    configs = synthetic_configs(count)
    upstream = RecordingConnect(configs)
    service = GraphService(facts_ttl=60, graph_ttl=30, refresh_interval=10, clock=time.monotonic)
    client = upstream.client()
    tracemalloc.start()
    started = time.perf_counter()
    cold = await service.graph("bench", client)
    cold_s = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    calls_after_cold = len(upstream.calls)
    started = time.perf_counter()
    warm = await service.graph("bench", client)
    warm_s = time.perf_counter() - started
    started = time.perf_counter()
    await asyncio.gather(*(service.graph("bench", client) for _ in range(8)))
    concurrent_warm_s = time.perf_counter() - started
    summary = _summarize(cold)
    dumped = json.dumps(cold)
    assert "hidden.invalid" not in dumped
    assert "password" not in dumped
    assert warm == cold
    assert len(upstream.calls) == calls_after_cold
    config_calls = [path for path in upstream.calls if path.endswith("/config")]
    assert Counter(config_calls).most_common(1)[0][1] == 1
    assert len(config_calls) == count
    assert upstream.calls.count("/connectors") == 1
    assert upstream.calls.count("/connector-plugins") == 1
    assert upstream.max_inflight <= 8
    return {
        "connectors": count,
        **summary,
        "cold_s": round(cold_s, 4),
        "warm_s": round(warm_s, 4),
        "concurrent_warm_s": round(concurrent_warm_s, 4),
        "upstream_calls": calls_after_cold,
        "max_inflight": upstream.max_inflight,
        "peak_bytes": peak,
    }


@pytest.mark.parametrize("count", COUNTS)
def test_graph_workload_stays_bounded(count):
    result = asyncio.run(_once(count))
    print("BENCH", json.dumps(result))


def test_slow_upstream_stays_at_eight_workers():
    configs = synthetic_configs(24)
    upstream = RecordingConnect(configs, delay=0.05)
    service = GraphService(clock=time.monotonic)
    started = time.perf_counter()
    payload = asyncio.run(service.graph("bench", upstream.client()))
    elapsed = time.perf_counter() - started
    assert payload["stats"]["connectors"] == 24
    assert upstream.max_inflight <= 8
    assert elapsed < 0.05 * 24
    print("BENCH", json.dumps({"slow_24_s": round(elapsed, 4), "max_inflight": upstream.max_inflight}))


def test_partial_failures_do_not_multiply_requests():
    configs = synthetic_configs(20)
    fail = {name for index, name in enumerate(sorted(configs)) if index % 5 == 0}
    upstream = RecordingConnect(configs, fail=fail)
    service = GraphService(clock=time.monotonic)
    payload = asyncio.run(service.graph("bench", upstream.client()))
    assert payload["partial"] is True
    assert payload["stats"]["unreadable"] == len(fail)
    assert len([path for path in upstream.calls if path.endswith("/config")]) == 20
    dumped = json.dumps(payload)
    assert "worker exploded" in dumped
    assert "hidden.invalid" not in dumped


def _percentile(samples: list[float], pct: float) -> float:
    ordered = sorted(samples)
    index = min(len(ordered) - 1, max(0, round((pct / 100) * (len(ordered) - 1))))
    return round(ordered[index], 4)


def test_latency_percentiles_for_cold_and_warm_graphs():
    async def run():
        recorded = {}
        for count, repeats in ((100, 21), (1000, 7)):
            cold, warm = [], []
            for _ in range(repeats):
                configs = synthetic_configs(count)
                upstream = RecordingConnect(configs)
                service = GraphService(clock=time.monotonic)
                client = upstream.client()
                started = time.perf_counter()
                await service.graph("bench", client)
                cold.append(time.perf_counter() - started)
                started = time.perf_counter()
                await service.graph("bench", client)
                warm.append(time.perf_counter() - started)
            recorded[count] = {
                "repeats": repeats,
                "cold_p50": _percentile(cold, 50),
                "cold_p95": _percentile(cold, 95),
                "cold_p99": _percentile(cold, 99),
                "warm_p50": _percentile(warm, 50),
                "warm_p95": _percentile(warm, 95),
            }
        return recorded

    print("BENCH", json.dumps(asyncio.run(run())))


def test_refresh_is_rate_limited_and_does_not_duplicate_a_cold_build():
    configs = synthetic_configs(10)
    upstream = RecordingConnect(configs)
    clock = {"now": 1000.0}
    service = GraphService(refresh_interval=10, clock=lambda: clock["now"])
    client = upstream.client()

    async def run():
        await service.graph("bench", client, refresh=True)
        clock["now"] = 1005.0
        with pytest.raises(GraphRateLimited):
            await service.graph("bench", client, refresh=True)
        clock["now"] = 1011.0
        await service.graph("bench", client, refresh=True)

    asyncio.run(run())
    assert len([path for path in upstream.calls if path.endswith("/config")]) == 20
