import asyncio
import json
import re
from urllib.parse import unquote

import httpx
import pytest
from fastapi.testclient import TestClient

from app.connect import ConnectClient
from app.graph import GraphRateLimited, GraphService, build_graph, parse_connector
from app.graph.facts import compile_topic_regex, literal_prefix
from app.main import app
from app.settings import Cluster

S3 = "io.confluent.connect.s3.S3SinkConnector"
JDBC_SINK = "io.confluent.connect.jdbc.JdbcSinkConnector"
JDBC_SOURCE = "io.confluent.connect.jdbc.JdbcSourceConnector"
FILE_SOURCE = "org.apache.kafka.connect.file.FileStreamSourceConnector"
SECRET_VALUES = [
    "jdbc:postgresql://db.internal:5432/app",
    "s3cr3t-password",
    "bucket-prod-eu",
    "kafka-a.internal:9093,kafka-b.internal:9093",
    "org.apache.kafka.connect.transforms.RegexRouter",
]


def sink(name, cls=S3, **config):
    return parse_connector(name, {"connector.class": cls, **config})


def source(name, cls=FILE_SOURCE, **config):
    return parse_connector(name, {"connector.class": cls, **config})


def graph_of(*facts, errors=None):
    return build_graph(list(facts), errors or [])


def diags(graph, code=None, severity=None):
    return [
        item
        for item in graph["diagnostics"]
        if (code is None or item["code"] == code) and (severity is None or item["severity"] == severity)
    ]


def edge_set(graph):
    return {(edge["kind"], edge["from"], edge["to"], edge["confidence"]) for edge in graph["edges"]}


# --- facts -------------------------------------------------------------------


def test_sink_facts_use_only_allow_listed_keys():
    facts = parse_connector(
        "orders-s3",
        {
            "connector.class": S3,
            "topics": " orders, payments ,,orders ",
            "consumer.override.group.id": " orders-consumers ",
            "errors.deadletterqueue.topic.name": "orders-dlq",
            "consumer.override.bootstrap.servers": SECRET_VALUES[3],
            "connection.url": SECRET_VALUES[0],
            "connection.password": SECRET_VALUES[1],
            "s3.bucket.name": SECRET_VALUES[2],
            "transforms": "route",
            "transforms.route.type": SECRET_VALUES[4],
        },
    )
    assert facts.type == "sink"
    assert facts.topics == ("orders", "payments")
    assert (facts.group.value, facts.group.origin) == ("orders-consumers", "explicit")
    assert facts.dlq_topic == "orders-dlq"
    assert facts.bootstrap_override is True
    assert not any(secret in repr(facts) for secret in SECRET_VALUES)


@pytest.mark.parametrize(
    ("config", "value", "origin"),
    [
        ({}, "connect-orders-s3", "derived"),
        ({"consumer.override.group.id": "team-a"}, "team-a", "explicit"),
        ({"consumer.override.group.id": "${file:/etc/kcv/groups.properties:orders}"}, None, "unknown"),
        ({"consumer.override.group.id": "******"}, None, "unknown"),
        ({"consumer.override.group.id": "   "}, None, "unknown"),
    ],
)
def test_sink_group_origin(config, value, origin):
    facts = sink("orders-s3", topics="orders", **config)
    assert (facts.group.value, facts.group.origin) == (value, origin)


def test_placeholder_topics_are_unknown():
    facts = sink("a", topics="${file:/x:topics}")
    assert facts.topics == ()
    assert facts.topics_unknown
    assert "topics" in facts.unknown


def test_source_facts():
    facts = source("files", topic="orders", **{"topic.prefix": "app.", "file": "/var/data/in.txt"})
    assert (facts.type, facts.topic, facts.topic_prefix) == ("source", "orders", "app.")
    assert facts.group is None
    assert "/var/data" not in repr(facts)


@pytest.mark.parametrize(
    ("cls", "config", "plugins", "expected"),
    [
        (S3, {}, {}, "sink"),
        (JDBC_SOURCE, {}, {}, "source"),
        ("com.example.Custom", {}, {"com.example.Custom": "sink"}, "sink"),
        ("com.example.Custom", {"topics": "a"}, {}, "sink"),
        ("com.example.Custom", {"topics.regex": "a.*"}, {}, "sink"),
        ("com.example.Custom", {"topic": "a"}, {}, "unknown"),
        (S3, {}, {S3: "source"}, "source"),
    ],
)
def test_connector_type_detection(cls, config, plugins, expected):
    assert parse_connector("x", {"connector.class": cls, **config}, plugins).type == expected


def test_topic_regex_uses_java_full_match_semantics():
    regex = compile_topic_regex("orders")
    assert regex.matches("orders")
    assert not regex.matches("orders-v2")
    assert compile_topic_regex("(?i)orders\\..*").matches("ORDERS.eu")
    assert compile_topic_regex("\\Qorders.v1\\E").matches("orders.v1")
    assert not compile_topic_regex("\\Qorders.v1\\E").matches("ordersXv1")
    assert compile_topic_regex("(?<region>eu|us)\\.orders").matches("eu.orders")
    broken = compile_topic_regex("orders(")
    assert not broken.analyzable
    assert not broken.matches("orders(")


@pytest.mark.parametrize(
    ("pattern", "prefix"),
    [
        ("orders\\..*", "orders."),
        ("^app\\.public\\..*", "app.public."),
        ("ab?c", "a"),
        ("ab*c", "a"),
        ("ab{0,2}c", "a"),
        ("ab+c", "ab"),
        ("(?i)Orders.*", "Orders"),
        ("a|b", ""),
        ("\\d+", ""),
        ("[ab]c", ""),
    ],
)
def test_literal_prefix(pattern, prefix):
    assert literal_prefix(pattern) == prefix


# --- graph -------------------------------------------------------------------


def test_graph_edges_for_three_columns():
    graph = graph_of(
        source("files", topic="orders"),
        source("cdc", JDBC_SOURCE, **{"topic.prefix": "app."}),
        sink("exact", topics="orders,app.users", **{"errors.deadletterqueue.topic.name": "exact-dlq"}),
        sink("by-regex", **{"topics.regex": "app\\..*"}),
    )
    edges = edge_set(graph)
    assert ("writes", "connector:files", "topic:orders", "declared") in edges
    assert ("writes", "connector:cdc", "pattern:prefix:app.", "pattern") in edges
    assert ("writes", "connector:cdc", "topic:app.users", "pattern") in edges
    assert ("reads", "topic:orders", "connector:exact", "declared") in edges
    assert ("reads", "topic:app.users", "connector:exact", "declared") in edges
    assert ("reads", "pattern:regex:app\\..*", "connector:by-regex", "pattern") in edges
    assert ("reads", "topic:app.users", "connector:by-regex", "pattern") in edges
    assert ("reads", "pattern:prefix:app.", "connector:by-regex", "possible") in edges
    assert ("dlq", "connector:exact", "topic:exact-dlq", "declared") in edges
    assert ("reads", "topic:orders", "connector:by-regex", "pattern") not in edges

    nodes = {node["id"]: node for node in graph["nodes"]}
    assert nodes["topic:exact-dlq"]["dlq"] is True
    assert nodes["topic:orders"]["dlq"] is False
    assert nodes["connector:exact"]["group"] == {"value": "connect-exact", "origin": "derived"}
    assert graph["stats"] == {
        "connectors": 4, "sources": 2, "sinks": 2, "topics": 3,
        "errors": 0, "warnings": 0, "ok": 1, "unreadable": 0,
    }


def test_graph_never_exposes_ignored_values():
    facts = parse_connector(
        "a",
        {
            "connector.class": JDBC_SINK,
            "topics": "orders",
            "connection.url": SECRET_VALUES[0],
            "connection.password": SECRET_VALUES[1],
            "consumer.override.bootstrap.servers": SECRET_VALUES[3],
        },
    )
    payload = json.dumps(graph_of(facts))
    for secret in SECRET_VALUES:
        assert secret not in payload
    assert '"bootstrap_override": true' in payload


# --- diagnostics -------------------------------------------------------------


def test_same_exact_topic_and_same_explicit_group_is_error():
    group = {"consumer.override.group.id": "orders-consumers"}
    graph = graph_of(sink("a", topics="orders", **group), sink("b", topics="orders,other", **group))
    [error] = diags(graph, severity="error")
    assert error["code"] == "shared_topic_same_group"
    assert (error["topic"], error["group"], error["connectors"]) == ("orders", "orders-consumers", ["a", "b"])
    assert error["edges"] == ["reads:topic:orders->connector:a", "reads:topic:orders->connector:b"]
    assert {edge for edge in error["edges"]} <= {edge["id"] for edge in graph["edges"]}


def test_exact_topic_matched_by_regex_with_same_explicit_group_is_error():
    group = {"consumer.override.group.id": "g"}
    graph = graph_of(sink("a", topics="orders.eu", **group), sink("b", **{"topics.regex": "orders\\..*"}, **group))
    [error] = diags(graph, severity="error")
    assert error["topic"] == "orders.eu"
    assert {member["connector"]: member["via"] for member in error["members"]} == {"a": "topics", "b": "topics.regex"}


def test_exact_topic_from_a_source_witnesses_regex_overlap():
    group = {"consumer.override.group.id": "g"}
    graph = graph_of(
        source("files", topic="orders.eu"),
        sink("a", **{"topics.regex": "orders\\..*"}, **group),
        sink("b", **{"topics.regex": "orders\\.(eu|us)"}, **group),
    )
    [error] = diags(graph, severity="error")
    assert (error["topic"], error["connectors"]) == ("orders.eu", ["a", "b"])
    assert not diags(graph, code="possible_regex_overlap_same_group")


def test_uncertain_regex_overlap_with_same_group_is_warning():
    group = {"consumer.override.group.id": "g"}
    graph = graph_of(sink("a", **{"topics.regex": "orders\\..*"}, **group), sink("b", **{"topics.regex": "orders\\.eu.*"}, **group))
    [warning] = diags(graph)
    assert (warning["severity"], warning["code"]) == ("warning", "possible_regex_overlap_same_group")
    assert warning["patterns"] == ["orders\\..*", "orders\\.eu.*"]
    assert "pattern:regex:orders\\..*" in warning["nodes"]


def test_incompatible_regex_prefixes_do_not_overlap():
    group = {"consumer.override.group.id": "g"}
    graph = graph_of(sink("a", **{"topics.regex": "orders\\..*"}, **group), sink("b", **{"topics.regex": "payments\\..*"}, **group))
    assert graph["diagnostics"] == []


def test_unanalyzable_regex_with_same_group_is_warning():
    group = {"consumer.override.group.id": "g"}
    graph = graph_of(sink("a", **{"topics.regex": "\\p{Alpha}+"}, **group), sink("b", topics="orders", **group))
    [warning] = diags(graph)
    assert warning["code"] == "possible_regex_overlap_same_group"


def test_unknown_groups_are_never_equal():
    placeholder = {"consumer.override.group.id": "${env:GROUP}"}
    graph = graph_of(sink("a", topics="orders", **placeholder), sink("b", topics="orders", **placeholder))
    assert not diags(graph, severity="error")
    [warning] = diags(graph, code="group_unknown")
    assert (warning["severity"], warning["undetermined"]) == ("warning", ["a", "b"])


def test_unknown_group_against_explicit_group_is_warning():
    graph = graph_of(
        sink("a", topics="orders", **{"consumer.override.group.id": "${env:GROUP}"}),
        sink("b", topics="orders", **{"consumer.override.group.id": "g"}),
    )
    assert [(item["severity"], item["code"]) for item in graph["diagnostics"]] == [("warning", "group_unknown")]


def test_unknown_group_with_possible_regex_overlap_is_warning():
    graph = graph_of(
        sink("a", **{"topics.regex": "orders.*", "consumer.override.group.id": "${env:GROUP}"}),
        sink("b", **{"topics.regex": "orders\\.eu.*"}),
    )
    [warning] = diags(graph)
    assert (warning["code"], warning["connectors"], warning["undetermined"]) == ("group_unknown", ["a", "b"], ["a"])


def test_explicit_group_equal_to_derived_default_is_warning():
    graph = graph_of(sink("a", topics="orders", **{"consumer.override.group.id": "connect-b"}), sink("b", topics="orders"))
    [warning] = diags(graph)
    assert (warning["severity"], warning["code"], warning["group"]) == ("warning", "same_group_as_default", "connect-b")


def test_different_groups_are_ok_and_defaults_are_expected():
    explicit = graph_of(
        sink("a", topics="orders", **{"consumer.override.group.id": "g1"}),
        sink("b", topics="orders", **{"consumer.override.group.id": "g2"}),
    )
    [ok] = diags(explicit)
    assert (ok["severity"], ok["code"], ok["expected"]) == ("ok", "shared_topic_different_groups", False)

    defaults = graph_of(sink("a", topics="orders"), sink("b", topics="orders"))
    [ok] = diags(defaults)
    assert (ok["severity"], ok["expected"]) == ("ok", True)


def test_bootstrap_override_never_produces_error():
    group = {"consumer.override.group.id": "g"}
    override = {"consumer.override.bootstrap.servers": SECRET_VALUES[3]}
    graph = graph_of(sink("a", topics="orders", **group), sink("b", topics="orders", **group, **override))
    assert not diags(graph, severity="error")
    [warning] = diags(graph, code="bootstrap_override")
    assert warning["overridden"] == ["b"]
    assert SECRET_VALUES[3] not in json.dumps(graph)

    graph = graph_of(
        sink("a", topics="orders", **group),
        sink("b", topics="orders", **group),
        sink("c", topics="orders", **group, **override),
    )
    [error] = diags(graph, severity="error")
    assert error["connectors"] == ["a", "b"]
    assert diags(graph, code="bootstrap_override")[0]["connectors"] == ["a", "b", "c"]


def test_connector_level_warnings():
    graph = graph_of(
        sink("both", topics="orders", **{"topics.regex": "orders.*"}),
        sink("hidden", topics="${file:/x:topics}"),
        errors=[{"connector": "broken", "message": "Kafka Connect ответил 500"}],
    )
    codes = {(item["code"], item["connectors"][0]) for item in diags(graph, severity="warning")}
    assert codes == {("topics_and_regex", "both"), ("topics_unknown", "hidden"), ("config_unreadable", "broken")}
    assert graph["partial"] is True
    assert graph["stats"]["unreadable"] == 1


def test_diagnostics_are_sorted_by_severity():
    group = {"consumer.override.group.id": "g"}
    graph = graph_of(
        sink("a", topics="orders,shared", **group),
        sink("b", topics="orders", **group),
        sink("c", topics="shared", **{"consumer.override.group.id": "other"}),
        sink("d", topics="x", **{"consumer.override.group.id": "${env:G}"}),
        sink("e", topics="x"),
    )
    assert [item["severity"] for item in graph["diagnostics"]] == ["error", "warning", "ok"]


# --- service -----------------------------------------------------------------


class FakeConnect:
    def __init__(self, configs, plugins=(), delay=0.0, fail=(), missing=()):
        self.configs = dict(configs)
        self.plugins = None if plugins is None else list(plugins)
        self.delay = delay
        self.fail = set(fail)
        self.missing = set(missing)
        self.calls = []
        self.active = 0
        self.max_active = 0

    async def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.calls.append((request.method, request.url.path))
        assert "expand" not in url, url
        path = request.url.path
        if request.method == "GET" and path == "/connectors":
            return httpx.Response(200, json=list(self.configs))
        if request.method == "GET" and path == "/connector-plugins":
            if self.plugins is None:
                return httpx.Response(500, json={"message": "boom"})
            return httpx.Response(200, json=self.plugins)
        match = re.fullmatch(r"/connectors/([^/]+)/config", path)
        if request.method == "GET" and match:
            name = unquote(match.group(1))
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                await asyncio.sleep(self.delay)
            finally:
                self.active -= 1
            if name in self.fail:
                return httpx.Response(500, json={"message": "worker exploded"})
            if name in self.missing or name not in self.configs:
                return httpx.Response(404, json={"message": "not found"})
            return httpx.Response(200, json=self.configs[name])
        if request.method in {"PUT", "POST", "DELETE"}:
            return httpx.Response(200, json={"name": "x", "config": {}})
        return httpx.Response(404, json={"message": "not found"})

    def config_calls(self):
        return [path for method, path in self.calls if path.endswith("/config") and method == "GET"]

    def client(self):
        return ConnectClient(Cluster(id="lab", name="lab", url="http://connect.test"), transport=httpx.MockTransport(self.handler))


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def many_sinks(count):
    return {f"sink-{index:03d}": {"connector.class": S3, "topics": f"t{index}"} for index in range(count)}


@pytest.mark.anyio
async def test_config_requests_are_individual_and_capped_at_eight():
    fake = FakeConnect(many_sinks(30), delay=0.02)
    client = fake.client()
    try:
        graph = await GraphService().graph("lab", client)
    finally:
        await client.aclose()
    assert fake.max_active == 8
    assert sorted(fake.config_calls()) == sorted(f"/connectors/sink-{index:03d}/config" for index in range(30))
    assert graph["stats"]["sinks"] == 30
    assert all("expand" not in path for _method, path in fake.calls)


def test_concurrency_cannot_be_raised_above_eight():
    assert GraphService(concurrency=50).concurrency == 8


@pytest.mark.anyio
async def test_graph_and_facts_are_cached():
    fake = FakeConnect(many_sinks(3))
    clock = Clock()
    service = GraphService(facts_ttl=60, graph_ttl=30, clock=clock)
    client = fake.client()
    try:
        first = await service.graph("lab", client)
        calls = len(fake.calls)
        assert await service.graph("lab", client) is first
        assert len(fake.calls) == calls

        clock.now += 31
        await service.graph("lab", client)
        assert fake.calls[calls:] == [("GET", "/connectors")]

        clock.now += 30
        await service.graph("lab", client)
        assert len(fake.config_calls()) == 6
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_refresh_bypasses_cache_and_is_rate_limited():
    fake = FakeConnect(many_sinks(2))
    clock = Clock()
    service = GraphService(clock=clock)
    client = fake.client()
    try:
        await service.graph("lab", client)
        await service.graph("lab", client, refresh=True)
        assert len(fake.config_calls()) == 4
        clock.now += 5
        with pytest.raises(GraphRateLimited) as raised:
            await service.graph("lab", client, refresh=True)
        assert raised.value.retry_after == pytest.approx(5)
        clock.now += 5
        await service.graph("lab", client, refresh=True)
        assert len(fake.config_calls()) == 6
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_concurrent_requests_share_one_build():
    fake = FakeConnect(many_sinks(5), delay=0.02)
    service = GraphService()
    client = fake.client()
    try:
        first, second = await asyncio.gather(service.graph("lab", client), service.graph("lab", client))
    finally:
        await client.aclose()
    assert first is second
    assert len(fake.config_calls()) == 5
    assert fake.calls.count(("GET", "/connectors")) == 1


@pytest.mark.anyio
async def test_failed_configs_make_graph_partial_and_missing_ones_are_skipped():
    configs = {**many_sinks(3), "gone": {"connector.class": S3, "topics": "x"}}
    fake = FakeConnect(configs, fail={"sink-001"}, missing={"gone"})
    client = fake.client()
    try:
        graph = await GraphService().graph("lab", client)
    finally:
        await client.aclose()
    assert graph["partial"] is True
    assert graph["errors"] == [{"connector": "sink-001", "message": "worker exploded"}]
    assert {node["name"] for node in graph["nodes"] if node["kind"] == "connector"} == {"sink-000", "sink-002"}
    assert [item["connectors"] for item in diags(graph, code="config_unreadable")] == [["sink-001"]]


@pytest.mark.anyio
async def test_invalidate_refetches_only_that_connector_and_drops_removed_ones():
    fake = FakeConnect(many_sinks(3))
    service = GraphService()
    client = fake.client()
    try:
        await service.graph("lab", client)
        service.invalidate("lab", "sink-001")
        await service.graph("lab", client)
        assert fake.config_calls()[3:] == ["/connectors/sink-001/config"]

        del fake.configs["sink-002"]
        service.invalidate("lab", "sink-002")
        graph = await service.graph("lab", client)
        assert "connector:sink-002" not in {node["id"] for node in graph["nodes"]}
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_build_started_before_invalidation_is_not_cached():
    fake = FakeConnect(many_sinks(2), delay=0.05)
    service = GraphService()
    client = fake.client()
    try:
        building = asyncio.ensure_future(service.graph("lab", client))
        await asyncio.sleep(0.01)
        service.invalidate("lab", "sink-000")
        await building
        await service.graph("lab", client)
        assert len(fake.config_calls()) == 4
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_plugin_types_are_used_and_plugin_failures_tolerated():
    configs = {"custom": {"connector.class": "com.example.Custom", "topic": "orders"}}
    fake = FakeConnect(configs, plugins=[{"class": "com.example.Custom", "type": "source"}])
    client = fake.client()
    try:
        graph = await GraphService().graph("lab", client)
        assert graph["nodes"][0]["type"] == "source"

        broken = FakeConnect({"s": {"connector.class": S3, "topics": "a"}}, plugins=None)
        broken_client = broken.client()
        try:
            graph = await GraphService().graph("lab", broken_client)
        finally:
            await broken_client.aclose()
        assert graph["nodes"][0]["type"] == "sink"
    finally:
        await client.aclose()


def test_graph_settings_from_env():
    service = GraphService.from_env({"GRAPH_FACTS_TTL": "5", "GRAPH_TTL": "2.5"})
    assert (service.facts_ttl, service.graph_ttl) == (5.0, 2.5)
    with pytest.raises(ValueError):
        GraphService.from_env({"GRAPH_TTL": "soon"})
    with pytest.raises(ValueError):
        GraphService.from_env({"GRAPH_FACTS_TTL": "-1"})


# --- API ---------------------------------------------------------------------


@pytest.fixture
def api(monkeypatch, tmp_path):
    fake = FakeConnect(
        {
            "files": {"connector.class": FILE_SOURCE, "topic": "orders", "file": "/srv/secret/in.txt"},
            "orders-s3": {
                "connector.class": S3,
                "topics": "orders",
                "consumer.override.group.id": "g",
                "s3.bucket.name": SECRET_VALUES[2],
            },
            "orders-jdbc": {
                "connector.class": JDBC_SINK,
                "topics": "orders",
                "consumer.override.group.id": "g",
                "connection.url": SECRET_VALUES[0],
                "connection.password": SECRET_VALUES[1],
                "consumer.override.bootstrap.servers": SECRET_VALUES[3],
            },
        }
    )
    monkeypatch.setenv("CLUSTERS_FILE", str(tmp_path / "missing.json"))
    monkeypatch.setenv("CONNECT_CLUSTERS", "lab=http://connect.test")
    monkeypatch.delenv("AUTH_ENABLED", raising=False)
    app.state.client_factory = lambda cluster: ConnectClient(cluster, transport=httpx.MockTransport(fake.handler))
    with TestClient(app) as client:
        yield client, fake
    app.state.client_factory = None


def test_graph_endpoint(api):
    client, fake = api
    response = client.get("/api/clusters/lab/graph")
    assert response.status_code == 200
    graph = response.json()
    assert {node["id"] for node in graph["nodes"]} == {
        "connector:files", "connector:orders-s3", "connector:orders-jdbc", "topic:orders",
    }
    assert [item["code"] for item in graph["diagnostics"]] == ["bootstrap_override"]
    assert graph["generated_at"]
    body = response.text
    for secret in [*SECRET_VALUES, "/srv/secret"]:
        assert secret not in body
    assert all("expand" not in path for _method, path in fake.calls)


def test_graph_endpoint_refresh_limit_and_unknown_cluster(api):
    client, _fake = api
    assert client.get("/api/clusters/lab/graph?refresh=true").status_code == 200
    limited = client.get("/api/clusters/lab/graph?refresh=true")
    assert limited.status_code == 429
    assert int(limited.headers["Retry-After"]) >= 1
    assert client.get("/api/clusters/nope/graph").status_code == 404


def test_connector_changes_invalidate_graph_cache(api):
    client, fake = api
    client.get("/api/clusters/lab/graph")
    before = len(fake.config_calls())
    assert client.get("/api/clusters/lab/graph").status_code == 200
    assert len(fake.config_calls()) == before

    fake.configs["orders-s3"]["consumer.override.group.id"] = "other"
    assert client.put("/api/clusters/lab/connectors/orders-s3/config", json={"config": {"connector.class": S3}}).status_code == 200
    graph = client.get("/api/clusters/lab/graph").json()
    assert fake.config_calls()[before:] == ["/connectors/orders-s3/config"]
    assert [item["code"] for item in graph["diagnostics"]] == ["shared_topic_different_groups"]

    fake.configs["new-sink"] = {"connector.class": S3, "topics": "orders"}
    assert client.post("/api/clusters/lab/connectors", json={"name": "new-sink", "config": fake.configs["new-sink"]}).status_code == 201
    assert "connector:new-sink" in {node["id"] for node in client.get("/api/clusters/lab/graph").json()["nodes"]}

    del fake.configs["files"]
    assert client.delete("/api/clusters/lab/connectors/files").status_code == 200
    assert "connector:files" not in {node["id"] for node in client.get("/api/clusters/lab/graph").json()["nodes"]}
