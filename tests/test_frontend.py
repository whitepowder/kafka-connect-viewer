import json
import shutil
import subprocess
from pathlib import Path

import pytest

from app.graph import build_graph, parse_connector

ROOT = Path(__file__).resolve().parent.parent
NODE = shutil.which("node")
BOOTSTRAP_VALUE = "kafka-mirror.internal:9093"

S3 = "io.confluent.connect.s3.S3SinkConnector"
JDBC_SINK = "io.confluent.connect.jdbc.JdbcSinkConnector"


def _run(script, *args):
    result = subprocess.run(
        [NODE, str(ROOT / "tests" / script), str(ROOT / "static" / "app.js"), *args],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=ROOT,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.skipif(NODE is None, reason="node is not installed")
@pytest.mark.parametrize("script", ["frontend_smoke.cjs", "create_editor.test.cjs"])
def test_frontend_script(script):
    _run(script)


def graph_fixture():
    connectors = {
        "files": {"connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector", "topic": "orders"},
        "orders-cdc": {"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector", "topic.prefix": "app."},
        "orders-s3": {
            "connector.class": S3,
            "topics": "orders",
            "consumer.override.group.id": "orders-consumers",
            "errors.deadletterqueue.topic.name": "orders-dlq",
        },
        "orders-jdbc": {"connector.class": JDBC_SINK, "topics.regex": "ord(ers)?", "consumer.override.group.id": "orders-consumers"},
        "audit-sink-with-a-very-long-connector-name-v2": {"connector.class": S3, "topics": "orders"},
        "mirror-sink": {
            "connector.class": S3,
            "topics": "app.users",
            "consumer.override.group.id": "${env:MIRROR_GROUP}",
            "consumer.override.bootstrap.servers": BOOTSTRAP_VALUE,
        },
        "users-sink": {"connector.class": S3, "topics": "app.users"},
    }
    facts = [parse_connector(name, config) for name, config in connectors.items()]
    graph = build_graph(facts, [{"connector": "broken", "message": "Kafka Connect returned 500"}])
    graph["generated_at"] = "2026-10-07T00:00:00+00:00"
    return graph


@pytest.mark.skipif(NODE is None, reason="node is not installed")
def test_graph_view(tmp_path):
    fixture = graph_fixture()
    payload = json.dumps(fixture)
    assert BOOTSTRAP_VALUE not in payload
    assert [item["code"] for item in fixture["diagnostics"]] == [
        "shared_topic_same_group", "config_unreadable", "group_unknown", "shared_topic_different_groups",
    ]
    path = tmp_path / "graph.json"
    path.write_text(payload, encoding="utf-8")
    _run("graph.test.cjs", str(path))
