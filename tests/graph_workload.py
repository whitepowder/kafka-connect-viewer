"""Deterministic synthetic Kafka Connect configs for graph benchmarks.

Names, topics and classes are invented. Nothing here is a real cluster.
"""

from __future__ import annotations

SOURCE = "org.apache.kafka.connect.file.FileStreamSourceConnector"
SINK = "io.confluent.connect.s3.S3SinkConnector"
JDBC = "io.confluent.connect.jdbc.JdbcSourceConnector"


def synthetic_configs(count: int) -> dict[str, dict[str, str]]:
    """Build ``count`` connector configs with shared topics, regex, DLQ and gaps."""
    if count < 1:
        raise ValueError("count must be positive")
    configs: dict[str, dict[str, str]] = {}
    for index in range(count):
        name = f"c{index:04d}"
        kind = index % 10
        if kind in {0, 1, 2, 3}:
            topic = f"topic.{index % max(1, count // 5)}"
            configs[name] = {"connector.class": SOURCE, "topic": topic}
        elif kind == 4:
            configs[name] = {"connector.class": JDBC, "topic.prefix": f"app.{index % 7}."}
        elif kind == 5:
            configs[name] = {
                "connector.class": SINK,
                "topics": f"topic.{index % max(1, count // 5)},topic.shared",
                "consumer.override.group.id": "shared-group" if index % 2 == 0 else f"group-{index % 11}",
                "errors.deadletterqueue.topic.name": "topic.dlq",
            }
        elif kind == 6:
            configs[name] = {
                "connector.class": SINK,
                "topics.regex": r"topic\.\d+",
                "consumer.override.group.id": "shared-group",
            }
        elif kind == 7:
            configs[name] = {
                "connector.class": SINK,
                "topics": ",".join(f"fan.{index}.{slot}" for slot in range(8)),
                "consumer.override.group.id": f"fan-{index}",
            }
        elif kind == 8:
            configs[name] = {"connector.class": SINK}
        else:
            configs[name] = {
                "connector.class": SOURCE,
                "topic": f"island.{index}",
            }
        if index % 17 == 0 and configs[name]["connector.class"] == SINK:
            configs[name]["consumer.override.bootstrap.servers"] = "hidden.invalid:9092"
    return configs


def plugin_catalog() -> list[dict[str, str]]:
    return [
        {"class": SOURCE, "type": "source", "version": "3.7.0"},
        {"class": JDBC, "type": "source", "version": "3.7.0"},
        {"class": SINK, "type": "sink", "version": "3.7.0"},
    ]
