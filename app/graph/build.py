from __future__ import annotations

from typing import Any, Iterable

from app.graph.diagnostics import diagnose
from app.graph.facts import (
    KEY_DLQ,
    KEY_TOPIC,
    KEY_TOPIC_PREFIX,
    KEY_TOPICS,
    KEY_TOPICS_REGEX,
    ConnectorFacts,
    prefixes_compatible,
)
from app.graph.ids import connector_id, edge_id, prefix_id, regex_id, topic_id


def known_topics(facts: Iterable[ConnectorFacts]) -> list[str]:
    seen: dict[str, None] = {}
    for item in facts:
        for topic in item.topics:
            seen.setdefault(topic, None)
        if item.topic:
            seen.setdefault(item.topic, None)
        if item.dlq_topic:
            seen.setdefault(item.dlq_topic, None)
    return sorted(seen, key=lambda value: (value.casefold(), value))


def build_graph(facts: list[ConnectorFacts], errors: list[dict[str, Any]]) -> dict[str, Any]:
    facts = sorted(facts, key=lambda item: (item.name.casefold(), item.name))
    topics = known_topics(facts)
    dlq_topics = {item.dlq_topic for item in facts if item.dlq_topic}

    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}

    def add_edge(kind: str, source: str, target: str, confidence: str, key: str) -> None:
        identifier = edge_id(kind, source, target)
        if identifier in edges:
            return
        edges[identifier] = {
            "id": identifier,
            "kind": kind,
            "from": source,
            "to": target,
            "confidence": confidence,
            "key": key,
        }

    for item in facts:
        nodes[connector_id(item.name)] = _connector_node(item)
    for topic in topics:
        nodes[topic_id(topic)] = {"id": topic_id(topic), "kind": "topic", "name": topic, "dlq": topic in dlq_topics}

    prefixes = sorted({item.topic_prefix for item in facts if item.topic_prefix})
    for prefix in prefixes:
        nodes[prefix_id(prefix)] = {"id": prefix_id(prefix), "kind": "pattern", "syntax": "prefix", "value": prefix}

    for item in facts:
        node = connector_id(item.name)
        if item.type == "sink":
            for topic in item.topics:
                add_edge("reads", topic_id(topic), node, "declared", KEY_TOPICS)
            regex = item.topics_regex
            if regex is not None:
                pattern_node = regex_id(regex.pattern)
                nodes.setdefault(
                    pattern_node,
                    {
                        "id": pattern_node,
                        "kind": "pattern",
                        "syntax": "regex",
                        "value": regex.pattern,
                        "analyzable": regex.analyzable,
                    },
                )
                add_edge("reads", pattern_node, node, "pattern", KEY_TOPICS_REGEX)
                for topic in topics:
                    if regex.matches(topic):
                        add_edge("reads", topic_id(topic), node, "pattern", KEY_TOPICS_REGEX)
                for prefix in prefixes:
                    if not regex.analyzable or prefixes_compatible(prefix, regex.prefix, regex.ignore_case):
                        add_edge("reads", prefix_id(prefix), node, "possible", KEY_TOPICS_REGEX)
            if item.dlq_topic:
                add_edge("dlq", node, topic_id(item.dlq_topic), "declared", KEY_DLQ)
        else:
            if item.topic:
                add_edge("writes", node, topic_id(item.topic), "declared", KEY_TOPIC)
            if item.topic_prefix:
                add_edge("writes", node, prefix_id(item.topic_prefix), "pattern", KEY_TOPIC_PREFIX)
                for topic in topics:
                    if topic.startswith(item.topic_prefix) and topic != item.topic:
                        add_edge("writes", node, topic_id(topic), "pattern", KEY_TOPIC_PREFIX)

    diagnostics = diagnose(facts, topics, errors)
    severities = [item["severity"] for item in diagnostics]
    return {
        "partial": bool(errors),
        "errors": sorted(errors, key=lambda item: item["connector"].casefold()),
        "nodes": list(nodes.values()),
        "edges": list(edges.values()),
        "diagnostics": diagnostics,
        "stats": {
            "connectors": len(facts),
            "sources": sum(1 for item in facts if item.type == "source"),
            "sinks": sum(1 for item in facts if item.type == "sink"),
            "topics": len(topics),
            "errors": severities.count("error"),
            "warnings": severities.count("warning"),
            "ok": severities.count("ok"),
            "unreadable": len(errors),
        },
    }


def _connector_node(item: ConnectorFacts) -> dict[str, Any]:
    node: dict[str, Any] = {
        "id": connector_id(item.name),
        "kind": "connector",
        "name": item.name,
        "type": item.type,
        "class": item.connector_class,
        "unknown": list(item.unknown),
    }
    if item.type == "sink":
        node.update(
            {
                "topics": list(item.topics),
                "topics_regex": item.topics_regex.pattern if item.topics_regex else None,
                "regex_analyzable": item.topics_regex.analyzable if item.topics_regex else None,
                "group": {"value": item.group.value, "origin": item.group.origin} if item.group else None,
                "dlq_topic": item.dlq_topic,
                "bootstrap_override": item.bootstrap_override,
            }
        )
    else:
        node.update({"topic": item.topic, "topic_prefix": item.topic_prefix})
    return node
