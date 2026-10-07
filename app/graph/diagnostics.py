"""Consumer-group conflict diagnostics for sink connectors.

Rules, per topic consumed by two or more sinks:

* same explicit group on a known exact topic (listed in ``topics`` or matched
  by ``topics.regex``) -> error;
* same group where one side is only the derived default ``connect-<name>`` ->
  warning, because the default is expected, not guaranteed;
* a group that cannot be determined -> warning; unknown groups are never
  treated as equal to anything;
* different groups -> ok;
* a member that sets ``consumer.override.bootstrap.servers`` may read another
  Kafka cluster, so it never produces an error.

Pairs of sinks whose ``topics.regex`` may overlap without any known topic as a
witness are reported as warnings when their groups match or are unknown.
"""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from typing import Any

from app.graph.facts import KEY_TOPICS, KEY_TOPICS_REGEX, ConnectorFacts, prefixes_compatible
from app.graph.ids import connector_id, edge_id, regex_id, topic_id

_SEVERITY_ORDER = {"error": 0, "warning": 1, "ok": 2}


def diagnose(facts: list[ConnectorFacts], topics: list[str], errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sinks = [item for item in facts if item.type == "sink"]
    diagnostics: list[dict[str, Any]] = []
    consumed: dict[str, set[str]] = defaultdict(set)

    def member(item: ConnectorFacts, via: str | None) -> dict[str, Any]:
        group = item.group
        return {
            "connector": item.name,
            "group": group.value if group else None,
            "origin": group.origin if group else "unknown",
            "via": via,
            "bootstrap_override": item.bootstrap_override,
        }

    def topic_diagnostic(severity: str, code: str, topic: str, group: str | None, members: list[tuple[ConnectorFacts, str]], **extra: Any) -> dict[str, Any]:
        names = sorted(item.name for item, _via in members)
        return {
            "id": f"{code}:{topic}:{group or ''}:{','.join(names)}",
            "severity": severity,
            "code": code,
            "topic": topic,
            "group": group,
            "connectors": names,
            "members": [member(item, via) for item, via in sorted(members, key=lambda pair: pair[0].name)],
            "nodes": [topic_id(topic), *(connector_id(name) for name in names)],
            "edges": [edge_id("reads", topic_id(topic), connector_id(name)) for name in names],
            **extra,
        }

    for topic in topics:
        readers: list[tuple[ConnectorFacts, str]] = []
        for item in sinks:
            if topic in item.topics:
                readers.append((item, KEY_TOPICS))
            elif item.topics_regex is not None and item.topics_regex.matches(topic):
                readers.append((item, KEY_TOPICS_REGEX))
        for item, _via in readers:
            consumed[item.name].add(topic)
        if len(readers) < 2:
            continue

        known = [(item, via) for item, via in readers if item.group and item.group.origin != "unknown"]
        undetermined = [item.name for item, _via in readers if not item.group or item.group.origin == "unknown"]
        buckets: dict[str, list[tuple[ConnectorFacts, str]]] = defaultdict(list)
        for item, via in known:
            buckets[item.group.value].append((item, via))

        for group, members in sorted(buckets.items()):
            if len(members) < 2:
                continue
            if all(item.group.origin == "explicit" for item, _via in members):
                core = [(item, via) for item, via in members if not item.bootstrap_override]
                overridden = sorted(item.name for item, _via in members if item.bootstrap_override)
                if len(core) >= 2:
                    diagnostics.append(topic_diagnostic("error", "shared_topic_same_group", topic, group, core))
                if overridden:
                    diagnostics.append(
                        topic_diagnostic("warning", "bootstrap_override", topic, group, members, overridden=overridden)
                    )
            else:
                diagnostics.append(topic_diagnostic("warning", "same_group_as_default", topic, group, members))

        if len(buckets) >= 2:
            expected = any(item.group.origin == "derived" for item, _via in known)
            diagnostics.append(
                topic_diagnostic("ok", "shared_topic_different_groups", topic, None, known, expected=expected)
            )
        if undetermined:
            diagnostics.append(
                topic_diagnostic("warning", "group_unknown", topic, None, readers, undetermined=sorted(undetermined))
            )

    possible_unknown: dict[str, set[str]] = defaultdict(set)
    for left, right in combinations(sorted(sinks, key=lambda item: item.name), 2):
        if consumed[left.name] & consumed[right.name]:
            continue
        if left.topics_unknown or right.topics_unknown or not _may_overlap(left, right):
            continue
        left_group, right_group = left.group, right.group
        if left_group.origin == "unknown" or right_group.origin == "unknown":
            for item in (left, right):
                if item.group.origin == "unknown":
                    possible_unknown[item.name].add(right.name if item is left else left.name)
            continue
        if left_group.value != right_group.value:
            continue
        both_explicit = left_group.origin == "explicit" and right_group.origin == "explicit"
        code = "possible_regex_overlap_same_group" if both_explicit else "same_group_as_default"
        names = [left.name, right.name]
        patterns = [item.topics_regex.pattern for item in (left, right) if item.topics_regex]
        diagnostics.append(
            {
                "id": f"{code}:possible:{left_group.value}:{','.join(names)}",
                "severity": "warning",
                "code": code,
                "topic": None,
                "group": left_group.value,
                "connectors": names,
                "members": [member(item, KEY_TOPICS_REGEX if item.topics_regex else KEY_TOPICS) for item in (left, right)],
                "patterns": patterns,
                "nodes": [*(regex_id(pattern) for pattern in patterns), *(connector_id(name) for name in names)],
                "edges": [
                    edge_id("reads", regex_id(item.topics_regex.pattern), connector_id(item.name))
                    for item in (left, right)
                    if item.topics_regex
                ],
            }
        )

    for name, partners in sorted(possible_unknown.items()):
        names = [name, *sorted(partners)]
        diagnostics.append(
            {
                "id": f"group_unknown:possible:{name}",
                "severity": "warning",
                "code": "group_unknown",
                "topic": None,
                "group": None,
                "connectors": names,
                "undetermined": [name],
                "members": [],
                "nodes": [connector_id(item) for item in names],
                "edges": [],
            }
        )

    for item in sinks:
        if item.topics and item.topics_regex is not None:
            diagnostics.append(_connector_diagnostic("topics_and_regex", item.name))
        if item.topics_unknown:
            diagnostics.append(_connector_diagnostic("topics_unknown", item.name))

    for error in errors:
        reason = {key: value for key, value in error.items() if key != "connector"}
        diagnostics.append(_connector_diagnostic("config_unreadable", error["connector"], error=reason))

    diagnostics.sort(key=lambda item: (_SEVERITY_ORDER[item["severity"]], item["id"]))
    return diagnostics


def _may_overlap(left: ConnectorFacts, right: ConnectorFacts) -> bool:
    left_regex, right_regex = left.topics_regex, right.topics_regex
    if left_regex is not None and right_regex is not None:
        if not left_regex.analyzable or not right_regex.analyzable:
            return True
        ignore_case = left_regex.ignore_case or right_regex.ignore_case
        return prefixes_compatible(left_regex.prefix, right_regex.prefix, ignore_case)
    if left_regex is not None and not left_regex.analyzable and right.topics:
        return True
    if right_regex is not None and not right_regex.analyzable and left.topics:
        return True
    return False


def _connector_diagnostic(code: str, name: str, **extra: Any) -> dict[str, Any]:
    return {
        "id": f"{code}:{name}",
        "severity": "warning",
        "code": code,
        "topic": None,
        "group": None,
        "connectors": [name],
        "members": [],
        "nodes": [connector_id(name)],
        "edges": [],
        **extra,
    }
