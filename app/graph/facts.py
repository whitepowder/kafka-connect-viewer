"""Connector facts for the graph.

Only an allow-list of keys is read from a connector config. Everything else —
URLs, hosts, credentials, destinations, transforms — is ignored, and the raw
config is dropped as soon as the facts are built. For
``consumer.override.bootstrap.servers`` only the presence of the key is kept.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

ConnectorType = Literal["source", "sink", "unknown"]
GroupOrigin = Literal["explicit", "derived", "unknown"]

KEY_CLASS = "connector.class"
KEY_TOPICS = "topics"
KEY_TOPICS_REGEX = "topics.regex"
KEY_TOPIC = "topic"
KEY_TOPIC_PREFIX = "topic.prefix"
KEY_GROUP = "consumer.override.group.id"
KEY_DLQ = "errors.deadletterqueue.topic.name"
KEY_BOOTSTRAP = "consumer.override.bootstrap.servers"

_PLACEHOLDER_RE = re.compile(r"\$\{[^}]*\}")
_MASK_RE = re.compile(r"^\*{2,}$")
_NAMED_GROUP_RE = re.compile(r"\(\?<([A-Za-z][A-Za-z0-9]*)>")
_QUOTE_RE = re.compile(r"\\Q(.*?)(?:\\E|$)", re.DOTALL)
_LEADING_FLAGS_RE = re.compile(r"^\(\?[A-Za-z]+\)")
_REGEX_META = set(".^$*+?()[]{}|\\")
_OPTIONAL_QUANTIFIERS = set("*?{")


@dataclass(frozen=True)
class Group:
    value: str | None
    origin: GroupOrigin


@dataclass(frozen=True)
class TopicRegex:
    pattern: str
    compiled: re.Pattern[str] | None = field(compare=False, repr=False)
    prefix: str = ""

    @property
    def analyzable(self) -> bool:
        return self.compiled is not None

    @property
    def ignore_case(self) -> bool:
        return bool(self.compiled is not None and self.compiled.flags & re.IGNORECASE)

    def matches(self, topic: str) -> bool:
        return self.compiled is not None and self.compiled.fullmatch(topic) is not None


@dataclass(frozen=True)
class ConnectorFacts:
    name: str
    connector_class: str | None
    type: ConnectorType
    topics: tuple[str, ...] = ()
    topics_regex: TopicRegex | None = None
    topic: str | None = None
    topic_prefix: str | None = None
    group: Group | None = None
    dlq_topic: str | None = None
    bootstrap_override: bool = False
    unknown: tuple[str, ...] = ()

    @property
    def topics_unknown(self) -> bool:
        return KEY_TOPICS in self.unknown or KEY_TOPICS_REGEX in self.unknown


def parse_connector(name: str, config: Mapping[str, Any], plugin_types: Mapping[str, str] | None = None) -> ConnectorFacts:
    unknown: list[str] = []

    def read(key: str) -> str | None:
        value, is_unknown = _literal(config, key)
        if is_unknown:
            unknown.append(key)
        return value

    connector_class = read(KEY_CLASS)
    connector_type = detect_type(connector_class, config, plugin_types or {})

    if connector_type != "sink":
        return ConnectorFacts(
            name=name,
            connector_class=connector_class,
            type=connector_type,
            topic=read(KEY_TOPIC),
            topic_prefix=read(KEY_TOPIC_PREFIX),
            unknown=tuple(unknown),
        )

    raw_topics = read(KEY_TOPICS)
    raw_regex = read(KEY_TOPICS_REGEX)
    group_value, group_unknown = _literal(config, KEY_GROUP)
    if group_unknown:
        group = Group(None, "unknown")
    elif group_value is None:
        group = Group(f"connect-{name}", "derived")
    else:
        group = Group(group_value, "explicit")

    return ConnectorFacts(
        name=name,
        connector_class=connector_class,
        type="sink",
        topics=split_topics(raw_topics),
        topics_regex=compile_topic_regex(raw_regex) if raw_regex is not None else None,
        group=group,
        dlq_topic=read(KEY_DLQ),
        bootstrap_override=KEY_BOOTSTRAP in config,
        unknown=tuple(unknown),
    )


def detect_type(connector_class: str | None, config: Mapping[str, Any], plugin_types: Mapping[str, str]) -> ConnectorType:
    if connector_class:
        declared = plugin_types.get(connector_class)
        if declared in ("source", "sink"):
            return declared
        simple = connector_class.rsplit(".", 1)[-1]
        if simple.endswith("SinkConnector"):
            return "sink"
        if simple.endswith("SourceConnector"):
            return "source"
    if KEY_TOPICS in config or KEY_TOPICS_REGEX in config:
        return "sink"
    return "unknown"


def split_topics(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    seen: dict[str, None] = {}
    for item in raw.split(","):
        topic = item.strip()
        if topic:
            seen.setdefault(topic, None)
    return tuple(seen)


def compile_topic_regex(pattern: str) -> TopicRegex:
    try:
        compiled = re.compile(_translate_java_regex(pattern))
    except (re.error, OverflowError):
        compiled = None
    return TopicRegex(pattern=pattern, compiled=compiled, prefix=literal_prefix(pattern))


def literal_prefix(pattern: str) -> str:
    """Literal text every match must start with; "" when it cannot be told."""
    if "|" in pattern:
        return ""
    text = _LEADING_FLAGS_RE.sub("", pattern, count=1)
    if text.startswith("^"):
        text = text[1:]
    prefix: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char == "\\":
            if index + 1 >= len(text) or text[index + 1].isalnum():
                break
            literal = text[index + 1]
            index += 2
        elif char in _REGEX_META:
            break
        else:
            literal = char
            index += 1
        if index < len(text) and text[index] in _OPTIONAL_QUANTIFIERS:
            break
        prefix.append(literal)
    return "".join(prefix)


def prefixes_compatible(left: str, right: str, ignore_case: bool = False) -> bool:
    if ignore_case:
        left, right = left.casefold(), right.casefold()
    return left.startswith(right) or right.startswith(left)


def _translate_java_regex(pattern: str) -> str:
    # Java and Python agree on everything topic regexes normally use. \Q...\E
    # quoting and (?<name>...) groups are rewritten; anything else Python cannot
    # compile is reported as "not analyzable" instead of being guessed.
    translated = _QUOTE_RE.sub(lambda match: re.escape(match.group(1)), pattern)
    return _NAMED_GROUP_RE.sub(r"(?P<\1>", translated)


def _literal(config: Mapping[str, Any], key: str) -> tuple[str | None, bool]:
    """Return (value, unknown). Kafka's ConfigDef trims string values."""
    if key not in config:
        return None, False
    raw = config[key]
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float)):
        return None, True
    value = str(raw).strip()
    if not value:
        return None, key == KEY_GROUP
    if _PLACEHOLDER_RE.search(value) or _MASK_RE.fullmatch(value):
        return None, True
    return value, False
