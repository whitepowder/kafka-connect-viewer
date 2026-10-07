from __future__ import annotations


def connector_id(name: str) -> str:
    return "connector:" + name


def topic_id(name: str) -> str:
    return "topic:" + name


def regex_id(pattern: str) -> str:
    return "pattern:regex:" + pattern


def prefix_id(prefix: str) -> str:
    return "pattern:prefix:" + prefix


def edge_id(kind: str, source: str, target: str) -> str:
    return f"{kind}:{source}->{target}"
