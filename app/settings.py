from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Mapping
from urllib.parse import urlparse


_SLUG_RE = re.compile(r"[^a-z0-9._-]+")


@dataclass(frozen=True)
class Cluster:
    id: str
    name: str
    url: str
    username: str | None = None
    password: str | None = None
    verify_ssl: bool = True
    headers: dict[str, str] = field(default_factory=dict)

    def public(self) -> dict:
        return {"id": self.id, "name": self.name, "url": self.url}


@dataclass(frozen=True)
class Settings:
    clusters: tuple[Cluster, ...]


class SettingsError(ValueError):
    pass


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    source = os.environ if env is None else env

    # Explicit CLUSTERS_FILE wins when supplied. Otherwise use ./clusters.json
    # when it exists. Environment configuration is the fallback when no file
    # is available, which is convenient for Docker/Kubernetes deployments.
    configured_file = source.get("CLUSTERS_FILE")
    file_path = (configured_file or "clusters.json").strip() or "clusters.json"
    if os.path.isfile(file_path):
        return Settings(tuple(_parse_file(file_path)))

    raw_clusters = source.get("CONNECT_CLUSTERS", "").strip()
    if raw_clusters:
        return Settings(tuple(_parse_env_clusters(raw_clusters)))

    # Backward-compatible single-cluster environment configuration.
    connect_url = source.get("CONNECT_URL", "").strip()
    if connect_url:
        name = source.get("CONNECT_NAME", "default").strip() or "default"
        username = _optional_str(source.get("CONNECT_USERNAME"))
        password = _resolve_secret(_optional_str(source.get("CONNECT_PASSWORD")))
        verify_raw = source.get("CONNECT_VERIFY_SSL", "true").strip().lower()
        if verify_raw not in {"true", "false", "1", "0", "yes", "no"}:
            raise SettingsError("CONNECT_VERIFY_SSL должен быть true или false")
        verify_ssl = verify_raw in {"true", "1", "yes"}
        return Settings((_build_cluster(name, connect_url, {}, username, password, verify_ssl),))

    return Settings(
        (
            Cluster(
                id="local",
                name="local",
                url="http://127.0.0.1:8083",
            ),
        )
    )


def _parse_env_clusters(raw: str) -> list[Cluster]:
    clusters: list[Cluster] = []
    seen: dict[str, int] = {}
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        if "=" not in item:
            raise SettingsError(
                "CONNECT_CLUSTERS: ожидается name=url, например local=http://127.0.0.1:8083"
            )
        name, url = item.split("=", 1)
        clusters.append(_build_cluster(name.strip(), url.strip(), seen))
    if not clusters:
        raise SettingsError("CONNECT_CLUSTERS пуст")
    return clusters


def _parse_file(path: str) -> list[Cluster]:
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise SettingsError(f"Не удалось прочитать {path}: {exc}") from exc
    if not isinstance(payload, list) or not payload:
        raise SettingsError(f"{path} должен быть непустым JSON-массивом кластеров")

    clusters: list[Cluster] = []
    seen: dict[str, int] = {}
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise SettingsError(f"{path}: элемент {index} должен быть объектом")
        name = str(item.get("name") or item.get("NAME") or "").strip()
        url = str(item.get("url") or item.get("KAFKA_CONNECT") or "").strip()
        username = _optional_str(item.get("username"))
        password = _resolve_secret(_optional_str(item.get("password")))
        verify_ssl = item.get("verify_ssl", True)
        if not isinstance(verify_ssl, bool):
            raise SettingsError(f"{path}: verify_ssl у «{name or index}» должен быть true или false")
        headers = item.get("headers") or {}
        if not isinstance(headers, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in headers.items()):
            raise SettingsError(f"{path}: headers у «{name or index}» должен быть объектом строк")
        cluster = _build_cluster(name, url, seen, username, password, verify_ssl, dict(headers))
        clusters.append(cluster)
    return clusters


def _build_cluster(
    name: str,
    url: str,
    seen: dict[str, int],
    username: str | None = None,
    password: str | None = None,
    verify_ssl: bool = True,
    headers: dict[str, str] | None = None,
) -> Cluster:
    if not name:
        raise SettingsError("У кластера нет имени")
    _validate_url(url, name)
    base = _slug(name)
    seen[base] = seen.get(base, 0) + 1
    cluster_id = base if seen[base] == 1 else f"{base}-{seen[base]}"
    return Cluster(
        id=cluster_id,
        name=name,
        url=url.rstrip("/"),
        username=username,
        password=password,
        verify_ssl=verify_ssl,
        headers=headers or {},
    )


def _validate_url(url: str, name: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise SettingsError(f"Кластер «{name}»: URL должен быть http(s)://host, сейчас «{url}»")
    if parsed.username or parsed.password:
        raise SettingsError(
            f"Кластер «{name}»: уберите логин и пароль из URL и задайте поля username/password"
        )


def _slug(name: str) -> str:
    slug = _SLUG_RE.sub("-", name.strip().lower()).strip("-")
    return slug or "cluster"


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_secret(value: str | None) -> str | None:
    if value and value.startswith("env:"):
        key = value[4:]
        if not key or key not in os.environ:
            raise SettingsError(f"Переменная окружения {key or value} для пароля кластера не задана")
        return os.environ[key]
    return value
