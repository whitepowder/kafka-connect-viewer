from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import quote

import httpx

from app.settings import Cluster

logger = logging.getLogger("connect")
_SECRET_MASK_RE = re.compile(r"^\*{2,}$")

# Kafka Connect names are free-form, but a slash would escape the REST path.
_NAME_RE = re.compile(r"^[^\x00-\x1f\x7f/\\]{1,512}$")


class ConnectError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class ConnectorNameError(ConnectError):
    def __init__(self, message: str = "Недопустимое имя коннектора"):
        super().__init__(400, message)


def validate_connector_name(name: str) -> str:
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name) or name in {".", ".."}:
        raise ConnectorNameError(
            "Имя коннектора не должно быть пустым и не может содержать / или \\"
        )
    return name


def normalize_connector_names(payload: Any) -> list[str]:
    """Keep names only.

    ``GET /connectors`` returns a JSON array of strings. ``?expand=status`` and
    ``?expand=info`` turn that into a map of configs and task state — that is
    what stalls a worker once there are ~100 connectors. If a proxy expands the
    payload anyway, drop everything except the keys.
    """
    if isinstance(payload, dict):
        logger.warning("connector list came back expanded; dropping status and config")
        raw = list(payload.keys())
    elif isinstance(payload, list):
        raw = payload
    else:
        raise ConnectError(502, "Kafka Connect вернул не список имён коннекторов")

    names: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, str) or item in seen:
            continue
        seen.add(item)
        names.append(item)
    names.sort(key=str.casefold)
    return names


def stringify_config(config: dict[str, Any]) -> dict[str, str]:
    if not isinstance(config, dict) or not config:
        raise ConnectError(400, "Конфиг коннектора пуст")
    rendered: dict[str, str] = {}
    for key, value in config.items():
        if not isinstance(key, str) or not key.strip():
            raise ConnectError(400, "Ключ конфига должен быть непустой строкой")
        if isinstance(value, (dict, list)):
            raise ConnectError(400, f"Значение «{key}» должно быть строкой")
        if isinstance(value, bool):
            rendered[key] = "true" if value else "false"
        elif value is None:
            rendered[key] = ""
        else:
            rendered[key] = str(value)
    return rendered


class ConnectClient:
    """Thin client that never calls ``/connectors?expand=...``."""

    def __init__(self, cluster: Cluster, transport: httpx.AsyncBaseTransport | None = None):
        self.cluster_id = cluster.id
        self._base = cluster.url.rstrip("/")
        headers = {"Accept": "application/json", **cluster.headers}
        auth = None
        if cluster.username is not None:
            auth = httpx.BasicAuth(cluster.username, cluster.password or "")
        self._http = httpx.AsyncClient(
            base_url=cluster.url,
            timeout=httpx.Timeout(connect=5.0, read=30.0, write=30.0, pool=5.0),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            headers=headers,
            auth=auth,
            verify=cluster.verify_ssl,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def info(self) -> dict[str, Any]:
        payload = await self._request("GET", "/")
        return payload if isinstance(payload, dict) else {}

    async def list_names(self) -> list[str]:
        # Intentionally no query string. expand=status/info is forbidden below.
        payload = await self._request("GET", "/connectors")
        return normalize_connector_names(payload)

    async def connector_info(self, name: str) -> dict[str, Any]:
        payload = await self._request("GET", _connector_path(name))
        if not isinstance(payload, dict):
            raise ConnectError(502, "Kafka Connect вернул пустое описание коннектора")
        return payload

    async def connector_status(self, name: str) -> dict[str, Any]:
        payload = await self._request("GET", _connector_path(name) + "/status")
        if not isinstance(payload, dict):
            raise ConnectError(502, "Kafka Connect вернул пустой статус коннектора")
        return payload

    async def create(self, name: str, config: dict[str, Any]) -> dict[str, Any]:
        validate_connector_name(name)
        rendered = stringify_config(config)
        rendered["name"] = name
        if "connector.class" not in rendered:
            raise ConnectError(400, "В конфиге нет connector.class")
        payload = await self._request(
            "POST",
            "/connectors",
            json={"name": name, "config": rendered},
        )
        return payload if isinstance(payload, dict) else {"name": name}

    async def delete(self, name: str) -> None:
        await self._request("DELETE", _connector_path(name))

    async def update_config(self, name: str, config: dict[str, Any]) -> dict[str, str]:
        rendered = stringify_config(config)
        rendered["name"] = validate_connector_name(name)

        # Kafka Connect masks ConfigDef password values as ***** on reads. Never
        # write that mask back: replace masked values with the currently stored
        # value immediately before PUT.
        if any(_SECRET_MASK_RE.fullmatch(value) for value in rendered.values()):
            current = await self.connector_info(name)
            current_config = current.get("config") if isinstance(current, dict) else None
            if not isinstance(current_config, dict):
                raise ConnectError(502, "Не удалось безопасно восстановить скрытые секреты")
            for key, value in list(rendered.items()):
                if _SECRET_MASK_RE.fullmatch(value):
                    existing = current_config.get(key)
                    if existing is None or (isinstance(existing, str) and _SECRET_MASK_RE.fullmatch(existing)):
                        raise ConnectError(400, f"Секрет «{key}» скрыт воркером; введите новое значение перед сохранением")
                    rendered[key] = str(existing)

        payload = await self._request("PUT", _connector_path(name) + "/config", json=rendered)
        if isinstance(payload, dict):
            return {str(key): "" if value is None else str(value) for key, value in payload.items()}
        return rendered

    async def pause(self, name: str) -> None:
        await self._request("PUT", _connector_path(name) + "/pause")

    async def resume(self, name: str) -> None:
        await self._request("PUT", _connector_path(name) + "/resume")

    async def restart(self, name: str, *, include_tasks: bool = False, only_failed: bool = False) -> None:
        params: dict[str, str] = {}
        if include_tasks:
            params["includeTasks"] = "true"
        if only_failed:
            params["onlyFailed"] = "true"
            params["includeTasks"] = "true"
        await self._request("POST", _connector_path(name) + "/restart", params=params or None)

    async def restart_task(self, name: str, task_id: int) -> None:
        if task_id < 0:
            raise ConnectError(400, "Номер задачи должен быть неотрицательным")
        await self._request("POST", f"{_connector_path(name)}/tasks/{task_id}/restart")

    async def plugins(self) -> list[dict[str, Any]]:
        payload = await self._request("GET", "/connector-plugins")
        if not isinstance(payload, list):
            raise ConnectError(502, "Kafka Connect вернул не список плагинов")
        return [item for item in payload if isinstance(item, dict)]

    async def validate(self, connector_class: str, config: dict[str, Any]) -> dict[str, Any]:
        if not connector_class.strip():
            raise ConnectError(400, "Не задан connector.class")
        rendered = stringify_config(config)
        path = "/connector-plugins/" + quote(connector_class, safe="") + "/config/validate"
        payload = await self._request("PUT", path, json=rendered)
        return payload if isinstance(payload, dict) else {}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        params: dict[str, str] | None = None,
    ) -> Any:
        if "expand=" in path or (params and any(key.lower() == "expand" for key in params)):
            raise RuntimeError("refusing to call Kafka Connect with expand=status or expand=info")
        logger.info("upstream %s %s cluster=%s", method, path, self.cluster_id)
        try:
            response = await self._http.request(method, self._join(path), json=json, params=params)
        except httpx.TimeoutException as exc:
            raise ConnectError(504, "Kafka Connect не ответил вовремя") from exc
        except httpx.RequestError as exc:
            raise ConnectError(502, f"Нет связи с Kafka Connect: {exc.__class__.__name__}") from exc

        if response.status_code >= 400:
            raise ConnectError(response.status_code, _error_message(response))
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise ConnectError(502, "Kafka Connect вернул не JSON") from exc

    def _join(self, path: str) -> str:
        if path == "/":
            return self._base + "/"
        return self._base + "/" + path.lstrip("/")


class ClientPool:
    def __init__(self, factory=None):
        self._factory = factory or (lambda cluster: ConnectClient(cluster))
        self._clients: dict[str, ConnectClient] = {}

    def client(self, cluster: Cluster) -> ConnectClient:
        cached = self._clients.get(cluster.id)
        if cached is None:
            cached = self._factory(cluster)
            self._clients[cluster.id] = cached
        return cached

    async def aclose(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        for client in clients:
            await client.aclose()


def _connector_path(name: str) -> str:
    return "/connectors/" + quote(validate_connector_name(name), safe="")


def _error_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        text = response.text.strip()
        return text or f"Kafka Connect ответил {response.status_code}"
    if isinstance(payload, dict):
        message = payload.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
    return f"Kafka Connect ответил {response.status_code}"
