from __future__ import annotations

import hashlib
import hmac
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import httpx

from app.audit.events import AuditStorageError, after_cursor, decode_cursor, parse_ts, sanitize_event

_S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


class S3AuditStore:
    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        region: str,
        access_key: str,
        secret_key: str,
        endpoint: str | None = None,
        transport: httpx.BaseTransport | None = None,
        retention_days: int = 90,
    ):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint = endpoint.rstrip("/") if endpoint else None
        self.retention_days = retention_days
        self.http = httpx.Client(timeout=10.0, transport=transport, follow_redirects=False)

    def close(self) -> None:
        self.http.close()

    def prepare(self) -> None:
        return None

    def append(self, event: dict) -> None:
        body = json.dumps(sanitize_event(event), ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        key = self._object_key(event)
        response = self._call("PUT", key, body=body, extra={"If-None-Match": "*"})
        if response.status_code not in {200, 204}:
            raise AuditStorageError(f"S3 PutObject returned {response.status_code}")
        self.purge()

    def query(
        self,
        *,
        cluster: str | None = None,
        connector: str | None = None,
        action: str | None = None,
        result: str | None = None,
        actor: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[dict], str | None]:
        marker = decode_cursor(cursor) if cursor else None
        cutoff = self._cutoff()
        keys = self._list_keys(since, until, cutoff)
        events: list[dict] = []
        for key in keys:
            event = self._get(key)
            if event is None:
                continue
            if self._keep(event, cluster, connector, action, result, actor, since, until, cutoff) and after_cursor(event, marker):
                events.append(event)
        events.sort(key=lambda item: (item["ts"], item["id"]), reverse=True)
        page = events[:limit]
        nxt = f'{page[-1]["ts"]}|{page[-1]["id"]}' if len(events) > limit else None
        return page, nxt

    def purge(self) -> None:
        cutoff = self._cutoff()
        if cutoff is None:
            return
        for key in self._list_keys(None, None, None):
            day = self._key_day(key)
            if day is not None and day < cutoff:
                self._call("DELETE", key)

    def _get(self, key: str) -> dict | None:
        response = self._call("GET", key)
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise AuditStorageError(f"S3 GetObject returned {response.status_code}")
        try:
            return sanitize_event(json.loads(response.content.decode("utf-8")))
        except (ValueError, json.JSONDecodeError) as exc:
            raise AuditStorageError("S3 object is not a valid audit event") from exc

    def _list_keys(self, since: datetime | None, until: datetime | None, cutoff: datetime | None) -> list[str]:
        keys: list[str] = []
        token = None
        while True:
            query = {"list-type": "2", "prefix": f"{self.prefix}/", "max-keys": "1000"}
            if token:
                query["continuation-token"] = token
            response = self._call("GET", "", query=query)
            if response.status_code != 200:
                raise AuditStorageError(f"S3 ListObjects returned {response.status_code}")
            root = ET.fromstring(response.content)
            for node in _xml_all(root, "Key"):
                if node.text:
                    keys.append(node.text)
            truncated = (_xml_text(root, "IsTruncated") or "false").lower() == "true"
            token = _xml_text(root, "NextContinuationToken")
            if not truncated or not token:
                break
        start = since or cutoff
        selected = []
        for key in keys:
            day = self._key_day(key)
            if day is None:
                continue
            if start and day < start.replace(hour=0, minute=0, second=0, microsecond=0):
                continue
            if until and day > until.replace(hour=0, minute=0, second=0, microsecond=0):
                continue
            selected.append(key)
        selected.sort(reverse=True)
        return selected

    def _object_key(self, event: dict) -> str:
        ts = parse_ts(event["ts"])
        stamp = ts.strftime("%Y%m%dT%H%M%S%f")[:-3] + "Z"
        return f"{self.prefix}/{ts:%Y/%m/%d}/{stamp}_{event['id']}.json"

    def _key_day(self, key: str) -> datetime | None:
        parts = key[len(self.prefix) + 1:].split("/") if key.startswith(self.prefix + "/") else []
        if len(parts) < 4:
            return None
        try:
            return datetime(int(parts[0]), int(parts[1]), int(parts[2]), tzinfo=timezone.utc)
        except ValueError:
            return None

    def _cutoff(self) -> datetime | None:
        if self.retention_days <= 0:
            return None
        return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=self.retention_days)

    def _call(self, method: str, key: str, body: bytes = b"", query: dict[str, str] | None = None, extra: dict[str, str] | None = None) -> httpx.Response:
        url, headers = self._signed(method, key, body, query or {}, extra or {})
        try:
            return self.http.request(method, url, headers=headers, content=body or None)
        except httpx.HTTPError as exc:
            raise AuditStorageError(f"S3 request failed: {exc.__class__.__name__}") from exc

    def _signed(self, method: str, key: str, body: bytes, query: dict[str, str], extra: dict[str, str]) -> tuple[str, dict[str, str]]:
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(body).hexdigest()
        if self.endpoint:
            host = self.endpoint.split("://", 1)[1]
            url_path = f"/{self.bucket}/{key}" if key else f"/{self.bucket}"
            url = f"{self.endpoint}{url_path}"
        else:
            host = f"{self.bucket}.s3.{self.region}.amazonaws.com"
            url_path = f"/{key}" if key else "/"
            url = f"https://{host}{url_path}"
        headers = {
            "host": host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
            **{name.lower(): value for name, value in extra.items()},
        }
        signed = ";".join(sorted(headers))
        canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in sorted(headers))
        canonical_query = "&".join(f"{quote(name, safe='-_.~')}={quote(query[name], safe='-_.~')}" for name in sorted(query))
        canonical = "\n".join([method, quote(url_path, safe="/-_.~"), canonical_query, canonical_headers, signed, payload_hash])
        scope = f"{datestamp}/{self.region}/s3/aws4_request"
        string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode("utf-8")).hexdigest()])
        signing_key = _signing_key(self.secret_key, datestamp, self.region)
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        headers["authorization"] = (
            f"AWS4-HMAC-SHA256 Credential={self.access_key}/{scope}, SignedHeaders={signed}, Signature={signature}"
        )
        if query:
            url = f"{url}?{canonical_query}"
        return url, headers

    @staticmethod
    def _keep(
        event: dict,
        cluster: str | None,
        connector: str | None,
        action: str | None,
        result: str | None,
        actor: str | None,
        since: datetime | None,
        until: datetime | None,
        cutoff: datetime | None,
    ) -> bool:
        try:
            ts = parse_ts(event["ts"])
        except ValueError:
            return False
        if cutoff and ts < cutoff:
            return False
        if since and ts < since:
            return False
        if until and ts > until:
            return False
        if action and event["action"] != action:
            return False
        if result and event["result"] != result:
            return False
        if connector and event.get("connector") != connector:
            return False
        if cluster and (not event.get("cluster") or event["cluster"].get("id") != cluster):
            return False
        if actor and str(event.get("actor", {}).get("name") or "") != actor:
            return False
        return True


def _xml_all(root: ET.Element, name: str) -> list[ET.Element]:
    found = root.findall(f".//s3:{name}", _S3_NS)
    return found or root.findall(f".//{name}")


def _xml_text(root: ET.Element, name: str) -> str | None:
    node = root.find(f".//s3:{name}", _S3_NS)
    if node is None:
        node = root.find(f".//{name}")
    return node.text if node is not None else None


def _signing_key(secret: str, datestamp: str, region: str) -> bytes:
    date_key = hmac.new(f"AWS4{secret}".encode("utf-8"), datestamp.encode("utf-8"), hashlib.sha256).digest()
    region_key = hmac.new(date_key, region.encode("utf-8"), hashlib.sha256).digest()
    service_key = hmac.new(region_key, b"s3", hashlib.sha256).digest()
    return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()
