"""Local Kafka Connect stand-in for UI checks. Synthetic data only."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tests.graph_workload import plugin_catalog, synthetic_configs

CONFIGS = synthetic_configs(12)
# One readable detail plus shapes the editor and graph need.
CONFIGS["orders-source"] = {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
    "topic": "orders",
    "file": "/tmp/orders.txt",
}
CONFIGS["orders-sink"] = {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "topics": "orders,billing",
    "consumer.override.group.id": "orders-consumers",
    "errors.deadletterqueue.topic.name": "orders.dlq",
    "s3.bucket.name": "example",
    "aws.secret.access.key": "*****",
}
CONFIGS["mirror-sink"] = {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "topics.regex": "ord(ers)?",
    "consumer.override.group.id": "orders-consumers",
}
FAIL: set[str] = set()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/":
            return self._json({"version": "3.7.0", "commit": "fixture", "kafka_cluster_id": "fixture"})
        if path == "/connectors":
            return self._json(sorted(CONFIGS))
        if path == "/connector-plugins":
            return self._json(plugin_catalog())
        if path.startswith("/connectors/") and path.endswith("/status"):
            name = path[len("/connectors/") : -len("/status")]
            return self._connector_status(name)
        if path.startswith("/connectors/") and path.endswith("/config"):
            name = path[len("/connectors/") : -len("/config")]
            return self._config(name)
        if path.startswith("/connectors/"):
            name = path[len("/connectors/") :]
            return self._info(name)
        self._json({"message": "not found"}, 404)

    def do_PUT(self) -> None:  # noqa: N802
        self._read_body()
        if self.path.endswith("/pause") or self.path.endswith("/resume"):
            return self._empty(204)
        if "/config/validate" in self.path:
            return self._json({"name": "plugin", "error_count": 0, "configs": []})
        if self.path.endswith("/config"):
            return self._json({"connector.class": "io.confluent.connect.s3.S3SinkConnector"})
        self._json({"message": "not found"}, 404)

    def do_POST(self) -> None:  # noqa: N802
        self._read_body()
        if self.path.endswith("/restart"):
            return self._empty(204)
        if self.path == "/connectors":
            return self._json({"name": "created"}, 201)
        self._json({"message": "not found"}, 404)

    def do_DELETE(self) -> None:  # noqa: N802
        self._empty(204)

    def _config(self, name: str) -> None:
        if name in FAIL:
            return self._json({"message": "worker exploded"}, 500)
        if name not in CONFIGS:
            return self._json({"message": "not found"}, 404)
        self._json(CONFIGS[name])

    def _info(self, name: str) -> None:
        if name not in CONFIGS:
            return self._json({"message": "not found"}, 404)
        config = CONFIGS[name]
        kind = "sink" if "Sink" in config["connector.class"] else "source"
        self._json({"name": name, "type": kind, "config": config})

    def _connector_status(self, name: str) -> None:
        if name not in CONFIGS:
            return self._json({"message": "not found"}, 404)
        self._json({
            "name": name,
            "connector": {"state": "RUNNING", "worker_id": "worker-1:8083"},
            "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "worker-1:8083"}],
            "type": "sink" if "Sink" in CONFIGS[name]["connector.class"] else "source",
        })

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _json(self, payload, status: int = 200) -> None:
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _empty(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 9099), Handler)
    print("mock connect on 127.0.0.1:9099", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
