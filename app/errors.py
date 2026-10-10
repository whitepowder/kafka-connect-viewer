"""Errors produced by KCV itself.

KCV never sends its own human-readable text to the UI. A KCV error is a
``code`` plus ``params`` and the frontend translates the code. Kafka Connect
errors keep their upstream ``message`` unchanged.
"""

from __future__ import annotations

from typing import Any

ERROR_CODES = frozenset(
    {
        "audit_disabled",
        "audit_invalid_cursor",
        "audit_invalid_filter",
        "audit_write_failed",
        "auth_required",
        "cluster_not_found",
        "config_empty",
        "config_key_invalid",
        "config_value_not_string",
        "connector_class_missing",
        "connector_config_failed",
        "connector_read_failed",
        "cross_origin_write",
        "graph_refresh_limited",
        "invalid_connector_name",
        "invalid_content_length",
        "oidc_flow_invalid",
        "oidc_flow_missing",
        "oidc_id_token_invalid",
        "oidc_id_token_missing",
        "oidc_no_role",
        "oidc_nonce_invalid",
        "oidc_state_invalid",
        "oidc_token_exchange_failed",
        "request_too_large",
        "role_required",
        "secret_masked",
        "secret_restore_failed",
        "task_id_negative",
        "upstream_empty_connector_config",
        "upstream_empty_connector_info",
        "upstream_empty_connector_status",
        "upstream_empty_response",
        "upstream_invalid_connector_list",
        "upstream_invalid_plugin_list",
        "upstream_not_json",
        "upstream_status",
        "upstream_timeout",
        "upstream_unreachable",
    }
)


def error_body(code: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    if code not in ERROR_CODES:
        raise ValueError(f"unknown KCV error code: {code}")
    return {"code": code, "params": dict(params or {})}


class KCVError(Exception):
    def __init__(self, status: int, code: str, params: dict[str, Any] | None = None):
        self.body = error_body(code, params)
        super().__init__(code)
        self.status = status
        self.code = code
        self.params = self.body["params"]
