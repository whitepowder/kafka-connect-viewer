from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


class AuditSettingsError(ValueError):
    pass


STORAGE_LOCAL = "local"
STORAGE_S3 = "s3"
STORAGES = frozenset({STORAGE_LOCAL, STORAGE_S3})


@dataclass(frozen=True)
class AuditSettings:
    enabled: bool
    storage: str
    directory: str
    retention_days: int
    s3_bucket: str | None
    s3_prefix: str
    s3_region: str
    s3_endpoint: str | None
    s3_access_key: str | None
    s3_secret_key: str | None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "AuditSettings":
        source = os.environ if env is None else env
        enabled = _bool(source, "AUDIT_ENABLED", True)
        storage = (source.get("AUDIT_STORAGE") or STORAGE_LOCAL).strip().lower()
        if storage not in STORAGES:
            raise AuditSettingsError("AUDIT_STORAGE must be local or s3")
        directory = (source.get("AUDIT_DIR") or "data/audit").strip() or "data/audit"
        retention_days = _non_negative_int(source, "AUDIT_RETENTION_DAYS", 90)
        bucket = _optional(source.get("AUDIT_S3_BUCKET"))
        prefix = (source.get("AUDIT_S3_PREFIX") or "kcv-audit").strip().strip("/") or "kcv-audit"
        region = (source.get("AUDIT_S3_REGION") or "us-east-1").strip() or "us-east-1"
        endpoint = _optional(source.get("AUDIT_S3_ENDPOINT"))
        access_key = _optional(source.get("AUDIT_S3_ACCESS_KEY"))
        secret_key = _resolve_secret(_optional(source.get("AUDIT_S3_SECRET_KEY")))
        if enabled and storage == STORAGE_S3:
            missing = [name for name, value in {
                "AUDIT_S3_BUCKET": bucket,
                "AUDIT_S3_ACCESS_KEY": access_key,
                "AUDIT_S3_SECRET_KEY": secret_key,
            }.items() if not value]
            if missing:
                raise AuditSettingsError("AUDIT_STORAGE=s3 but missing " + ", ".join(missing))
            if endpoint and not (endpoint.startswith("http://") or endpoint.startswith("https://")):
                raise AuditSettingsError("AUDIT_S3_ENDPOINT must be http(s)://host")
        return cls(
            enabled=enabled,
            storage=storage,
            directory=directory,
            retention_days=retention_days,
            s3_bucket=bucket,
            s3_prefix=prefix,
            s3_region=region,
            s3_endpoint=endpoint,
            s3_access_key=access_key,
            s3_secret_key=secret_key,
        )


def _bool(source: Mapping[str, str], name: str, default: bool) -> bool:
    raw = source.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise AuditSettingsError(f"{name} must be true or false")


def _non_negative_int(source: Mapping[str, str], name: str, default: int) -> int:
    raw = source.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = int(str(raw).strip())
    except ValueError as exc:
        raise AuditSettingsError(f"{name} must be a number of days") from exc
    if value < 0:
        raise AuditSettingsError(f"{name} must not be negative")
    return value


def _optional(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_secret(value: str | None) -> str | None:
    if value and value.startswith("env:"):
        key = value[4:]
        if not key or key not in os.environ:
            raise AuditSettingsError(f"Environment variable {key or value} for AUDIT_S3_SECRET_KEY is not set")
        return os.environ[key]
    return value
