import pytest

from app.main import app


@pytest.fixture(autouse=True)
def isolate_audit_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("AUDIT_DIR", str(tmp_path / "kcv-audit"))
    monkeypatch.setenv("AUDIT_STORAGE", "local")
    monkeypatch.delenv("AUDIT_S3_BUCKET", raising=False)
    monkeypatch.delenv("AUDIT_S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AUDIT_S3_SECRET_KEY", raising=False)
    monkeypatch.delenv("AUDIT_S3_ENDPOINT", raising=False)
    app.state.audit_store = None
