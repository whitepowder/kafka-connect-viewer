import json

import pytest

from app.settings import SettingsError, load_settings


def test_default_cluster_when_no_configuration(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = load_settings({})
    assert len(settings.clusters) == 1
    assert settings.clusters[0].url == "http://127.0.0.1:8083"


def test_clusters_from_environment():
    settings = load_settings({"CONNECT_CLUSTERS": "dev=http://dev:8083,prod=https://prod:8083"})
    assert [cluster.name for cluster in settings.clusters] == ["dev", "prod"]
    assert [cluster.id for cluster in settings.clusters] == ["dev", "prod"]


def test_clusters_file_resolves_password_from_environment(tmp_path, monkeypatch):
    path = tmp_path / "clusters.json"
    path.write_text(json.dumps([{
        "name": "prod",
        "url": "https://connect:8083",
        "username": "connect",
        "password": "env:CONNECT_PASSWORD"
    }]))
    monkeypatch.setenv("CONNECT_PASSWORD", "secret")
    settings = load_settings({"CLUSTERS_FILE": str(path)})
    assert settings.clusters[0].password == "secret"


def test_missing_secret_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / "clusters.json"
    path.write_text(json.dumps([{
        "name": "prod",
        "url": "https://connect:8083",
        "password": "env:MISSING_PASSWORD"
    }]))
    monkeypatch.delenv("MISSING_PASSWORD", raising=False)
    with pytest.raises(SettingsError):
        load_settings({"CLUSTERS_FILE": str(path)})


def test_clusters_file_has_priority_over_environment(tmp_path, monkeypatch):
    path = tmp_path / "clusters.json"
    path.write_text(json.dumps([{"name": "file", "url": "http://from-file:8083"}]))
    monkeypatch.chdir(tmp_path)
    settings = load_settings({"CONNECT_CLUSTERS": "env=http://from-env:8083"})
    assert [cluster.name for cluster in settings.clusters] == ["file"]
    assert settings.clusters[0].url == "http://from-file:8083"


def test_environment_is_fallback_when_clusters_file_is_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = load_settings({"CONNECT_CLUSTERS": "env=http://from-env:8083"})
    assert [cluster.name for cluster in settings.clusters] == ["env"]
    assert settings.clusters[0].url == "http://from-env:8083"


def test_single_cluster_environment_fallback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = load_settings({
        "CONNECT_URL": "https://connect:8083",
        "CONNECT_NAME": "prod",
        "CONNECT_USERNAME": "connect",
        "CONNECT_PASSWORD": "secret",
        "CONNECT_VERIFY_SSL": "false",
    })
    cluster = settings.clusters[0]
    assert cluster.name == "prod"
    assert cluster.url == "https://connect:8083"
    assert cluster.username == "connect"
    assert cluster.password == "secret"
    assert cluster.verify_ssl is False
