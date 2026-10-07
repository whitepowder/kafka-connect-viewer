from __future__ import annotations

import base64
import hashlib
import time
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from authlib.jose import JsonWebKey, JsonWebToken
from fastapi.testclient import TestClient

from app.connect import ConnectClient
from app.main import app

ISSUER = "https://idp.test/realms/data"
CLIENT_ID = "kafka-connect-viewer"
REDIRECT_URI = "https://testserver/auth/callback"
KID = "test-key"

SIGNING_KEY = JsonWebKey.generate_key("RSA", 2048, is_private=True, options={"kid": KID})
OTHER_KEY = JsonWebKey.generate_key("RSA", 2048, is_private=True, options={"kid": KID})
UNKNOWN_KID_KEY = JsonWebKey.generate_key("RSA", 2048, is_private=True, options={"kid": "unknown"})


class FakeIdP:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []
        self.authorize_params: dict[str, str] = {}
        self.token_forms: list[dict[str, str]] = []
        self.role_claims: dict = {}
        self.claims_override: dict = {}
        self.drop_claims: set[str] = set()
        self.sign_key = SIGNING_KEY
        self.token_status = 200
        self.transport = httpx.MockTransport(self.handler)

    def authorize(self, location: str) -> dict[str, str]:
        parsed = urlparse(location)
        assert f"{parsed.scheme}://{parsed.netloc}{parsed.path}" == f"{ISSUER}/protocol/openid-connect/auth"
        self.authorize_params = {key: values[0] for key, values in parse_qs(parsed.query).items()}
        return self.authorize_params

    def id_token(self, role_claims: dict) -> str:
        now = int(time.time())
        claims = {
            "iss": ISSUER,
            "aud": CLIENT_ID,
            "sub": "user-1",
            "preferred_username": "alice",
            "iat": now,
            "exp": now + 300,
            "nonce": self.authorize_params.get("nonce"),
            **role_claims,
            **self.claims_override,
        }
        for name in self.drop_claims:
            claims.pop(name, None)
        return JsonWebToken(["RS256"]).encode({"alg": "RS256"}, claims, self.sign_key).decode()

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.calls.append((request.method, url))
        if url == f"{ISSUER}/.well-known/openid-configuration":
            return httpx.Response(
                200,
                json={
                    "issuer": ISSUER,
                    "authorization_endpoint": f"{ISSUER}/protocol/openid-connect/auth",
                    "token_endpoint": f"{ISSUER}/protocol/openid-connect/token",
                    "jwks_uri": f"{ISSUER}/protocol/openid-connect/certs",
                    "end_session_endpoint": f"{ISSUER}/protocol/openid-connect/logout",
                },
            )
        if url == f"{ISSUER}/protocol/openid-connect/certs":
            return httpx.Response(200, json={"keys": [SIGNING_KEY.as_dict(is_private=False)]})
        if url == f"{ISSUER}/protocol/openid-connect/token" and request.method == "POST":
            form = {key: values[0] for key, values in parse_qs(request.content.decode()).items()}
            self.token_forms.append(form)
            if self.token_status != 200:
                return httpx.Response(self.token_status, json={"error": "invalid_grant"})
            challenge = base64.urlsafe_b64encode(
                hashlib.sha256(form.get("code_verifier", "").encode()).digest()
            ).rstrip(b"=").decode()
            if (
                form.get("grant_type") != "authorization_code"
                or form.get("code") != "good-code"
                or form.get("client_id") != CLIENT_ID
                or form.get("redirect_uri") != REDIRECT_URI
                or challenge != self.authorize_params.get("code_challenge")
            ):
                return httpx.Response(400, json={"error": "invalid_grant"})
            return httpx.Response(200, json={"id_token": self.id_token(self.role_claims)})
        return httpx.Response(404)


def _connect_factory(cluster):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/connectors":
            return httpx.Response(200, json=["alpha"])
        if request.method in {"PUT", "POST", "DELETE"}:
            return httpx.Response(204)
        return httpx.Response(404, json={"message": "not found"})

    return ConnectClient(cluster, transport=httpx.MockTransport(handler))


@pytest.fixture
def cluster_env(monkeypatch, tmp_path):
    monkeypatch.setenv("CLUSTERS_FILE", str(tmp_path / "missing.json"))
    monkeypatch.setenv("CONNECT_CLUSTERS", "lab=http://connect.test")
    app.state.client_factory = _connect_factory
    yield
    app.state.client_factory = None


@pytest.fixture
def idp(monkeypatch, cluster_env):
    fake = FakeIdP()
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("KEYCLOAK_ISSUER", ISSUER)
    monkeypatch.setenv("KEYCLOAK_CLIENT_ID", CLIENT_ID)
    monkeypatch.setenv("KEYCLOAK_REDIRECT_URI", REDIRECT_URI)
    monkeypatch.setenv("SESSION_SECRET", "s" * 40)
    app.state.oidc_transport = fake.transport
    yield fake
    app.state.oidc_transport = None


@pytest.fixture
def client(idp):
    with TestClient(app, base_url="https://testserver") as test_client:
        yield test_client


def _start(client: TestClient, idp: FakeIdP, next_path: str = "/") -> dict[str, str]:
    response = client.get("/auth/login", params={"next": next_path}, follow_redirects=False)
    assert response.status_code == 302
    return idp.authorize(response.headers["location"])


def _login(client: TestClient, idp: FakeIdP, roles: dict, next_path: str = "/") -> httpx.Response:
    idp.role_claims = roles
    params = _start(client, idp, next_path)
    return client.get(
        "/auth/callback",
        params={"code": "good-code", "state": params["state"]},
        follow_redirects=False,
    )


def _realm(*roles: str) -> dict:
    return {"realm_access": {"roles": list(roles)}}


def test_login_callback_session_me(client, idp):
    login = client.get("/auth/login", params={"next": "/#lab"}, follow_redirects=False)
    assert login.status_code == 302
    flow_cookie = login.headers["set-cookie"]
    assert "kcui_oidc_flow=" in flow_cookie
    assert "HttpOnly" in flow_cookie and "Secure" in flow_cookie and "SameSite=lax" in flow_cookie

    params = idp.authorize(login.headers["location"])
    assert params["client_id"] == CLIENT_ID
    assert params["response_type"] == "code"
    assert params["redirect_uri"] == REDIRECT_URI
    assert "openid" in params["scope"].split()
    assert params["state"] and params["nonce"]
    assert params["code_challenge_method"] == "S256"
    assert len(params["code_challenge"]) == 43

    idp.role_claims = _realm("kafka-connect-viewer")
    callback = client.get(
        "/auth/callback",
        params={"code": "good-code", "state": params["state"]},
        follow_redirects=False,
    )
    assert callback.status_code == 302
    assert callback.headers["location"] == "/#lab"
    assert "kcui_session=" in callback.headers.get_list("set-cookie")[0]
    assert "kcui_oidc_flow" not in client.cookies

    me = client.get("/api/me")
    assert me.status_code == 200
    assert me.json() == {"auth_enabled": True, "name": "alice", "role": "viewer"}

    index = client.get("/", follow_redirects=False)
    assert index.status_code == 200


def test_pkce_verifier_matches_challenge_and_is_not_in_url(client, idp):
    response = _login(client, idp, _realm("kafka-connect-viewer"))
    assert response.status_code == 302
    form = idp.token_forms[-1]
    verifier = form["code_verifier"]
    assert 43 <= len(verifier) <= 128
    assert verifier not in str(idp.authorize_params)
    expected = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    assert expected == idp.authorize_params["code_challenge"]


def test_each_login_uses_fresh_state_nonce_and_challenge(client, idp):
    first = _start(client, idp)
    second = _start(client, idp)
    for key in ("state", "nonce", "code_challenge"):
        assert first[key] != second[key]


def test_token_endpoint_rejection_is_401(client, idp):
    idp.token_status = 400
    response = _login(client, idp, _realm("kafka-connect-admin"))
    assert response.status_code == 401
    assert response.json() == {"code": "oidc_token_exchange_failed", "params": {}}
    assert client.get("/api/me").status_code == 401


def test_unauthenticated_requests(client, idp):
    response = client.get("/api/me")
    assert response.status_code == 401
    assert response.json() == {"code": "auth_required", "params": {}}
    middleware = client.get("/api/clusters/lab/connectors")
    assert middleware.status_code == 401
    assert middleware.json() == {"code": "auth_required", "params": {}}
    assert client.get("/api/clusters").status_code == 401
    assert client.get("/api/clusters/lab/graph").status_code == 401
    assert client.delete("/api/clusters/lab/connectors/alpha").status_code == 401
    assert client.get("/api/health").status_code == 200
    index = client.get("/", follow_redirects=False)
    assert index.status_code == 302
    assert index.headers["location"].startswith(f"{ISSUER}/protocol/openid-connect/auth?")


@pytest.mark.parametrize(
    ("roles", "role", "read", "operate", "admin"),
    [
        (_realm("kafka-connect-viewer"), "viewer", 200, 403, 403),
        (_realm("kafka-connect-operator"), "operator", 200, 200, 403),
        (_realm("kafka-connect-admin"), "admin", 200, 200, 200),
        ({"resource_access": {CLIENT_ID: {"roles": ["kafka-connect-admin"]}}}, "admin", 200, 200, 200),
        (_realm("kafka-connect-viewer", "kafka-connect-operator"), "operator", 200, 200, 403),
    ],
)
def test_rbac(client, idp, roles, role, read, operate, admin):
    assert _login(client, idp, roles).status_code == 302
    assert client.get("/api/me").json()["role"] == role
    assert client.get("/api/clusters").status_code == read
    assert client.get("/api/clusters/lab/connectors").status_code == read
    assert client.get("/api/clusters/lab/graph").status_code == read
    assert client.post("/api/clusters/lab/connectors/alpha/pause").status_code == operate
    assert client.post("/api/clusters/lab/connectors/alpha/restart").status_code == operate
    assert client.post("/api/clusters/lab/connectors/alpha/tasks/0/restart").status_code == operate
    deleted = client.delete("/api/clusters/lab/connectors/alpha")
    assert deleted.status_code == admin
    if admin == 403:
        assert deleted.json() == {"code": "role_required", "params": {"role": "admin"}}
    assert (
        client.put("/api/clusters/lab/connectors/alpha/config", json={"config": {"connector.class": "X"}}).status_code
        == admin
    )


def test_role_from_other_client_is_ignored(client, idp):
    roles = {"resource_access": {"other-app": {"roles": ["kafka-connect-admin"]}}}
    response = _login(client, idp, roles)
    assert response.status_code == 403
    assert "kcui_session" not in client.cookies


def test_missing_role_is_forbidden_without_session(client, idp):
    response = _login(client, idp, _realm("unrelated"))
    assert response.status_code == 403
    assert response.json() == {"code": "oidc_no_role", "params": {}}
    assert "kcui_session" not in client.cookies
    assert client.get("/api/me").status_code == 401


def test_invalid_state(client, idp):
    _start(client, idp)
    response = client.get("/auth/callback", params={"code": "good-code", "state": "forged"}, follow_redirects=False)
    assert response.status_code == 400
    assert response.json() == {"code": "oidc_state_invalid", "params": {}}
    assert idp.token_forms == []


def test_missing_flow_cookie(client, idp):
    params = _start(client, idp)
    client.cookies.clear()
    response = client.get(
        "/auth/callback", params={"code": "good-code", "state": params["state"]}, follow_redirects=False
    )
    assert response.status_code == 400
    assert response.json() == {"code": "oidc_flow_missing", "params": {}}
    assert idp.token_forms == []


def test_tampered_flow_cookie(client, idp):
    params = _start(client, idp)
    raw = client.cookies["kcui_oidc_flow"]
    client.cookies.clear()
    client.cookies.set("kcui_oidc_flow", raw[:-2] + ("AA" if raw[-2:] != "AA" else "BB"))
    response = client.get(
        "/auth/callback", params={"code": "good-code", "state": params["state"]}, follow_redirects=False
    )
    assert response.status_code == 400
    assert response.json() == {"code": "oidc_flow_invalid", "params": {}}


def test_tampered_session_cookie(client, idp):
    assert _login(client, idp, _realm("kafka-connect-admin")).status_code == 302
    raw = client.cookies["kcui_session"]
    client.cookies.clear()
    client.cookies.set("kcui_session", raw[:-2] + ("AA" if raw[-2:] != "AA" else "BB"))
    assert client.get("/api/me").status_code == 401


@pytest.mark.parametrize(
    ("setup", "status"),
    [
        pytest.param(lambda idp: idp.claims_override.update(nonce="other"), 401, id="wrong-nonce"),
        pytest.param(lambda idp: idp.drop_claims.add("nonce"), 401, id="missing-nonce"),
        pytest.param(lambda idp: setattr(idp, "sign_key", OTHER_KEY), 401, id="bad-signature"),
        pytest.param(lambda idp: setattr(idp, "sign_key", UNKNOWN_KID_KEY), 401, id="unknown-kid"),
        pytest.param(lambda idp: idp.claims_override.update(iss="https://evil.test/realms/data"), 401, id="issuer"),
        pytest.param(lambda idp: idp.claims_override.update(aud="other-client"), 401, id="audience"),
        pytest.param(lambda idp: idp.claims_override.update(exp=int(time.time()) - 120), 401, id="expired"),
        pytest.param(lambda idp: idp.drop_claims.add("exp"), 401, id="missing-exp"),
    ],
)
def test_invalid_id_token(client, idp, setup, status):
    setup(idp)
    response = _login(client, idp, _realm("kafka-connect-admin"))
    assert response.status_code == status
    assert response.json()["code"] in {"oidc_id_token_invalid", "oidc_nonce_invalid"}
    assert "kcui_session" not in client.cookies
    assert client.get("/api/me").status_code == 401


def test_expiry_within_leeway_is_accepted(client, idp):
    idp.claims_override["exp"] = int(time.time()) - 5
    assert _login(client, idp, _realm("kafka-connect-viewer")).status_code == 302


def test_open_redirect_in_next_is_neutralised(client, idp):
    response = _login(client, idp, _realm("kafka-connect-viewer"), next_path="//evil.test/")
    assert response.status_code == 302
    assert response.headers["location"] == "/"


def test_logout_clears_session(client, idp):
    assert _login(client, idp, _realm("kafka-connect-viewer")).status_code == 302
    response = client.get("/auth/logout", follow_redirects=False)
    assert response.status_code == 302
    location = urlparse(response.headers["location"])
    assert f"{location.scheme}://{location.netloc}{location.path}" == f"{ISSUER}/protocol/openid-connect/logout"
    assert "id_token_hint" in parse_qs(location.query)
    assert client.get("/api/me").status_code == 401


def test_auth_disabled_behaviour_is_unchanged(monkeypatch, cluster_env):
    monkeypatch.delenv("AUTH_ENABLED", raising=False)
    fake = FakeIdP()
    app.state.oidc_transport = fake.transport
    try:
        with TestClient(app) as client:
            assert client.get("/api/me").json() == {"auth_enabled": False, "name": "local", "role": "admin"}
            assert client.get("/api/clusters").status_code == 200
            assert client.get("/api/clusters/lab/connectors").json() == {"count": 1, "connectors": ["alpha"]}
            assert client.delete("/api/clusters/lab/connectors/alpha").status_code == 200
            assert client.get("/", follow_redirects=False).status_code == 200
            for path in ("/auth/login", "/auth/logout", "/auth/callback?code=x&state=y"):
                response = client.get(path, follow_redirects=False)
                assert response.status_code == 302
                assert response.headers["location"] == "/"
    finally:
        app.state.oidc_transport = None
    assert fake.calls == []


def test_auth_enabled_requires_settings(monkeypatch, cluster_env):
    monkeypatch.setenv("AUTH_ENABLED", "true")
    for name in ("KEYCLOAK_ISSUER", "KEYCLOAK_CLIENT_ID", "KEYCLOAK_REDIRECT_URI", "SESSION_SECRET"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError, match="missing"):
        with TestClient(app):
            pass
