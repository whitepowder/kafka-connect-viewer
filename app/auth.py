from __future__ import annotations

import base64
import hashlib
import os
import secrets
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx
from authlib.jose import JsonWebToken, JoseError
from fastapi import Request
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, URLSafeTimedSerializer

from app.errors import KCVError

ROLE_LEVEL = {"viewer": 10, "operator": 20, "admin": 30}


@dataclass(frozen=True)
class AuthSettings:
    enabled: bool
    issuer: str | None
    client_id: str | None
    client_secret: str | None
    redirect_uri: str | None
    post_logout_redirect_uri: str | None
    session_secret: str | None
    cookie_secure: bool
    viewer_role: str
    operator_role: str
    admin_role: str

    @classmethod
    def from_env(cls) -> "AuthSettings":
        enabled = _bool_env("AUTH_ENABLED", False)
        issuer = _value("KEYCLOAK_ISSUER")
        client_id = _value("KEYCLOAK_CLIENT_ID")
        client_secret = _value("KEYCLOAK_CLIENT_SECRET")
        redirect_uri = _value("KEYCLOAK_REDIRECT_URI")
        session_secret = _value("SESSION_SECRET")
        if enabled:
            missing = [name for name, value in {
                "KEYCLOAK_ISSUER": issuer, "KEYCLOAK_CLIENT_ID": client_id,
                "KEYCLOAK_REDIRECT_URI": redirect_uri, "SESSION_SECRET": session_secret,
            }.items() if not value]
            if missing:
                raise RuntimeError("AUTH_ENABLED=true but missing " + ", ".join(missing))
            if len(session_secret or "") < 32:
                raise RuntimeError("SESSION_SECRET must be at least 32 characters")
        return cls(
            enabled=enabled,
            issuer=issuer.rstrip("/") if issuer else None,
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            post_logout_redirect_uri=_value("KEYCLOAK_POST_LOGOUT_REDIRECT_URI"),
            session_secret=session_secret,
            cookie_secure=_bool_env("AUTH_COOKIE_SECURE", True),
            viewer_role=os.getenv("AUTH_ROLE_VIEWER", "kafka-connect-viewer"),
            operator_role=os.getenv("AUTH_ROLE_OPERATOR", "kafka-connect-operator"),
            admin_role=os.getenv("AUTH_ROLE_ADMIN", "kafka-connect-admin"),
        )


class OIDCAuth:
    session_cookie = "kcui_session"
    flow_cookie = "kcui_oidc_flow"

    def __init__(self, settings: AuthSettings, transport: httpx.AsyncBaseTransport | None = None):
        self.settings = settings
        self.http = httpx.AsyncClient(timeout=10.0, follow_redirects=False, transport=transport)
        self.serializer = URLSafeTimedSerializer(settings.session_secret or secrets.token_hex(32), salt="kcui")
        self._metadata: dict[str, Any] | None = None
        self._jwks: dict[str, Any] | None = None

    async def close(self) -> None:
        await self.http.aclose()

    async def metadata(self) -> dict[str, Any]:
        if self._metadata is None:
            response = await self.http.get(f"{self.settings.issuer}/.well-known/openid-configuration")
            response.raise_for_status()
            self._metadata = response.json()
        return self._metadata

    async def jwks(self, refresh: bool = False) -> dict[str, Any]:
        if self._jwks is None or refresh:
            metadata = await self.metadata()
            response = await self.http.get(metadata["jwks_uri"])
            response.raise_for_status()
            self._jwks = response.json()
        return self._jwks

    def session(self, request: Request) -> dict[str, Any] | None:
        raw = request.cookies.get(self.session_cookie)
        if not raw:
            return None
        try:
            data = self.serializer.loads(raw, max_age=8 * 60 * 60)
        except BadSignature:
            return None
        return data if isinstance(data, dict) else None

    def effective_role(self, claims: dict[str, Any]) -> str | None:
        roles: set[str] = set()
        realm = claims.get("realm_access")
        if isinstance(realm, dict) and isinstance(realm.get("roles"), list):
            roles.update(map(str, realm["roles"]))
        resources = claims.get("resource_access")
        if isinstance(resources, dict):
            client = resources.get(self.settings.client_id or "")
            if isinstance(client, dict) and isinstance(client.get("roles"), list):
                roles.update(map(str, client["roles"]))
        if self.settings.admin_role in roles:
            return "admin"
        if self.settings.operator_role in roles:
            return "operator"
        if self.settings.viewer_role in roles:
            return "viewer"
        return None

    async def begin_login(self, next_path: str = "/") -> RedirectResponse:
        metadata = await self.metadata()
        state = secrets.token_urlsafe(32)
        nonce = secrets.token_urlsafe(32)
        verifier = secrets.token_urlsafe(64)
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
        flow = self.serializer.dumps(
            {"state": state, "nonce": nonce, "verifier": verifier, "next": _safe_next(next_path)}
        )
        query = urlencode({
            "client_id": self.settings.client_id, "response_type": "code",
            "scope": "openid profile email", "redirect_uri": self.settings.redirect_uri,
            "state": state, "nonce": nonce,
            "code_challenge": challenge, "code_challenge_method": "S256",
        })
        response = RedirectResponse(f'{metadata["authorization_endpoint"]}?{query}', status_code=302)
        response.set_cookie(self.flow_cookie, flow, max_age=600, httponly=True, secure=self.settings.cookie_secure, samesite="lax", path="/")
        return response

    async def finish_login(self, request: Request, code: str, state: str) -> RedirectResponse:
        raw = request.cookies.get(self.flow_cookie)
        if not raw:
            raise KCVError(400, "oidc_flow_missing")
        try:
            flow = self.serializer.loads(raw, max_age=600)
        except BadSignature as exc:
            raise KCVError(400, "oidc_flow_invalid") from exc
        if not secrets.compare_digest(str(flow.get("state", "")), state):
            raise KCVError(400, "oidc_state_invalid")
        metadata = await self.metadata()
        data = {
            "grant_type": "authorization_code", "code": code,
            "redirect_uri": self.settings.redirect_uri, "client_id": self.settings.client_id,
            "code_verifier": flow["verifier"],
        }
        auth = (self.settings.client_id or "", self.settings.client_secret) if self.settings.client_secret else None
        response = await self.http.post(metadata["token_endpoint"], data=data, auth=auth)
        if response.status_code >= 400:
            raise KCVError(401, "oidc_token_exchange_failed")
        token = response.json().get("id_token")
        if not isinstance(token, str):
            raise KCVError(401, "oidc_id_token_missing")
        claims = await self.verify(token)
        expected_nonce = str(flow.get("nonce", ""))
        if not expected_nonce or not secrets.compare_digest(str(claims.get("nonce", "")), expected_nonce):
            raise KCVError(401, "oidc_nonce_invalid")
        role = self.effective_role(claims)
        if not role:
            raise KCVError(403, "oidc_no_role")
        session = {
            "sub": claims.get("sub"),
            "name": claims.get("preferred_username") or claims.get("name") or claims.get("email") or claims.get("sub"),
            "role": role, "id_token": token,
        }
        result = RedirectResponse(_safe_next(str(flow.get("next", "/"))), status_code=302)
        result.set_cookie(self.session_cookie, self.serializer.dumps(session), max_age=8*60*60, httponly=True, secure=self.settings.cookie_secure, samesite="lax", path="/")
        result.delete_cookie(self.flow_cookie, path="/")
        return result

    async def verify(self, token: str) -> dict[str, Any]:
        jwt = JsonWebToken(["RS256", "PS256", "ES256"])
        options = {
            "iss": {"essential": True, "value": self.settings.issuer},
            "aud": {"essential": True, "value": self.settings.client_id},
            "exp": {"essential": True},
        }
        for refresh in (False, True):
            try:
                claims = jwt.decode(token, await self.jwks(refresh), claims_options=options)
                claims.validate(leeway=30)
                return dict(claims)
            # authlib raises ValueError, not JoseError, when no JWKS key matches the token kid.
            except (JoseError, ValueError):
                if refresh:
                    raise KCVError(401, "oidc_id_token_invalid")
        raise KCVError(401, "oidc_id_token_invalid")

    async def logout(self, request: Request) -> RedirectResponse:
        session = self.session(request)
        target = self.settings.post_logout_redirect_uri or "/"
        metadata = await self.metadata()
        query = {"client_id": self.settings.client_id, "post_logout_redirect_uri": target}
        if session and session.get("id_token"):
            query["id_token_hint"] = session["id_token"]
        response = RedirectResponse(f'{metadata["end_session_endpoint"]}?{urlencode(query)}', status_code=302)
        response.delete_cookie(self.session_cookie, path="/")
        return response


def require_role(request: Request, minimum: str) -> dict[str, Any]:
    auth: OIDCAuth = request.app.state.auth
    if not auth.settings.enabled:
        return {"name": "local", "role": "admin"}
    session = auth.session(request)
    if not session:
        raise KCVError(401, "auth_required")
    role = str(session.get("role", ""))
    if ROLE_LEVEL.get(role, 0) < ROLE_LEVEL[minimum]:
        raise KCVError(403, "role_required", {"role": minimum})
    return session


def _safe_next(value: str) -> str:
    return value if value.startswith("/") and not value.startswith("//") else "/"


def _value(name: str) -> str | None:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else None


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}
