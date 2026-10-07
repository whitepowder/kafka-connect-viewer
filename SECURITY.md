# Security

## Deployment model

Despite its name, KCV (Kafka Connect Viewer) can create, modify, pause, restart and delete Kafka Connect connectors. Treat it as an administrative application, not a public internet service.

Run it behind TLS and network access controls. For shared deployments enable OIDC (`AUTH_ENABLED=true`) and grant the least role needed: viewer, operator or admin. RBAC is enforced by the backend.

## Built-in protections

- OIDC authorization code flow with PKCE (S256), `state` and `nonce`; ID tokens are verified against the issuer's JWKS together with `iss`, `aud` and `exp`.
- Signed, `HttpOnly`, `SameSite=Lax` session cookies; `Secure` by default.
- Cross-site browser writes are rejected; mutating API payloads are limited to 1 MiB.
- Restrictive browser security headers; FastAPI's public API docs are disabled.
- The container runs as a non-root user.

## Secrets

Do not commit `clusters.json`, `.env`, passwords, tokens, private keys or host inventories. Prefer `env:VARIABLE_NAME` references in `clusters.json` and inject the variables from your secret manager. The same applies to `KEYCLOAK_CLIENT_SECRET` and `SESSION_SECRET`.

Kafka Connect may mask password-type connector properties as `*****`. KCV refuses to write an unresolved mask back; enter a new secret explicitly when the worker cannot return the existing value.

## TLS

Keep `verify_ssl: true` for HTTPS Kafka Connect endpoints. Set it to `false` only in controlled development environments.

## Reporting a vulnerability

Report vulnerabilities privately to the project maintainers, for example through a confidential issue in the project's issue tracker or by contacting a maintainer directly. Do not open a public issue containing credentials, connector configurations, internal hostnames or stack traces with sensitive data.
