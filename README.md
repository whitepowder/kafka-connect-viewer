# KCV — Kafka Connect Viewer

KCV (Kafka Connect Viewer) is a lightweight web UI for viewing and managing one or more Kafka Connect clusters.

The connector list calls only `GET /connectors`. Status, tasks and configuration are fetched only when you open a connector, so the first page stays cheap even on workers with hundreds of connectors.

## Features

- Multiple Kafka Connect clusters
- Russian and English UI
- Fast connector list with client-side search
- Connector status and task state
- Pause, resume, restart; restart all, failed, or a single task
- View and edit configuration (Properties (key=value), JSON and cURL views)
- Plugin validation and plugin browser
- Create and delete connectors
- Basic Auth to upstream Kafka Connect, TLS verification on by default
- Optional Keycloak / OIDC login with viewer/operator/admin roles
- Health endpoints for Docker and Kubernetes

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp clusters.example.json clusters.json
# edit clusters.json: set your worker URLs, remove clusters you don't need
export CONNECT_PASSWORD='...'
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000`.

Every `"password": "env:NAME"` in `clusters.json` needs `NAME` exported before start; otherwise KCV refuses to start and names the missing variable.

## Configuration

`clusters.json` is a list of clusters:

```json
[
  {
    "name": "dev",
    "url": "http://kafka-connect-dev:8083",
    "verify_ssl": true
  },
  {
    "name": "prod",
    "url": "https://kafka-connect-prod:8083",
    "username": "connect",
    "password": "env:CONNECT_PASSWORD",
    "verify_ssl": true
  }
]
```

`env:CONNECT_PASSWORD` reads the password from the environment. A literal password also works for local use. `clusters.json` is ignored by Git and Docker.

Without a file, clusters can come from the environment:

```bash
export CONNECT_CLUSTERS='dev=http://connect-dev:8083,prod=http://connect-prod:8083'
```

Priority:

1. `CLUSTERS_FILE` if it points to an existing file, otherwise `./clusters.json` if present.
2. `CONNECT_CLUSTERS`.
3. Single cluster: `CONNECT_URL`, `CONNECT_NAME`, `CONNECT_USERNAME`, `CONNECT_PASSWORD`, `CONNECT_VERIFY_SSL`.
4. `http://127.0.0.1:8083`.

## Docker

```bash
docker build -t kafka-connect-viewer:local .

docker run --rm -p 8000:8000 \
  -e CONNECT_CLUSTERS='prod=http://kafka-connect:8083' \
  kafka-connect-viewer:local

# or mount a cluster file
docker run --rm -p 8000:8000 \
  -v "$PWD/clusters.json:/app/clusters.json:ro" \
  kafka-connect-viewer:local
```

The image runs as a non-root user.

## Connector actions

Available in the details of an opened connector:

| Action | What it does |
| --- | --- |
| **Pause** | Pauses processing: the connector and its tasks stop until resumed. |
| **Resume** | Resumes a paused connector and its tasks. |
| **Restart connector** | Restarts only the connector instance; tasks are not explicitly restarted. |
| **Connector + tasks** | Restarts the connector and all of its tasks. |
| **Restart failed** | Restarts only the connector and tasks that are in the `FAILED` state. |
| **Restart task** | Restarts only the selected task (button in the task row). |
| **Refresh** | Reloads status, tasks and config. Nothing is restarted. |
| **Delete** | Deletes the connector and its config from the worker. Destructive, cannot be undone. |

Roles:

- **operator** — Pause, Resume, Restart connector, Connector + tasks, Restart failed and Restart task.
- **admin** — everything an operator can do, plus Delete (and create / edit config).
- **viewer** — Refresh only; it just reads data.

With `AUTH_ENABLED=false` every action is available.

## Health endpoints

- `GET /health` — liveness
- `GET /ready` — configuration loaded
- `GET /api/health` — API health

None of them call Kafka Connect, so probes add no upstream load.

## Keycloak / OIDC authentication

Authentication is off by default. With `AUTH_ENABLED=false` every local session is an admin.

```bash
AUTH_ENABLED=true
KEYCLOAK_ISSUER=https://keycloak.example.com/realms/example
KEYCLOAK_CLIENT_ID=kafka-connect-viewer
KEYCLOAK_CLIENT_SECRET=...            # optional for a public client
KEYCLOAK_REDIRECT_URI=https://kafka-connect-viewer.example.com/auth/callback
KEYCLOAK_POST_LOGOUT_REDIRECT_URI=https://kafka-connect-viewer.example.com/
SESSION_SECRET=...                    # random, at least 32 characters
AUTH_COOKIE_SECURE=true
```

Login uses the authorization code flow with PKCE (S256), `state` and `nonce`. The ID token signature is checked against the issuer's JWKS, along with `iss`, `aud` and `exp`.

Roles (realm roles or client roles of `KEYCLOAK_CLIENT_ID`):

- `kafka-connect-viewer` — read clusters, connectors, status, tasks, config and plugins
- `kafka-connect-operator` — viewer plus pause, resume and restart
- `kafka-connect-admin` — everything, including create, edit config, validate and delete

The `kafka-connect-viewer` role is unrelated to the `kafka-connect-viewer` client ID; it grants read-only access only. Rename roles with `AUTH_ROLE_VIEWER`, `AUTH_ROLE_OPERATOR`, `AUTH_ROLE_ADMIN`. RBAC is enforced by the backend; hidden buttons in the UI are only a convenience.

The Keycloak client must allow the exact `/auth/callback` redirect URI and the post-logout URI. Use HTTPS in production.

## Tests

```bash
pytest
node tests/frontend_smoke.cjs static/app.js
node tests/create_editor.test.cjs
```

`pytest` covers configuration loading, the Kafka Connect client, and OIDC against a mocked identity provider: the login → callback → session flow, PKCE, `state` and `nonce`, JWT validation (signature, unknown key, issuer, audience, expiry), and viewer/operator/admin RBAC. It also checks that `AUTH_ENABLED=false` is unchanged.

The frontend smoke test loads `static/app.js` with a stubbed DOM to catch startup errors. The create-editor test drives the Properties / JSON / cURL tabs through a fake DOM: key=value parsing, bidirectional sync, preserved invalid drafts, and the config sent by Validate and Create. When Node.js is installed, `pytest` runs both frontend scripts too.

## Security

Despite the name, KCV has write access to Kafka Connect. Do not expose it to the public internet. Run it behind TLS and network access controls, and enable OIDC for shared deployments. See [SECURITY.md](SECURITY.md).

## Design goal

KCV never uses `GET /connectors?expand=status&expand=info` for the main list, so opening the UI does not fetch status and config for every connector on the worker.
