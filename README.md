# KCV — Kafka Connect Viewer

KCV (Kafka Connect Viewer) is a lightweight web UI for viewing and managing one or more Kafka Connect clusters.

The connector list calls only `GET /connectors`. Status, tasks and configuration are fetched only when you open a connector, so the first page stays cheap even on workers with hundreds of connectors.

## Features

- Multiple Kafka Connect clusters
- English UI with optional Russian and Simplified Chinese (zh-CN) localizations (language switcher in the sidebar)
- Fast connector list with client-side search
- Connector status and task state
- Pause, resume, restart; restart all, failed, or a single task
- View and edit configuration (Properties (key=value), JSON and cURL views)
- Plugin validation and plugin browser
- Create and delete connectors
- Graph view: source connectors → Kafka topics → sink connectors, with consumer-group conflict diagnostics
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

## Graph

The **Graph** switch above the connector list (`#<cluster>?view=graph`) shows three columns: source connectors, Kafka topics and sink connectors. It needs the viewer role and is served by `GET /api/clusters/{id}/graph`.

The graph reads `GET /connectors`, `GET /connector-plugins` (connector types) and then `GET /connectors/{name}/config` for each connector, at most 8 requests at a time. It never uses `expand=info` or `expand=status`. Only these keys are used:

- sources: `topic`, `topic.prefix`;
- sinks: `topics`, `topics.regex`, `consumer.override.group.id`, `errors.deadletterqueue.topic.name`;
- `consumer.override.bootstrap.servers`: only whether it is set; the value is never read into the graph or shown.

Other config values (URLs, hosts, credentials, transforms) are not parsed, cached or returned. Raw configs are not cached either; only these facts are.

A sink without `consumer.override.group.id` is shown with the default group `connect-<name>` as *default, expected*: Kafka Connect normally uses it, but it is not guaranteed. Placeholders such as `${file:...}` and masked values make the group or topics *undetermined*; an undetermined group is never treated as equal to another group.

Diagnostics:

| Severity | When |
| --- | --- |
| Error | Two sinks read the same topic (listed in `topics` or matched by `topics.regex`) with the same explicit group. |
| Warning | Two `topics.regex` patterns may overlap, or a pattern cannot be analyzed, and the sinks use the same explicit group. |
| Warning | A group cannot be determined, so a conflict cannot be ruled out. |
| Warning | An explicit group equals another sink's default `connect-<name>`. |
| Warning | A sink in an otherwise conflicting group sets `consumer.override.bootstrap.servers`, so it may read another Kafka cluster (never an error). |
| Warning | A sink sets both `topics` and `topics.regex`, its topics cannot be determined, or its config could not be read. |
| OK | A topic is read by several sinks with different groups (marked expected when a default group is involved). |

The toolbar has search (matches and their neighbours), **Only problems**, **Refresh** and an error/warning counter. Clicking a node highlights everything upstream and downstream of it; clicking a diagnostic highlights its topic and connectors. If some configs could not be read, the graph is marked partial and the rest is still shown.

Caching:

- `GRAPH_FACTS_TTL` (default `60`) — seconds the per-connector facts are kept;
- `GRAPH_TTL` (default `30`) — seconds the built graph is kept;
- **Refresh** (`?refresh=true`) re-reads every config and is limited to once per 10 seconds per cluster; earlier calls get `429` with `Retry-After`.

Creating, deleting or editing a connector in KCV drops its cached facts right away. Changes made outside KCV show up after the TTL or a refresh.

## API errors

Errors raised by KCV itself carry a stable code and its parameters instead of text, for example `{"code": "secret_masked", "params": {"key": "password"}}`; the UI translates the code into the selected language (English, Russian or Simplified Chinese), falling back to English. The codes are listed in `app/errors.py`. Errors returned by Kafka Connect are passed through unchanged as `{"message": "..."}` and are never translated. HTTP status codes are the same in both cases.

Invalid KCV configuration (cluster settings, graph TTLs, OIDC settings) stops startup with a plain English message in the log. These operator-facing errors are not API errors and have no codes.

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

`pytest` covers configuration loading, the Kafka Connect client, the graph (config facts, Java regex handling, edges, every diagnostic rule, the 8-request limit, caching, refresh limit and cache invalidation), and OIDC against a mocked identity provider: the login → callback → session flow, PKCE, `state` and `nonce`, JWT validation (signature, unknown key, issuer, audience, expiry), and viewer/operator/admin RBAC. It also checks that `AUTH_ENABLED=false` is unchanged.

The frontend smoke test loads `static/app.js` with a stubbed DOM to catch startup errors. The create-editor test drives the Properties / JSON / cURL tabs through a fake DOM: key=value parsing, bidirectional sync, preserved invalid drafts, and the config sent by Validate and Create. The graph test (`tests/graph.test.cjs`) renders a graph built by the backend and checks columns, group labels, search, **Only problems**, focus, the Diagnostics panel and refresh; it is run by `pytest`, which passes it the graph fixture. The error test (`tests/errors.test.cjs`) receives the backend error codes and checks that each has English, Russian and Simplified Chinese text and that Kafka Connect messages are shown unchanged. The language test (`tests/language.test.cjs`) checks that new users get English regardless of the browser locale, that a stored choice is kept, that Russian and Simplified Chinese cover every English text, and that missing translations fall back to English. When Node.js is installed, `pytest` runs all frontend scripts too.

## Security

Despite the name, KCV has write access to Kafka Connect. Do not expose it to the public internet. Run it behind TLS and network access controls, and enable OIDC for shared deployments. See [SECURITY.md](SECURITY.md).

## Design goal

KCV never uses `GET /connectors?expand=status&expand=info` for the main list, so opening the UI does not fetch status and config for every connector on the worker.
