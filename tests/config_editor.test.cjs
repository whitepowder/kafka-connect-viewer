// Tests for the connector configuration editor in the detail pane:
// Properties | JSON | cURL tabs, synchronized drafts, preserved invalid and unsaved edits,
// masked secrets, exact round trips, RBAC and EN/RU/zh-CN labels.
// The cURL command is run through bash with a stubbed curl to check its quoting.
// Usage: node tests/config_editor.test.cjs path/to/app.js
const assert = require("assert");
const { spawnSync } = require("child_process");
const path = require("path");
const { findAll, loadApp, runTests, tick } = require("./fake_dom.cjs");

const appPath = path.resolve(process.argv[2] || path.join(__dirname, "..", "static/app.js"));
const MASK = "*****";
const BASE = "/api/clusters/c/connectors/orders";

const initialConfig = () => ({
  "connector.class": "io.example.JdbcSink",
  "tasks.max": "1",
  topics: "orders",
  "connection.url": "jdbc:postgresql://db.example.com/app?ssl=true&sslmode=require",
  "connection.password": MASK,
  "transforms.route.regex": "a=b=c",
  "header.value": "  padded  ",
});

function server(role = "admin", config = initialConfig()) {
  const backend = { role, config, requests: [] };
  backend.fetch = async (url, options = {}) => {
    const method = options.method || "GET";
    const body = options.body ? JSON.parse(options.body) : null;
    backend.requests.push({ method, url, body });
    let status = 200;
    let payload = {};
    if (url === "/api/me") payload = { role: backend.role, auth_enabled: false };
    else if (url === "/api/clusters") payload = { clusters: [{ id: "c", name: "c", url: "https://user:secret@connect.example.com:8083/" }] };
    else if (url === "/api/clusters/c/connectors") payload = { count: 1, connectors: ["orders"] };
    else if (url === "/api/clusters/c") payload = { version: "3.7.0" };
    else if (url === BASE && method === "GET") {
      payload = { name: "orders", type: "sink", config: backend.config, connector: { state: "RUNNING" }, tasks: [] };
    } else if (url === `${BASE}/config` && method === "PUT") {
      const stored = { ...body.config };
      for (const [key, value] of Object.entries(stored)) if (value === MASK) stored[key] = backend.config[key];
      backend.config = Object.fromEntries(Object.entries(stored).map(([key, value]) => [key, key === "connection.password" ? MASK : value]));
      payload = stored;
    } else if (url === "/api/clusters/c/plugins/validate") payload = { configs: [] };
    else if (url === `${BASE}/pause`) payload = {};
    else [status, payload] = [404, { message: "not found" }];
    return { ok: status < 400, status, statusText: "Status", text: async () => JSON.stringify(payload) };
  };
  return backend;
}

async function open(role = "admin", options = {}) {
  const backend = server(role, options.config);
  const page = loadApp(appPath, { storedLanguage: options.language || "en", hash: "#c/orders", fetch: backend.fetch, expose: "state, updateCurlCommand" });
  await tick();
  await tick();
  const pane = page.roots["detail-pane"];
  const byId = (id) => findAll(pane, (node) => node.id === id)[0] || null;
  const all = (predicate) => findAll(pane, predicate);
  const ui = {
    page,
    backend,
    text: () => byId("config-text"),
    tabs: () => all((node) => node.classList.contains("tab") && node.dataset.action === "config-tab"),
    tab: (mode) => all((node) => node.dataset.action === "config-tab" && node.dataset.mode === mode)[0],
    activeTab: () => all((node) => node.classList.contains("tab") && node.classList.contains("is-active")).map((node) => node.dataset.mode),
    button: (action) => all((node) => node.dataset.action === action)[0] || null,
    banner: () => all((node) => node.classList.contains("editor-error")).map((node) => node.textContent)[0] || null,
    dirty: () => !byId("config-dirty").hidden,
    toast: () => page.roots.toast.textContent,
    puts: () => backend.requests.filter((request) => request.method === "PUT"),
    async click(node) {
      assert.ok(node, "the element to click exists");
      for (const handler of pane.listeners.click || []) await handler({ target: node, preventDefault() {} });
      await tick();
    },
    type(value) {
      const text = ui.text();
      text.value = value;
      text.dispatch("input");
    },
    json: () => JSON.parse(ui.text().value),
    curl: () => byId("config-curl")?.textContent ?? null,
    hints: () => all((node) => node.classList.contains("hint")).map((node) => node.textContent),
  };
  return ui;
}

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

test("the editor has Properties | JSON | cURL tabs and opens in Properties", async () => {
  const ui = await open();
  assert.deepStrictEqual(ui.tabs().map((node) => node.textContent), ["Properties", "JSON", "cURL"]);
  assert.deepStrictEqual(ui.activeTab(), ["properties"]);
  assert.strictEqual(ui.text().dataset.mode, "properties");
  assert.strictEqual(ui.text().value, [
    "connector.class=io.example.JdbcSink",
    "tasks.max=1",
    "topics=orders",
    "connection.url=jdbc:postgresql://db.example.com/app?ssl=true&sslmode=require",
    `connection.password=${MASK}`,
    "transforms.route.regex=a=b=c",
    "header.value=  padded  ",
  ].join("\n"));
  assert.strictEqual(ui.dirty(), false);
});

test("an unchanged save sends the stored config exactly, including padded values and the mask", async () => {
  const ui = await open();
  await ui.click(ui.button("save-config"));
  assert.deepStrictEqual(ui.puts().map((request) => request.body), [{ config: initialConfig() }]);
});

test("Properties edits are saved and keep values containing =", async () => {
  const ui = await open();
  ui.type(ui.text().value.replace("tasks.max=1", "tasks.max=3") + "\nbatch.size=500\n# a comment");
  assert.strictEqual(ui.dirty(), true);
  await ui.click(ui.button("save-config"));
  const [put] = ui.puts();
  assert.strictEqual(put.url, `${BASE}/config`);
  assert.strictEqual(put.body.config["tasks.max"], "3");
  assert.strictEqual(put.body.config["batch.size"], "500");
  assert.strictEqual(put.body.config["transforms.route.regex"], "a=b=c");
  assert.strictEqual(put.body.config["connection.url"], "jdbc:postgresql://db.example.com/app?ssl=true&sslmode=require");
  assert.strictEqual(put.body.config["connection.password"], MASK, "the mask is sent unchanged so the backend restores the stored secret");
  assert.strictEqual(ui.toast(), "Config saved");
  assert.strictEqual(ui.dirty(), false, "after saving, the editor shows the stored config");
  assert.ok(ui.text().value.includes("batch.size=500"));
  assert.strictEqual(ui.backend.config["connection.password"], MASK);
});

test("switching tabs keeps edits in sync in both directions", async () => {
  const ui = await open();
  const edited = ui.text().value.replace("topics=orders", "topics=orders,refunds") + "\n# keep me";
  ui.type(edited);
  await ui.click(ui.tab("json"));
  assert.deepStrictEqual(ui.activeTab(), ["json"]);
  assert.strictEqual(ui.json().topics, "orders,refunds");
  assert.strictEqual(ui.json()["transforms.route.regex"], "a=b=c");
  assert.strictEqual(ui.json()["connection.password"], MASK);
  await ui.click(ui.tab("properties"));
  assert.strictEqual(ui.text().value, edited, "an unchanged round trip keeps the Properties text and comments");

  await ui.click(ui.tab("json"));
  ui.type(JSON.stringify({ ...ui.json(), "tasks.max": "4", "key.with=equals": "x=y" }, null, 2));
  await ui.click(ui.tab("properties"));
  assert.ok(ui.text().value.includes("tasks.max=4"));
  assert.ok(ui.text().value.includes("topics=orders,refunds"));
  assert.ok(!ui.text().value.includes("# keep me"), "Properties is rebuilt after a JSON edit");
});

test("saving from the JSON tab sends the JSON edits", async () => {
  const ui = await open();
  await ui.click(ui.tab("json"));
  ui.type(JSON.stringify({ ...ui.json(), "tasks.max": "2" }, null, 2));
  await ui.click(ui.button("save-config"));
  assert.deepStrictEqual(ui.puts().map((request) => request.body.config), [{ ...initialConfig(), "tasks.max": "2" }]);
});

test("an invalid Properties draft is preserved and blocks Validate and Save", async () => {
  const ui = await open();
  const broken = ui.text().value + "\nnot a pair";
  ui.type(broken);
  await ui.click(ui.tab("json"));
  assert.match(ui.toast(), /^Properties: Line 8: expected key=value$/);
  assert.deepStrictEqual(ui.json(), initialConfig(), "JSON shows the last valid config");
  await ui.click(ui.button("save-config"));
  assert.match(ui.toast(), /^Properties: Line 8: expected key=value$/);
  await ui.click(ui.tab("properties"));
  assert.strictEqual(ui.text().value, broken);
  assert.strictEqual(ui.banner(), "Line 8: expected key=value");
  await ui.click(ui.button("validate-config"));
  await ui.click(ui.button("save-config"));
  assert.deepStrictEqual(ui.puts(), [], "nothing is sent while a draft is invalid");
});

test("an invalid JSON draft is preserved and blocks Save", async () => {
  const ui = await open();
  await ui.click(ui.tab("json"));
  ui.type('{"tasks.max": "2",');
  await ui.click(ui.button("save-config"));
  assert.match(ui.toast(), /^JSON: Config is not valid JSON/);
  assert.strictEqual(ui.text().value, '{"tasks.max": "2",');
  await ui.click(ui.tab("properties"));
  assert.ok(ui.text().value.includes("tasks.max=1"));
  await ui.click(ui.tab("json"));
  assert.strictEqual(ui.text().value, '{"tasks.max": "2",');
  ui.type("[]");
  await ui.click(ui.button("save-config"));
  assert.match(ui.toast(), /^JSON: Config must be a JSON object$/);
  assert.deepStrictEqual(ui.puts(), []);
});

test("unsaved edits survive actions, refresh and language switches, and can be discarded", async () => {
  const ui = await open();
  const edited = ui.text().value.replace("tasks.max=1", "tasks.max=9");
  ui.type(edited);
  await ui.click(ui.button("pause"));
  await tick();
  assert.strictEqual(ui.toast(), "Pause requested");
  assert.strictEqual(ui.text().value, edited, "kept after an action reloads the connector");
  await ui.click(ui.button("refresh-detail"));
  await tick();
  assert.strictEqual(ui.text().value, edited, "kept after a refresh");

  ui.page.kcv.setLanguage("ru");
  await tick();
  assert.strictEqual(ui.text().value, edited);
  assert.deepStrictEqual(ui.tabs().map((node) => node.textContent), ["Свойства", "JSON", "cURL"]);
  assert.strictEqual(findAll(ui.page.roots["detail-pane"], (node) => node.id === "config-dirty")[0].textContent, "Есть несохранённые изменения");
  assert.strictEqual(ui.button("discard-config").textContent, "Отменить изменения");

  ui.page.kcv.setLanguage("zh-CN");
  await tick();
  assert.strictEqual(ui.text().value, edited);
  assert.deepStrictEqual(ui.tabs().map((node) => node.textContent), ["属性", "JSON", "cURL"]);
  assert.strictEqual(findAll(ui.page.roots["detail-pane"], (node) => node.id === "config-dirty")[0].textContent, "有未保存的更改");
  assert.strictEqual(ui.button("discard-config").textContent, "放弃更改");
  assert.strictEqual(ui.button("save-config").textContent, "保存");

  await ui.click(ui.button("discard-config"));
  assert.ok(ui.text().value.includes("tasks.max=1"));
  assert.strictEqual(ui.dirty(), false);
  assert.deepStrictEqual(ui.puts(), []);
});

test("an invalid draft also survives a refresh", async () => {
  const ui = await open();
  ui.type("broken line");
  await ui.click(ui.button("refresh-detail"));
  await tick();
  assert.strictEqual(ui.text().value, "broken line");
  assert.strictEqual(ui.dirty(), true);
});

test("the selected tab is kept for the next connector view", async () => {
  const ui = await open();
  await ui.click(ui.tab("json"));
  await ui.click(ui.button("refresh-detail"));
  await tick();
  assert.deepStrictEqual(ui.activeTab(), ["json"]);
});

test("Validate uses the edited config and does not save it", async () => {
  const ui = await open();
  ui.type(ui.text().value.replace("tasks.max=1", "tasks.max=5"));
  await ui.click(ui.button("validate-config"));
  const validate = ui.backend.requests.filter((request) => request.url === "/api/clusters/c/plugins/validate");
  assert.strictEqual(validate.length, 1);
  assert.strictEqual(validate[0].body.config["tasks.max"], "5");
  assert.strictEqual(validate[0].body.config["connection.password"], MASK);
  assert.ok(!ui.puts().some((request) => request.url.endsWith("/config")));
  assert.strictEqual(ui.dirty(), true);
});

test("masked secrets are never replaced with another value by the editor", async () => {
  const ui = await open();
  assert.strictEqual(ui.text().value.match(/^connection\.password=.*$/m)[0], `connection.password=${MASK}`);
  await ui.click(ui.tab("json"));
  assert.strictEqual(ui.json()["connection.password"], MASK);
  ui.type(JSON.stringify({ ...ui.json(), topics: "orders,audit" }, null, 2));
  await ui.click(ui.tab("properties"));
  assert.ok(ui.text().value.includes(`connection.password=${MASK}`));
  await ui.click(ui.button("save-config"));
  assert.strictEqual(ui.puts()[0].body.config["connection.password"], MASK);
  const hint = findAll(ui.page.roots["detail-pane"], (node) => node.classList.contains("hint")).map((node) => node.textContent);
  assert.ok(hint.some((text) => text.startsWith("Asterisks are a secret mask")));
});

test("multi-line values are read-only in Properties and editable in JSON", async () => {
  const config = { "connector.class": "io.example.JdbcSource", query: "SELECT *\nFROM orders" };
  const ui = await open("admin", { config });
  assert.ok(ui.text().readOnly);
  assert.strictEqual(ui.banner(), "query: value contains a line break, edit it in JSON");
  await ui.click(ui.tab("json"));
  assert.ok(!ui.text().readOnly);
  ui.type(JSON.stringify({ ...config, "tasks.max": "1" }, null, 2));
  await ui.click(ui.button("save-config"));
  assert.deepStrictEqual(ui.puts()[0].body.config, { ...config, "tasks.max": "1" });
});

test("an empty config is rejected before it reaches the backend", async () => {
  const ui = await open();
  ui.type("# nothing here");
  await ui.click(ui.button("save-config"));
  assert.strictEqual(ui.toast(), "Config is empty");
  assert.deepStrictEqual(ui.puts(), []);
});

test("viewers and operators can read both tabs but cannot edit, validate or save", async () => {
  for (const role of ["viewer", "operator"]) {
    const ui = await open(role);
    assert.ok(ui.text().readOnly, role);
    assert.strictEqual(ui.button("save-config"), null);
    assert.strictEqual(ui.button("validate-config"), null);
    await ui.click(ui.tab("json"));
    assert.ok(ui.text().readOnly);
    assert.deepStrictEqual(ui.json(), initialConfig());
    assert.deepStrictEqual(ui.puts(), []);
  }
});

function runCurl(command, env = {}) {
  const result = spawnSync("/bin/bash", ["-c", `curl() { printf '%s\\0' "$@"; }\n${command}`], {
    env: { PATH: "/usr/bin:/bin", ...env },
    encoding: "utf8",
    timeout: 10000,
  });
  return { status: result.status, args: result.stdout ? result.stdout.split("\0").slice(0, -1) : [], stderr: result.stderr };
}

function curlRequestOf(command, env) {
  const run = runCurl(command, { CONNECT_URL: "http://worker.test:8083", ...env });
  assert.strictEqual(run.status, 0, run.stderr);
  const [, method, url, , header, , data] = run.args;
  assert.deepStrictEqual([run.args[0], run.args[3], run.args[5]], ["-X", "-H", "--data"]);
  assert.strictEqual(run.args.length, 7);
  return { method, url, header, body: JSON.parse(data) };
}

test("cURL is a read-only PUT of the current unsaved config", async () => {
  const ui = await open();
  ui.type(ui.text().value.replace("tasks.max=1", "tasks.max=7") + "\ntransforms.route.replacement=$1-'x'-`y`\\z");
  await ui.click(ui.tab("curl"));
  assert.deepStrictEqual(ui.activeTab(), ["curl"]);
  assert.strictEqual(ui.text(), null, "there is no editable text in the cURL tab");
  const command = ui.curl();
  assert.ok(command.startsWith('curl -X PUT "${CONNECT_URL:?}/connectors/orders/config" \\'));
  const request = curlRequestOf(command, { CONNECTION_PASSWORD: "s3cr3t" });
  assert.strictEqual(request.method, "PUT");
  assert.strictEqual(request.url, "http://worker.test:8083/connectors/orders/config");
  assert.strictEqual(request.header, "Content-Type: application/json");
  assert.deepStrictEqual(request.body, {
    ...initialConfig(),
    "tasks.max": "7",
    "header.value": "padded",
    "connection.password": "s3cr3t",
    "transforms.route.replacement": "$1-'x'-`y`\\z",
    name: "orders",
  });
  assert.strictEqual(ui.dirty(), true);
  await ui.click(ui.button("save-config"));
  const saved = ui.puts().find((put) => put.url === `${BASE}/config`).body.config;
  assert.deepStrictEqual(
    { ...request.body, "connection.password": MASK },
    { ...saved, name: "orders" },
    "the command describes exactly what Save sends, with the secret as a variable",
  );
  assert.ok(ui.hints().includes("Kafka Connect REST API request that replaces this connector's config with the current edits. Set CONNECT_URL to the worker REST URL before running. Credentials and authorization headers are not included."));
});

test("cURL never contains the cluster URL, credentials, authorization headers or secret masks", async () => {
  const ui = await open();
  await ui.click(ui.tab("curl"));
  const command = ui.curl();
  for (const forbidden of ["connect.example.com", "user:secret", "secret@", "https://", "localhost", "Authorization", "Bearer", "Cookie", " -u ", "--user", MASK]) {
    assert.ok(!command.includes(forbidden), `the command must not contain ${forbidden}`);
  }
  assert.deepStrictEqual(command.match(/ -H /g), [" -H "], "only the Content-Type header is sent");
  assert.ok(command.includes('"${CONNECTION_PASSWORD:?}"'));
  assert.ok(ui.hints().includes("Masked secrets are not included. Set CONNECTION_PASSWORD to the real values before running; the command stops if they are not set."));
});

test("cURL refuses to run until CONNECT_URL and masked secrets are set", async () => {
  const ui = await open();
  await ui.click(ui.tab("curl"));
  const command = ui.curl();
  const noSecret = runCurl(command, { CONNECT_URL: "http://worker.test:8083" });
  assert.notStrictEqual(noSecret.status, 0);
  assert.deepStrictEqual(noSecret.args, [], "curl is not called without the secret");
  assert.match(noSecret.stderr, /CONNECTION_PASSWORD/);
  const noUrl = runCurl(command, { CONNECTION_PASSWORD: "s3cr3t" });
  assert.notStrictEqual(noUrl.status, 0);
  assert.deepStrictEqual(noUrl.args, []);
  assert.match(noUrl.stderr, /CONNECT_URL/);
});

test("connector names and secret variable names are shell-safe", async () => {
  const ui = await open();
  const { command, variables } = ui.page.kcv.updateCurlCommand("orders sink's (v2)!", {
    "connector.class": "X",
    "connection.password": MASK,
    "connection-password": "******",
    "1st.secret": "**",
    "ssl.key.password": "not-a-mask",
  });
  assert.deepStrictEqual([...variables], ["CONNECTION_PASSWORD", "CONNECTION_PASSWORD_2", "SECRET_1ST_SECRET"]);
  const request = curlRequestOf(command, { CONNECTION_PASSWORD: "a", CONNECTION_PASSWORD_2: "b", SECRET_1ST_SECRET: "c" });
  assert.strictEqual(request.url, "http://worker.test:8083/connectors/orders%20sink%27s%20%28v2%29%21/config");
  assert.deepStrictEqual(request.body, {
    "connector.class": "X",
    "connection.password": "a",
    "connection-password": "b",
    "1st.secret": "c",
    "ssl.key.password": "not-a-mask",
    name: "orders sink's (v2)!",
  });
});

test("switching through cURL keeps Properties and JSON drafts unchanged", async () => {
  const ui = await open();
  const properties = ui.text().value + "\n# keep this comment\nbatch.size=100";
  ui.type(properties);
  await ui.click(ui.tab("curl"));
  assert.strictEqual(curlRequestOf(ui.curl(), { CONNECTION_PASSWORD: "x" }).body["batch.size"], "100");
  await ui.click(ui.tab("properties"));
  assert.strictEqual(ui.text().value, properties);

  await ui.click(ui.tab("json"));
  const json = ui.text().value.replace('"batch.size": "100"', '"batch.size": "200"');
  ui.type(json);
  await ui.click(ui.tab("curl"));
  assert.strictEqual(curlRequestOf(ui.curl(), { CONNECTION_PASSWORD: "x" }).body["batch.size"], "200");
  await ui.click(ui.tab("json"));
  assert.strictEqual(ui.text().value, json);
});

test("cURL uses the last valid config and says so when a draft is invalid", async () => {
  const ui = await open();
  ui.type(ui.text().value.replace("tasks.max=1", "tasks.max=2"));
  await ui.click(ui.tab("json"));
  ui.type("{ broken");
  await ui.click(ui.tab("curl"));
  assert.match(ui.toast(), /^JSON: Config is not valid JSON/);
  assert.strictEqual(ui.banner(), "JSON: has an error, so this command uses the last valid config");
  assert.strictEqual(curlRequestOf(ui.curl(), { CONNECTION_PASSWORD: "x" }).body["tasks.max"], "2");
  await ui.click(ui.button("save-config"));
  assert.deepStrictEqual(ui.puts(), [], "Save stays blocked while a draft is invalid");
  await ui.click(ui.tab("json"));
  assert.strictEqual(ui.text().value, "{ broken");
});

test("Copy copies the command; Save and Validate work from the cURL tab", async () => {
  const ui = await open();
  ui.type(ui.text().value.replace("tasks.max=1", "tasks.max=6"));
  await ui.click(ui.tab("curl"));
  await ui.click(ui.button("copy-curl"));
  assert.deepStrictEqual(ui.page.clipboard, [ui.curl()]);
  assert.strictEqual(ui.toast(), "Copied");
  await ui.click(ui.button("validate-config"));
  const validate = ui.backend.requests.filter((request) => request.url === "/api/clusters/c/plugins/validate");
  assert.strictEqual(validate[0].body.config["tasks.max"], "6");
  await ui.click(ui.button("save-config"));
  const saves = ui.puts().filter((request) => request.url === `${BASE}/config`).map((request) => request.body.config);
  assert.deepStrictEqual(saves, [{ ...initialConfig(), "tasks.max": "6", "header.value": "padded" }]);
  assert.strictEqual(saves[0]["connection.password"], MASK, "KCV saves keep the mask for the backend to restore");
});

test("viewers and operators can read and copy cURL but cannot save", async () => {
  for (const role of ["viewer", "operator"]) {
    const ui = await open(role);
    await ui.click(ui.tab("curl"));
    assert.ok(ui.curl().startsWith("curl -X PUT"), role);
    assert.strictEqual(ui.button("save-config"), null);
    assert.strictEqual(ui.button("validate-config"), null);
    await ui.click(ui.button("copy-curl"));
    assert.deepStrictEqual(ui.page.clipboard, [ui.curl()]);
  }
});

test("cURL hints are translated in RU and zh-CN; the command itself is not", async () => {
  const ui = await open("admin", { language: "ru" });
  await ui.click(ui.tab("curl"));
  const command = ui.curl();
  assert.ok(ui.hints().some((text) => text.startsWith("Запрос к REST API Kafka Connect")));
  assert.ok(ui.hints().includes("Скрытые секреты не подставляются. Перед запуском задайте CONNECTION_PASSWORD реальными значениями; без них команда не выполнится."));
  assert.strictEqual(ui.button("copy-curl").textContent, "Копировать");
  ui.page.kcv.setLanguage("zh-CN");
  await tick();
  assert.deepStrictEqual(ui.activeTab(), ["curl"]);
  assert.strictEqual(ui.curl(), command);
  assert.ok(ui.hints().some((text) => text.startsWith("用当前编辑内容替换此连接器配置")));
  assert.ok(ui.hints().includes("被掩码的密钥不会包含在内。运行前请将 CONNECTION_PASSWORD 设置为真实值；未设置时命令不会执行。"));
  assert.strictEqual(ui.button("copy-curl").textContent, "复制");
});

runTests(tests);
