// Regression tests for the create-connector editor tabs (Properties / JSON / cURL).
// Runs static/app.js against a minimal fake DOM and drives it through real clicks.
// Usage: node tests/create_editor.test.cjs [path/to/app.js]
const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const { spawnSync } = require("child_process");

const root = path.resolve(__dirname, "..");
const appPath = path.resolve(process.argv[2] || path.join(root, "static/app.js"));
const pageIds = [...fs.readFileSync(path.join(root, "static/index.html"), "utf8").matchAll(/\bid="([^"]+)"/g)].map((m) => m[1]);

class FakeElement {
  constructor(tag) {
    this.nodeType = 1;
    this.tagName = tag.toUpperCase();
    this.attrs = {};
    this.children = [];
    this.listeners = {};
    this.dataset = {};
    this.style = {};
    this.hidden = false;
    this.disabled = false;
    this.parent = null;
    this.classes = new Set();
    const classes = this.classes;
    this.classList = {
      add: (...names) => names.forEach((name) => classes.add(name)),
      remove: (...names) => names.forEach((name) => classes.delete(name)),
      toggle: (name, force) => ((force ?? !classes.has(name)) ? classes.add(name) : classes.delete(name)),
      contains: (name) => classes.has(name),
    };
  }
  set className(value) { this.classes.clear(); String(value).split(/\s+/).filter(Boolean).forEach((name) => this.classes.add(name)); }
  get className() { return [...this.classes].join(" "); }
  get id() { return this.attrs.id; }
  get isConnected() { let node = this; while (node.parent) node = node.parent; return node.isRoot === true; }
  setAttribute(key, value) { this.attrs[key] = String(value); }
  getAttribute(key) { return this.attrs[key] ?? null; }
  removeAttribute(key) { delete this.attrs[key]; }
  append(...nodes) { for (const node of nodes) { if (typeof node === "string") node = { nodeType: 3, textContent: node }; node.parent = this; this.children.push(node); } }
  replaceChildren(...nodes) { this.children = []; this._text = undefined; this.append(...nodes); }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((child) => child !== this); this.parent = null; }
  addEventListener(type, handler) { (this.listeners[type] ||= []).push(handler); }
  removeEventListener() {}
  dispatch(type) {
    const event = { type, target: this, preventDefault() {}, stopPropagation() {} };
    for (const handler of this.listeners[type] || []) handler(event);
  }
  get textContent() { return this._text ?? this.children.map((child) => child.textContent).join(""); }
  set textContent(value) { this.children = []; this._text = String(value); }
  get value() { return this._value ?? (this.tagName === "TEXTAREA" ? this.textContent : this.attrs.value ?? ""); }
  set value(value) { this._value = String(value); }
  get readOnly() { return "readonly" in this.attrs; }
  focus() {}
  scrollIntoView() {}
  closest() { return null; }
  querySelector() { return null; }
  querySelectorAll() { return []; }
}

function walk(node, visit) {
  visit(node);
  for (const child of node.children || []) if (child.nodeType === 1) walk(child, visit);
}

function findAll(start, predicate) {
  const found = [];
  walk(start, (node) => { if (predicate(node)) found.push(node); });
  return found;
}

const roots = Object.fromEntries(pageIds.map((id) => {
  const node = new FakeElement("div");
  node.setAttribute("id", id);
  node.isRoot = true;
  return [id, node];
}));

const requests = [];
const copied = [];
const plugins = [
  { class: "io.confluent.connect.jdbc.JdbcSinkConnector", type: "sink", version: "10.7.4" },
  { class: "io.debezium.connector.postgresql.PostgresConnector", type: "source", version: "2.5.0" },
];

async function fakeFetch(url, options = {}) {
  const method = options.method || "GET";
  requests.push({ url, method, body: options.body ? JSON.parse(options.body) : null });
  let payload = {};
  if (url === "/api/me") payload = { role: "admin" };
  else if (url === "/api/clusters") payload = { clusters: [{ id: "c", name: "c", url: "https://user:secret@connect.example.com:8083/" }] };
  else if (url.endsWith("/plugins")) payload = { plugins };
  else if (url.endsWith("/plugins/validate")) payload = { configs: [] };
  else if (url.endsWith("/connectors") && method === "GET") payload = { count: 0, connectors: [] };
  else if (url.endsWith("/connectors") && method === "POST") payload = { name: "created" };
  return { ok: true, status: 200, statusText: "OK", text: async () => JSON.stringify(payload) };
}

const context = {
  console, setTimeout, clearTimeout, URL, JSON, Promise,
  localStorage: { getItem: () => "en", setItem() {} },
  navigator: { language: "en", clipboard: { writeText: async (text) => { copied.push(text); } } },
  location: { hash: "#c" },
  window: { addEventListener() {} },
  fetch: fakeFetch,
  document: {
    documentElement: { dataset: {} },
    activeElement: null,
    addEventListener() {},
    createElement: (tag) => new FakeElement(tag),
    createTextNode: (text) => ({ nodeType: 3, textContent: String(text) }),
    getElementById(id) {
      for (const node of Object.values(roots)) {
        const [match] = findAll(node, (candidate) => candidate.id === id);
        if (match) return match;
      }
      return null;
    },
    querySelector: () => null,
    querySelectorAll: () => [],
  },
};
vm.createContext(context);
vm.runInContext(fs.readFileSync(appPath, "utf8") + "\n;globalThis.__kcv = { getModal: () => modal };", context, { filename: appPath });

const tick = () => new Promise((resolve) => setTimeout(resolve, 5));
const modalRoot = roots.modal;
const byId = (id) => context.document.getElementById(id);
const textarea = () => byId("create-text");
const activeTab = () => findAll(modalRoot, (node) => node.attrs.role === "tab" && node.classList.contains("is-active"))[0]?.textContent;
const errorBox = () => findAll(modalRoot, (node) => node.classList.contains("editor-error"))[0]?.textContent || "";
const toastText = () => roots.toast.textContent;

async function clickTab(label) {
  const tab = findAll(modalRoot, (node) => node.attrs.role === "tab" && node.textContent === label)[0];
  assert.ok(tab, `tab ${label} exists`);
  tab.dispatch("click");
  await tick();
  assert.strictEqual(activeTab(), label, `switched to ${label}`);
}

function type(text) {
  const area = textarea();
  assert.ok(area, "editor textarea is present");
  area.value = text;
}

function jsonTextConfig() {
  const text = textarea().value;
  assert.ok(!/^\s*[\w.]+=/m.test(text), "JSON tab never contains key=value text");
  return JSON.parse(text);
}

function runCurl(command, env = {}) {
  const result = spawnSync("/bin/bash", ["-c", `curl() { printf '%s\\0' "$@"; }\n${command}`], {
    env: { PATH: "/usr/bin:/bin", ...env },
    encoding: "utf8",
    timeout: 10000,
  });
  return { status: result.status, args: result.stdout ? result.stdout.split("\0").slice(0, -1) : [], stderr: result.stderr };
}

function curlRequestOf(command) {
  const run = runCurl(command, { CONNECT_URL: "http://worker.test:8083" });
  assert.strictEqual(run.status, 0, run.stderr);
  assert.deepStrictEqual([run.args.length, run.args[0], run.args[3], run.args[5]], [7, "-X", "-H", "--data"]);
  return { method: run.args[1], url: run.args[2], header: run.args[4], body: JSON.parse(run.args[6]) };
}

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

async function openEditor(pluginClass = plugins[0].class) {
  const open = byId("create-open");
  assert.ok(open, "admin sees the create button");
  open.dispatch("click");
  await tick();
  const option = findAll(modalRoot, (node) => node.classList.contains("plugin-option") && node.textContent.includes(pluginClass))[0];
  assert.ok(option, "plugin is listed in the picker");
  option.dispatch("click");
  await tick();
  assert.strictEqual(activeTab(), "Properties");
}

async function clickButton(id) {
  requests.length = 0;
  byId(id).dispatch("click");
  await tick();
}

async function submitForm() {
  requests.length = 0;
  byId("create-form").dispatch("submit");
  await tick();
}

const PROPS = [
  "# JDBC sink",
  "",
  "name=orders-sink",
  "connector.class=io.confluent.connect.jdbc.JdbcSinkConnector",
  "tasks.max=2",
  "connection.url=jdbc:postgresql://db:5432/app?ssl=true&sslmode=require",
  "transforms.route.regex=(.*)=(.*)",
].join("\n");
const PROPS_CONFIG = {
  name: "orders-sink",
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "tasks.max": "2",
  "connection.url": "jdbc:postgresql://db:5432/app?ssl=true&sslmode=require",
  "transforms.route.regex": "(.*)=(.*)",
};

test("Properties starts as key=value lines for the selected plugin", async () => {
  await openEditor();
  assert.strictEqual(textarea().value, "name=\nconnector.class=io.confluent.connect.jdbc.JdbcSinkConnector\ntasks.max=1");
});

test("Properties -> JSON writes formatted JSON, never key=value text", async () => {
  await openEditor();
  type(PROPS);
  await clickTab("JSON");
  assert.deepStrictEqual(jsonTextConfig(), PROPS_CONFIG);
  assert.strictEqual(textarea().value, JSON.stringify(PROPS_CONFIG, null, 2));
  assert.strictEqual(errorBox(), "");
});

test("JSON -> Properties serializes edits back to key=value lines", async () => {
  await openEditor();
  type(PROPS);
  await clickTab("JSON");
  type(JSON.stringify({ ...PROPS_CONFIG, "tasks.max": 3, "topics": "orders" }, null, 2));
  await clickTab("Properties");
  const text = textarea().value;
  assert.ok(!text.includes("{"), "Properties never contains JSON");
  assert.match(text, /^tasks\.max=3$/m);
  assert.match(text, /^topics=orders$/m);
  assert.match(text, /^transforms\.route\.regex=\(\.\*\)=\(\.\*\)$/m);
});

test("unchanged round trip keeps the user's Properties text and comments", async () => {
  await openEditor();
  type(PROPS);
  await clickTab("JSON");
  await clickTab("Properties");
  assert.strictEqual(textarea().value, PROPS);
});

test("cURL is generated from the internal config only", async () => {
  await openEditor();
  type(PROPS);
  await clickTab("cURL");
  assert.strictEqual(textarea(), null, "cURL tab has no editable textarea");
  const command = byId("create-curl").textContent;
  assert.ok(command.startsWith('curl -X POST "${CONNECT_URL:?}/connectors" \\'));
  const request = curlRequestOf(command);
  assert.strictEqual(request.method, "POST");
  assert.strictEqual(request.url, "http://worker.test:8083/connectors");
  assert.strictEqual(request.header, "Content-Type: application/json");
  assert.deepStrictEqual(request.body, { name: "orders-sink", config: PROPS_CONFIG });
  await clickTab("JSON");
  assert.deepStrictEqual(jsonTextConfig(), PROPS_CONFIG);
  await clickTab("cURL");
  await clickTab("Properties");
  assert.strictEqual(textarea().value, PROPS);
});

test("Create cURL never contains the cluster URL, credentials or authorization headers", async () => {
  await openEditor();
  type(PROPS);
  await clickTab("cURL");
  const command = byId("create-curl").textContent;
  for (const forbidden of ["connect.example.com", "user:secret", "secret@", "https://", "localhost", "Authorization", "Bearer", "Cookie", " -u ", "--user"]) {
    assert.ok(!command.includes(forbidden), `the command must not contain ${forbidden}`);
  }
  assert.deepStrictEqual(command.match(/ -H /g), [" -H "], "only the Content-Type header is sent");
  const hint = findAll(modalRoot, (node) => node.classList.contains("editor-hint"))[0]?.textContent;
  assert.strictEqual(hint, "Kafka Connect REST API request that creates this connector from the current config. Set CONNECT_URL to the worker REST URL before running. Credentials and authorization headers are not included.");
});

test("Create cURL refuses to run until CONNECT_URL is set and copies verbatim", async () => {
  await openEditor();
  type(PROPS.replace("tasks.max=2", "tasks.max=2\ntransforms.route.replacement=$1-'x'-`y`\\z"));
  await clickTab("cURL");
  const command = byId("create-curl").textContent;
  const noUrl = runCurl(command);
  assert.notStrictEqual(noUrl.status, 0);
  assert.deepStrictEqual(noUrl.args, [], "curl is not called without CONNECT_URL");
  assert.match(noUrl.stderr, /CONNECT_URL/);
  assert.strictEqual(curlRequestOf(command).body.config["transforms.route.replacement"], "$1-'x'-`y`\\z");
  copied.length = 0;
  findAll(modalRoot, (node) => node.tagName === "BUTTON" && node.textContent === "Copy")[0].dispatch("click");
  await tick();
  assert.deepStrictEqual(copied, [command]);
});

test("invalid JSON does not block switching and its draft is preserved", async () => {
  await openEditor();
  type(PROPS);
  await clickTab("JSON");
  type("{ \"name\": \"orders-sink\", oops");
  await clickTab("Properties");
  assert.match(toastText(), /^JSON: Config is not valid JSON/);
  assert.strictEqual(textarea().value, PROPS, "Properties shows the last valid config");
  await clickTab("cURL");
  assert.match(errorBox(), /^JSON: has an error/);
  assert.ok(byId("create-curl").textContent.includes("orders-sink"));
  await clickTab("JSON");
  assert.strictEqual(textarea().value, "{ \"name\": \"orders-sink\", oops", "invalid JSON draft is preserved");
  assert.match(errorBox(), /Config is not valid JSON/);
  await clickTab("Properties");
  await clickTab("JSON");
  assert.strictEqual(textarea().value, "{ \"name\": \"orders-sink\", oops");
});

test("invalid Properties does not block switching and its draft is preserved", async () => {
  await openEditor();
  type(PROPS);
  await clickTab("JSON");
  await clickTab("Properties");
  type(PROPS + "\nthis line has no equals sign");
  await clickTab("JSON");
  assert.match(toastText(), /^Properties: Line 8: expected key=value/);
  assert.deepStrictEqual(jsonTextConfig(), PROPS_CONFIG, "JSON shows the last valid config");
  await clickTab("Properties");
  assert.strictEqual(textarea().value, PROPS + "\nthis line has no equals sign");
  assert.match(errorBox(), /Line 8: expected key=value/);
});

test("a newer valid edit in another tab replaces a stale invalid draft", async () => {
  await openEditor();
  type(PROPS);
  await clickTab("JSON");
  type("not json");
  await clickTab("Properties");
  type(PROPS.replace("tasks.max=2", "tasks.max=5"));
  await clickTab("JSON");
  assert.strictEqual(jsonTextConfig()["tasks.max"], "5");
  assert.strictEqual(errorBox(), "");
});

test("Validate and Create use the config of the tab being edited", async () => {
  await openEditor();
  type(PROPS);
  await clickTab("JSON");
  type(JSON.stringify({ ...PROPS_CONFIG, topics: "orders" }));
  await clickButton("create-validate");
  const validate = requests.find((request) => request.url.endsWith("/plugins/validate"));
  assert.ok(validate, "validate request sent");
  assert.strictEqual(validate.method, "PUT");
  assert.deepStrictEqual(validate.body, { config: { ...PROPS_CONFIG, topics: "orders" } });
  await clickTab("Properties");
  type(textarea().value.replace("tasks.max=2", "tasks.max=4"));
  await submitForm();
  const create = requests.find((request) => request.url.endsWith("/connectors") && request.method === "POST");
  assert.ok(create, "create request sent");
  assert.deepStrictEqual(create.body, { name: "orders-sink", config: { ...PROPS_CONFIG, "tasks.max": "4", topics: "orders" } });
});

test("Validate and Create report invalid drafts normally and send nothing", async () => {
  await openEditor();
  type(PROPS);
  await clickTab("JSON");
  type("{bad");
  await clickButton("create-validate");
  assert.match(toastText(), /Config is not valid JSON/);
  assert.ok(!requests.some((request) => request.url.endsWith("/plugins/validate")));
  await clickTab("cURL");
  await submitForm();
  assert.match(toastText(), /Config is not valid JSON/);
  assert.ok(!requests.some((request) => request.method === "POST"));
  await clickTab("JSON");
  type(JSON.stringify(PROPS_CONFIG));
  await clickButton("create-validate");
  assert.ok(requests.some((request) => request.url.endsWith("/plugins/validate")));
});

test("multiline JSON values switch to a read-only Properties view without data loss", async () => {
  await openEditor();
  const config = { ...PROPS_CONFIG, query: "select *\nfrom orders" };
  await clickTab("JSON");
  type(JSON.stringify(config, null, 2));
  await clickTab("Properties");
  assert.ok(textarea().readOnly);
  assert.match(errorBox(), /query: value contains a line break/);
  assert.ok(!textarea().value.includes("{"));
  await clickTab("JSON");
  assert.deepStrictEqual(jsonTextConfig(), config);
  await clickTab("Properties");
  await submitForm();
  const create = requests.find((request) => request.method === "POST");
  assert.deepStrictEqual(create?.body, { name: "orders-sink", config });
});

(async () => {
  await tick();
  await tick();
  let failed = 0;
  for (const [name, fn] of tests) {
    try {
      await fn();
      console.log(`ok - ${name}`);
    } catch (error) {
      failed += 1;
      console.log(`not ok - ${name}\n  ${String(error.stack || error).split("\n").slice(0, 3).join("\n  ")}`);
    } finally {
      vm.runInContext("closeModal()", context);
      await tick();
    }
  }
  console.log(failed ? `${failed} of ${tests.length} failed` : `all ${tests.length} passed`);
  process.exit(failed ? 1 : 0);
})();
