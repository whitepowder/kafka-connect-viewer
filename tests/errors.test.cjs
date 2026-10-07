// Tests for backend error rendering: KCV error codes are translated (RU/EN),
// Kafka Connect messages are shown exactly as received.
// Usage: node tests/errors.test.cjs path/to/app.js '["code", ...]'
const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const root = path.resolve(__dirname, "..");
const appPath = path.resolve(process.argv[2] || path.join(root, "static/app.js"));
assert.ok(process.argv[3], "pass the backend error codes as a JSON list (tests/test_frontend.py does this)");
const backendCodes = JSON.parse(process.argv[3]);
const pageIds = [...fs.readFileSync(path.join(root, "static/index.html"), "utf8").matchAll(/\bid="([^"]+)"/g)].map((m) => m[1]);

class FakeElement {
  constructor(tag) {
    this.nodeType = 1;
    this.tagName = tag.toUpperCase();
    this.attrs = {};
    this.children = [];
    this.listeners = {};
    this.dataset = {};
    this.hidden = false;
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
  setAttribute(key, value) { this.attrs[key] = String(value); }
  getAttribute(key) { return this.attrs[key] ?? null; }
  append(...nodes) { for (let node of nodes) { if (typeof node === "string") node = { nodeType: 3, textContent: node }; node.parent = this; this.children.push(node); } }
  replaceChildren(...nodes) { this.children = []; this._text = undefined; this.append(...nodes); }
  addEventListener(type, handler) { (this.listeners[type] ||= []).push(handler); }
  get textContent() { return this._text ?? this.children.map((child) => child.textContent).join(""); }
  set textContent(value) { this.children = []; this._text = String(value); }
  focus() {}
  scrollIntoView() {}
  closest() { return null; }
  querySelector(selector) {
    assert.match(selector, /^#[\w-]+$/, "the fake DOM only supports #id selectors");
    return findAll(this, (node) => node !== this && node.id === selector.slice(1))[0] || null;
  }
}

function findAll(start, predicate, found = []) {
  if (predicate(start)) found.push(start);
  for (const child of start.children || []) if (child.nodeType === 1) findAll(child, predicate, found);
  return found;
}

const roots = Object.fromEntries(pageIds.map((id) => {
  const node = new FakeElement("div");
  node.attrs.id = id;
  return [id, node];
}));

const UPSTREAM_RU = "Коннектор alpha уже существует";
const responses = new Map([
  ["/api/clusters/c/connectors/detail", [200, {
    name: "detail", type: "sink", config: {}, connector: null, tasks: [],
    info_error: { code: "upstream_empty_connector_info", params: {} },
    status_error: { message: UPSTREAM_RU },
  }]],
  ["/api/clusters/c/kcv-error", [400, { code: "config_value_not_string", params: { key: "tasks.max" } }]],
  ["/api/clusters/c/upstream-error", [409, { message: "Connector alpha already exists" }]],
  ["/api/clusters/c/upstream-looks-like-code", [500, { message: "cluster_not_found" }]],
  ["/api/clusters/c/validation", [422, { detail: [{ loc: ["body", "name"], msg: "Field required" }] }]],
]);

async function fakeFetch(url) {
  let status = 200;
  let payload = {};
  if (url === "/api/me") payload = { role: "admin", auth_enabled: false };
  else if (url === "/api/clusters") payload = { clusters: [{ id: "c", name: "c", url: "http://connect.example.com:8083" }] };
  else if (url === "/api/clusters/c/connectors") payload = { count: 1, connectors: ["detail"] };
  else if (url === "/api/clusters/c") payload = { version: "3.7.0" };
  else if (responses.has(url)) [status, payload] = responses.get(url);
  return { ok: status < 400, status, statusText: "Status", text: async () => JSON.stringify(payload) };
}

const location = { hash: "#c" };
const context = {
  console, setTimeout, clearTimeout, URL, JSON, Promise, Map, Set,
  localStorage: { getItem: () => "en", setItem() {} },
  navigator: { language: "en" },
  location,
  window: { addEventListener() {} },
  fetch: fakeFetch,
  document: {
    documentElement: {},
    activeElement: null,
    addEventListener() {},
    createElement: (tag) => new FakeElement(tag),
    createElementNS: (_namespace, tag) => new FakeElement(tag),
    createTextNode: (text) => ({ nodeType: 3, textContent: String(text) }),
    createDocumentFragment: () => new FakeElement("fragment"),
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
vm.runInContext(
  fs.readFileSync(appPath, "utf8") +
    "\n;globalThis.__kcv = { translations, errorText, diagnosticMessage, api, applyHash: () => applyHash(), setLang: (code) => { language = code; } };",
  context,
  { filename: appPath },
);

const tick = () => new Promise((resolve) => setTimeout(resolve, 5));
const kcv = () => context.__kcv;
const placeholders = (text) => [...text.matchAll(/\{(\w+)\}/g)].map((match) => match[1]).sort();

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

test("every backend error code has RU and EN text with the same placeholders", async () => {
  const { translations } = kcv();
  assert.ok(backendCodes.length > 20, "received the backend code list");
  for (const code of backendCodes) {
    const ru = translations.ru[`e_${code}`];
    const en = translations.en[`e_${code}`];
    assert.ok(ru, `ru translation for ${code}`);
    assert.ok(en, `en translation for ${code}`);
    assert.deepStrictEqual(placeholders(ru), placeholders(en), `placeholders of ${code}`);
  }
  for (const lang of ["ru", "en"]) {
    const extra = Object.keys(translations[lang]).filter((key) => key.startsWith("e_") && !backendCodes.includes(key.slice(2)));
    assert.deepStrictEqual(extra, [], `${lang} has no translations for codes the backend does not send`);
  }
});

test("KCV error codes are translated with their params in both languages", async () => {
  const body = { code: "secret_masked", params: { key: "connection.password" } };
  kcv().setLang("en");
  assert.strictEqual(kcv().errorText(body), "Secret “connection.password” is masked by the worker; enter a new value before saving");
  assert.strictEqual(kcv().errorText({ code: "upstream_status", params: { status: 503 } }), "Kafka Connect responded with 503");
  kcv().setLang("ru");
  assert.strictEqual(kcv().errorText(body), "Секрет «connection.password» скрыт воркером; введите новое значение перед сохранением");
  assert.strictEqual(kcv().errorText({ code: "role_required", params: { role: "admin" } }), "Нужна роль admin");
  kcv().setLang("en");
});

test("Kafka Connect messages are never translated or replaced", async () => {
  for (const lang of ["en", "ru"]) {
    kcv().setLang(lang);
    assert.strictEqual(kcv().errorText({ message: UPSTREAM_RU }), UPSTREAM_RU);
    assert.strictEqual(kcv().errorText({ message: "Connector alpha already exists" }), "Connector alpha already exists");
    assert.strictEqual(kcv().errorText({ message: "cluster_not_found" }), "cluster_not_found", "upstream text that looks like a code stays as is");
    assert.strictEqual(kcv().errorText("<html>Bad Gateway</html>"), "<html>Bad Gateway</html>");
  }
  kcv().setLang("en");
});

test("an unknown code is shown as the code, not hidden", async () => {
  assert.strictEqual(kcv().errorText({ code: "brand_new_code", params: {} }), "brand_new_code");
});

test("api() errors carry translated KCV text and unchanged upstream text", async () => {
  await assert.rejects(kcv().api("/api/clusters/c/kcv-error"), (error) => {
    assert.strictEqual(error.message, "The value of “tasks.max” must be a string");
    assert.strictEqual(error.status, 400);
    assert.strictEqual(error.code, "config_value_not_string");
    return true;
  });
  kcv().setLang("ru");
  await assert.rejects(kcv().api("/api/clusters/c/kcv-error"), /Значение «tasks\.max» должно быть строкой/);
  for (const [url, message, status] of [
    ["/api/clusters/c/upstream-error", "Connector alpha already exists", 409],
    ["/api/clusters/c/upstream-looks-like-code", "cluster_not_found", 500],
  ]) {
    await assert.rejects(kcv().api(url), (error) => {
      assert.strictEqual(error.message, message);
      assert.strictEqual(error.status, status);
      assert.strictEqual(error.code, null);
      return true;
    });
  }
  await assert.rejects(kcv().api("/api/clusters/c/validation"), /Проверьте поля формы/);
  kcv().setLang("en");
});

test("connector detail banners translate KCV errors and keep upstream ones", async () => {
  location.hash = "#c/detail";
  await kcv().applyHash();
  await tick();
  const banners = findAll(roots["detail-pane"], (node) => node.classList.contains("banner")).map((node) => node.textContent);
  assert.deepStrictEqual(banners, [UPSTREAM_RU, "Kafka Connect returned an empty connector description"]);
});

test("graph diagnostics render config errors from codes and upstream messages", async () => {
  const base = { severity: "warning", code: "config_unreadable", topic: null, group: null, members: [], nodes: [], edges: [] };
  assert.strictEqual(
    kcv().diagnosticMessage({ ...base, connectors: ["a"], error: { code: "upstream_timeout", params: {} } }),
    "The config of a could not be read: Kafka Connect did not respond in time",
  );
  assert.strictEqual(
    kcv().diagnosticMessage({ ...base, connectors: ["b"], error: { message: UPSTREAM_RU } }),
    `The config of b could not be read: ${UPSTREAM_RU}`,
  );
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
      console.log(`not ok - ${name}\n  ${String(error.stack || error).split("\n").slice(0, 4).join("\n  ")}`);
    }
  }
  console.log(failed ? `${failed} of ${tests.length} failed` : `all ${tests.length} passed`);
  process.exit(failed ? 1 : 0);
})();
