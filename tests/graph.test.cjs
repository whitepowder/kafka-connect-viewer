// Tests for the 3-column Graph view: rendering, filters, focus, diagnostics panel and refresh.
// Runs static/app.js against a minimal fake DOM (including SVG elements).
// Usage: node tests/graph.test.cjs path/to/app.js path/to/graph.json
const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const root = path.resolve(__dirname, "..");
const appPath = path.resolve(process.argv[2] || path.join(root, "static/app.js"));
assert.ok(process.argv[3], "pass the graph fixture JSON produced by tests/test_frontend.py");
const graph = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const pageIds = [...fs.readFileSync(path.join(root, "static/index.html"), "utf8").matchAll(/\bid="([^"]+)"/g)].map((m) => m[1]);

class FakeElement {
  constructor(tag, namespace = null) {
    this.nodeType = 1;
    this.namespace = namespace;
    this.tagName = namespace ? tag : tag.toUpperCase();
    this.attrs = {};
    this.children = [];
    this.listeners = {};
    this.dataset = {};
    this.style = {};
    this.hidden = false;
    this.disabled = false;
    this.checked = false;
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
  setAttribute(key, value) {
    if (key === "class") throw new Error("class must be set through classList so SVG nodes work");
    this.attrs[key] = String(value);
  }
  getAttribute(key) { return this.attrs[key] ?? null; }
  removeAttribute(key) { delete this.attrs[key]; }
  append(...nodes) { for (let node of nodes) { if (typeof node === "string") node = { nodeType: 3, textContent: node }; node.parent = this; this.children.push(node); } }
  replaceChildren(...nodes) { this.children = []; this._text = undefined; this.append(...nodes); }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((child) => child !== this); this.parent = null; }
  addEventListener(type, handler) { (this.listeners[type] ||= []).push(handler); }
  removeEventListener() {}
  dispatch(type, extra = {}) {
    const event = { type, target: this, key: extra.key, preventDefault() {}, stopPropagation() {} };
    for (const handler of this.listeners[type] || []) handler(event);
  }
  get textContent() { return this._text ?? this.children.map((child) => child.textContent).join(""); }
  set textContent(value) { this.children = []; this._text = String(value); }
  get value() { return this._value ?? this.attrs.value ?? ""; }
  set value(value) { this._value = String(value); }
  focus() {}
  scrollIntoView() {}
  closest() { return null; }
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
  node.attrs.id = id;
  return [id, node];
}));

const requests = [];
let graphStatus = 200;

async function fakeFetch(url, options = {}) {
  const method = options.method || "GET";
  requests.push({ url, method });
  let status = 200;
  let payload = {};
  if (url === "/api/me") payload = { role: "viewer", auth_enabled: true, name: "viewer" };
  else if (url === "/api/clusters") payload = { clusters: [{ id: "c", name: "c", url: "http://connect.example.com:8083" }] };
  else if (url.startsWith("/api/clusters/c/graph")) {
    status = graphStatus;
    payload = status === 200 ? graph : { code: "graph_refresh_limited", params: { seconds: 10 } };
  } else if (url === "/api/clusters/c/connectors") payload = { count: 1, connectors: ["files"] };
  else if (url === "/api/clusters/c") payload = { version: "3.7.0" };
  return { ok: status < 400, status, statusText: status === 200 ? "OK" : "Too Many Requests", text: async () => JSON.stringify(payload) };
}

const location = { hash: "#c?view=graph" };
const context = {
  console, setTimeout, clearTimeout, URL, JSON, Promise, Map, Set,
  localStorage: { getItem: () => "en", setItem() {} },
  navigator: { language: "en" },
  location,
  window: { addEventListener() {} },
  fetch: fakeFetch,
  document: {
    documentElement: { dataset: {} },
    activeElement: null,
    addEventListener() {},
    createElement: (tag) => new FakeElement(tag),
    createElementNS: (namespace, tag) => {
      assert.strictEqual(namespace, "http://www.w3.org/2000/svg");
      return new FakeElement(tag, namespace);
    },
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
vm.runInContext(fs.readFileSync(appPath, "utf8") + "\n;globalThis.__kcv = { graphView, layoutGraph, applyHash: () => applyHash() };", context, { filename: appPath });

const tick = () => new Promise((resolve) => setTimeout(resolve, 5));
const kcv = () => context.__kcv;
const byId = (id) => context.document.getElementById(id);
const svgRoot = () => findAll(roots["list-pane"], (node) => node.tagName === "svg")[0];
const nodeEls = () => findAll(roots["list-pane"], (node) => node.attrs["data-node"]);
const nodeIds = () => nodeEls().map((node) => node.attrs["data-node"]).sort();
const nodeEl = (id) => nodeEls().find((node) => node.attrs["data-node"] === id);
const edgeEl = (id) => findAll(roots["list-pane"], (node) => node.attrs["data-edge"] === id)[0];
const visibleText = (node) => (node.nodeType === 3 ? node.textContent : node.tagName === "title" ? "" : node._text ?? node.children.map(visibleText).join(""));
const textOf = (node, cls) => findAll(node, (child) => child.classList.contains(cls)).map(visibleText);
const tooltipOf = (node, cls) => findAll(node, (child) => child.classList.contains(cls))[0].children.find((child) => child.tagName === "title")?.textContent;
const side = () => roots["detail-pane"];
const diagItems = () => findAll(side(), (node) => node.classList.contains("diag-item"));
const activeTab = () => findAll(side(), (node) => node.attrs.role === "tab" && node.classList.contains("is-active"))[0]?.attrs["data-tab"];
const column = (id) => Number(/translate\((\d+)/.exec(nodeEl(id).attrs.transform)[1]);
function assertNoClusterUrl() {
  for (const [name, node] of [["sidebar", roots.sidebar], ["connectors or graph", roots["list-pane"]], ["details", roots["detail-pane"]]]) {
    assert.ok(!node.textContent.includes("connect.example.com"), `${name} hides the cluster URL`);
  }
  const names = findAll(roots.sidebar, (node) => node.classList.contains("cluster-name")).map((node) => node.textContent);
  assert.deepStrictEqual(names, ["c"]);
}

async function navigate(hash) {
  location.hash = hash;
  await kcv().applyHash();
  await tick();
}

async function reset() {
  Object.assign(kcv().graphView, { query: "", problemsOnly: false, focus: null, tab: "diagnostics", showOk: false });
  graphStatus = 200;
  await navigate("#c");
  await navigate("#c?view=graph");
  requests.length = 0;
}

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

test("graph route fetches only the graph endpoint and renders three columns", async () => {
  await navigate("#c");
  requests.length = 0;
  await navigate("#c?view=graph");
  assert.deepStrictEqual(requests.map((request) => request.url), ["/api/clusters/c/graph"]);
  assert.ok(roots.app.classList.contains("is-graph"));
  assert.deepStrictEqual(textOf(svgRoot(), "graph-col-title"), ["Sources", "Kafka topics", "Sinks"]);
  assert.strictEqual(nodeEls().length, graph.nodes.length);
  assert.strictEqual(column("connector:files"), 0);
  assert.strictEqual(column("connector:orders-cdc"), 0);
  assert.strictEqual(column("topic:orders"), 300);
  assert.strictEqual(column("pattern:prefix:app."), 300);
  assert.strictEqual(column("connector:orders-s3"), 600);
  assert.strictEqual(findAll(roots["list-pane"], (node) => node.attrs["data-edge"]).length, graph.edges.length);
  assert.strictEqual(byId("cluster-meta").textContent, "c · Connect 3.7.0");
  assertNoClusterUrl();
});

test("nodes show group origin, bootstrap override presence, DLQ and patterns", async () => {
  await reset();
  assert.deepStrictEqual(textOf(nodeEl("connector:orders-s3"), "gnode-sub"), ["group: orders-consumers"]);
  assert.deepStrictEqual(textOf(nodeEl("connector:users-sink"), "gnode-sub"), ["group: default, expected"]);
  assert.strictEqual(tooltipOf(nodeEl("connector:users-sink"), "gnode-sub"), "connect-users-sink");
  assert.deepStrictEqual(textOf(nodeEl("connector:mirror-sink"), "gnode-sub"), ["group: undetermined"]);
  assert.ok(nodeEl("connector:mirror-sink").children.some((child) => child.classList?.contains("gnode-icon")));
  assert.ok(!nodeEl("connector:orders-s3").children.some((child) => child.classList?.contains("gnode-icon")));
  assert.ok(nodeEl("topic:orders-dlq").classList.contains("is-dlq"));
  assert.deepStrictEqual(textOf(nodeEl("topic:orders-dlq"), "gnode-badge"), ["DLQ"]);
  assert.ok(edgeEl("dlq:connector:orders-s3->topic:orders-dlq").classList.contains("is-dlq"));
  assert.ok(nodeEl("pattern:prefix:app.").classList.contains("is-prefix"));
  assert.deepStrictEqual(textOf(nodeEl("pattern:prefix:app."), "gnode-name"), ["app.*"]);
  assert.ok(edgeEl("reads:topic:orders->connector:orders-jdbc").classList.contains("is-pattern"));
  assert.ok(edgeEl("reads:topic:orders->connector:orders-s3").classList.contains("is-declared"));
  for (const id of ["topic:orders", "connector:orders-s3", "connector:orders-jdbc"]) assert.ok(nodeEl(id).classList.contains("has-error"), id);
  assert.ok(nodeEl("connector:mirror-sink").classList.contains("has-warning"));
  assert.ok(!nodeEl("connector:files").classList.contains("has-error"));
});

test("long names are truncated and keep the full name in a tooltip", async () => {
  await reset();
  const node = nodeEl("connector:audit-sink-with-a-very-long-connector-name-v2");
  const [name] = textOf(node, "gnode-name");
  assert.ok(name.endsWith("…") && name.length === 27, name);
  const title = node.children.find((child) => child.tagName === "title");
  assert.ok(title.textContent.startsWith("audit-sink-with-a-very-long-connector-name-v2\n"));
});

test("counter and partial banner reflect diagnostics", async () => {
  await reset();
  assert.strictEqual(byId("graph-counter").textContent, "1 errors · 2 warnings");
  assert.ok(byId("graph-counter").classList.contains("has-errors"));
  assert.strictEqual(byId("graph-status").textContent, "The graph is partial: 1 connector configs could not be read.");
});

test("search shows matching nodes and their neighbours", async () => {
  await reset();
  const search = byId("search");
  search.value = "mirror";
  search.dispatch("input");
  assert.deepStrictEqual(nodeIds(), ["connector:mirror-sink", "topic:app.users"]);
  search.value = "zzz";
  search.dispatch("input");
  assert.strictEqual(nodeEls().length, 0);
  assert.strictEqual(byId("graph-canvas").textContent, "Nothing matches.");
  search.value = "";
  search.dispatch("input");
  assert.strictEqual(nodeEls().length, graph.nodes.length);
});

test("only problems keeps nodes of errors and warnings", async () => {
  await reset();
  const box = byId("graph-problems");
  box.checked = true;
  box.dispatch("change");
  assert.deepStrictEqual(nodeIds(), [
    "connector:mirror-sink", "connector:orders-jdbc", "connector:orders-s3", "connector:users-sink", "topic:app.users", "topic:orders",
  ]);
  const search = byId("search");
  search.value = "jdbc";
  search.dispatch("input");
  assert.deepStrictEqual(nodeIds(), ["connector:orders-jdbc", "topic:orders"]);
  box.checked = false;
  box.dispatch("change");
  search.value = "";
  search.dispatch("input");
});

test("clicking a node focuses its upstream and downstream and opens details", async () => {
  await reset();
  nodeEl("connector:files").dispatch("click");
  assert.ok(svgRoot().classList.contains("has-focus"));
  const active = nodeEls().filter((node) => node.classList.contains("is-active")).map((node) => node.attrs["data-node"]).sort();
  assert.deepStrictEqual(active, [
    "connector:audit-sink-with-a-very-long-connector-name-v2", "connector:files", "connector:orders-jdbc",
    "connector:orders-s3", "topic:orders", "topic:orders-dlq",
  ]);
  assert.ok(nodeEl("connector:files").classList.contains("is-focused"));
  assert.ok(!nodeEl("connector:users-sink").classList.contains("is-active"));
  assert.strictEqual(activeTab(), "details");
  const link = findAll(side(), (node) => node.tagName === "A")[0];
  assert.strictEqual(link.textContent, "Open connector");
  assert.strictEqual(link.attrs.href, "#c/files");

  nodeEl("connector:mirror-sink").dispatch("click");
  const facts = findAll(side(), (node) => node.tagName === "DD").map((node) => node.textContent);
  assert.ok(facts.includes("set, value hidden"));
  assert.ok(facts.includes("undetermined"));
  assert.ok(!JSON.stringify(facts).includes("internal"));
  assert.strictEqual(diagItems().length, 1, "related diagnostics are listed");

  nodeEl("connector:mirror-sink").dispatch("click");
  assert.ok(!svgRoot().classList.contains("has-focus"), "second click clears focus");
});

test("diagnostics panel groups by severity, collapses OK and focuses a diagnostic", async () => {
  await reset();
  assert.strictEqual(activeTab(), "diagnostics");
  const items = diagItems();
  assert.deepStrictEqual(items.map((item) => item.classes.has("is-error") ? "error" : item.classes.has("is-warning") ? "warning" : "ok"), ["error", "warning", "warning"]);
  assert.strictEqual(items[0].children[1].textContent, "orders-jdbc, orders-s3 read orders with the same group orders-consumers: partitions are split between them, so each gets only part of the data.");
  assert.ok(items[1].textContent.includes("broken") && items[1].textContent.includes("Kafka Connect returned 500"));
  assert.ok(items[2].textContent.includes("The group of mirror-sink is undetermined"));
  byId("graph-ok-toggle").dispatch("click");
  const ok = diagItems().filter((item) => item.classes.has("is-ok"));
  assert.strictEqual(ok.length, 1);
  assert.ok(ok[0].textContent.includes("default groups are expected, not guaranteed"));

  diagItems()[0].dispatch("click");
  const active = nodeEls().filter((node) => node.classList.contains("is-active")).map((node) => node.attrs["data-node"]).sort();
  assert.deepStrictEqual(active, ["connector:orders-jdbc", "connector:orders-s3", "topic:orders"]);
  assert.ok(edgeEl("reads:topic:orders->connector:orders-s3").classList.contains("is-active"));
  assert.ok(!edgeEl("reads:topic:orders->connector:audit-sink-with-a-very-long-connector-name-v2").classList.contains("is-active"));
  assert.ok(diagItems()[0].classes.has("is-selected"));
  assert.strictEqual(activeTab(), "diagnostics");

  diagItems()[1].dispatch("click");
  assert.ok(svgRoot().classList.contains("has-focus"), "a diagnostic about an unreadable connector does not break focus");
});

test("refresh requests refresh=true and reports the rate limit", async () => {
  await reset();
  byId("graph-refresh").dispatch("click");
  await tick();
  assert.deepStrictEqual(requests.map((request) => request.url), ["/api/clusters/c/graph?refresh=true"]);
  graphStatus = 429;
  byId("graph-refresh").dispatch("click");
  await tick();
  assert.strictEqual(roots.toast.textContent, "The graph can be refreshed at most once every 10 s");
  assert.strictEqual(nodeEls().length, graph.nodes.length, "the previous graph stays on screen");
});

test("view switch returns to the connector list", async () => {
  await reset();
  const listButton = findAll(roots["list-pane"], (node) => node.attrs["data-view"] === "list")[0];
  listButton.dispatch("click");
  assert.strictEqual(location.hash, "#c");
  await kcv().applyHash();
  await tick();
  assert.ok(!roots.app.classList.contains("is-graph"));
  const rows = findAll(roots["list-pane"], (node) => node.classList.contains("connector-row"));
  assert.deepStrictEqual(rows.map((row) => row.dataset.name), ["files"], "the already loaded connector list is shown again");
  assert.strictEqual(byId("cluster-meta").textContent, "c · Connect 3.7.0");
  assertNoClusterUrl();
  assert.ok(requests.every((request) => !request.url.includes("expand")));
  await navigate("#c?view=graph");
});

test("layout orders columns by barycenter", async () => {
  const nodes = [
    { id: "connector:a", kind: "connector", type: "source", name: "a" },
    { id: "connector:b", kind: "connector", type: "source", name: "b" },
    { id: "topic:t1", kind: "topic", name: "t1" },
    { id: "topic:t2", kind: "topic", name: "t2" },
    { id: "connector:s", kind: "connector", type: "sink", name: "s" },
  ];
  const edges = [
    { id: "1", from: "connector:a", to: "topic:t2" },
    { id: "2", from: "connector:b", to: "topic:t1" },
    { id: "3", from: "topic:t1", to: "connector:s" },
  ];
  const layout = kcv().layoutGraph(nodes, edges);
  assert.deepStrictEqual(JSON.parse(JSON.stringify(layout.columns)), [["connector:a", "connector:b"], ["topic:t2", "topic:t1"], ["connector:s"]]);
  assert.ok(layout.positions.get("topic:t2").y < layout.positions.get("topic:t1").y);
  assert.strictEqual(layout.positions.get("connector:s").x, 600);
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
    }
  }
  console.log(failed ? `${failed} of ${tests.length} failed` : `all ${tests.length} passed`);
  process.exit(failed ? 1 : 0);
})();
