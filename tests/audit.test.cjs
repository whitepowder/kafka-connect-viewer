// Tests for the admin-only Audit log view: filters, pagination, navigation, translations and themes.
const assert = require("assert");
const fs = require("fs");
const path = require("path");
const { findAll, loadApp, runTests, tick } = require("./fake_dom.cjs");

const appPath = path.resolve(process.argv[2] || path.join(__dirname, "..", "static/app.js"));
const css = fs.readFileSync(path.join(path.dirname(appPath), "styles.css"), "utf8");
const EVENTS = [
  {
    id: "e1", ts: "2026-10-10T12:02:00.000Z", action: "DELETE", result: "denied", status: 403,
    actor: { name: "bob", role: "viewer", sub: "2" }, cluster: { id: "lab", name: "Lab" },
    connector: "orders-sink", task_id: null, request_id: "r1",
  },
  {
    id: "e2", ts: "2026-10-10T12:01:00.000Z", action: "CREATE", result: "success", status: 201,
    actor: { name: "anonymous", role: "admin", sub: null }, cluster: { id: "lab", name: "Lab" },
    connector: "orders-sink", task_id: null, request_id: "r2",
  },
];
const CLUSTERS = [{ id: "lab", name: "Lab" }, { id: "prod", name: "Prod" }];

function backend(role, options = {}) {
  const requests = [];
  const methods = [];
  const fetch = async (url, init = {}) => {
    const method = (init.method || "GET").toUpperCase();
    requests.push(url);
    methods.push(method);
    let payload = {};
    let status = 200;
    let header = null;
    let body = JSON.stringify;
    if (url === "/api/me") payload = { role, auth_enabled: role !== "admin" };
    else if (method === "GET" && url === "/api/clusters") payload = { clusters: CLUSTERS };
    else if (method === "GET" && (url === "/api/clusters/lab/connectors" || url === "/api/clusters/prod/connectors")) payload = { count: 1, connectors: ["orders-sink"] };
    else if (method === "GET" && (url === "/api/clusters/lab" || url === "/api/clusters/prod")) payload = { version: "3.7.0" };
    else if (method === "GET" && url.startsWith("/api/clusters/") && url.includes("/graph")) {
      payload = { nodes: [], edges: [], diagnostics: [], stats: { errors: 0, warnings: 0 }, generated_at: "2026-10-10T00:00:00Z" };
    }
    else if (method === "GET" && url.startsWith("/api/audit")) {
      if (role !== "admin") {
        status = 403;
        payload = { code: "role_required", params: { role: "admin" } };
      } else if (options.auditStatus) {
        status = options.auditStatus;
        payload = options.auditPayload || { code: "audit_write_failed", params: {} };
      } else if (options.empty) payload = { events: [], next_cursor: null };
      else if (url.includes("cursor=")) payload = { events: [EVENTS[1]], next_cursor: null };
      else payload = { events: [EVENTS[0]], next_cursor: "next" };
    } else if (method !== "GET") {
      status = options.writeStatus || 204;
      header = options.auditHeader || null;
      payload = options.writeBody == null ? null : options.writeBody;
      if (status === 204) body = () => "";
    }
    return {
      ok: status < 400,
      status,
      statusText: "Status",
      headers: { get: (name) => (String(name).toLowerCase() === "x-kcv-audit" ? header : null) },
      text: async () => (payload == null ? "" : body(payload)),
    };
  };
  return { requests, methods, fetch };
}

async function open(options = {}) {
  const server = backend(options.role || "admin", options);
  const page = loadApp(appPath, {
    storedLanguage: options.language || "en",
    storedTheme: options.theme,
    prefersDark: options.prefersDark,
    hash: options.hash || "#audit",
    fetch: server.fetch,
    expose: "state, parseHash, auditView, targetClusterId, toast, graphView",
  });
  await waitReady(page);
  return { page, server };
}

async function waitReady(page) {
  for (let i = 0; i < 40; i++) {
    const view = page.kcv.state.view;
    if (view === "graph" && page.kcv.graphView.data && !page.kcv.graphView.loading) return;
    if (view === "audit" && (page.kcv.auditView.loaded || page.kcv.auditView.error) && !page.kcv.auditView.loading) return;
    if (view === "list" && page.kcv.state.namesCluster && !page.kcv.state.loadingList) return;
    await tick();
  }
}

async function settle() {
  await tick();
  await tick();
}

async function go(page, hash) {
  page.location.hash = hash;
  await page.kcv.applyHash();
  await waitReady(page);
}

async function clickTab(page, view) {
  tabs(page).find((node) => node.getAttribute("data-view") === view).dispatch("click");
  await page.kcv.applyHash();
  await waitReady(page);
}

const everywhere = (page, predicate) => Object.values(page.roots).flatMap((node) => findAll(node, predicate));
const textOf = (page, predicate) => everywhere(page, predicate).map((node) => node.textContent);
const byId = (page, id) => everywhere(page, (node) => node.id === id)[0];
const tabs = (page) => everywhere(page, (node) => node.classList.contains("view-option"));
const tabLabels = (page) => tabs(page).map((node) => node.textContent);
const activeTab = (page) => tabs(page).filter((node) => node.classList.contains("is-active")).map((node) => node.getAttribute("data-view"));

function chromeOrder(page) {
  const pane = page.roots["list-pane"];
  return {
    pane,
    head: pane.children.findIndex((node) => node.classList && node.classList.contains("list-head")),
    tabs: pane.children.findIndex((node) => node.classList && node.classList.contains("view-switch")),
    actions: pane.children.findIndex((node) => node.classList && node.classList.contains("head-actions")),
  };
}

function assertStableChrome(page, view) {
  const { pane, head, tabs: tabIndex, actions } = chromeOrder(page);
  assert.ok(head >= 0, "page header is present");
  assert.strictEqual(tabIndex, head + 1, "tabs sit immediately below the title and cluster name");
  assert.strictEqual(actions, tabIndex + 1, "page actions sit below the tabs");
  const header = pane.children[head];
  assert.deepStrictEqual(findAll(header, (node) => node.classList.contains("view-switch") || node.classList.contains("head-actions")), []);
  const block = header.children[0];
  assert.strictEqual(block.children[0].tagName, "H1");
  assert.ok(block.children[1].classList.contains("meta"));
  assert.deepStrictEqual(tabLabels(page), ["Connectors", "Graph", "Audit log"]);
  assert.deepStrictEqual(activeTab(page), [view]);
}

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

test("admins open the audit view from #audit and see localized rows", async () => {
  const { page, server } = await open();
  assert.strictEqual(page.kcv.state.view, "audit");
  assert.ok(server.requests.some((url) => url === "/api/audit"));
  assert.deepStrictEqual(textOf(page, (node) => node.tagName === "H1"), ["Audit log"]);
  assert.ok(everywhere(page, (node) => node.classList.contains("audit-result")).some((node) => node.classList.contains("is-denied")));
  assert.ok(textOf(page, (node) => node.classList.contains("audit-row")).join(" ").includes("Delete"));
  assert.ok(textOf(page, (node) => node.classList.contains("audit-row")).join(" ").includes("Denied"));
  assert.ok(!textOf(page, () => true).join(" ").includes("connect.example"));
  assertStableChrome(page, "audit");
});

test("Apply sends server-side filters; Load more uses the cursor", async () => {
  const { page, server } = await open();
  const action = everywhere(page, (node) => node.id === "audit-action")[0];
  action.value = "CREATE";
  everywhere(page, (node) => node.id === "audit-connector")[0].value = "orders-sink";
  everywhere(page, (node) => node.id === "audit-filters")[0].dispatch("submit");
  await tick();
  assert.ok(server.requests.some((url) => url.includes("action=CREATE") && url.includes("connector=orders-sink")));
  everywhere(page, (node) => node.id === "audit-load-more")[0].dispatch("click");
  await tick();
  assert.ok(server.requests.some((url) => url.includes("cursor=next")));
  assert.ok(textOf(page, (node) => node.classList.contains("audit-row")).join(" ").includes("Create"));
});

test("Reset clears filters and pagination and returns to the global audit URL", async () => {
  const { page, server } = await open({ hash: "#lab?view=audit" });
  assert.strictEqual(byId(page, "audit-cluster").value, "lab");
  byId(page, "audit-connector").value = "orders-sink";
  byId(page, "audit-action").value = "CREATE";
  byId(page, "audit-result").value = "success";
  byId(page, "audit-actor").value = "alice";
  byId(page, "audit-filters").dispatch("submit");
  await tick();
  assert.ok(server.requests.some((url) => url.includes("cluster=lab") && url.includes("actor=alice")));
  const before = server.requests.length;
  byId(page, "audit-reset").dispatch("click");
  await settle();
  assert.strictEqual(page.location.hash, "#audit");
  const filters = page.kcv.auditView.filters;
  assert.strictEqual(filters.cluster, "");
  assert.strictEqual(filters.connector, "");
  assert.strictEqual(filters.action, "");
  assert.strictEqual(filters.result, "");
  assert.strictEqual(filters.actor, "");
  assert.strictEqual(filters.since, "");
  assert.strictEqual(filters.until, "");
  assert.strictEqual(byId(page, "audit-cluster").value, "");
  assert.strictEqual(byId(page, "audit-connector").value, "");
  assert.ok(server.requests.slice(before).some((url) => url === "/api/audit"));
  assert.ok(server.requests.slice(before).every((url) => !url.includes("connector=") && !url.includes("actor=")));
});

test("loading, empty and error states are distinct", async () => {
  const empty = await open({ empty: true });
  assert.deepStrictEqual(textOf(empty.page, (node) => node.id === "audit-empty"), ["No matching events."]);
  assert.ok(byId(empty.page, "audit-table").hidden);
  const failed = await open({ auditStatus: 503, auditPayload: { code: "audit_write_failed", params: {} } });
  assert.deepStrictEqual(textOf(failed.page, (node) => node.id === "audit-error"), ["The audit log could not be loaded."]);
  assert.ok(!textOf(failed.page, (node) => node.id === "audit-error")[0].includes("succeeded"));
});

test("viewers do not see the Audit log and stay on connectors", async () => {
  const { page, server } = await open({ role: "viewer", hash: "#audit" });
  assert.strictEqual(page.kcv.state.view, "list");
  assert.deepStrictEqual(everywhere(page, (node) => node.id === "audit-open"), []);
  assert.ok(!server.requests.some((url) => url.startsWith("/api/audit")));
  assert.ok(textOf(page, (node) => node.classList.contains("view-option")).every((label) => label !== "Audit log"));
});

test("RU and zh-CN translate actions, results, filters and the audit warning", async () => {
  for (const [language, title, denied, apply, reset, warning] of [
    ["ru", "Журнал аудита", "Отказ", "Применить", "Сбросить", "Операция выполнена, но событие аудита сохранить не удалось."],
    ["zh-CN", "审计日志", "拒绝", "应用", "重置", "操作已成功，但无法保存其审计事件。"],
  ]) {
    const { page } = await open({ language });
    await tick();
    assert.deepStrictEqual(textOf(page, (node) => node.tagName === "H1"), [title]);
    assert.ok(textOf(page, (node) => node.classList.contains("audit-result")).includes(denied));
    assert.ok(textOf(page, (node) => node.id === "audit-apply").includes(apply));
    assert.ok(textOf(page, (node) => node.id === "audit-reset").includes(reset));
    assert.strictEqual(page.kcv.t("e_audit_write_failed"), warning);
  }
});

test("Light, Dark and System themes keep the audit table in place", async () => {
  for (const [theme, prefersDark, expected] of [
    ["light", false, "light"],
    ["dark", false, "dark"],
    ["system", true, "dark"],
    ["system", false, "light"],
  ]) {
    const { page } = await open({ theme, prefersDark });
    assert.strictEqual(page.documentElement.dataset.theme, expected);
    assert.strictEqual(page.kcv.state.view, "audit");
    assert.ok(everywhere(page, (node) => node.classList.contains("audit-table")).length);
    assertStableChrome(page, "audit");
  }
});

test("Connectors places the tabs above Refresh and Add", async () => {
  const { page } = await open({ hash: "#lab" });
  const { pane, head, tabs: tabIndex, actions } = chromeOrder(page);
  assert.deepStrictEqual([head, tabIndex, actions], [0, 1, 2]);
  assert.deepStrictEqual(textOf(page, (node) => node.tagName === "H1"), ["Connectors"]);
  assert.ok(byId(page, "cluster-meta").textContent.includes("Lab"));
  const actionText = pane.children[actions].children.map((node) => node.textContent);
  assert.deepStrictEqual(actionText, ["Refresh", "Add"]);
  assert.ok(pane.children[actions + 1].classList.contains("search-wrap"));
  for (const view of ["graph", "audit"]) {
    await clickTab(page, view);
    assert.deepStrictEqual(activeTab(page), [view]);
    const order = chromeOrder(page);
    assert.deepStrictEqual([order.head, order.tabs, order.actions], [0, 1, 2], view);
    assert.ok(order.pane.children[order.actions].children.length >= 1);
  }
});

test("tabs stay left-aligned below the header across Connectors, Graph and Audit Log", async () => {
  const { page, server } = await open({ hash: "#lab" });
  assertStableChrome(page, "list");
  await clickTab(page, "graph");
  assert.strictEqual(page.location.hash, "#lab?view=graph");
  assertStableChrome(page, "graph");
  await clickTab(page, "audit");
  assert.strictEqual(page.location.hash, "#lab?view=audit");
  assertStableChrome(page, "audit");
  await clickTab(page, "list");
  assert.strictEqual(page.location.hash, "#lab");
  assertStableChrome(page, "list");
  assert.strictEqual(server.requests.filter((url) => url.includes("/graph")).length, 1);
});

test("global Audit Log returns to the last selected cluster and keeps it selected", async () => {
  const { page } = await open({ hash: "#prod" });
  assert.strictEqual(page.kcv.state.clusterId, "prod");
  await clickTab(page, "audit");
  await go(page, "#audit");
  assert.strictEqual(page.kcv.state.view, "audit");
  assert.strictEqual(page.kcv.state.clusterId, "prod");
  assert.strictEqual(page.kcv.targetClusterId(), "prod");
  await clickTab(page, "graph");
  assert.strictEqual(page.location.hash, "#prod?view=graph");
  assert.strictEqual(page.kcv.state.clusterId, "prod");
});

test("opening #audit first returns to the first available cluster", async () => {
  const { page } = await open({ hash: "#audit" });
  assert.strictEqual(page.kcv.targetClusterId(), "lab");
  await clickTab(page, "list");
  assert.strictEqual(page.location.hash, "#lab");
  assert.strictEqual(page.kcv.state.clusterId, "lab");
});

test("browser Back and Forward restore views without extra graph or audit fetches", async () => {
  const { page, server } = await open({ hash: "#lab" });
  const history = ["#lab"];
  const remember = async (hash) => {
    history.push(page.location.hash);
    await go(page, hash);
  };
  await remember("#lab?view=graph");
  await remember("#audit");
  const graphCalls = server.requests.filter((url) => url.includes("/graph")).length;
  const auditCalls = server.requests.filter((url) => url.startsWith("/api/audit")).length;
  await go(page, history.pop());
  assert.strictEqual(page.kcv.state.view, "graph");
  await go(page, history.pop());
  assert.strictEqual(page.kcv.state.view, "list");
  await go(page, "#lab?view=graph");
  await go(page, "#audit");
  assert.strictEqual(page.kcv.state.view, "audit");
  assert.strictEqual(server.requests.filter((url) => url.includes("/graph")).length, graphCalls);
  assert.strictEqual(server.requests.filter((url) => url.startsWith("/api/audit")).length, auditCalls);
});

test("switching back to an already loaded view does not refetch", async () => {
  const { page, server } = await open({ hash: "#lab?view=graph" });
  await go(page, "#lab");
  await go(page, "#lab?view=graph");
  await go(page, "#audit");
  const afterFirst = server.requests.filter((url) => url.startsWith("/api/audit")).length;
  await go(page, "#lab");
  await go(page, "#audit");
  assert.strictEqual(server.requests.filter((url) => url.startsWith("/api/audit")).length, afterFirst);
  assert.strictEqual(server.requests.filter((url) => url.includes("/graph")).length, 1);
});

test("compact filter CSS keeps desktop inputs from going full width", async () => {
  assert.ok(css.includes("align-self: flex-start"));
  assert.ok(css.includes("flex-wrap: wrap"));
  assert.ok(css.includes("minmax(0, 1fr)"));
  assert.ok(css.includes("align-content: start"));
  assert.match(css, /\.audit-filters input\[type="search"\][\s\S]*width:\s*auto/);
  assert.match(css, /\.audit-filters[\s\S]*max-width:\s*220px/);
  assert.match(css, /@media \(max-width: 640px\)[\s\S]*\.audit-filters input[\s\S]*width:\s*100%/);
  const { page } = await open();
  assert.ok(byId(page, "audit-filters").classList.contains("audit-filters"));
  assert.ok(byId(page, "audit-reset"));
});

test("a 204 success with X-KCV-Audit shows a warning and does not retry", async () => {
  const { page, server } = await open({ hash: "#lab", auditHeader: "write_failed", writeStatus: 204 });
  const before = server.methods.length;
  const data = await page.kcv.api("/api/clusters/lab/connectors/orders-sink/pause", { method: "POST" });
  assert.strictEqual(data, null);
  await tick();
  const toast = page.roots.toast;
  assert.strictEqual(toast.hidden, false);
  assert.ok(toast.classList.contains("is-warning"));
  assert.strictEqual(toast.textContent, "The operation succeeded, but its audit event could not be saved.");
  assert.strictEqual(server.methods.slice(before).filter((method) => method === "POST").length, 1);
});

test("a JSON success with an audit warning is not shown as a Kafka Connect failure", async () => {
  const { page } = await open({
    hash: "#lab",
    language: "ru",
    auditHeader: "write_failed",
    writeStatus: 201,
    writeBody: { name: "orders-sink" },
  });
  const created = await page.kcv.api("/api/clusters/lab/connectors", { method: "POST", body: "{}" });
  assert.deepStrictEqual(created, { name: "orders-sink" });
  await tick();
  const toast = page.roots.toast;
  assert.ok(toast.classList.contains("is-warning"));
  assert.ok(!toast.classList.contains("is-error"));
  assert.strictEqual(toast.textContent, "Операция выполнена, но событие аудита сохранить не удалось.");
});

test("failed operations keep their error toast and do not show an audit warning", async () => {
  const { page, server } = await open({
    hash: "#lab",
    writeStatus: 409,
    writeBody: { message: "Connector is already running" },
  });
  await assert.rejects(page.kcv.api("/api/clusters/lab/connectors/orders-sink/resume", { method: "POST" }), (error) => {
    assert.strictEqual(error.status, 409);
    assert.strictEqual(error.message, "Connector is already running");
    return true;
  });
  await tick();
  assert.strictEqual(page.roots.toast.textContent, "");
  assert.ok(!page.roots.toast.classList.contains("is-warning"));
  assert.strictEqual(server.methods.filter((method) => method === "POST").length, 1);
});

runTests(tests);
