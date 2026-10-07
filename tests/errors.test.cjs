// Tests for backend error rendering: KCV error codes are translated (EN/RU/zh-CN),
// Kafka Connect messages are shown exactly as received.
// Usage: node tests/errors.test.cjs path/to/app.js '["code", ...]'
const assert = require("assert");
const path = require("path");
const { findAll, loadApp, runTests, tick } = require("./fake_dom.cjs");

const appPath = path.resolve(process.argv[2] || path.join(__dirname, "..", "static/app.js"));
assert.ok(process.argv[3], "pass the backend error codes as a JSON list (tests/test_frontend.py does this)");
const backendCodes = JSON.parse(process.argv[3]);

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

const page = loadApp(appPath, { storedLanguage: "en", fetch: fakeFetch });
const { roots, location } = page;
const kcv = () => page.kcv;
const placeholders = (text) => [...text.matchAll(/\{(\w+)\}/g)].map((match) => match[1]).sort();

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

const LANGUAGES = ["en", "ru", "zh-CN"];

test("every backend error code has EN, RU and zh-CN text with the same placeholders", async () => {
  const { translations } = kcv();
  assert.deepStrictEqual(Object.keys(translations), LANGUAGES);
  assert.ok(backendCodes.length > 20, "received the backend code list");
  for (const code of backendCodes) {
    const en = translations.en[`e_${code}`];
    assert.ok(en, `en translation for ${code}`);
    for (const lang of LANGUAGES.slice(1)) {
      const text = translations[lang][`e_${code}`];
      assert.ok(text, `${lang} translation for ${code}`);
      assert.deepStrictEqual(placeholders(text), placeholders(en), `${lang} placeholders of ${code}`);
    }
  }
  for (const lang of LANGUAGES) {
    const extra = Object.keys(translations[lang]).filter((key) => key.startsWith("e_") && !backendCodes.includes(key.slice(2)));
    assert.deepStrictEqual(extra, [], `${lang} has no translations for codes the backend does not send`);
  }
});

test("KCV error codes are translated with their params in every language", async () => {
  const body = { code: "secret_masked", params: { key: "connection.password" } };
  kcv().setLang("en");
  assert.strictEqual(kcv().errorText(body), "Secret “connection.password” is masked by the worker; enter a new value before saving");
  assert.strictEqual(kcv().errorText({ code: "upstream_status", params: { status: 503 } }), "Kafka Connect responded with 503");
  kcv().setLang("ru");
  assert.strictEqual(kcv().errorText(body), "Секрет «connection.password» скрыт воркером; введите новое значение перед сохранением");
  assert.strictEqual(kcv().errorText({ code: "role_required", params: { role: "admin" } }), "Нужна роль admin");
  kcv().setLang("zh-CN");
  assert.strictEqual(kcv().errorText(body), "密钥“connection.password”已被 Worker 掩码；保存前请输入新值");
  assert.strictEqual(kcv().errorText({ code: "graph_refresh_limited", params: { seconds: 10 } }), "图最多每 10 秒刷新一次");
  assert.strictEqual(kcv().errorText({ code: "upstream_unreachable", params: { error: "ConnectError" } }), "无法连接 Kafka Connect：ConnectError");
  kcv().setLang("en");
});

test("Kafka Connect messages are never translated or replaced", async () => {
  for (const lang of LANGUAGES) {
    kcv().setLang(lang);
    assert.strictEqual(kcv().errorText({ message: UPSTREAM_RU }), UPSTREAM_RU);
    assert.strictEqual(kcv().errorText({ message: "连接器 alpha 已存在" }), "连接器 alpha 已存在");
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
  kcv().setLang("zh-CN");
  assert.strictEqual(
    kcv().diagnosticMessage({ ...base, connectors: ["orders-sink"], error: { code: "upstream_timeout", params: {} } }),
    "无法读取 orders-sink 的配置：Kafka Connect 未及时响应",
  );
  assert.strictEqual(
    kcv().diagnosticMessage({ ...base, connectors: ["orders-sink"], error: { message: "Connector orders-sink not found" } }),
    "无法读取 orders-sink 的配置：Connector orders-sink not found",
  );
  kcv().setLang("en");
});

runTests(tests);
