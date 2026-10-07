// Tests for UI language selection: English is the default and the fallback,
// Russian and Simplified Chinese are optional, complete localizations chosen via the switcher.
// Usage: node tests/language.test.cjs path/to/app.js
const assert = require("assert");
const path = require("path");
const { findAll, loadApp, runTests, tick } = require("./fake_dom.cjs");

const appPath = path.resolve(process.argv[2] || path.join(__dirname, "..", "static/app.js"));
const LANGUAGES = ["en", "ru", "zh-CN"];
const LABELS = ["EN", "RU", "中文"];
const LOGOUT = { en: "Logout", ru: "Выйти", "zh-CN": "退出登录" };

async function fakeFetch(url) {
  let payload = {};
  if (url === "/api/me") payload = { role: "admin", auth_enabled: true, user: "operator" };
  else if (url === "/api/clusters") payload = { clusters: [{ id: "c", name: "c" }] };
  else if (url === "/api/clusters/c/connectors") payload = { count: 3, connectors: ["orders-sink", "orders-source", "audit"] };
  else if (url === "/api/clusters/c") payload = { version: "3.7.0" };
  return { ok: true, status: 200, statusText: "OK", text: async () => JSON.stringify(payload) };
}

const open = (options) => loadApp(appPath, { fetch: fakeFetch, ...options });
const everywhere = (page, predicate) => Object.values(page.roots).flatMap((node) => findAll(node, predicate));
const switcher = (page) => everywhere(page, (node) => node.classList.contains("language-option"));
const logoutText = (page) => everywhere(page, (node) => node.classList.contains("auth-logout")).map((node) => node.textContent);
const choose = async (page, label) => {
  switcher(page).find((node) => node.textContent === label).dispatch("click");
  await tick();
};

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

test("new users get English, whatever the browser locale is", async () => {
  for (const browserLanguage of ["en-US", "ru-RU", "ru", "zh-CN", "zh", "de-DE"]) {
    const page = open({ browserLanguage });
    await tick();
    assert.strictEqual(page.kcv.currentLanguage(), "en", `browser ${browserLanguage}`);
    assert.strictEqual(page.documentElement.lang, "en");
    assert.strictEqual(page.kcv.t("requestError"), "Request failed");
    assert.deepStrictEqual(logoutText(page), ["Logout"]);
    assert.strictEqual(page.storage.has("kc-language"), false, "the default is not written as an explicit choice");
  }
});

test("English is the canonical language and the switcher lists EN, RU and 中文", async () => {
  const page = open();
  await tick();
  assert.strictEqual(page.kcv.DEFAULT_LANGUAGE, "en");
  assert.deepStrictEqual(Object.keys(page.kcv.translations), LANGUAGES);
  const buttons = switcher(page);
  assert.deepStrictEqual(buttons.map((node) => node.textContent), LABELS);
  assert.deepStrictEqual(buttons.map((node) => node.getAttribute("lang")), LANGUAGES);
  assert.deepStrictEqual(buttons.filter((node) => node.classList.contains("is-selected")).map((node) => node.textContent), ["EN"]);
});

test("a stored explicit choice is preserved", async () => {
  for (const [storedLanguage, requestError] of [["ru", "Ошибка запроса"], ["zh-CN", "请求失败"], ["en", "Request failed"]]) {
    const page = open({ storedLanguage, browserLanguage: storedLanguage === "en" ? "ru-RU" : "en-US" });
    await tick();
    assert.strictEqual(page.kcv.currentLanguage(), storedLanguage);
    assert.strictEqual(page.documentElement.lang, storedLanguage);
    assert.strictEqual(page.kcv.t("requestError"), requestError);
    assert.deepStrictEqual(logoutText(page), [LOGOUT[storedLanguage]]);
    const selected = switcher(page).filter((node) => node.classList.contains("is-selected")).map((node) => node.getAttribute("lang"));
    assert.deepStrictEqual(selected, [storedLanguage]);
  }
});

test("an unknown stored value falls back to English", async () => {
  for (const storedLanguage of ["de", "", "RU", "zh", "zh-cn", "zh-TW"]) {
    const page = open({ storedLanguage, browserLanguage: "zh-CN" });
    await tick();
    assert.strictEqual(page.kcv.currentLanguage(), "en", `stored ${JSON.stringify(storedLanguage)}`);
  }
});

test("the switcher selects each language, stores the choice and re-renders", async () => {
  const page = open();
  await tick();
  for (const [label, code] of [["中文", "zh-CN"], ["RU", "ru"], ["EN", "en"]]) {
    await choose(page, label);
    assert.strictEqual(page.kcv.currentLanguage(), code);
    assert.strictEqual(page.storage.get("kc-language"), code);
    assert.strictEqual(page.documentElement.lang, code);
    assert.deepStrictEqual(logoutText(page), [LOGOUT[code]]);
  }
  await choose(page, "中文");
  const reopened = open({ storedLanguage: page.storage.get("kc-language") });
  await tick();
  assert.strictEqual(reopened.kcv.currentLanguage(), "zh-CN", "the choice survives a reload");
});

test("RU and zh-CN are complete localizations of the English texts", async () => {
  const { translations } = open().kcv;
  const placeholders = (text) => [...text.matchAll(/\{(\w+)\}/g)].map((match) => match[1]).sort();
  const englishKeys = Object.keys(translations.en).sort();
  for (const lang of LANGUAGES.slice(1)) {
    assert.deepStrictEqual(Object.keys(translations[lang]).sort(), englishKeys, `${lang} has exactly the English keys`);
    for (const [key, text] of Object.entries(translations.en)) {
      assert.ok(translations[lang][key], `${lang} text for ${key}`);
      assert.deepStrictEqual(placeholders(translations[lang][key]), placeholders(text), `${lang} placeholders of ${key}`);
    }
  }
});

test("zh-CN keeps Kafka Connect terms, config keys and names unchanged", async () => {
  const page = open({ storedLanguage: "zh-CN" });
  await tick();
  const { translations, fmt, errorText } = page.kcv;
  const zh = translations["zh-CN"];
  for (const key of ["bootstrapOverride", "classRequired", "e_connector_class_missing", "d_topics_and_regex", "d_bootstrap_override"]) {
    for (const term of translations.en[key].match(/[a-z]+(?:\.[a-z]+)+/g)) assert.ok(zh[key].includes(term), `${key} keeps ${term}`);
  }
  assert.ok(zh.d_same_group_as_default.includes("connect-<name>"));
  assert.ok(zh.propertiesHint.includes("key=value"));
  assert.ok(zh.e_upstream_status.startsWith("Kafka Connect"));
  assert.strictEqual(
    fmt("d_shared_topic_same_group", { connectors: "orders-sink, audit", topic: "orders.v1", group: "connect-orders" }),
    "orders-sink, audit 使用同一消费者组 connect-orders 读取 orders.v1：分区会在它们之间分配，因此每个连接器只能获得部分数据。",
  );
  assert.strictEqual(errorText({ code: "config_value_not_string", params: { key: "tasks.max" } }), "“tasks.max”的值必须是字符串");
  assert.strictEqual(errorText({ message: "Connector orders-sink already exists" }), "Connector orders-sink already exists");
});

test("count and task toasts are full templates in every language", async () => {
  const page = open();
  for (const [lang, shown, task] of [
    ["en", "2 of 3", "Task 4 is restarting"],
    ["ru", "2 из 3", "Задача 4 перезапускается"],
    ["zh-CN", "2 / 3", "任务 4 正在重启"],
  ]) {
    page.kcv.setLang(lang);
    assert.strictEqual(page.kcv.fmt("shownOf", { visible: 2, total: 3 }), shown);
    assert.strictEqual(page.kcv.fmt("taskRestarting", { task: 4 }), task);
  }
});

test("a text missing from RU or zh-CN falls back to English", async () => {
  for (const lang of ["ru", "zh-CN"]) {
    const page = open({ storedLanguage: lang });
    await tick();
    const { translations, t, fmt, errorText, diagnosticMessage } = page.kcv;
    const existing = translations[lang].checkFields;
    delete translations[lang].requestError;
    delete translations[lang].e_role_required;
    delete translations[lang].d_config_unreadable;
    assert.strictEqual(t("requestError"), "Request failed", lang);
    assert.strictEqual(t("checkFields"), existing, "existing translations are still used");
    assert.strictEqual(errorText({ code: "role_required", params: { role: "admin" } }), "The admin role is required");
    assert.strictEqual(errorText(null), "Request failed");
    assert.strictEqual(fmt("e_role_required", { role: "viewer" }), "The viewer role is required");
    assert.strictEqual(
      diagnosticMessage({
        severity: "warning", code: "config_unreadable", topic: null, group: null, members: [], nodes: [], edges: [],
        connectors: ["a"], error: { message: "upstream text" },
      }),
      "The config of a could not be read: upstream text",
    );
  }
});

test("a text missing from every language shows its key", async () => {
  const page = open({ storedLanguage: "zh-CN" });
  assert.strictEqual(page.kcv.t("not_a_real_key"), "not_a_real_key");
  assert.strictEqual(page.kcv.errorText({ code: "brand_new_code", params: {} }), "brand_new_code");
});

runTests(tests);
