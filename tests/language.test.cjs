// Tests for UI language selection: English is the default and the fallback,
// Russian is an optional, complete localization chosen via the switcher.
// Usage: node tests/language.test.cjs path/to/app.js
const assert = require("assert");
const path = require("path");
const { findAll, loadApp, runTests, tick } = require("./fake_dom.cjs");

const appPath = path.resolve(process.argv[2] || path.join(__dirname, "..", "static/app.js"));

async function fakeFetch(url) {
  let payload = {};
  if (url === "/api/me") payload = { role: "admin", auth_enabled: true, user: "operator" };
  else if (url === "/api/clusters") payload = { clusters: [{ id: "c", name: "c" }] };
  else if (url === "/api/clusters/c/connectors") payload = { count: 0, connectors: [] };
  else if (url === "/api/clusters/c") payload = { version: "3.7.0" };
  return { ok: true, status: 200, statusText: "OK", text: async () => JSON.stringify(payload) };
}

const open = (options) => loadApp(appPath, { fetch: fakeFetch, ...options });
const everywhere = (page, predicate) => Object.values(page.roots).flatMap((node) => findAll(node, predicate));
const switcher = (page) => everywhere(page, (node) => node.classList.contains("language-option"));
const logoutText = (page) => everywhere(page, (node) => node.classList.contains("auth-logout")).map((node) => node.textContent);

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

test("new users get English, whatever the browser locale is", async () => {
  for (const browserLanguage of ["en-US", "ru-RU", "ru", "de-DE"]) {
    const page = open({ browserLanguage });
    await tick();
    assert.strictEqual(page.kcv.currentLanguage(), "en", `browser ${browserLanguage}`);
    assert.strictEqual(page.documentElement.lang, "en");
    assert.strictEqual(page.kcv.t("requestError"), "Request failed");
    assert.deepStrictEqual(logoutText(page), ["Logout"]);
    assert.strictEqual(page.storage.has("kc-language"), false, "the default is not written as an explicit choice");
  }
});

test("English is the canonical language and is listed first", async () => {
  const page = open();
  await tick();
  assert.strictEqual(page.kcv.DEFAULT_LANGUAGE, "en");
  assert.deepStrictEqual(Object.keys(page.kcv.translations), ["en", "ru"]);
  const buttons = switcher(page);
  assert.deepStrictEqual(buttons.map((node) => node.textContent), ["EN", "RU"]);
  assert.deepStrictEqual(buttons.filter((node) => node.classList.contains("is-selected")).map((node) => node.textContent), ["EN"]);
});

test("a stored explicit choice is preserved", async () => {
  const ru = open({ storedLanguage: "ru", browserLanguage: "en-US" });
  await tick();
  assert.strictEqual(ru.kcv.currentLanguage(), "ru");
  assert.strictEqual(ru.documentElement.lang, "ru");
  assert.strictEqual(ru.kcv.t("requestError"), "Ошибка запроса");
  assert.deepStrictEqual(logoutText(ru), ["Выйти"]);

  const en = open({ storedLanguage: "en", browserLanguage: "ru-RU" });
  await tick();
  assert.strictEqual(en.kcv.currentLanguage(), "en");
});

test("an unknown stored value falls back to English", async () => {
  for (const storedLanguage of ["de", "", "RU"]) {
    const page = open({ storedLanguage, browserLanguage: "ru-RU" });
    await tick();
    assert.strictEqual(page.kcv.currentLanguage(), "en", `stored ${JSON.stringify(storedLanguage)}`);
  }
});

test("the switcher selects Russian, stores the choice and re-renders", async () => {
  const page = open();
  await tick();
  switcher(page).find((node) => node.textContent === "RU").dispatch("click");
  await tick();
  assert.strictEqual(page.kcv.currentLanguage(), "ru");
  assert.strictEqual(page.storage.get("kc-language"), "ru");
  assert.strictEqual(page.documentElement.lang, "ru");
  assert.deepStrictEqual(logoutText(page), ["Выйти"]);
  switcher(page).find((node) => node.textContent === "EN").dispatch("click");
  await tick();
  assert.strictEqual(page.storage.get("kc-language"), "en");
  assert.deepStrictEqual(logoutText(page), ["Logout"]);
});

test("Russian is a complete localization of the English texts", async () => {
  const { translations } = open().kcv;
  const placeholders = (text) => [...text.matchAll(/\{(\w+)\}/g)].map((match) => match[1]).sort();
  assert.deepStrictEqual(Object.keys(translations.ru).sort(), Object.keys(translations.en).sort());
  for (const [key, text] of Object.entries(translations.en)) {
    assert.ok(translations.ru[key], `ru text for ${key}`);
    assert.deepStrictEqual(placeholders(translations.ru[key]), placeholders(text), `placeholders of ${key}`);
  }
});

test("a text missing from Russian falls back to English", async () => {
  const page = open({ storedLanguage: "ru" });
  await tick();
  const { translations, t, fmt, errorText, diagnosticMessage } = page.kcv;
  delete translations.ru.requestError;
  delete translations.ru.e_role_required;
  delete translations.ru.d_config_unreadable;
  assert.strictEqual(t("requestError"), "Request failed");
  assert.strictEqual(t("checkFields"), "Проверьте поля формы", "existing Russian texts are still used");
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
});

test("a text missing from every language shows its key", async () => {
  const page = open({ storedLanguage: "ru" });
  assert.strictEqual(page.kcv.t("not_a_real_key"), "not_a_real_key");
  assert.strictEqual(page.kcv.errorText({ code: "brand_new_code", params: {} }), "brand_new_code");
});

runTests(tests);
