// Tests for Light / Dark / System themes and the mascot in the sidebar brand.
// Usage: node tests/theme.test.cjs path/to/app.js
const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const { findAll, loadApp, runTests, tick } = require("./fake_dom.cjs");

const appPath = path.resolve(process.argv[2] || path.join(__dirname, "..", "static/app.js"));
const bootPath = path.join(path.dirname(appPath), "theme-boot.js");
const LIGHT = "/static/assets/kcv-snake-light.png";
const DARK = "/static/assets/kcv-snake-dark.png";

function backend() {
  const requests = [];
  const fetch = async (url) => {
    requests.push(url);
    let payload = {};
    if (url === "/api/me") payload = { role: "viewer", auth_enabled: false };
    else if (url === "/api/clusters") payload = { clusters: [{ id: "c", name: "c" }] };
    else if (url === "/api/clusters/c/connectors") payload = { count: 0, connectors: [] };
    else if (url === "/api/clusters/c") payload = { version: "3.7.0" };
    return { ok: true, status: 200, statusText: "OK", text: async () => JSON.stringify(payload) };
  };
  return { requests, fetch };
}

async function open(options = {}) {
  const server = backend();
  const page = loadApp(appPath, { storedLanguage: "en", fetch: server.fetch, ...options });
  await tick();
  const sidebar = page.roots.sidebar;
  const ui = {
    page,
    server,
    theme: () => page.documentElement.dataset.theme,
    mascot: () => findAll(sidebar, (node) => node.id === "brand-mascot")[0],
    options: () => findAll(sidebar, (node) => node.classList.contains("theme-option")),
    group: () => findAll(sidebar, (node) => node.classList.contains("theme-switch"))[0],
    pressed: () => ui.options().filter((node) => node.getAttribute("aria-pressed") === "true").map((node) => node.dataset.themeOption),
    async choose(theme) {
      ui.options().find((node) => node.dataset.themeOption === theme).dispatch("click");
      await tick();
    },
  };
  return ui;
}

function bootTheme({ storedTheme = null, prefersDark = false, storageThrows = false } = {}) {
  const documentElement = { dataset: {} };
  const context = {
    localStorage: { getItem: (key) => { if (storageThrows) throw new Error("blocked"); return key === "kc-theme" ? storedTheme : null; } },
    window: { matchMedia: () => ({ matches: prefersDark }) },
    document: { documentElement },
  };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(bootPath, "utf8"), context, { filename: bootPath });
  return documentElement.dataset.theme;
}

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

test("System is the default and follows the OS scheme", async () => {
  for (const [prefersDark, theme, src] of [[false, "light", LIGHT], [true, "dark", DARK]]) {
    const ui = await open({ prefersDark });
    assert.strictEqual(ui.theme(), theme);
    assert.strictEqual(ui.mascot().getAttribute("src"), src);
    assert.deepStrictEqual(ui.pressed(), ["system"]);
    assert.strictEqual(ui.page.storage.has("kc-theme"), false, "the default is not stored as a choice");
  }
});

test("System switches live when the OS scheme changes", async () => {
  const ui = await open({ prefersDark: false });
  ui.page.setSystemDark(true);
  assert.strictEqual(ui.theme(), "dark");
  assert.strictEqual(ui.mascot().getAttribute("src"), DARK);
  ui.page.setSystemDark(false);
  assert.strictEqual(ui.theme(), "light");
  assert.strictEqual(ui.mascot().getAttribute("src"), LIGHT);
});

test("an explicit choice is stored, applied and wins over the OS scheme", async () => {
  const ui = await open({ prefersDark: false });
  await ui.choose("dark");
  assert.strictEqual(ui.page.storage.get("kc-theme"), "dark");
  assert.strictEqual(ui.theme(), "dark");
  assert.strictEqual(ui.mascot().getAttribute("src"), DARK);
  assert.deepStrictEqual(ui.pressed(), ["dark"]);
  ui.page.setSystemDark(false);
  assert.strictEqual(ui.theme(), "dark", "OS changes are ignored for an explicit choice");

  await ui.choose("light");
  ui.page.setSystemDark(true);
  assert.strictEqual(ui.theme(), "light");
  assert.strictEqual(ui.mascot().getAttribute("src"), LIGHT);

  await ui.choose("system");
  assert.strictEqual(ui.page.storage.get("kc-theme"), "system");
  assert.strictEqual(ui.theme(), "dark");
});

test("a stored choice is restored after a reload; unknown values fall back to System", async () => {
  for (const [storedTheme, prefersDark, theme, pressed] of [
    ["dark", false, "dark", "dark"],
    ["light", true, "light", "light"],
    ["system", true, "dark", "system"],
    ["sepia", true, "dark", "system"],
    ["", false, "light", "system"],
  ]) {
    const ui = await open({ storedTheme, prefersDark });
    assert.strictEqual(ui.theme(), theme, `stored ${JSON.stringify(storedTheme)}`);
    assert.deepStrictEqual(ui.pressed(), [pressed]);
  }
});

test("the boot script applies the same theme as app.js before the first paint", async () => {
  for (const storedTheme of [null, "light", "dark", "system", "sepia"]) {
    for (const prefersDark of [false, true]) {
      const ui = await open({ storedTheme, prefersDark });
      assert.strictEqual(bootTheme({ storedTheme, prefersDark }), ui.theme(), `${storedTheme} / dark=${prefersDark}`);
    }
  }
  assert.strictEqual(bootTheme({ storageThrows: true, prefersDark: true }), "dark", "blocked storage falls back to the OS scheme");
});

test("the mascot is decorative, sized and next to the brand name", async () => {
  const ui = await open();
  const mascot = ui.mascot();
  assert.strictEqual(mascot.tagName, "IMG");
  assert.strictEqual(mascot.getAttribute("alt"), "", "decorative: the brand name is the accessible text");
  assert.strictEqual(mascot.getAttribute("width"), "1964");
  assert.strictEqual(mascot.getAttribute("height"), "400");
  assert.strictEqual(mascot.getAttribute("draggable"), "false");
  assert.ok(mascot.parent.classList.contains("brand-mascot"));
  const brand = findAll(ui.page.roots.sidebar, (node) => node.classList.contains("brand"))[0];
  assert.ok(brand.textContent.includes("KCV"));
  assert.ok(brand.textContent.includes("Kafka Connect Viewer"));
});

test("the theme switcher is an accessible, translated button group", async () => {
  const ui = await open();
  assert.strictEqual(ui.group().getAttribute("role"), "group");
  assert.strictEqual(ui.group().getAttribute("aria-label"), "Theme");
  assert.deepStrictEqual(ui.options().map((node) => node.textContent), ["Light", "Dark", "System"]);
  assert.ok(ui.options().every((node) => node.getAttribute("type") === "button"));
  for (const [lang, label, names] of [
    ["ru", "Тема", ["Светлая", "Тёмная", "Системная"]],
    ["zh-CN", "主题", ["浅色", "深色", "跟随系统"]],
  ]) {
    ui.page.kcv.setLanguage(lang);
    await tick();
    assert.strictEqual(ui.group().getAttribute("aria-label"), label);
    assert.deepStrictEqual(ui.options().map((node) => node.textContent), names);
  }
  await ui.choose("dark");
  ui.page.kcv.setLanguage("en");
  await tick();
  assert.strictEqual(ui.theme(), "dark", "changing the language keeps the theme");
  assert.deepStrictEqual(ui.pressed(), ["dark"]);
});

test("switching themes does not call the API", async () => {
  const ui = await open();
  const before = ui.server.requests.length;
  await ui.choose("dark");
  await ui.choose("light");
  await ui.choose("system");
  assert.strictEqual(ui.server.requests.length, before);
});

runTests(tests);
