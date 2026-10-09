// A small fake DOM that loads static/app.js in its own VM context.
// Each loadApp() call is a fresh page load with its own storage, roots and fetch.
const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const root = path.resolve(__dirname, "..");
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
  setAttribute(key, value) {
    this.attrs[key] = String(value);
    if (key.startsWith("data-")) this.dataset[key.slice(5).replace(/-([a-z])/g, (_, char) => char.toUpperCase())] = String(value);
  }
  getAttribute(key) { return this.attrs[key] ?? null; }
  get value() { return this._value ?? (this.tagName === "TEXTAREA" ? this.textContent : this.attrs.value ?? ""); }
  set value(value) { this._value = String(value); }
  get readOnly() { return "readonly" in this.attrs; }
  append(...nodes) { for (let node of nodes) { if (typeof node === "string") node = { nodeType: 3, textContent: node }; node.parent = this; this.children.push(node); } }
  replaceChildren(...nodes) { this.children = []; this._text = undefined; this.append(...nodes); }
  addEventListener(type, handler) { (this.listeners[type] ||= []).push(handler); }
  dispatch(type) { for (const handler of this.listeners[type] || []) handler({ target: this, preventDefault() {} }); }
  get textContent() { return this._text ?? this.children.map((child) => child.textContent).join(""); }
  set textContent(value) { this.children = []; this._text = String(value); }
  focus() {}
  scrollIntoView() {}
  closest(selector) {
    const attribute = selector.match(/^\[([\w-]+)\]$/);
    assert.ok(attribute, "the fake DOM only supports [attribute] in closest()");
    for (let node = this; node && node.nodeType === 1; node = node.parent) {
      if (attribute[1] in node.attrs) return node;
    }
    return null;
  }
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

function loadApp(appPath, { storedLanguage = null, browserLanguage = "en-US", fetch, hash = "#c", expose = "" } = {}) {
  const roots = Object.fromEntries(pageIds.map((id) => {
    const node = new FakeElement("div");
    node.attrs.id = id;
    return [id, node];
  }));
  const storage = new Map(storedLanguage == null ? [] : [["kc-language", storedLanguage]]);
  const documentElement = { lang: "" };
  const location = { hash };
  const clipboard = [];
  const context = {
    console, setTimeout, clearTimeout, URL, JSON, Promise, Map, Set,
    localStorage: { getItem: (key) => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, String(value)) },
    navigator: { language: browserLanguage, languages: [browserLanguage], clipboard: { writeText: async (text) => { clipboard.push(text); } } },
    location,
    window: { addEventListener() {} },
    fetch,
    document: {
      documentElement,
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
      "\n;globalThis.__kcv = { translations, DEFAULT_LANGUAGE, t, fmt, errorText, diagnosticMessage, api, setLanguage," +
      " applyHash: () => applyHash(), currentLanguage: () => language, setLang: (code) => { language = code; }" +
      (expose ? `, ${expose}` : "") + " };",
    context,
    { filename: appPath },
  );
  return { kcv: context.__kcv, roots, storage, documentElement, location, clipboard };
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 5));

async function runTests(tests) {
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
}

module.exports = { FakeElement, findAll, loadApp, runTests, tick };
