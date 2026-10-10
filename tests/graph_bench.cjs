// Times layoutGraph and SVG construction for synthetic graphs.
// This measures JavaScript work in Node, not browser paint.
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const appPath = path.resolve(process.argv[2] || path.join(__dirname, "..", "static/app.js"));

class FakeElement {
  constructor(tag) {
    this.nodeType = 1;
    this.tagName = tag;
    this.attrs = {};
    this.children = [];
    this.classes = new Set();
    this.hidden = false;
    const classes = this.classes;
    this.classList = {
      add: (...names) => names.forEach((name) => classes.add(name)),
      remove: () => {},
      toggle: () => {},
      contains: (name) => classes.has(name),
    };
  }
  set className(value) { this.classes = new Set(String(value).split(/\s+/).filter(Boolean)); }
  get id() { return this.attrs.id; }
  setAttribute(key, value) { this.attrs[key] = String(value); }
  getAttribute(key) { return this.attrs[key] ?? null; }
  append(...nodes) { for (const node of nodes) this.children.push(node); }
  replaceChildren(...nodes) { this.children = []; this.append(...nodes); }
  addEventListener() {}
  focus() {}
  scrollIntoView() {}
}

const pageIds = [...fs.readFileSync(path.join(path.dirname(appPath), "index.html"), "utf8").matchAll(/\bid="([^"]+)"/g)].map((match) => match[1]);
const roots = Object.fromEntries(pageIds.map((id) => {
  const node = new FakeElement("div");
  node.attrs.id = id;
  return [id, node];
}));
const canvas = new FakeElement("div");
canvas.attrs.id = "graph-canvas";
roots["graph-canvas"] = canvas;
const context = {
  console, setTimeout, clearTimeout, URL, URLSearchParams, JSON, Promise, Map, Set,
  localStorage: { getItem: () => "en", setItem() {} },
  navigator: { language: "en" },
  location: { hash: "#c?view=graph" },
  window: { addEventListener() {} },
  fetch: async () => ({ ok: true, status: 200, headers: { get: () => null }, text: async () => "{}" }),
  document: {
    documentElement: { lang: "en", dataset: {} },
    activeElement: null,
    addEventListener() {},
    createElement: (tag) => new FakeElement(tag),
    createElementNS: (_ns, tag) => new FakeElement(tag),
    createTextNode: (text) => ({ nodeType: 3, textContent: String(text) }),
    getElementById(id) {
      const walk = (node) => {
        if (node.attrs && node.attrs.id === id) return node;
        for (const child of node.children || []) {
          if (child.nodeType === 1) {
            const found = walk(child);
            if (found) return found;
          }
        }
        return null;
      };
      for (const node of Object.values(roots)) {
        const found = walk(node);
        if (found) return found;
      }
      return null;
    },
    querySelector: () => null,
    querySelectorAll: () => [],
  },
};
vm.createContext(context);
vm.runInContext(
  fs.readFileSync(appPath, "utf8") + "\n;globalThis.__kcv = { layoutGraph, renderGraphCanvas, graphView };",
  context,
  { filename: appPath },
);

function synth(nodeCount, degree) {
  const third = Math.floor(nodeCount / 3);
  const nodes = [];
  for (let i = 0; i < third; i += 1) nodes.push({ id: `s${i}`, kind: "connector", type: "source", name: `source-${i}` });
  for (let i = 0; i < third; i += 1) nodes.push({ id: `t${i}`, kind: "topic", name: `topic.${i}` });
  for (let i = 0; i < nodeCount - third * 2; i += 1) nodes.push({ id: `k${i}`, kind: "connector", type: "sink", name: `sink-${i}` });
  const edges = [];
  const sinks = nodes.filter((node) => node.type === "sink");
  const topics = nodes.filter((node) => node.kind === "topic");
  const sources = nodes.filter((node) => node.type === "source");
  sources.forEach((node, index) => {
    edges.push({ id: `e-w-${node.id}`, from: node.id, to: topics[index % topics.length].id, kind: "writes", confidence: "declared", key: "topic" });
  });
  sinks.forEach((node, index) => {
    for (let slot = 0; slot < degree; slot += 1) {
      const topic = topics[(index * degree + slot) % topics.length];
      edges.push({ id: `e-r-${node.id}-${slot}`, from: topic.id, to: node.id, kind: "reads", confidence: "declared", key: "topics" });
    }
  });
  return {
    nodes,
    edges,
    diagnostics: [],
    stats: { errors: 0, warnings: 0 },
    partial: false,
    errors: [],
  };
}

function countElements(node, total = { n: 0 }) {
  total.n += 1;
  for (const child of node.children || []) if (child.nodeType === 1) countElements(child, total);
  return total.n;
}

const sizes = [100, 500, 1000, 2500, 5000];
for (const size of sizes) {
  for (const degree of size >= 2500 ? [1] : [1, 8]) {
    const data = synth(size, degree);
    const layoutStarted = process.hrtime.bigint();
    context.__kcv.layoutGraph(data.nodes, data.edges);
    const layoutMs = Number(process.hrtime.bigint() - layoutStarted) / 1e6;
    context.__kcv.graphView.data = data;
    context.__kcv.graphView.query = "";
    context.__kcv.graphView.focus = null;
    context.__kcv.graphView.problemsOnly = false;
    const renderStarted = process.hrtime.bigint();
    context.__kcv.renderGraphCanvas();
    const renderMs = Number(process.hrtime.bigint() - renderStarted) / 1e6;
    const dom = countElements(roots["graph-canvas"]);
    const searchStarted = process.hrtime.bigint();
    context.__kcv.graphView.query = "topic.1";
    context.__kcv.renderGraphCanvas();
    const searchMs = Number(process.hrtime.bigint() - searchStarted) / 1e6;
    context.__kcv.graphView.query = "";
    console.log(JSON.stringify({
      nodes: data.nodes.length,
      edges: data.edges.length,
      degree,
      layout_ms: Number(layoutMs.toFixed(2)),
      render_ms: Number(renderMs.toFixed(2)),
      search_ms: Number(searchMs.toFixed(2)),
      dom_elements: dom,
    }));
  }
}
