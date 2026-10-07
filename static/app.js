const state = {
  clusters: [],
  clusterId: null,
  clusterInfo: null,
  names: [],
  query: "",
  selected: null,
  detail: null,
  detailError: null,
  listError: null,
  loadingList: false,
  loadingDetail: false,
  plugins: null,
  pluginsError: null,
  busy: false,
  me: null,
};

let listSeq = 0;
let detailSeq = 0;
let hashSeq = 0;
let toastTimer = 0;
let modal = null;

const translations = {
  ru: {
    requestError: "Ошибка запроса", checkFields: "Проверьте поля формы", noClusters: "Кластеры не настроены",
    namesOnly: "в списке только имена", clusters: "Кластеры", connectors: "Коннекторы", refresh: "Обновить",
    add: "Добавить", searchByName: "Поиск по имени", listNote: "Состояние и конфиг запрашиваются только у открытого коннектора.",
    loadingNames: "Загружаю имена…", retry: "Повторить", noConnectors: "На этом воркере нет коннекторов.",
    noNames: "Нет имён по этому запросу.", of: "из", connectorNotSelected: "Выберите коннектор",
    selectHint: "Выберите коннектор слева, чтобы открыть детали.",
    connector: "Коннектор", loadingDetail: "Загружаю состояние и конфиг…", delete: "Удалить", pause: "Пауза",
    resume: "Возобновить", restartConnector: "Перезапустить коннектор", restartWithTasks: "Коннектор + задачи", restartFailed: "Перезапустить упавшие",
    tasks: "Задачи", noTasks: "Задач нет.", state: "состояние", worker: "воркер", restartTask: "Перезапустить задачу",
    configuration: "Конфигурация", secretMask: "Звёздочки — маска секрета. Для сохранения замените маску новым значением секрета.",
    field: "Поле", validate: "Проверить", save: "Сохранить", configNotOpen: "Конфиг не открыт",
    configNotJson: "Конфиг не JSON", configMustObject: "Конфиг должен быть JSON-объектом", duplicateKey: "Повторяющийся ключ",
    configEmpty: "Конфиг пуст", fields: "Поля", pauseSent: "Пауза отправлена", resumed: "Коннектор возобновлён",
    restartSent: "Перезапуск отправлен", restartTasksSent: "Перезапуск с задачами отправлен",
    restartFailedSent: "Перезапуск упавших задач отправлен", taskRestarting: "перезапускается", configSaved: "Конфиг сохранён",
    validationOk: "Конфиг проходит проверку плагина.", errors: "Ошибок", cancel: "Отмена", deleteConnector: "Удалить коннектор",
    deleteHint: "Воркер остановит задачи и уберёт коннектор. Это нельзя отменить.", pluginFilter: "Фильтр плагинов",
    create: "Создать", newConnector: "Новый коннектор", createHint: "Выберите плагин, установленный на воркере.",
    name: "Имя", extraFields: "Дополнительные поля", loadingPlugins: "Загружаю плагины…",
    noPlugins: "Плагины не найдены. Класс можно указать вручную.", nameRequired: "Нужно имя коннектора",
    sources: "Источники", sinks: "Приёмники", otherPlugins: "Другие", backToPlugins: "Плагины",
    properties: "Свойства", manualClass: "Указать класс вручную", copy: "Копировать", copied: "Скопировано",
    propertiesHint: "По одной паре key=value на строку. Пустые строки и # комментарии пропускаются.",
    curlHint: "Запрос к REST API воркера по текущему конфигу. Учётные данные не подставляются.",
    line: "Строка", expectedKeyValue: "нужен формат key=value", multilineValue: "значение с переносом строки, редактируйте его в JSON",
    staleCurl: "есть ошибка, команда собрана из последнего корректного конфига", devMode: "Режим разработки",
    invalidName: "Имя не может содержать / или \\", classRequired: "Нужен connector.class", created: "Создан", deleted: "Удалён",
    language: "Язык"
  },
  en: {
    requestError: "Request failed", checkFields: "Check the form fields", noClusters: "No clusters configured",
    namesOnly: "list loads names only", clusters: "Clusters", connectors: "Connectors", refresh: "Refresh",
    add: "Add", searchByName: "Search by name", listNote: "Status and config are fetched only for the connector you open.",
    loadingNames: "Loading names…", retry: "Retry", noConnectors: "There are no connectors on this worker.",
    noNames: "No connector names match this search.", of: "of", connectorNotSelected: "Select a connector",
    selectHint: "Choose a connector on the left to open its details.",
    connector: "Connector", loadingDetail: "Loading status and config…", delete: "Delete", pause: "Pause",
    resume: "Resume", restartConnector: "Restart connector", restartWithTasks: "Connector + tasks", restartFailed: "Restart failed",
    tasks: "Tasks", noTasks: "No tasks.", state: "state", worker: "worker", restartTask: "Restart task",
    configuration: "Configuration", secretMask: "Asterisks are a secret mask. Replace the mask with a new secret value before saving.",
    field: "Field", validate: "Validate", save: "Save", configNotOpen: "Config is not open",
    configNotJson: "Config is not valid JSON", configMustObject: "Config must be a JSON object", duplicateKey: "Duplicate key",
    configEmpty: "Config is empty", fields: "Fields", pauseSent: "Pause requested", resumed: "Connector resumed",
    restartSent: "Restart requested", restartTasksSent: "Connector and tasks restart requested",
    restartFailedSent: "Failed tasks restart requested", taskRestarting: "is restarting", configSaved: "Config saved",
    validationOk: "Config passes plugin validation.", errors: "Errors", cancel: "Cancel", deleteConnector: "Delete connector",
    deleteHint: "The worker will stop its tasks and remove the connector. This cannot be undone.", pluginFilter: "Filter plugins",
    create: "Create", newConnector: "New connector", createHint: "Choose a plugin installed on this worker.",
    name: "Name", extraFields: "Additional fields", loadingPlugins: "Loading plugins…",
    noPlugins: "No plugins found. You can enter the class manually.", nameRequired: "Connector name is required",
    sources: "Sources", sinks: "Sinks", otherPlugins: "Other", backToPlugins: "Plugins",
    properties: "Properties", manualClass: "Enter class manually", copy: "Copy", copied: "Copied",
    propertiesHint: "One key=value pair per line. Blank lines and # comments are ignored.",
    curlHint: "Worker REST API request for the current config. Credentials are not included.",
    line: "Line", expectedKeyValue: "expected key=value", multilineValue: "value contains a line break, edit it in JSON",
    staleCurl: "has an error, so this command uses the last valid config", devMode: "Development mode",
    invalidName: "Name cannot contain / or \\", classRequired: "connector.class is required", created: "Created", deleted: "Deleted",
    language: "Language"
  }
};

let language = localStorage.getItem("kc-language");
if (!["ru", "en"].includes(language)) language = navigator.language?.toLowerCase().startsWith("ru") ? "ru" : "en";
const t = (key) => translations[language][key] || key;

function setLanguage(next) {
  if (!translations[next] || next === language) return;
  language = next;
  localStorage.setItem("kc-language", language);
  document.documentElement.lang = language;
  renderSidebar();
  if (state.clusterId) {
    renderListShell();
    renderRows();
    renderDetail();
  }
  if (modal) renderModal();
}

function languageControl() {
  const wrap = el("div", { class: "language-switch", title: t("language") });
  for (const code of ["ru", "en"]) {
    const button = el("button", { class: "language-option", type: "button" }, code.toUpperCase());
    if (code === language) button.classList.add("is-selected");
    button.addEventListener("click", () => setLanguage(code));
    wrap.append(button);
  }
  return wrap;
}

const $ = (id) => document.getElementById(id);

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value == null || value === false) continue;
    if (key === "class") node.className = value;
    else if (key === "dataset") Object.assign(node.dataset, value);
    else node.setAttribute(key, value === true ? "" : value);
  }
  for (const child of [].concat(children)) {
    if (child == null || child === false) continue;
    node.append(child.nodeType ? child : document.createTextNode(String(child)));
  }
  return node;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  });
  const text = await response.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = { message: text };
    }
  }
  if (!response.ok) {
    let message = response.statusText || t("requestError");
    if (data && typeof data.message === "string") message = data.message;
    else if (data && typeof data.detail === "string") message = data.detail;
    else if (data && Array.isArray(data.detail)) message = t("checkFields");
    const error = new Error(message);
    error.status = response.status;
    throw error;
  }
  return data;
}

function toast(message, isError = false) {
  const node = $("toast");
  node.hidden = false;
  node.textContent = message;
  node.classList.toggle("is-error", isError);
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    node.hidden = true;
  }, 4200);
}

function hasRole(minimum) {
  const level = { viewer: 10, operator: 20, admin: 30 };
  const role = state.me?.role || "viewer";
  return (level[role] || 0) >= level[minimum];
}

function currentCluster() {
  return state.clusters.find((cluster) => cluster.id === state.clusterId) || null;
}

function decodePart(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function parseHash() {
  const raw = location.hash.replace(/^#/, "");
  if (!raw) return { clusterId: null, name: null };
  const slash = raw.indexOf("/");
  if (slash === -1) return { clusterId: decodePart(raw), name: null };
  return {
    clusterId: decodePart(raw.slice(0, slash)),
    name: decodePart(raw.slice(slash + 1)),
  };
}

async function boot() {
  document.documentElement.lang = language;
  $("detail-pane").addEventListener("click", onDetailClick);
  document.addEventListener("keydown", onKeydown);
  $("modal").addEventListener("click", (event) => {
    if (event.target === $("modal")) closeModal();
  });
  try {
    state.me = await api("/api/me");
    const data = await api("/api/clusters");
    state.clusters = data.clusters || [];
  } catch (error) {
    $("sidebar").append(el("p", {}, error.message));
    return;
  }
  if (!state.clusters.length) {
    $("sidebar").append(el("p", {}, t("noClusters")));
    return;
  }
  window.addEventListener("hashchange", () => {
    void applyHash();
  });
  await applyHash();
}

async function applyHash() {
  const seq = ++hashSeq;
  let { clusterId, name } = parseHash();
  if (!clusterId || !state.clusters.some((cluster) => cluster.id === clusterId)) {
    const next = "#" + encodeURIComponent(state.clusters[0].id);
    if (location.hash !== next) location.hash = next;
    return;
  }
  const clusterChanged = clusterId !== state.clusterId;
  if (clusterChanged) {
    state.clusterId = clusterId;
    state.query = "";
    state.names = [];
    state.selected = null;
    state.detail = null;
    state.detailError = null;
    state.listError = null;
    state.clusterInfo = null;
    state.plugins = null;
    state.pluginsError = null;
    renderSidebar();
    renderListShell();
    renderDetail();
    await loadNames();
    if (seq !== hashSeq) return;
    void loadClusterInfo();
  } else {
    renderSidebar();
  }
  if (seq !== hashSeq) return;
  if (name) {
    await loadDetail(name);
  } else if (state.selected || state.detail || state.loadingDetail) {
    detailSeq += 1;
    state.selected = null;
    state.detail = null;
    state.detailError = null;
    state.loadingDetail = false;
    markSelected();
    renderDetail();
  }
}

async function loadNames() {
  const seq = ++listSeq;
  const clusterId = state.clusterId;
  state.loadingList = true;
  state.listError = null;
  renderRows();
  try {
    const data = await api(`/api/clusters/${encodeURIComponent(clusterId)}/connectors`);
    if (seq !== listSeq || clusterId !== state.clusterId) return;
    state.names = data.connectors || [];
    state.loadingList = false;
  } catch (error) {
    if (seq !== listSeq || clusterId !== state.clusterId) return;
    state.loadingList = false;
    state.listError = error.message;
    state.names = [];
  }
  renderRows();
}

async function loadClusterInfo() {
  const clusterId = state.clusterId;
  try {
    const data = await api(`/api/clusters/${encodeURIComponent(clusterId)}`);
    if (clusterId !== state.clusterId) return;
    state.clusterInfo = data;
  } catch {
    if (clusterId !== state.clusterId) return;
    state.clusterInfo = null;
  }
  const meta = $("cluster-meta");
  if (!meta) return;
  const cluster = currentCluster();
  const version = state.clusterInfo && state.clusterInfo.version;
  meta.textContent = version ? `${cluster.url} · Connect ${version}` : cluster.url;
}

async function loadDetail(name) {
  const seq = ++detailSeq;
  const clusterId = state.clusterId;
  state.selected = name;
  state.detail = null;
  state.detailError = null;
  state.loadingDetail = true;
  markSelected();
  renderDetail();
  try {
    const data = await api(
      `/api/clusters/${encodeURIComponent(clusterId)}/connectors/${encodeURIComponent(name)}`,
    );
    if (seq !== detailSeq) return;
    state.detail = data;
    state.loadingDetail = false;
  } catch (error) {
    if (seq !== detailSeq) return;
    state.loadingDetail = false;
    state.detailError = error.message;
  }
  renderDetail();
  document.querySelector(".connector-row.is-selected")?.scrollIntoView({ block: "nearest" });
}

function renderSidebar() {
  const list = el("div", { class: "cluster-list", id: "cluster-list" });
  for (const cluster of state.clusters) {
    const button = el("button", { class: "cluster", type: "button" }, [
      el("span", { class: "cluster-name" }, cluster.name),
      el("span", { class: "cluster-url" }, cluster.url),
    ]);
    if (cluster.id === state.clusterId) button.classList.add("is-selected");
    button.addEventListener("click", () => {
      const next = "#" + encodeURIComponent(cluster.id);
      if (location.hash !== next) location.hash = next;
    });
    list.append(button);
  }
  $("sidebar").replaceChildren(
    el("div", { class: "brand" }, [
      el("span", { class: "brand-mark", "aria-hidden": "true" }),
      el("div", { class: "brand-text" }, [
        el("strong", { class: "brand-title" }, "KCV"),
        el("span", { class: "brand-subtitle" }, "Kafka Connect Viewer"),
      ]),
    ]),
    el("div", { class: "cluster-label" }, t("clusters")),
    list,
    state.me ? el("div", { class: "auth-user" }, [
      el("strong", {}, state.me.auth_enabled ? state.me.name || "" : t("devMode")),
      el("span", {}, state.me.role || ""),
      state.me.auth_enabled ? el("a", { href: "/auth/logout", class: "auth-logout" }, language === "ru" ? "Выйти" : "Logout") : null,
    ]) : null,
    languageControl(),
  );
}

function renderListShell() {
  const cluster = currentCluster();
  $("list-pane").replaceChildren(
    el("header", { class: "list-head" }, [
      el("div", {}, [
        el("h1", {}, t("connectors")),
        el("p", { class: "meta", id: "cluster-meta" }, cluster ? cluster.url : ""),
      ]),
      el("div", { class: "head-actions" }, [
        el("button", { class: "btn", type: "button", id: "refresh-list" }, t("refresh")),
        hasRole("admin") ? el("button", { class: "btn primary", type: "button", id: "create-open" }, t("add")) : null,
      ]),
    ]),
    el("div", { class: "search-wrap" }, [
      el("input", {
        id: "search",
        type: "search",
        placeholder: t("searchByName"),
        autocomplete: "off",
        spellcheck: "false",
        value: state.query,
      }),
    ]),
    el("div", { class: "list-meta", id: "list-meta" }),
    el("div", { class: "rows", id: "rows" }),
    el("p", { class: "list-note" }, t("listNote")),
  );
  $("search").addEventListener("input", (event) => {
    state.query = event.target.value;
    applyFilter();
  });
  $("refresh-list").addEventListener("click", () => {
    void loadNames();
  });
  $("create-open")?.addEventListener("click", () => {
    void openCreate();
  });
}

function renderRows() {
  const rows = $("rows");
  if (!rows) return;
  if (state.loadingList && state.names.length === 0) {
    rows.replaceChildren(el("div", { class: "status-line" }, t("loadingNames")));
    applyFilter();
    return;
  }
  if (state.listError) {
    const retry = el("button", { class: "btn", type: "button" }, t("retry"));
    retry.addEventListener("click", () => {
      void loadNames();
    });
    rows.replaceChildren(
      el("div", { class: "banner is-error" }, state.listError),
      el("div", { class: "status-line" }, [retry]),
    );
    applyFilter();
    return;
  }
  if (state.names.length === 0) {
    rows.replaceChildren(el("div", { class: "status-line" }, t("noConnectors")));
    applyFilter();
    return;
  }
  const fragment = document.createDocumentFragment();
  for (const name of state.names) {
    const button = el("button", { class: "connector-row", type: "button", title: name }, [
      el("span", { class: "connector-name" }, name),
    ]);
    button.dataset.name = name;
    if (name === state.selected) button.classList.add("is-selected");
    button.addEventListener("click", () => {
      const next = `#${encodeURIComponent(state.clusterId)}/${encodeURIComponent(name)}`;
      if (location.hash !== next) location.hash = next;
      else void loadDetail(name);
    });
    fragment.append(button);
  }
  fragment.append(el("div", { class: "filter-empty", id: "filter-empty", hidden: true }, t("noNames")));
  rows.replaceChildren(fragment);
  applyFilter();
}

function applyFilter() {
  const query = state.query.trim().toLowerCase();
  let visible = 0;
  document.querySelectorAll(".connector-row").forEach((row) => {
    const match = !query || row.dataset.name.toLowerCase().includes(query);
    row.hidden = !match;
    if (match) visible += 1;
  });
  const meta = $("list-meta");
  if (meta) {
    if (state.loadingList && state.names.length === 0) meta.textContent = t("loadingNames");
    else if (state.listError) meta.textContent = "";
    else if (query) meta.textContent = `${visible} ${t("of")} ${state.names.length}`;
    else meta.textContent = state.names.length ? String(state.names.length) : "";
  }
  const empty = $("filter-empty");
  if (empty) empty.hidden = !(query && visible === 0 && state.names.length > 0);
}

function markSelected() {
  document.querySelectorAll(".connector-row").forEach((row) => {
    row.classList.toggle("is-selected", row.dataset.name === state.selected);
  });
}

function renderDetail() {
  const pane = $("detail-pane");
  if (!state.selected) {
    pane.replaceChildren(
      el("div", { class: "empty" }, [
        el("h2", {}, t("connectorNotSelected")),
        el(
          "p",
          {},
          t("selectHint"),
        ),
      ]),
    );
    return;
  }
  if (state.loadingDetail) {
    pane.replaceChildren(
      el("div", { class: "detail-scroll" }, [
        el("div", { class: "detail-kicker" }, t("connector")),
        el("h2", {}, state.selected),
        el("p", { class: "hint" }, t("loadingDetail")),
      ]),
    );
    return;
  }
  if (state.detailError) {
    const retry = el("button", { class: "btn", type: "button", "data-action": "refresh-detail" }, t("retry"));
    pane.replaceChildren(
      el("div", { class: "detail-scroll" }, [
        el("h2", {}, state.selected),
        el("div", { class: "banner is-error" }, state.detailError),
        retry,
      ]),
    );
    return;
  }
  const detail = state.detail;
  if (!detail) return;
  const connector = detail.connector || {};
  const stateName = connector.state || "";
  const head = el("header", { class: "detail-head" }, [
    el("div", {}, [
      el("div", { class: "detail-kicker" }, [
        detail.type ? el("span", {}, detail.type) : null,
        stateName ? el("span", { class: `badge ${stateName}` }, stateName) : null,
      ]),
      el("h2", {}, detail.name || state.selected),
      connector.worker_id ? el("div", { class: "worker-id" }, connector.worker_id) : null,
    ]),
    el("div", { class: "head-actions" }, [
      el("button", { class: "btn", type: "button", "data-action": "refresh-detail" }, t("refresh")),
      hasRole("admin") ? el("button", { class: "btn danger", type: "button", "data-action": "delete" }, t("delete")) : null,
    ]),
  ]);
  const scroll = el("div", { class: "detail-scroll" });
  if (detail.status_error) scroll.append(el("div", { class: "banner is-error" }, detail.status_error));
  if (detail.info_error) scroll.append(el("div", { class: "banner is-error" }, detail.info_error));
  if (connector.trace) scroll.append(el("pre", { class: "trace" }, connector.trace));
  scroll.append(actionBar());
  scroll.append(tasksPanel(detail.tasks || []));
  scroll.append(configPanel(detail.config || {}));
  pane.replaceChildren(head, scroll);
}

function actionBar() {
  if (!hasRole("operator")) return el("div", { class: "action-bar" });
  return el("div", { class: "action-bar" }, [
    el("button", { class: "btn", type: "button", "data-action": "pause" }, t("pause")),
    el("button", { class: "btn", type: "button", "data-action": "resume" }, t("resume")),
    el("button", { class: "btn", type: "button", "data-action": "restart" }, t("restartConnector")),
    el("button", { class: "btn", type: "button", "data-action": "restart-tasks" }, t("restartWithTasks")),
    el("button", { class: "btn", type: "button", "data-action": "restart-failed" }, t("restartFailed")),
  ]);
}

function tasksPanel(tasks) {
  const panel = el("section", { class: "panel" }, [el("h3", {}, t("tasks"))]);
  if (!tasks.length) {
    panel.append(el("p", { class: "hint" }, t("noTasks")));
    return panel;
  }
  const table = el("table", { class: "tasks" }, [
    el("thead", {}, [
      el("tr", {}, [
        el("th", {}, "id"),
        el("th", {}, t("state")),
        el("th", {}, t("worker")),
        el("th", {}, ""),
      ]),
    ]),
  ]);
  const body = el("tbody");
  for (const task of tasks) {
    const taskState = task.state || "";
    const restart = hasRole("operator") ? el(
      "button",
      { class: "btn", type: "button", "data-action": "restart-task", "data-task": String(task.id) },
      t("restartTask"),
    ) : null;
    const row = el("tr", {}, [
      el("td", { class: "id" }, String(task.id)),
      el("td", {}, [el("span", { class: `badge ${taskState}` }, taskState || "—")]),
      el("td", { class: "worker" }, task.worker_id || "—"),
      el("td", {}, [restart]),
    ]);
    body.append(row);
    if (task.trace) {
      body.append(el("tr", {}, [el("td", { colspan: "4" }, [el("pre", { class: "trace" }, task.trace)])]));
    }
  }
  table.append(body);
  panel.append(table);
  return panel;
}

function configPanel(config) {
  const secrets = Object.values(config).some(isSecret);
  const panel = el("section", { class: "panel" }, [
    el("h3", {}, t("configuration")),
    secrets
      ? el(
          "p",
          { class: "hint" },
          t("secretMask"),
        )
      : null,
    el("div", { id: "config-body" }),
    hasRole("admin") ? el("div", { class: "editor-actions" }, [
      el("button", { class: "btn", type: "button", "data-action": "add-config" }, t("field")),
      el("button", { class: "btn", type: "button", "data-action": "config-mode" }, "JSON"),
      el("button", { class: "btn", type: "button", "data-action": "validate-config" }, t("validate")),
      el("button", { class: "btn primary", type: "button", "data-action": "save-config" }, t("save")),
    ]) : null,
    el("div", { id: "validation" }),
  ]);
  panel.querySelector("#config-body").append(fieldEditor(config));
  panel.querySelector("#config-body").dataset.mode = "fields";
  return panel;
}

function fieldEditor(config) {
  const wrap = el("div", { id: "config-rows" });
  const entries = Object.entries(config);
  if (!entries.length) entries.push(["connector.class", ""]);
  for (const [key, value] of entries) wrap.append(configRow(key, value));
  return wrap;
}

function configRow(key, value) {
  const keyInput = el("input", { class: "text-input config-key", type: "text", value: key, spellcheck: "false" });
  const valueInput = el("input", {
    class: "text-input config-value" + (isSecret(value) ? " is-secret" : ""),
    type: "text",
    value: value ?? "",
    spellcheck: "false",
  });
  valueInput.addEventListener("input", () => {
    valueInput.classList.toggle("is-secret", isSecret(valueInput.value));
  });
  const remove = el("button", { class: "btn-ghost", type: "button", "data-action": "remove-config" }, "×");
  return el("div", { class: "config-row" }, [keyInput, valueInput, remove]);
}

function isSecret(value) {
  return typeof value === "string" && /^\*+$/.test(value) && value.length >= 2;
}

function jsonEditor(config) {
  return el("textarea", { id: "config-json", spellcheck: "false" }, [JSON.stringify(config, null, 2)]);
}

function readConfigFromDom() {
  const body = $("config-body");
  if (!body) throw new Error(t("configNotOpen"));
  if (body.dataset.mode === "json") {
    let parsed;
    try {
      parsed = JSON.parse($("config-json").value);
    } catch (error) {
      throw new Error(`${t("configNotJson")}: ${error.message}`);
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error(t("configMustObject"));
    }
    return parsed;
  }
  const config = {};
  document.querySelectorAll("#config-rows .config-row").forEach((row) => {
    const key = row.querySelector(".config-key").value.trim();
    const value = row.querySelector(".config-value").value;
    if (!key) return;
    if (Object.prototype.hasOwnProperty.call(config, key)) {
      throw new Error(`${t("duplicateKey")}: ${key}`);
    }
    config[key] = value;
  });
  if (!Object.keys(config).length) throw new Error(t("configEmpty"));
  return config;
}

function replaceConfigEditor(mode, config) {
  const body = $("config-body");
  body.dataset.mode = mode;
  body.replaceChildren(mode === "json" ? jsonEditor(config) : fieldEditor(config));
  const toggle = document.querySelector('[data-action="config-mode"]');
  if (toggle) toggle.textContent = mode === "json" ? t("fields") : "JSON";
}

async function onDetailClick(event) {
  const button = event.target.closest("[data-action]");
  if (!button) return;
  const action = button.dataset.action;
  if (action === "remove-config") {
    button.closest(".config-row")?.remove();
    return;
  }
  if (action === "add-config") {
    $("config-rows")?.append(configRow("", ""));
    return;
  }
  if (action === "config-mode") {
    try {
      const config = readConfigFromDom();
      const body = $("config-body");
      replaceConfigEditor(body.dataset.mode === "json" ? "fields" : "json", config);
    } catch (error) {
      toast(error.message, true);
    }
    return;
  }
  if (!state.selected || state.busy) return;
  const name = state.selected;
  const clusterId = state.clusterId;
  const base = `/api/clusters/${encodeURIComponent(clusterId)}/connectors/${encodeURIComponent(name)}`;
  state.busy = true;
  button.disabled = true;
  try {
    if (action === "refresh-detail") {
      await loadDetail(name);
    } else if (action === "delete") {
      openDelete();
    } else if (action === "pause") {
      await api(`${base}/pause`, { method: "POST" });
      toast(t("pauseSent"));
      await loadDetail(name);
    } else if (action === "resume") {
      await api(`${base}/resume`, { method: "POST" });
      toast(t("resumed"));
      await loadDetail(name);
    } else if (action === "restart") {
      await api(`${base}/restart`, { method: "POST" });
      toast(t("restartSent"));
      await loadDetail(name);
    } else if (action === "restart-tasks") {
      await api(`${base}/restart?include_tasks=true`, { method: "POST" });
      toast(t("restartTasksSent"));
      await loadDetail(name);
    } else if (action === "restart-failed") {
      await api(`${base}/restart?include_tasks=true&only_failed=true`, { method: "POST" });
      toast(t("restartFailedSent"));
      await loadDetail(name);
    } else if (action === "restart-task") {
      await api(`${base}/tasks/${encodeURIComponent(button.dataset.task)}/restart`, { method: "POST" });
      toast(`Task ${button.dataset.task} ${t("taskRestarting")}`);
      await loadDetail(name);
    } else if (action === "save-config") {
      const config = readConfigFromDom();
      await api(`${base}/config`, { method: "PUT", body: JSON.stringify({ config }) });
      toast(t("configSaved"));
      await loadDetail(name);
    } else if (action === "validate-config") {
      const config = readConfigFromDom();
      const result = await api(`/api/clusters/${encodeURIComponent(clusterId)}/plugins/validate`, {
        method: "PUT",
        body: JSON.stringify({ config }),
      });
      showValidation(result);
    }
  } catch (error) {
    toast(error.message, true);
  } finally {
    state.busy = false;
    if (button.isConnected) button.disabled = false;
  }
}

function showValidation(result) {
  const host = modal && modal.type === "create" ? $("create-validation") : $("validation");
  if (!host) return;
  const errors = [];
  for (const item of result.configs || []) {
    const valueErrors = (item.value && item.value.errors) || [];
    for (const message of valueErrors) errors.push(message);
  }
  host.replaceChildren();
  host.className = "validation " + (errors.length ? "is-bad" : "is-ok");
  if (!errors.length) {
    host.textContent = t("validationOk");
    return;
  }
  host.append(el("strong", {}, `${t("errors")}: ${errors.length}`));
  host.append(el("ul", {}, errors.map((message) => el("li", {}, message))));
}

function openDelete() {
  modal = { type: "delete", name: state.selected };
  renderModal();
}

async function openCreate() {
  modal = { type: "create", step: "pick", query: "" };
  renderModal();
  if (state.plugins || state.pluginsError) return;
  try {
    const data = await api(`/api/clusters/${encodeURIComponent(state.clusterId)}/plugins`);
    state.plugins = data.plugins || [];
  } catch (error) {
    state.pluginsError = error.message;
  }
  if (modal && modal.type === "create" && modal.step === "pick") renderPluginResults();
}

function closeModal() {
  modal = null;
  const root = $("modal");
  root.hidden = true;
  root.replaceChildren();
}

function renderModal() {
  const root = $("modal");
  if (!modal) {
    closeModal();
    return;
  }
  root.hidden = false;
  if (modal.type === "delete") {
    const confirm = el("button", { class: "btn danger", type: "button", id: "confirm-delete" }, t("delete"));
    const cancel = el("button", { class: "btn", type: "button" }, t("cancel"));
    cancel.addEventListener("click", closeModal);
    confirm.addEventListener("click", () => {
      void confirmDelete(confirm);
    });
    root.replaceChildren(
      el("div", { class: "dialog", role: "dialog", "aria-modal": "true" }, [
        el("h2", {}, t("deleteConnector")),
        el("p", { class: "hint" }, t("deleteHint")),
        el("p", {}, [el("strong", {}, modal.name)]),
        el("div", { class: "dialog-actions" }, [cancel, confirm]),
      ]),
    );
    return;
  }
  syncCreateDraft();
  if (modal.step === "edit") renderCreateEditor(root);
  else renderPluginPicker(root);
}

function renderPluginPicker(root) {
  const query = el("input", {
    id: "plugin-query",
    class: "text-input",
    type: "search",
    placeholder: t("pluginFilter"),
    autocomplete: "off",
    spellcheck: "false",
    value: modal.query || "",
  });
  query.addEventListener("input", () => {
    modal.query = query.value;
    renderPluginResults();
  });
  const manual = el("button", { class: "btn-ghost dialog-aside", type: "button" }, t("manualClass"));
  manual.addEventListener("click", () => selectPlugin({ class: "", type: null, manual: true }));
  const cancel = el("button", { class: "btn", type: "button" }, t("cancel"));
  cancel.addEventListener("click", closeModal);
  root.replaceChildren(
    el("div", { class: "dialog dialog-wide dialog-picker", role: "dialog", "aria-modal": "true", "aria-labelledby": "create-title" }, [
      el("h2", { id: "create-title" }, t("newConnector")),
      el("p", { class: "hint" }, t("createHint")),
      query,
      el("div", { id: "plugin-results", class: "plugin-results" }),
      el("div", { class: "dialog-actions" }, [manual, cancel]),
    ]),
  );
  renderPluginResults();
  query.focus();
}

function renderPluginResults() {
  const host = $("plugin-results");
  if (!host) return;
  if (state.pluginsError) {
    host.replaceChildren(el("div", { class: "status-line" }, state.pluginsError));
    return;
  }
  if (!state.plugins) {
    host.replaceChildren(el("div", { class: "status-line" }, t("loadingPlugins")));
    return;
  }
  const query = (modal?.query || "").trim().toLowerCase();
  const matches = state.plugins.filter((plugin) => {
    const blob = `${plugin.class} ${plugin.type || ""}`.toLowerCase();
    return !query || blob.includes(query);
  });
  if (!matches.length) {
    host.replaceChildren(el("div", { class: "status-line" }, t("noPlugins")));
    return;
  }
  const groups = [
    ["sources", matches.filter((plugin) => plugin.type === "source")],
    ["sinks", matches.filter((plugin) => plugin.type === "sink")],
    ["otherPlugins", matches.filter((plugin) => plugin.type !== "source" && plugin.type !== "sink")],
  ];
  host.replaceChildren(
    ...groups
      .filter(([, items]) => items.length)
      .map(([label, items]) =>
        el("section", { class: "plugin-group" }, [
          el("h3", {}, [t(label), el("span", {}, String(items.length))]),
          el("div", { class: "plugin-grid" }, items.map(pluginOption)),
        ]),
      ),
  );
}

function pluginName(className) {
  return className.split(".").pop() || className;
}

function pluginOption(plugin) {
  const button = el("button", {
    class: "plugin-option",
    type: "button",
    title: [plugin.class, plugin.version].filter(Boolean).join(" · "),
  }, [
    el("strong", {}, pluginName(plugin.class)),
    el("span", {}, plugin.class),
  ]);
  button.addEventListener("click", () => selectPlugin(plugin));
  return button;
}

function selectPlugin(plugin) {
  modal.step = "edit";
  modal.plugin = plugin;
  modal.mode = "properties";
  modal.config = { name: "", "connector.class": plugin.class, "tasks.max": "1" };
  modal.version = 0;
  modal.drafts = {};
  prepareDraft("properties");
  renderModal();
}

function renderCreateEditor(root) {
  const { plugin } = modal;
  const back = el("button", { class: "btn-ghost create-back", type: "button" }, `← ${t("backToPlugins")}`);
  back.addEventListener("click", () => {
    modal.step = "pick";
    renderModal();
  });
  const tabs = el("div", { class: "tabs", role: "tablist" }, [["properties", t("properties")], ["json", "JSON"], ["curl", "cURL"]].map(([mode, label]) => {
    const active = modal.mode === mode;
    const tab = el("button", { class: "tab" + (active ? " is-active" : ""), type: "button", role: "tab", "aria-selected": String(active) }, label);
    tab.addEventListener("click", () => switchCreateMode(mode));
    return tab;
  }));
  const body = createEditorBody();
  const cancel = el("button", { class: "btn", type: "button" }, t("cancel"));
  cancel.addEventListener("click", closeModal);
  const validate = el("button", { class: "btn", type: "button", id: "create-validate" }, t("validate"));
  validate.addEventListener("click", () => {
    void submitCreate(validate, true);
  });
  const submit = el("button", { class: "btn primary", type: "submit", id: "create-submit" }, t("create"));
  const form = el("form", { class: "dialog dialog-wide", id: "create-form", role: "dialog", "aria-modal": "true", "aria-labelledby": "create-title" }, [
    el("header", { class: "create-head" }, [
      back,
      el("h2", { id: "create-title" }, plugin.class ? pluginName(plugin.class) : t("newConnector")),
      el("div", { class: "create-class" }, [
        el("code", {}, plugin.class || "connector.class"),
        plugin.type ? el("span", { class: "plugin-type" }, plugin.type) : null,
      ]),
    ]),
    tabs,
    el("div", { class: "create-body" }, body),
    el("div", { id: "create-validation" }),
    el("div", { class: "dialog-actions" }, [cancel, validate, submit]),
  ]);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    void submitCreate(submit, false);
  });
  root.replaceChildren(form);
  $("create-text")?.focus();
}

function createEditorBody() {
  if (modal.mode === "curl") {
    const command = curlCommand(modal.config);
    const copy = el("button", { class: "btn", type: "button" }, t("copy"));
    copy.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(command);
        toast(t("copied"));
      } catch (error) {
        toast(error.message, true);
      }
    });
    const broken = brokenDraft();
    return [
      broken
        ? el("div", { class: "validation is-bad editor-error", role: "alert" }, `${tabLabel(broken.mode)}: ${t("staleCurl")}`)
        : el("p", { class: "hint editor-hint" }, t("curlHint")),
      el("pre", { class: "create-curl", id: "create-curl" }, command),
      el("div", { class: "editor-actions" }, copy),
    ];
  }
  const properties = modal.mode === "properties";
  const draft = modal.drafts[modal.mode] || { text: "" };
  return [
    draft.error ? el("div", { class: "validation is-bad editor-error", role: "alert" }, draft.error) : null,
    properties && !draft.error ? el("p", { class: "hint editor-hint" }, t("propertiesHint")) : null,
    el("textarea", {
      id: "create-text",
      class: "create-text",
      spellcheck: "false",
      autocomplete: "off",
      readonly: Boolean(draft.readonly),
      dataset: { mode: modal.mode },
      "aria-label": tabLabel(modal.mode),
      placeholder: properties ? "key=value" : null,
    }, [draft.text]),
  ];
}

function tabLabel(mode) {
  return mode === "properties" ? t("properties") : mode === "json" ? "JSON" : "cURL";
}

function syncCreateDraft() {
  if (!modal || modal.type !== "create") return;
  if (modal.step === "pick") {
    const query = $("plugin-query");
    if (query) modal.query = query.value;
    return;
  }
  const text = $("create-text");
  const mode = text?.dataset?.mode;
  if (!mode || text.readOnly) return;
  if (modal.drafts[mode]?.text !== text.value) modal.drafts[mode] = { text: text.value };
}

const configText = (value) => (value == null ? "" : typeof value === "string" ? value : JSON.stringify(value));

function parseProperties(text) {
  const config = {};
  text.split(/\r?\n/).forEach((raw, index) => {
    const line = raw.trim();
    if (!line || line.startsWith("#")) return;
    const split = line.indexOf("=");
    const key = split < 0 ? "" : line.slice(0, split).trim();
    if (!key) throw new Error(`${t("line")} ${index + 1}: ${t("expectedKeyValue")}`);
    if (Object.prototype.hasOwnProperty.call(config, key)) throw new Error(`${t("duplicateKey")}: ${key}`);
    config[key] = line.slice(split + 1).trim();
  });
  return config;
}

function toProperties(config) {
  return Object.entries(config)
    .map(([key, value]) => {
      const text = configText(value);
      if (/[\r\n]/.test(text)) throw new Error(`${key}: ${t("multilineValue")}`);
      return `${key}=${text}`;
    })
    .join("\n");
}

function parseJsonConfig(text) {
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new Error(`${t("configNotJson")}: ${error.message}`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error(t("configMustObject"));
  return parsed;
}

function parseConfigText(mode, text) {
  return mode === "json" ? parseJsonConfig(text) : parseProperties(text);
}

function formatConfigText(mode, config) {
  return mode === "json" ? JSON.stringify(config, null, 2) : toProperties(config);
}

function propertiesPreview(config) {
  return Object.entries(config)
    .map(([key, value]) => {
      const text = configText(value);
      return /[\r\n]/.test(text) ? `# ${key}: ${t("multilineValue")}` : `${key}=${text}`;
    })
    .join("\n");
}

function sameConfig(left, right) {
  const normalize = (config) => JSON.stringify(Object.keys(config).sort().map((key) => [key, configText(config[key])]));
  return normalize(left) === normalize(right);
}

function commitDraft(mode) {
  const draft = modal.drafts[mode];
  if (!draft || draft.readonly) return null;
  let parsed;
  try {
    parsed = parseConfigText(mode, draft.text);
  } catch (error) {
    modal.drafts[mode] = { text: draft.text, error: error.message, version: modal.version };
    return error.message;
  }
  if (!sameConfig(parsed, modal.config)) modal.version += 1;
  modal.config = parsed;
  modal.drafts[mode] = { text: draft.text };
  const klass = configText(parsed["connector.class"]).trim();
  if (klass !== modal.plugin.class) {
    modal.plugin = state.plugins?.find((plugin) => plugin.class === klass) || { class: klass, type: null, manual: true };
  }
  return null;
}

function describesConfig(mode, text, config) {
  try {
    return sameConfig(parseConfigText(mode, text), config);
  } catch {
    return false;
  }
}

function prepareDraft(mode) {
  const kept = modal.drafts[mode];
  if (kept && !kept.readonly) {
    if (kept.error ? kept.version === modal.version : describesConfig(mode, kept.text, modal.config)) return;
  }
  try {
    modal.drafts[mode] = { text: formatConfigText(mode, modal.config) };
  } catch (error) {
    modal.drafts[mode] = { text: propertiesPreview(modal.config), readonly: true, error: error.message };
  }
}

function brokenDraft() {
  for (const mode of ["properties", "json"]) {
    const draft = modal.drafts[mode];
    if (draft?.error && !draft.readonly && draft.version === modal.version) return { mode, ...draft };
  }
  return null;
}

function switchCreateMode(mode) {
  if (modal.mode === mode) return;
  syncCreateDraft();
  const error = modal.mode === "curl" ? null : commitDraft(modal.mode);
  if (error) toast(`${tabLabel(modal.mode)}: ${error}`, true);
  if (mode !== "curl") prepareDraft(mode);
  modal.mode = mode;
  renderModal();
}

function withoutCredentials(raw) {
  try {
    const url = new URL(raw);
    url.username = "";
    url.password = "";
    return url.toString();
  } catch {
    return raw;
  }
}

function curlCommand(config) {
  const quote = (value) => `'${String(value).replace(/'/g, "'\\''")}'`;
  const base = withoutCredentials(currentCluster()?.url || "http://localhost:8083");
  const body = JSON.stringify({ name: configText(config.name).trim(), config }, null, 2);
  return [
    `curl -X POST ${quote(base.replace(/\/+$/, "") + "/connectors")} \\`,
    "  -H 'Content-Type: application/json' \\",
    `  --data ${quote(body)}`,
  ].join("\n");
}

function readCreateConfig() {
  syncCreateDraft();
  const error = modal.mode === "curl" ? brokenDraft()?.error : commitDraft(modal.mode);
  if (error) throw new Error(error);
  const config = modal.config;
  const name = typeof config.name === "string" ? config.name.trim() : "";
  const connectorClass = config["connector.class"];
  if (!name) throw new Error(t("nameRequired"));
  if (name.includes("/") || name.includes("\\")) throw new Error(t("invalidName"));
  if (typeof connectorClass !== "string" || !connectorClass.trim()) throw new Error(t("classRequired"));
  return { name, config: { ...config, name } };
}

async function submitCreate(button, validateOnly) {
  if (state.busy) return;
  let payload;
  try {
    payload = readCreateConfig();
  } catch (error) {
    toast(error.message, true);
    return;
  }
  state.busy = true;
  button.disabled = true;
  try {
    if (validateOnly) {
      const result = await api(`/api/clusters/${encodeURIComponent(state.clusterId)}/plugins/validate`, {
        method: "PUT",
        body: JSON.stringify({ config: payload.config }),
      });
      showValidation(result);
      return;
    }
    await api(`/api/clusters/${encodeURIComponent(state.clusterId)}/connectors`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    const created = payload.name;
    closeModal();
    toast(`${t("created")} ${created}`);
    await loadNames();
    const next = `#${encodeURIComponent(state.clusterId)}/${encodeURIComponent(created)}`;
    if (location.hash !== next) location.hash = next;
    else await loadDetail(created);
  } catch (error) {
    toast(error.message, true);
  } finally {
    state.busy = false;
    if (button.isConnected) button.disabled = false;
  }
}

async function confirmDelete(button) {
  if (!modal || modal.type !== "delete" || state.busy) return;
  const name = modal.name;
  const clusterId = state.clusterId;
  state.busy = true;
  button.disabled = true;
  try {
    await api(`/api/clusters/${encodeURIComponent(clusterId)}/connectors/${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
    closeModal();
    toast(`${t("deleted")} ${name}`);
    await loadNames();
    const next = "#" + encodeURIComponent(clusterId);
    if (location.hash !== next) location.hash = next;
    else {
      state.selected = null;
      state.detail = null;
      renderDetail();
    }
  } catch (error) {
    toast(error.message, true);
    button.disabled = false;
  } finally {
    state.busy = false;
  }
}

function onKeydown(event) {
  if (event.key === "Escape" && modal) {
    closeModal();
    return;
  }
  const typing = document.activeElement && /^(INPUT|TEXTAREA)$/.test(document.activeElement.tagName);
  if (event.key === "/" && !typing) {
    event.preventDefault();
    $("search")?.focus();
  }
}

void boot().catch((error) => {
  console.error("KCV failed to start", error);
  const pane = document.getElementById("detail-pane");
  if (pane) {
    const box = document.createElement("div");
    box.className = "empty";
    const title = document.createElement("h2");
    title.textContent = "UI startup error";
    const message = document.createElement("pre");
    message.className = "trace";
    message.textContent = error?.message || String(error);
    box.append(title, message);
    pane.replaceChildren(box);
  }
});
