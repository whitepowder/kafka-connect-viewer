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
  view: "list",
  namesCluster: null,
  configEditor: null,
  configMode: "properties",
};

const graphView = {
  data: null,
  loading: false,
  error: null,
  seq: 0,
  query: "",
  problemsOnly: false,
  focus: null,
  tab: "diagnostics",
  showOk: false,
};

let listSeq = 0;
let detailSeq = 0;
let hashSeq = 0;
let toastTimer = 0;
let modal = null;

const translations = {
  en: {
    requestError: "Request failed", checkFields: "Check the form fields", noClusters: "No clusters configured",
    namesOnly: "list loads names only", clusters: "Clusters", connectors: "Connectors", refresh: "Refresh",
    add: "Add", searchByName: "Search by name", listNote: "Status and config are fetched only for the connector you open.",
    loadingNames: "Loading names…", retry: "Retry", noConnectors: "There are no connectors on this worker.",
    noNames: "No connector names match this search.", shownOf: "{visible} of {total}", connectorNotSelected: "Select a connector",
    selectHint: "Choose a connector on the left to open its details.",
    connector: "Connector", loadingDetail: "Loading status and config…", delete: "Delete", pause: "Pause",
    resume: "Resume", restartConnector: "Restart connector", restartWithTasks: "Connector + tasks", restartFailed: "Restart failed",
    tasks: "Tasks", noTasks: "No tasks.", state: "state", worker: "worker", restartTask: "Restart task",
    configuration: "Configuration", secretMask: "Asterisks are a secret mask. Replace the mask with a new secret value before saving.",
    validate: "Validate", save: "Save", configNotOpen: "Config is not open",
    configNotJson: "Config is not valid JSON", configMustObject: "Config must be a JSON object", duplicateKey: "Duplicate key",
    configEmpty: "Config is empty", pauseSent: "Pause requested", resumed: "Connector resumed",
    restartSent: "Restart requested", restartTasksSent: "Connector and tasks restart requested",
    restartFailedSent: "Failed tasks restart requested", taskRestarting: "Task {task} is restarting", configSaved: "Config saved", configUnsaved: "Unsaved changes", discardChanges: "Discard changes",
    validationOk: "Config passes plugin validation.", errors: "Errors", cancel: "Cancel", deleteConnector: "Delete connector",
    deleteHint: "The worker will stop its tasks and remove the connector. This cannot be undone.", pluginFilter: "Filter plugins",
    create: "Create", newConnector: "New connector", createHint: "Choose a plugin installed on this worker.",
    name: "Name", extraFields: "Additional fields", loadingPlugins: "Loading plugins…",
    noPlugins: "No plugins found. You can enter the class manually.", nameRequired: "Connector name is required",
    sources: "Sources", sinks: "Sinks", otherPlugins: "Other", backToPlugins: "Plugins",
    properties: "Properties", manualClass: "Enter class manually", copy: "Copy", copied: "Copied",
    propertiesHint: "One key=value pair per line. Blank lines and # comments are ignored.",
    curlHint: "Worker REST API request for the current config. Credentials are not included.", curlUpdateHint: "Kafka Connect REST API request that replaces this connector's config with the current edits. Set CONNECT_URL to the worker REST URL before running. Credentials and authorization headers are not included.",
    curlSecrets: "Masked secrets are not included. Set {variables} to the real values before running; the command stops if they are not set.",
    line: "Line", expectedKeyValue: "expected key=value", multilineValue: "value contains a line break, edit it in JSON",
    staleCurl: "has an error, so this command uses the last valid config", devMode: "Development mode",
    invalidName: "Name cannot contain / or \\", classRequired: "connector.class is required", created: "Created", deleted: "Deleted",
    language: "Language", logout: "Logout",
    graph: "Graph", graphSearch: "Search connectors and topics", onlyProblems: "Only problems", graphLoading: "Building graph…",
    graphNoMatch: "Nothing matches.", graphPartial: "The graph is partial: {n} connector configs could not be read.",
    graphCounter: "{errors} errors · {warnings} warnings",
    topicsColumn: "Kafka topics", diagnostics: "Diagnostics", details: "Details", severityError: "Errors",
    severityWarning: "Warnings", severityOk: "No conflict", show: "Show", hide: "Hide",
    noDiagnostics: "No problems found.", selectNodeHint: "Click a node in the graph to see its details.",
    openConnector: "Open connector", groupExplicit: "group: {group}", groupDerived: "group: default, expected",
    groupUnknown: "group: undetermined", unknownType: "unknown type",
    bootstrapOverride: "consumer.override.bootstrap.servers is set (value hidden): may read another Kafka cluster",
    notAnalyzable: "pattern cannot be analyzed", legendDeclared: "declared in config", legendPattern: "via pattern",
    legendPossible: "possible", legendPatternNode: "pattern", originExplicit: "explicit", originDerived: "default, expected",
    originUnknown: "undetermined", bootstrapSet: "set, value hidden", undeterminedKeys: "Could not determine",
    type: "type", class: "class", group: "group", writtenBy: "Written by", readBy: "Read by", dlqOf: "DLQ of", topic: "Topic",
    pattern: "Pattern", targetTopic: "topic {topic}", targetPatterns: "overlapping patterns",
    d_shared_topic_same_group: "{connectors} read {topic} with the same group {group}: partitions are split between them, so each gets only part of the data.",
    d_bootstrap_override: "{connectors} use group {group} on {topic}, but {overridden} override bootstrap.servers and may read another Kafka cluster.",
    d_same_group_as_default: "{connectors} share group {group} ({target}); for one of them it is the default connect-<name> group.",
    d_shared_topic_different_groups: "{connectors} read {topic} with different groups.",
    d_shared_topic_different_groups_expected: "{connectors} read {topic} with different groups (default groups are expected, not guaranteed).",
    d_group_unknown: "The group of {undetermined} is undetermined, so a conflict ({target}) cannot be ruled out.",
    d_possible_regex_overlap_same_group: "{connectors} use group {group} and their patterns may overlap: {patterns}.",
    d_topics_and_regex: "{connectors} sets both topics and topics.regex.",
    d_topics_unknown: "The topics of {connectors} could not be determined.",
    d_config_unreadable: "The config of {connectors} could not be read: {message}",
    e_auth_required: "Authentication required", e_role_required: "The {role} role is required",
    e_cluster_not_found: "Cluster not found", e_config_empty: "Connector config is empty",
    e_config_key_invalid: "Config keys must be non-empty strings", e_config_value_not_string: "The value of “{key}” must be a string",
    e_connector_class_missing: "The config has no connector.class", e_connector_config_failed: "The config could not be read",
    e_connector_read_failed: "Could not read the connector", e_cross_origin_write: "Cross-origin writes are not allowed",
    e_graph_refresh_limited: "The graph can be refreshed at most once every {seconds} s",
    e_invalid_connector_name: "Connector name must not be empty and cannot contain / or \\",
    e_invalid_content_length: "Invalid Content-Length", e_request_too_large: "Request body is too large",
    e_oidc_flow_missing: "Login session not found, start the login again", e_oidc_flow_invalid: "Login session is invalid, start the login again",
    e_oidc_state_invalid: "Invalid OIDC state", e_oidc_token_exchange_failed: "Keycloak token exchange failed",
    e_oidc_id_token_missing: "Keycloak did not return an id_token", e_oidc_id_token_invalid: "Invalid Keycloak ID token",
    e_oidc_nonce_invalid: "Invalid OIDC nonce", e_oidc_no_role: "No KCV role is assigned to this user",
    e_secret_masked: "Secret “{key}” is masked by the worker; enter a new value before saving",
    e_secret_restore_failed: "Could not safely restore masked secrets", e_task_id_negative: "Task number must not be negative",
    e_upstream_empty_connector_config: "Kafka Connect returned an empty connector config",
    e_upstream_empty_connector_info: "Kafka Connect returned an empty connector description",
    e_upstream_empty_connector_status: "Kafka Connect returned an empty connector status", e_upstream_empty_response: "Kafka Connect returned an empty response",
    e_upstream_invalid_connector_list: "Kafka Connect did not return a list of connector names",
    e_upstream_invalid_plugin_list: "Kafka Connect did not return a list of plugins", e_upstream_not_json: "Kafka Connect did not return JSON",
    e_upstream_status: "Kafka Connect responded with {status}", e_upstream_timeout: "Kafka Connect did not respond in time",
    e_upstream_unreachable: "Cannot reach Kafka Connect: {error}"
  },
  ru: {
    requestError: "Ошибка запроса", checkFields: "Проверьте поля формы", noClusters: "Кластеры не настроены",
    namesOnly: "в списке только имена", clusters: "Кластеры", connectors: "Коннекторы", refresh: "Обновить",
    add: "Добавить", searchByName: "Поиск по имени", listNote: "Состояние и конфиг запрашиваются только у открытого коннектора.",
    loadingNames: "Загружаю имена…", retry: "Повторить", noConnectors: "На этом воркере нет коннекторов.",
    noNames: "Нет имён по этому запросу.", shownOf: "{visible} из {total}", connectorNotSelected: "Выберите коннектор",
    selectHint: "Выберите коннектор слева, чтобы открыть детали.",
    connector: "Коннектор", loadingDetail: "Загружаю состояние и конфиг…", delete: "Удалить", pause: "Пауза",
    resume: "Возобновить", restartConnector: "Перезапустить коннектор", restartWithTasks: "Коннектор + задачи", restartFailed: "Перезапустить упавшие",
    tasks: "Задачи", noTasks: "Задач нет.", state: "состояние", worker: "воркер", restartTask: "Перезапустить задачу",
    configuration: "Конфигурация", secretMask: "Звёздочки — маска секрета. Для сохранения замените маску новым значением секрета.",
    validate: "Проверить", save: "Сохранить", configNotOpen: "Конфиг не открыт",
    configNotJson: "Конфиг не JSON", configMustObject: "Конфиг должен быть JSON-объектом", duplicateKey: "Повторяющийся ключ",
    configEmpty: "Конфиг пуст", pauseSent: "Пауза отправлена", resumed: "Коннектор возобновлён",
    restartSent: "Перезапуск отправлен", restartTasksSent: "Перезапуск с задачами отправлен",
    restartFailedSent: "Перезапуск упавших задач отправлен", taskRestarting: "Задача {task} перезапускается", configSaved: "Конфиг сохранён", configUnsaved: "Есть несохранённые изменения", discardChanges: "Отменить изменения",
    validationOk: "Конфиг проходит проверку плагина.", errors: "Ошибок", cancel: "Отмена", deleteConnector: "Удалить коннектор",
    deleteHint: "Воркер остановит задачи и уберёт коннектор. Это нельзя отменить.", pluginFilter: "Фильтр плагинов",
    create: "Создать", newConnector: "Новый коннектор", createHint: "Выберите плагин, установленный на воркере.",
    name: "Имя", extraFields: "Дополнительные поля", loadingPlugins: "Загружаю плагины…",
    noPlugins: "Плагины не найдены. Класс можно указать вручную.", nameRequired: "Нужно имя коннектора",
    sources: "Источники", sinks: "Приёмники", otherPlugins: "Другие", backToPlugins: "Плагины",
    properties: "Свойства", manualClass: "Указать класс вручную", copy: "Копировать", copied: "Скопировано",
    propertiesHint: "По одной паре key=value на строку. Пустые строки и # комментарии пропускаются.",
    curlHint: "Запрос к REST API воркера по текущему конфигу. Учётные данные не подставляются.", curlUpdateHint: "Запрос к REST API Kafka Connect, который заменяет конфиг этого коннектора текущими правками. Перед запуском задайте CONNECT_URL — REST URL воркера. Учётные данные и заголовки авторизации не подставляются.",
    curlSecrets: "Скрытые секреты не подставляются. Перед запуском задайте {variables} реальными значениями; без них команда не выполнится.",
    line: "Строка", expectedKeyValue: "нужен формат key=value", multilineValue: "значение с переносом строки, редактируйте его в JSON",
    staleCurl: "есть ошибка, команда собрана из последнего корректного конфига", devMode: "Режим разработки",
    invalidName: "Имя не может содержать / или \\", classRequired: "Нужен connector.class", created: "Создан", deleted: "Удалён",
    language: "Язык", logout: "Выйти",
    graph: "Граф", graphSearch: "Поиск коннекторов и топиков", onlyProblems: "Только проблемы", graphLoading: "Строю граф…",
    graphNoMatch: "Ничего не найдено.", graphPartial: "Граф неполный: не удалось прочитать конфиг у {n} коннекторов.",
    graphCounter: "Ошибок: {errors} · Предупреждений: {warnings}",
    topicsColumn: "Топики Kafka", diagnostics: "Диагностика", details: "Детали", severityError: "Ошибки",
    severityWarning: "Предупреждения", severityOk: "Без конфликтов", show: "Показать", hide: "Скрыть",
    noDiagnostics: "Проблем не найдено.", selectNodeHint: "Нажмите на узел графа, чтобы увидеть детали.",
    openConnector: "Открыть коннектор", groupExplicit: "группа: {group}", groupDerived: "группа: по умолчанию, ожидаемо",
    groupUnknown: "группа: не определена", unknownType: "тип не определён",
    bootstrapOverride: "Задан consumer.override.bootstrap.servers (значение скрыто): коннектор может читать другой кластер Kafka",
    notAnalyzable: "шаблон не анализируется", legendDeclared: "задано в конфиге", legendPattern: "по шаблону",
    legendPossible: "возможно", legendPatternNode: "шаблон", originExplicit: "явная", originDerived: "по умолчанию, ожидаемо",
    originUnknown: "не определена", bootstrapSet: "задан, значение скрыто", undeterminedKeys: "Не удалось определить",
    type: "тип", class: "класс", group: "группа", writtenBy: "Пишут", readBy: "Читают", dlqOf: "DLQ для", topic: "Топик",
    pattern: "Шаблон", targetTopic: "топик {topic}", targetPatterns: "пересечение шаблонов",
    d_shared_topic_same_group: "{connectors} читают {topic} одной группой {group}: партиции делятся между ними, каждый получает только часть данных.",
    d_bootstrap_override: "{connectors} используют группу {group} на {topic}, но у {overridden} задан свой bootstrap.servers — возможно, это другой кластер Kafka.",
    d_same_group_as_default: "{connectors}: общая группа {group} ({target}); у одного из них это группа по умолчанию connect-<name>.",
    d_shared_topic_different_groups: "{connectors} читают {topic} разными группами.",
    d_shared_topic_different_groups_expected: "{connectors} читают {topic} разными группами (группы по умолчанию ожидаемы, но не гарантированы).",
    d_group_unknown: "Группа {undetermined} не определена, поэтому конфликт ({target}) исключить нельзя.",
    d_possible_regex_overlap_same_group: "{connectors} используют группу {group}, и их шаблоны могут пересекаться: {patterns}.",
    d_topics_and_regex: "У {connectors} заданы и topics, и topics.regex.",
    d_topics_unknown: "Не удалось определить топики {connectors}.",
    d_config_unreadable: "Не удалось прочитать конфиг {connectors}: {message}",
    e_auth_required: "Нужно войти в систему", e_role_required: "Нужна роль {role}",
    e_cluster_not_found: "Кластер не найден", e_config_empty: "Конфиг коннектора пуст",
    e_config_key_invalid: "Ключ конфига должен быть непустой строкой", e_config_value_not_string: "Значение «{key}» должно быть строкой",
    e_connector_class_missing: "В конфиге нет connector.class", e_connector_config_failed: "Конфиг не удалось прочитать",
    e_connector_read_failed: "Не удалось прочитать коннектор", e_cross_origin_write: "Запись с другого сайта запрещена",
    e_graph_refresh_limited: "Граф можно обновлять не чаще раза в {seconds} с",
    e_invalid_connector_name: "Имя коннектора не должно быть пустым и не может содержать / или \\",
    e_invalid_content_length: "Некорректный Content-Length", e_request_too_large: "Тело запроса слишком большое",
    e_oidc_flow_missing: "Сессия входа не найдена, начните вход заново", e_oidc_flow_invalid: "Сессия входа недействительна, начните вход заново",
    e_oidc_state_invalid: "Неверный параметр state OIDC", e_oidc_token_exchange_failed: "Keycloak не выдал токен",
    e_oidc_id_token_missing: "Keycloak не вернул id_token", e_oidc_id_token_invalid: "Недействительный ID-токен Keycloak",
    e_oidc_nonce_invalid: "Неверный nonce OIDC", e_oidc_no_role: "Пользователю не назначена роль KCV",
    e_secret_masked: "Секрет «{key}» скрыт воркером; введите новое значение перед сохранением",
    e_secret_restore_failed: "Не удалось безопасно восстановить скрытые секреты", e_task_id_negative: "Номер задачи должен быть неотрицательным",
    e_upstream_empty_connector_config: "Kafka Connect вернул пустой конфиг коннектора",
    e_upstream_empty_connector_info: "Kafka Connect вернул пустое описание коннектора",
    e_upstream_empty_connector_status: "Kafka Connect вернул пустой статус коннектора", e_upstream_empty_response: "Kafka Connect вернул пустой ответ",
    e_upstream_invalid_connector_list: "Kafka Connect вернул не список имён коннекторов",
    e_upstream_invalid_plugin_list: "Kafka Connect вернул не список плагинов", e_upstream_not_json: "Kafka Connect вернул не JSON",
    e_upstream_status: "Kafka Connect ответил {status}", e_upstream_timeout: "Kafka Connect не ответил вовремя",
    e_upstream_unreachable: "Нет связи с Kafka Connect: {error}"
  },
  "zh-CN": {
    requestError: "请求失败", checkFields: "请检查表单字段", noClusters: "未配置集群",
    namesOnly: "列表仅加载名称", clusters: "集群", connectors: "连接器", refresh: "刷新",
    add: "添加", searchByName: "按名称搜索", listNote: "仅在打开连接器时才获取其状态和配置。",
    loadingNames: "正在加载名称…", retry: "重试", noConnectors: "此 Worker 上没有连接器。",
    noNames: "没有与搜索匹配的连接器名称。", shownOf: "{visible} / {total}", connectorNotSelected: "请选择连接器",
    selectHint: "在左侧选择一个连接器以查看详情。",
    connector: "连接器", loadingDetail: "正在加载状态和配置…", delete: "删除", pause: "暂停",
    resume: "恢复", restartConnector: "重启连接器", restartWithTasks: "连接器 + 任务", restartFailed: "重启失败的任务",
    tasks: "任务", noTasks: "没有任务。", state: "状态", worker: "Worker", restartTask: "重启任务",
    configuration: "配置", secretMask: "星号是密钥掩码。保存前请用新的密钥值替换掩码。",
    validate: "校验", save: "保存", configNotOpen: "配置未打开",
    configNotJson: "配置不是有效的 JSON", configMustObject: "配置必须是 JSON 对象", duplicateKey: "重复的键",
    configEmpty: "配置为空", pauseSent: "已请求暂停", resumed: "连接器已恢复",
    restartSent: "已请求重启", restartTasksSent: "已请求重启连接器和任务",
    restartFailedSent: "已请求重启失败的任务", taskRestarting: "任务 {task} 正在重启", configSaved: "配置已保存", configUnsaved: "有未保存的更改", discardChanges: "放弃更改",
    validationOk: "配置已通过插件校验。", errors: "错误", cancel: "取消", deleteConnector: "删除连接器",
    deleteHint: "Worker 将停止其任务并删除该连接器。此操作无法撤销。", pluginFilter: "筛选插件",
    create: "创建", newConnector: "新建连接器", createHint: "选择此 Worker 上已安装的插件。",
    name: "名称", extraFields: "附加字段", loadingPlugins: "正在加载插件…",
    noPlugins: "未找到插件。可以手动输入类名。", nameRequired: "必须填写连接器名称",
    sources: "Source 连接器", sinks: "Sink 连接器", otherPlugins: "其他", backToPlugins: "插件",
    properties: "属性", manualClass: "手动输入类名", copy: "复制", copied: "已复制",
    propertiesHint: "每行一个 key=value。空行和 # 注释会被忽略。",
    curlHint: "当前配置对应的 Worker REST API 请求。不包含凭据。", curlUpdateHint: "用当前编辑内容替换此连接器配置的 Kafka Connect REST API 请求。运行前请将 CONNECT_URL 设置为 Worker 的 REST URL。不包含凭据和授权头。",
    curlSecrets: "被掩码的密钥不会包含在内。运行前请将 {variables} 设置为真实值；未设置时命令不会执行。",
    line: "行", expectedKeyValue: "应为 key=value", multilineValue: "值包含换行符，请在 JSON 中编辑",
    staleCurl: "存在错误，因此此命令使用最后一个有效配置", devMode: "开发模式",
    invalidName: "名称不能包含 / 或 \\", classRequired: "必须填写 connector.class", created: "已创建", deleted: "已删除",
    language: "语言", logout: "退出登录",
    graph: "图", graphSearch: "搜索连接器和主题", onlyProblems: "仅显示问题", graphLoading: "正在构建图…",
    graphNoMatch: "没有匹配项。", graphPartial: "图不完整：{n} 个连接器配置无法读取。",
    graphCounter: "{errors} 个错误 · {warnings} 个警告",
    topicsColumn: "Kafka 主题", diagnostics: "诊断", details: "详情", severityError: "错误",
    severityWarning: "警告", severityOk: "无冲突", show: "显示", hide: "隐藏",
    noDiagnostics: "未发现问题。", selectNodeHint: "点击图中的节点查看详情。",
    openConnector: "打开连接器", groupExplicit: "消费者组：{group}", groupDerived: "消费者组：默认（推断）",
    groupUnknown: "消费者组：无法确定", unknownType: "未知类型",
    bootstrapOverride: "已设置 consumer.override.bootstrap.servers（值已隐藏）：可能读取其他 Kafka 集群",
    notAnalyzable: "无法分析该模式", legendDeclared: "在配置中声明", legendPattern: "通过模式匹配",
    legendPossible: "可能", legendPatternNode: "模式", originExplicit: "显式指定", originDerived: "默认（推断）",
    originUnknown: "无法确定", bootstrapSet: "已设置，值已隐藏", undeterminedKeys: "无法确定",
    type: "类型", class: "类", group: "消费者组", writtenBy: "写入方", readBy: "读取方", dlqOf: "DLQ 来源", topic: "主题",
    pattern: "模式", targetTopic: "主题 {topic}", targetPatterns: "重叠的模式",
    d_shared_topic_same_group: "{connectors} 使用同一消费者组 {group} 读取 {topic}：分区会在它们之间分配，因此每个连接器只能获得部分数据。",
    d_bootstrap_override: "{connectors} 在 {topic} 上使用消费者组 {group}，但 {overridden} 覆盖了 bootstrap.servers，可能读取其他 Kafka 集群。",
    d_same_group_as_default: "{connectors} 共用消费者组 {group}（{target}）；对其中一个连接器而言，这是默认的 connect-<name> 组。",
    d_shared_topic_different_groups: "{connectors} 使用不同的消费者组读取 {topic}。",
    d_shared_topic_different_groups_expected: "{connectors} 使用不同的消费者组读取 {topic}（默认组为推断结果，不保证准确）。",
    d_group_unknown: "无法确定 {undetermined} 的消费者组，因此不能排除冲突（{target}）。",
    d_possible_regex_overlap_same_group: "{connectors} 使用消费者组 {group}，且它们的模式可能重叠：{patterns}。",
    d_topics_and_regex: "{connectors} 同时设置了 topics 和 topics.regex。",
    d_topics_unknown: "无法确定 {connectors} 的主题。",
    d_config_unreadable: "无法读取 {connectors} 的配置：{message}",
    e_auth_required: "需要登录", e_role_required: "需要 {role} 角色",
    e_cluster_not_found: "未找到集群", e_config_empty: "连接器配置为空",
    e_config_key_invalid: "配置键必须是非空字符串", e_config_value_not_string: "“{key}”的值必须是字符串",
    e_connector_class_missing: "配置中缺少 connector.class", e_connector_config_failed: "无法读取配置",
    e_connector_read_failed: "无法读取连接器", e_cross_origin_write: "不允许跨源写入",
    e_graph_refresh_limited: "图最多每 {seconds} 秒刷新一次",
    e_invalid_connector_name: "连接器名称不能为空，且不能包含 / 或 \\",
    e_invalid_content_length: "无效的 Content-Length", e_request_too_large: "请求体过大",
    e_oidc_flow_missing: "未找到登录会话，请重新登录", e_oidc_flow_invalid: "登录会话无效，请重新登录",
    e_oidc_state_invalid: "无效的 OIDC state", e_oidc_token_exchange_failed: "Keycloak 令牌交换失败",
    e_oidc_id_token_missing: "Keycloak 未返回 id_token", e_oidc_id_token_invalid: "无效的 Keycloak ID 令牌",
    e_oidc_nonce_invalid: "无效的 OIDC nonce", e_oidc_no_role: "该用户未分配 KCV 角色",
    e_secret_masked: "密钥“{key}”已被 Worker 掩码；保存前请输入新值",
    e_secret_restore_failed: "无法安全地恢复被掩码的密钥", e_task_id_negative: "任务编号不能为负数",
    e_upstream_empty_connector_config: "Kafka Connect 返回了空的连接器配置",
    e_upstream_empty_connector_info: "Kafka Connect 返回了空的连接器描述",
    e_upstream_empty_connector_status: "Kafka Connect 返回了空的连接器状态", e_upstream_empty_response: "Kafka Connect 返回了空响应",
    e_upstream_invalid_connector_list: "Kafka Connect 未返回连接器名称列表",
    e_upstream_invalid_plugin_list: "Kafka Connect 未返回插件列表", e_upstream_not_json: "Kafka Connect 未返回 JSON",
    e_upstream_status: "Kafka Connect 返回状态 {status}", e_upstream_timeout: "Kafka Connect 未及时响应",
    e_upstream_unreachable: "无法连接 Kafka Connect：{error}"
  }
};

const DEFAULT_LANGUAGE = "en";
const LANGUAGES = Object.keys(translations);
const LANGUAGE_LABELS = { en: "EN", ru: "RU", "zh-CN": "中文" };
let language = localStorage.getItem("kc-language");
if (!LANGUAGES.includes(language)) language = DEFAULT_LANGUAGE;
const hasText = (key) => key in translations[language] || key in translations[DEFAULT_LANGUAGE];
const t = (key) => translations[language][key] ?? translations[DEFAULT_LANGUAGE][key] ?? key;

function setLanguage(next) {
  if (!translations[next] || next === language) return;
  language = next;
  localStorage.setItem("kc-language", language);
  document.documentElement.lang = language;
  renderSidebar();
  if (state.clusterId && state.view === "graph") {
    renderGraphShell();
    renderGraph();
  } else if (state.clusterId) {
    renderListShell();
    renderRows();
    renderDetail();
  }
  if (modal) renderModal();
}

const fmt = (key, values) => t(key).replace(/\{(\w+)\}/g, (match, name) => (values[name] == null ? match : String(values[name])));

function languageControl() {
  const wrap = el("div", { class: "language-switch", title: t("language") });
  for (const code of LANGUAGES) {
    const button = el("button", { class: "language-option", type: "button", lang: code }, LANGUAGE_LABELS[code] || code);
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
    const error = new Error(errorText(data, response.statusText || t("requestError")));
    error.status = response.status;
    error.code = data && typeof data.code === "string" ? data.code : null;
    throw error;
  }
  return data;
}

function errorText(body, fallback = t("requestError")) {
  if (typeof body === "string") return body;
  if (!body || typeof body !== "object") return fallback;
  if (typeof body.code === "string") {
    const key = `e_${body.code}`;
    return hasText(key) ? fmt(key, body.params || {}) : body.code;
  }
  if (typeof body.message === "string") return body.message;
  if (typeof body.detail === "string") return body.detail;
  if (Array.isArray(body.detail)) return t("checkFields");
  return fallback;
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
  let raw = location.hash.replace(/^#/, "");
  let view = "list";
  const query = raw.indexOf("?");
  if (query !== -1) {
    if (/(?:^|&)view=graph(?:&|$)/.test(raw.slice(query + 1))) view = "graph";
    raw = raw.slice(0, query);
  }
  if (!raw) return { clusterId: null, name: null, view };
  const slash = raw.indexOf("/");
  if (slash === -1) return { clusterId: decodePart(raw), name: null, view };
  return {
    clusterId: decodePart(raw.slice(0, slash)),
    name: view === "graph" ? null : decodePart(raw.slice(slash + 1)),
    view,
  };
}

function clusterHash(clusterId, view = "list") {
  return "#" + encodeURIComponent(clusterId) + (view === "graph" ? "?view=graph" : "");
}

function viewSwitch() {
  const wrap = el("div", { class: "view-switch", role: "tablist" });
  for (const [view, label] of [["list", t("connectors")], ["graph", t("graph")]]) {
    const button = el("button", { class: "view-option", type: "button", role: "tab", "data-view": view }, label);
    if (view === state.view) button.classList.add("is-active");
    button.setAttribute("aria-selected", view === state.view ? "true" : "false");
    button.addEventListener("click", () => {
      const next = clusterHash(state.clusterId, view);
      if (location.hash !== next) location.hash = next;
    });
    wrap.append(button);
  }
  return wrap;
}

function clusterMetaText() {
  const cluster = currentCluster();
  if (!cluster) return "";
  const version = state.clusterInfo && state.clusterInfo.version;
  return version ? `${cluster.name} · Connect ${version}` : cluster.name;
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
  const { clusterId, name, view } = parseHash();
  if (!clusterId || !state.clusters.some((cluster) => cluster.id === clusterId)) {
    const next = clusterHash(state.clusters[0].id, view);
    if (location.hash !== next) location.hash = next;
    return;
  }
  const clusterChanged = clusterId !== state.clusterId;
  const viewChanged = view !== state.view;
  state.view = view;
  $("app")?.classList.toggle("is-graph", view === "graph");
  if (clusterChanged) {
    state.clusterId = clusterId;
    state.query = "";
    state.names = [];
    state.namesCluster = null;
    state.selected = null;
    state.detail = null;
    state.detailError = null;
    state.listError = null;
    state.clusterInfo = null;
    state.plugins = null;
    state.pluginsError = null;
    resetGraph();
  }
  renderSidebar();
  if (view === "graph") {
    if (!clusterChanged && !viewChanged) return;
    renderGraphShell();
    renderGraph();
    if (clusterChanged) void loadClusterInfo();
    await loadGraph();
    return;
  }
  if (clusterChanged || viewChanged) {
    renderListShell();
    renderRows();
    renderDetail();
  }
  if (state.namesCluster !== clusterId) {
    await loadNames();
    if (seq !== hashSeq) return;
  }
  if (clusterChanged) void loadClusterInfo();
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
  state.namesCluster = clusterId;
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
  if (meta) meta.textContent = clusterMetaText();
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
    ]);
    if (cluster.id === state.clusterId) button.classList.add("is-selected");
    button.addEventListener("click", () => {
      const next = clusterHash(cluster.id, state.view);
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
      state.me.auth_enabled ? el("a", { href: "/auth/logout", class: "auth-logout" }, t("logout")) : null,
    ]) : null,
    languageControl(),
  );
}

function renderListShell() {
  $("list-pane").replaceChildren(
    el("header", { class: "list-head" }, [
      el("div", {}, [
        el("h1", {}, t("connectors")),
        el("p", { class: "meta", id: "cluster-meta" }, clusterMetaText()),
      ]),
      el("div", { class: "head-actions" }, [
        viewSwitch(),
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
    else if (query) meta.textContent = fmt("shownOf", { visible, total: state.names.length });
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
  if (state.view === "graph") return;
  syncConfigDraft();
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
  if (detail.status_error) scroll.append(el("div", { class: "banner is-error" }, errorText(detail.status_error)));
  if (detail.info_error) scroll.append(el("div", { class: "banner is-error" }, errorText(detail.info_error)));
  if (connector.trace) scroll.append(el("pre", { class: "trace" }, connector.trace));
  scroll.append(actionBar());
  scroll.append(tasksPanel(detail.tasks || []));
  scroll.append(configPanel(detail));
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

const CONFIG_MODES = ["properties", "json", "curl"];

function configPanel(detail) {
  const editor = configEditorFor(detail);
  const secrets = Object.values(editor.base).some(isSecret);
  const host = el("div", { id: "config-editor" });
  host.replaceChildren(...configEditorChildren(editor));
  return el("section", { class: "panel" }, [
    el("h3", {}, t("configuration")),
    secrets ? el("p", { class: "hint" }, t("secretMask")) : null,
    host,
    el("div", { id: "validation" }),
  ]);
}

function configEditorFor(detail) {
  const current = state.configEditor;
  if (current && current.cluster === state.clusterId && current.name === state.selected && configDirty(current)) return current;
  const config = { ...(detail.config || {}) };
  const editor = {
    cluster: state.clusterId,
    name: state.selected,
    base: config,
    config: { ...config },
    mode: state.configMode,
    version: 0,
    drafts: {},
  };
  if (editor.mode !== "curl") prepareEditorDraft(editor, editor.mode);
  state.configEditor = editor;
  return editor;
}

function configDirty(editor) {
  if (!sameConfig(editor.config, editor.base)) return true;
  return Object.entries(editor.drafts).some(([mode, draft]) =>
    !draft.readonly && draft.text !== formattedConfig(mode, editor.base) && !describesConfig(mode, draft.text, editor.base));
}

function configEditorChildren(editor) {
  const admin = hasRole("admin");
  const properties = editor.mode === "properties";
  const draft = editor.drafts[editor.mode] || { text: "" };
  const tabs = el("div", { class: "tabs config-tabs", role: "tablist" }, CONFIG_MODES.map((mode) => {
    const active = editor.mode === mode;
    return el("button", {
      class: "tab" + (active ? " is-active" : ""),
      type: "button",
      role: "tab",
      "aria-selected": String(active),
      "data-action": "config-tab",
      "data-mode": mode,
    }, tabLabel(mode));
  }));
  const text = el("textarea", {
    id: "config-text",
    class: "create-text config-text",
    spellcheck: "false",
    autocomplete: "off",
    readonly: !admin || Boolean(draft.readonly),
    dataset: { mode: editor.mode, connector: editor.name },
    "aria-label": tabLabel(editor.mode),
    placeholder: properties ? "key=value" : null,
  }, [draft.text]);
  const dirty = el("span", { id: "config-dirty", class: "config-dirty" }, t("configUnsaved"));
  const discard = el("button", { class: "btn-ghost", type: "button", id: "config-discard", "data-action": "discard-config" }, t("discardChanges"));
  const updateDirty = () => {
    const changed = configDirty(editor);
    dirty.hidden = !changed;
    discard.hidden = !changed;
  };
  text.addEventListener("input", () => {
    syncConfigDraft();
    updateDirty();
  });
  updateDirty();
  return [
    tabs,
    ...(editor.mode === "curl" ? configCurlBody(editor) : [
      draft.error ? el("div", { class: "validation is-bad editor-error", role: "alert" }, draft.error) : null,
      properties && admin && !draft.error ? el("p", { class: "hint editor-hint" }, t("propertiesHint")) : null,
      text,
    ]),
    admin ? el("div", { class: "editor-actions" }, [
      dirty,
      discard,
      el("button", { class: "btn", type: "button", "data-action": "validate-config" }, t("validate")),
      el("button", { class: "btn primary", type: "button", "data-action": "save-config" }, t("save")),
    ]) : null,
  ].filter(Boolean);
}

function configCurlBody(editor) {
  const { command, variables } = updateCurlCommand(editor.name, editor.config);
  const broken = brokenEditorDraft(editor);
  return [
    broken
      ? el("div", { class: "validation is-bad editor-error", role: "alert" }, `${tabLabel(broken.mode)}: ${t("staleCurl")}`)
      : el("p", { class: "hint editor-hint" }, t("curlUpdateHint")),
    variables.length ? el("p", { class: "hint editor-hint", id: "config-curl-secrets" }, fmt("curlSecrets", { variables: variables.join(", ") })) : null,
    el("pre", { class: "create-curl", id: "config-curl", "aria-label": "cURL" }, command),
    el("div", { class: "editor-actions" }, el("button", { class: "btn", type: "button", "data-action": "copy-curl" }, t("copy"))),
  ];
}

function renderConfigEditor() {
  const host = $("config-editor");
  if (host && state.configEditor) host.replaceChildren(...configEditorChildren(state.configEditor));
}

function syncConfigDraft() {
  const editor = state.configEditor;
  const text = $("config-text");
  if (editor && text?.dataset?.connector === editor.name) syncEditorDraft(editor, text);
}

function switchConfigMode(mode) {
  const editor = state.configEditor;
  if (!editor || editor.mode === mode || !CONFIG_MODES.includes(mode)) return;
  syncConfigDraft();
  const error = commitEditorDraft(editor, editor.mode);
  if (error) toast(`${tabLabel(editor.mode)}: ${error}`, true);
  if (mode !== "curl") prepareEditorDraft(editor, mode);
  editor.mode = mode;
  state.configMode = mode;
  renderConfigEditor();
}

function readConfigEditor() {
  const editor = state.configEditor;
  if (!editor || !$("config-editor")) throw new Error(t("configNotOpen"));
  syncConfigDraft();
  const error = commitEditorDraft(editor, editor.mode);
  const broken = error ? { mode: editor.mode, error } : brokenEditorDraft(editor);
  if (broken) {
    renderConfigEditor();
    throw new Error(`${tabLabel(broken.mode)}: ${broken.error}`);
  }
  if (!Object.keys(editor.config).length) throw new Error(t("configEmpty"));
  return { ...editor.config };
}

function isSecret(value) {
  return typeof value === "string" && /^\*+$/.test(value) && value.length >= 2;
}

async function onDetailClick(event) {
  const button = event.target.closest("[data-action]");
  if (!button) return;
  const action = button.dataset.action;
  if (action === "config-tab") {
    switchConfigMode(button.dataset.mode);
    return;
  }
  if (action === "copy-curl") {
    void copyText($("config-curl")?.textContent || "");
    return;
  }
  if (action === "discard-config") {
    state.configEditor = null;
    renderDetail();
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
      toast(fmt("taskRestarting", { task: button.dataset.task }));
      await loadDetail(name);
    } else if (action === "save-config") {
      const config = readConfigEditor();
      await api(`${base}/config`, { method: "PUT", body: JSON.stringify({ config }) });
      toast(t("configSaved"));
      state.configEditor = null;
      await loadDetail(name);
    } else if (action === "validate-config") {
      const config = readConfigEditor();
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
    copy.addEventListener("click", () => {
      void copyText(command);
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
  syncEditorDraft(modal, $("create-text"));
}

function syncEditorDraft(editor, text) {
  const mode = text?.dataset?.mode;
  if (!mode || text.readOnly) return;
  if (editor.drafts[mode]?.text !== text.value) editor.drafts[mode] = { text: text.value };
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
  const error = commitEditorDraft(modal, mode);
  if (error) return error;
  const klass = configText(modal.config["connector.class"]).trim();
  if (klass !== modal.plugin.class) {
    modal.plugin = state.plugins?.find((plugin) => plugin.class === klass) || { class: klass, type: null, manual: true };
  }
  return null;
}

function commitEditorDraft(editor, mode) {
  const draft = editor.drafts[mode];
  if (!draft || draft.readonly) return null;
  if (draft.text === formattedConfig(mode, editor.config)) {
    editor.drafts[mode] = { text: draft.text };
    return null;
  }
  let parsed;
  try {
    parsed = parseConfigText(mode, draft.text);
  } catch (error) {
    editor.drafts[mode] = { text: draft.text, error: error.message, version: editor.version };
    return error.message;
  }
  if (!sameConfig(parsed, editor.config)) editor.version += 1;
  editor.config = parsed;
  editor.drafts[mode] = { text: draft.text };
  return null;
}

function formattedConfig(mode, config) {
  try {
    return formatConfigText(mode, config);
  } catch {
    return null;
  }
}

function describesConfig(mode, text, config) {
  try {
    return sameConfig(parseConfigText(mode, text), config);
  } catch {
    return false;
  }
}

function prepareDraft(mode) {
  prepareEditorDraft(modal, mode);
}

function prepareEditorDraft(editor, mode) {
  const kept = editor.drafts[mode];
  if (kept && !kept.readonly) {
    if (kept.error ? kept.version === editor.version : describesConfig(mode, kept.text, editor.config)) return;
  }
  try {
    editor.drafts[mode] = { text: formatConfigText(mode, editor.config) };
  } catch (error) {
    editor.drafts[mode] = { text: propertiesPreview(editor.config), readonly: true, error: error.message };
  }
}

function brokenDraft() {
  return brokenEditorDraft(modal);
}

function brokenEditorDraft(editor) {
  for (const mode of ["properties", "json"]) {
    const draft = editor.drafts[mode];
    if (draft?.error && !draft.readonly && draft.version === editor.version) return { mode, ...draft };
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

const shellQuote = (value) => `'${String(value).replace(/'/g, "'\\''")}'`;

function curlRequest(method, target, payload, secrets = new Map()) {
  let data = shellQuote(JSON.stringify(payload, null, 2));
  for (const [token, variable] of secrets) data = data.replace(token, () => `'"\${${variable}:?}"'`);
  return [
    `curl -X ${method} ${target} \\`,
    "  -H 'Content-Type: application/json' \\",
    `  --data ${data}`,
  ].join("\n");
}

function curlCommand(config) {
  const base = withoutCredentials(currentCluster()?.url || "http://localhost:8083");
  return curlRequest("POST", shellQuote(base.replace(/\/+$/, "") + "/connectors"), { name: configText(config.name).trim(), config });
}

function updateCurlCommand(name, config) {
  const secrets = new Map();
  const used = new Set(["CONNECT_URL"]);
  const body = {};
  for (const [key, value] of Object.entries(config)) {
    if (!isSecret(value)) {
      body[key] = value;
      continue;
    }
    const base = key.toUpperCase().replace(/[^A-Z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "SECRET";
    let variable = /^[0-9]/.test(base) ? `SECRET_${base}` : base;
    for (let n = 2; used.has(variable); n += 1) variable = `${base}_${n}`;
    used.add(variable);
    const token = `__KCV_SECRET_${secrets.size}__`;
    secrets.set(token, variable);
    body[key] = token;
  }
  body.name = name;
  const path = `/connectors/${encodeURIComponent(name).replace(/[!'()*]/g, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`)}/config`;
  return { command: curlRequest("PUT", `"\${CONNECT_URL:?}${path}"`, body, secrets), variables: [...secrets.values()] };
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    toast(t("copied"));
  } catch (error) {
    toast(error.message, true);
  }
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

const SVG_NS = "http://www.w3.org/2000/svg";
const GRAPH_COLUMNS = [0, 300, 600];
const GRAPH_NODE_WIDTH = 220;
const GRAPH_TOP = 34;
const GRAPH_GAP = 10;
const SEVERITY_ORDER = ["error", "warning", "ok"];

function svg(tag, attrs = {}, children = []) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value == null || value === false) continue;
    if (key === "class") String(value).split(/\s+/).filter(Boolean).forEach((name) => node.classList.add(name));
    else node.setAttribute(key, value === true ? "" : String(value));
  }
  for (const child of [].concat(children)) {
    if (child == null || child === false) continue;
    node.append(child.nodeType ? child : document.createTextNode(String(child)));
  }
  return node;
}

function truncate(text, limit) {
  const value = String(text ?? "");
  return value.length > limit ? value.slice(0, limit - 1) + "…" : value;
}

function resetGraph() {
  graphView.seq += 1;
  Object.assign(graphView, {
    data: null, loading: false, error: null, query: "", problemsOnly: false, focus: null, tab: "diagnostics", showOk: false,
  });
}

async function loadGraph(refresh = false) {
  const seq = ++graphView.seq;
  const clusterId = state.clusterId;
  graphView.loading = true;
  graphView.error = null;
  renderGraphStatus();
  try {
    const data = await api(`/api/clusters/${encodeURIComponent(clusterId)}/graph${refresh ? "?refresh=true" : ""}`);
    if (seq !== graphView.seq || clusterId !== state.clusterId) return;
    graphView.data = data;
    const focus = graphView.focus;
    if (focus && !graphFocusExists(focus)) graphView.focus = null;
  } catch (error) {
    if (seq !== graphView.seq || clusterId !== state.clusterId) return;
    if (error.status === 429 || graphView.data) toast(error.message, true);
    else graphView.error = error.message;
  }
  graphView.loading = false;
  if (state.view === "graph") renderGraph();
}

function graphFocusExists(focus) {
  const data = graphView.data;
  if (!data) return false;
  if (focus.kind === "diagnostic") return data.diagnostics.some((item) => item.id === focus.id);
  return data.nodes.some((node) => node.id === focus.id);
}

function renderGraphShell() {
  const search = el("input", {
    id: "search",
    type: "search",
    placeholder: t("graphSearch"),
    autocomplete: "off",
    spellcheck: "false",
    value: graphView.query,
  });
  const problems = el("input", { id: "graph-problems", type: "checkbox" });
  problems.checked = graphView.problemsOnly;
  const refresh = el("button", { class: "btn", type: "button", id: "graph-refresh" }, t("refresh"));
  $("list-pane").replaceChildren(
    el("header", { class: "list-head" }, [
      el("div", {}, [
        el("h1", {}, t("graph")),
        el("p", { class: "meta", id: "cluster-meta" }, clusterMetaText()),
      ]),
      el("div", { class: "head-actions" }, [viewSwitch(), refresh]),
    ]),
    el("div", { class: "graph-toolbar" }, [
      search,
      el("label", { class: "graph-toggle" }, [problems, t("onlyProblems")]),
      el("span", { class: "graph-counter", id: "graph-counter" }),
    ]),
    el("div", { id: "graph-status" }),
    el("div", { class: "graph-canvas", id: "graph-canvas" }),
    graphLegend(),
  );
  search.addEventListener("input", (event) => {
    graphView.query = event.target.value;
    renderGraphCanvas();
  });
  problems.addEventListener("change", (event) => {
    graphView.problemsOnly = Boolean(event.target.checked);
    renderGraphCanvas();
  });
  refresh.addEventListener("click", () => {
    void loadGraph(true);
  });
}

function graphLegend() {
  const line = (kind, label) => el("span", { class: "legend-item" }, [el("span", { class: `legend-line is-${kind}` }), label]);
  return el("div", { class: "graph-legend" }, [
    line("declared", t("legendDeclared")),
    line("pattern", t("legendPattern")),
    line("possible", t("legendPossible")),
    line("dlq", "DLQ"),
    el("span", { class: "legend-item" }, [el("span", { class: "legend-node is-pattern" }), t("legendPatternNode")]),
    el("span", { class: "legend-item" }, [el("span", { class: "legend-icon" }, "⇄"), "bootstrap.servers"]),
  ]);
}

function renderGraph() {
  renderGraphStatus();
  renderGraphCanvas();
  renderGraphSide();
}

function renderGraphStatus() {
  const status = $("graph-status");
  const counter = $("graph-counter");
  const refresh = $("graph-refresh");
  if (refresh) refresh.disabled = graphView.loading;
  const data = graphView.data;
  if (counter) {
    counter.textContent = data ? fmt("graphCounter", data.stats) : "";
    counter.classList.toggle("has-errors", Boolean(data && data.stats.errors));
  }
  if (!status) return;
  if (!data && graphView.loading) {
    status.replaceChildren(el("div", { class: "status-line" }, t("graphLoading")));
  } else if (!data && graphView.error) {
    const retry = el("button", { class: "btn", type: "button" }, t("retry"));
    retry.addEventListener("click", () => {
      void loadGraph();
    });
    status.replaceChildren(el("div", { class: "banner is-error" }, graphView.error), el("div", { class: "status-line" }, [retry]));
  } else if (data && data.partial) {
    status.replaceChildren(el("div", { class: "banner is-warning" }, fmt("graphPartial", { n: data.errors.length })));
  } else {
    status.replaceChildren();
  }
}

function graphColumn(node) {
  if (node.kind !== "connector") return 1;
  return node.type === "sink" ? 2 : 0;
}

function graphNodeLabel(node) {
  if (node.kind === "pattern") return node.syntax === "prefix" ? `${node.value}*` : node.value;
  return node.name;
}

function graphNodeHeight(node) {
  return node.kind === "connector" ? 50 : 30;
}

function compareLabels(left, right) {
  const a = graphNodeLabel(left).toLowerCase();
  const b = graphNodeLabel(right).toLowerCase();
  return a < b ? -1 : a > b ? 1 : 0;
}

function layoutGraph(nodes, edges) {
  const byId = new Map(nodes.map((node) => [node.id, node]));
  const columns = [[], [], []];
  for (const node of [...nodes].sort(compareLabels)) columns[graphColumn(node)].push(node.id);
  const links = new Map(nodes.map((node) => [node.id, []]));
  for (const edge of edges) {
    if (!byId.has(edge.from) || !byId.has(edge.to)) continue;
    links.get(edge.from).push(edge.to);
    links.get(edge.to).push(edge.from);
  }
  const rank = new Map();
  const updateRanks = () => {
    for (const column of columns) column.forEach((id, index) => rank.set(id, column.length > 1 ? index / (column.length - 1) : 0.5));
  };
  const reorder = (index, neighbourColumns) => {
    const score = new Map();
    for (const id of columns[index]) {
      const around = links.get(id).filter((other) => neighbourColumns.includes(graphColumn(byId.get(other))));
      score.set(id, around.length ? around.reduce((sum, other) => sum + rank.get(other), 0) / around.length : rank.get(id));
    }
    columns[index].sort((left, right) => score.get(left) - score.get(right));
    updateRanks();
  };
  updateRanks();
  for (let pass = 0; pass < 4; pass += 1) {
    reorder(1, [0, 2]);
    reorder(0, [1]);
    reorder(2, [1]);
  }
  const positions = new Map();
  let height = GRAPH_TOP + 40;
  columns.forEach((column, index) => {
    let y = GRAPH_TOP;
    for (const id of column) {
      const h = graphNodeHeight(byId.get(id));
      positions.set(id, { x: GRAPH_COLUMNS[index], y, w: GRAPH_NODE_WIDTH, h, column: index });
      y += h + GRAPH_GAP;
    }
    height = Math.max(height, y);
  });
  return { columns, positions, width: GRAPH_COLUMNS[2] + GRAPH_NODE_WIDTH, height };
}

function existingNodes(data, ids) {
  const known = new Set(data.nodes.map((node) => node.id));
  return ids.filter((id) => known.has(id));
}

function visibleGraph(data) {
  let base = data.nodes;
  if (graphView.problemsOnly) {
    const problem = new Set();
    for (const item of data.diagnostics) if (item.severity !== "ok") item.nodes.forEach((id) => problem.add(id));
    base = base.filter((node) => problem.has(node.id));
  }
  const allowed = new Set(base.map((node) => node.id));
  const query = graphView.query.trim().toLowerCase();
  let visible = allowed;
  if (query) {
    visible = new Set(base.filter((node) => graphNodeLabel(node).toLowerCase().includes(query)).map((node) => node.id));
    const matched = new Set(visible);
    for (const edge of data.edges) {
      if (matched.has(edge.from) && allowed.has(edge.to)) visible.add(edge.to);
      if (matched.has(edge.to) && allowed.has(edge.from)) visible.add(edge.from);
    }
  }
  return {
    nodes: data.nodes.filter((node) => visible.has(node.id)),
    edges: data.edges.filter((edge) => visible.has(edge.from) && visible.has(edge.to)),
  };
}

function graphFocusSets(data) {
  const focus = graphView.focus;
  if (!focus) return null;
  if (focus.kind === "diagnostic") {
    const item = data.diagnostics.find((candidate) => candidate.id === focus.id);
    if (!item) return null;
    return { nodes: new Set(item.nodes), edges: new Set(item.edges) };
  }
  const nodes = new Set([focus.id]);
  const edges = new Set();
  for (const [from, to] of [["from", "to"], ["to", "from"]]) {
    const queue = [focus.id];
    const seen = new Set(queue);
    while (queue.length) {
      const current = queue.shift();
      for (const edge of data.edges) {
        if (edge[from] !== current) continue;
        edges.add(edge.id);
        nodes.add(edge[to]);
        if (!seen.has(edge[to])) {
          seen.add(edge[to]);
          queue.push(edge[to]);
        }
      }
    }
  }
  return { nodes, edges };
}

function severityMaps(data) {
  const nodes = new Map();
  const edges = new Map();
  const mark = (map, id, severity) => {
    if (map.get(id) !== "error") map.set(id, severity);
  };
  for (const item of data.diagnostics) {
    if (item.severity === "ok") continue;
    item.nodes.forEach((id) => mark(nodes, id, item.severity));
    item.edges.forEach((id) => mark(edges, id, item.severity));
  }
  return { nodes, edges };
}

function setGraphFocus(focus) {
  graphView.focus = focus;
  if (focus && focus.kind === "node") graphView.tab = "details";
  renderGraphCanvas();
  renderGraphSide();
}

function toggleGraphFocus(kind, id) {
  const current = graphView.focus;
  setGraphFocus(current && current.kind === kind && current.id === id ? null : { kind, id });
}

function renderGraphCanvas() {
  const canvas = $("graph-canvas");
  if (!canvas) return;
  const data = graphView.data;
  if (!data) {
    canvas.replaceChildren();
    return;
  }
  if (!data.nodes.length) {
    canvas.replaceChildren(el("div", { class: "status-line" }, t("noConnectors")));
    return;
  }
  const { nodes, edges } = visibleGraph(data);
  if (!nodes.length) {
    canvas.replaceChildren(el("div", { class: "status-line" }, t("graphNoMatch")));
    return;
  }
  const layout = layoutGraph(nodes, edges);
  const focus = graphFocusSets(data);
  const severity = severityMaps(data);
  const root = svg("svg", {
    class: focus ? "graph-svg has-focus" : "graph-svg",
    width: layout.width,
    height: layout.height,
    viewBox: `0 0 ${layout.width} ${layout.height}`,
    role: "group",
    "aria-label": t("graph"),
  });
  [t("sources"), t("topicsColumn"), t("sinks")].forEach((title, index) => {
    root.append(svg("text", { class: "graph-col-title", x: GRAPH_COLUMNS[index], y: 14 }, title));
  });
  const edgeLayer = svg("g", { class: "graph-edges" });
  for (const edge of edges) edgeLayer.append(graphEdge(edge, layout, focus, severity.edges.get(edge.id)));
  const nodeLayer = svg("g", { class: "graph-nodes" });
  for (const node of nodes) nodeLayer.append(graphNode(node, layout.positions.get(node.id), focus, severity.nodes.get(node.id)));
  root.append(edgeLayer, nodeLayer);
  canvas.replaceChildren(root);
}

function graphEdge(edge, layout, focus, severity) {
  const from = layout.positions.get(edge.from);
  const to = layout.positions.get(edge.to);
  const forward = from.column < to.column;
  const x1 = forward ? from.x + from.w : from.x;
  const x2 = forward ? to.x : to.x + to.w;
  const y1 = from.y + from.h / 2;
  const y2 = to.y + to.h / 2;
  const middle = (x1 + x2) / 2;
  const classes = ["gedge", `is-${edge.confidence}`];
  if (edge.kind === "dlq") classes.push("is-dlq");
  if (severity) classes.push(`has-${severity}`);
  if (focus && focus.edges.has(edge.id)) classes.push("is-active");
  return svg("path", {
    class: classes.join(" "),
    d: `M${x1} ${y1} C${middle} ${y1} ${middle} ${y2} ${x2} ${y2}`,
    "data-edge": edge.id,
  }, [svg("title", {}, edge.key)]);
}

function groupLine(group) {
  if (!group || group.origin === "unknown") return t("groupUnknown");
  return fmt(group.origin === "derived" ? "groupDerived" : "groupExplicit", { group: group.value });
}

function graphNode(node, box, focus, severity) {
  const classes = ["gnode", `gnode-${node.kind}`];
  if (node.kind === "connector") classes.push(`is-${node.type}`);
  if (node.kind === "pattern") classes.push(`is-${node.syntax}`);
  if (node.dlq) classes.push("is-dlq");
  if (severity) classes.push(`has-${severity}`);
  if (focus && focus.nodes.has(node.id)) classes.push("is-active");
  if (graphView.focus && graphView.focus.kind === "node" && graphView.focus.id === node.id) classes.push("is-focused");
  const label = graphNodeLabel(node);
  const group = svg("g", {
    class: classes.join(" "),
    transform: `translate(${box.x} ${box.y})`,
    "data-node": node.id,
    tabindex: "0",
    role: "button",
    "aria-label": label,
  }, [
    svg("title", {}, node.kind === "connector" && node.class ? `${label}\n${node.class}` : label),
    svg("rect", { width: box.w, height: box.h, rx: node.kind === "topic" ? box.h / 2 : 8 }),
  ]);
  const iconRoom = node.bootstrap_override || node.dlq ? 4 : 0;
  group.append(svg("text", { class: "gnode-name", x: 12, y: node.kind === "connector" ? 20 : 19 }, truncate(label, 27 - iconRoom)));
  if (node.kind === "connector") {
    const sub = node.type === "sink" ? groupLine(node.group) : node.type === "source" ? pluginName(node.class || "") : t("unknownType");
    const full = node.group && node.group.value ? node.group.value : sub;
    group.append(svg("text", { class: `gnode-sub${node.group && node.group.origin === "unknown" ? " is-unknown" : ""}`, x: 12, y: 38 }, [
      truncate(sub, 32),
      svg("title", {}, full),
    ]));
  }
  if (node.bootstrap_override) {
    group.append(svg("text", { class: "gnode-icon", x: box.w - 12, y: 20, "text-anchor": "end" }, ["⇄", svg("title", {}, t("bootstrapOverride"))]));
  }
  if (node.dlq) {
    group.append(svg("g", { class: "gnode-badge", transform: `translate(${box.w - 44} 8)` }, [
      svg("rect", { width: 32, height: 14, rx: 7 }),
      svg("text", { x: 16, y: 10.5, "text-anchor": "middle" }, "DLQ"),
    ]));
  }
  group.addEventListener("click", () => toggleGraphFocus("node", node.id));
  group.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    toggleGraphFocus("node", node.id);
  });
  return group;
}

function diagnosticMessage(item) {
  const list = (values) => (values && values.length ? values.join(", ") : "—");
  const others = item.connectors.filter((name) => !(item.undetermined || []).includes(name));
  const target = item.topic ? fmt("targetTopic", { topic: item.topic }) : t("targetPatterns");
  const key = item.code === "shared_topic_different_groups" && item.expected ? `d_${item.code}_expected` : `d_${item.code}`;
  if (!hasText(key)) return item.code;
  return fmt(key, {
    connectors: list(item.connectors),
    topic: item.topic || "",
    group: item.group || "",
    overridden: list(item.overridden),
    undetermined: list(item.undetermined),
    others: list(others),
    patterns: list(item.patterns),
    message: item.error ? errorText(item.error, "") : "",
    target,
  });
}

function diagnosticItem(item) {
  const focus = graphView.focus;
  const selected = focus && focus.kind === "diagnostic" && focus.id === item.id;
  const button = el("button", { class: `diag-item is-${item.severity}${selected ? " is-selected" : ""}`, type: "button", "data-diagnostic": item.id }, [
    el("span", { class: "diag-code" }, item.code),
    el("span", { class: "diag-text" }, diagnosticMessage(item)),
  ]);
  button.addEventListener("click", () => toggleGraphFocus("diagnostic", item.id));
  return button;
}

function renderGraphSide() {
  if (state.view !== "graph") return;
  const data = graphView.data;
  const count = data ? data.stats.errors + data.stats.warnings : 0;
  const tab = (id, label) => {
    const button = el("button", { class: `tab${graphView.tab === id ? " is-active" : ""}`, type: "button", role: "tab", "data-tab": id }, label);
    button.setAttribute("aria-selected", graphView.tab === id ? "true" : "false");
    button.addEventListener("click", () => {
      graphView.tab = id;
      renderGraphSide();
    });
    return button;
  };
  $("detail-pane").replaceChildren(
    el("div", { class: "graph-side" }, [
      el("div", { class: "tabs graph-tabs", role: "tablist" }, [
        tab("diagnostics", count ? `${t("diagnostics")} · ${count}` : t("diagnostics")),
        tab("details", t("details")),
      ]),
      el("div", { class: "detail-scroll graph-side-body", id: "graph-side-body" },
        graphView.tab === "details" ? graphDetails(data) : graphDiagnostics(data)),
    ]),
  );
}

function graphDiagnostics(data) {
  if (!data) return el("p", { class: "hint" }, graphView.loading ? t("graphLoading") : graphView.error || "");
  const sections = [];
  for (const severity of SEVERITY_ORDER) {
    const items = data.diagnostics.filter((item) => item.severity === severity);
    if (!items.length) continue;
    const label = t(severity === "error" ? "severityError" : severity === "warning" ? "severityWarning" : "severityOk");
    const heading = el("h3", {}, [label, el("span", {}, String(items.length))]);
    const section = el("section", { class: `diag-group is-${severity}` }, [heading]);
    if (severity === "ok") {
      const toggle = el("button", { class: "btn-ghost diag-toggle", type: "button", id: "graph-ok-toggle" }, graphView.showOk ? t("hide") : t("show"));
      toggle.addEventListener("click", () => {
        graphView.showOk = !graphView.showOk;
        renderGraphSide();
      });
      heading.append(toggle);
      if (!graphView.showOk) {
        sections.push(section);
        continue;
      }
    }
    items.forEach((item) => section.append(diagnosticItem(item)));
    sections.push(section);
  }
  if (!data.stats.errors && !data.stats.warnings) sections.unshift(el("p", { class: "hint" }, t("noDiagnostics")));
  return sections;
}

function graphFacts(rows) {
  const list = el("dl", { class: "graph-facts" });
  for (const [label, value] of rows) {
    if (value == null || value === "") continue;
    list.append(el("dt", {}, label), el("dd", {}, value));
  }
  return list;
}

function graphDetails(data) {
  const focus = graphView.focus;
  if (!data || !focus) return el("p", { class: "hint" }, t("selectNodeHint"));
  if (focus.kind === "diagnostic") {
    const item = data.diagnostics.find((candidate) => candidate.id === focus.id);
    return item ? [diagnosticItem(item)] : el("p", { class: "hint" }, t("selectNodeHint"));
  }
  const node = data.nodes.find((candidate) => candidate.id === focus.id);
  if (!node) return el("p", { class: "hint" }, t("selectNodeHint"));
  const names = (ids) => ids.map((id) => {
    const other = data.nodes.find((candidate) => candidate.id === id);
    return other ? graphNodeLabel(other) : id;
  }).join(", ");
  const parts = [];
  if (node.kind === "connector") {
    const origin = node.group ? t(`origin${node.group.origin[0].toUpperCase()}${node.group.origin.slice(1)}`) : "";
    parts.push(
      el("div", { class: "detail-kicker" }, node.type === "unknown" ? t("unknownType") : node.type),
      el("h2", {}, node.name),
      el("a", { class: "btn graph-open", href: `#${encodeURIComponent(state.clusterId)}/${encodeURIComponent(node.name)}` }, t("openConnector")),
    );
    const rows = [[t("class"), node.class]];
    if (node.type === "sink") {
      rows.push(
        ["topics", node.topics && node.topics.length ? node.topics.join(", ") : null],
        ["topics.regex", node.topics_regex ? `${node.topics_regex}${node.regex_analyzable === false ? ` · ${t("notAnalyzable")}` : ""}` : null],
        [t("group"), node.group ? (node.group.value ? `${node.group.value} · ${origin}` : origin) : null],
        ["DLQ", node.dlq_topic],
        ["bootstrap.servers", node.bootstrap_override ? t("bootstrapSet") : null],
      );
    } else {
      rows.push(["topic", node.topic], ["topic.prefix", node.topic_prefix]);
    }
    parts.push(graphFacts(rows));
    if (node.unknown && node.unknown.length) parts.push(el("p", { class: "hint" }, `${t("undeterminedKeys")}: ${node.unknown.join(", ")}`));
  } else {
    const incoming = data.edges.filter((edge) => edge.to === node.id);
    const outgoing = data.edges.filter((edge) => edge.from === node.id);
    parts.push(
      el("div", { class: "detail-kicker" }, node.kind === "topic" ? (node.dlq ? `${t("topic")} · DLQ` : t("topic")) : `${t("pattern")} · ${node.syntax}`),
      el("h2", {}, graphNodeLabel(node)),
      graphFacts([
        [t("writtenBy"), names(incoming.filter((edge) => edge.kind === "writes").map((edge) => edge.from))],
        [t("dlqOf"), names(incoming.filter((edge) => edge.kind === "dlq").map((edge) => edge.from))],
        [t("readBy"), names(outgoing.map((edge) => edge.to))],
        [node.analyzable === false ? t("pattern") : null, node.analyzable === false ? t("notAnalyzable") : null],
      ]),
    );
  }
  const related = data.diagnostics.filter((item) => item.nodes.includes(node.id));
  if (related.length) {
    parts.push(el("h3", { class: "graph-related" }, t("diagnostics")));
    related.forEach((item) => parts.push(diagnosticItem(item)));
  }
  return parts;
}

function onKeydown(event) {
  if (event.key === "Escape" && modal) {
    closeModal();
    return;
  }
  if (event.key === "Escape" && state.view === "graph" && graphView.focus) {
    setGraphFocus(null);
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
