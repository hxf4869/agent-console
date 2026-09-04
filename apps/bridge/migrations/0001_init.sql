-- Bridge 本地库初始 schema(权威规格 §19)。
-- 本库只属于 Bridge,由 Bridge 自行 migration;绝不能对 Codex SQLite 运行。
-- 隐私边界(§19/§25.3):不存会话历史、输出、Diff;queue 正文是唯一例外(§15.3);
-- 不存凭据(凭据只在 macOS Keychain);工作区根目录是规格明确允许的唯一路径存储。

-- 绑定状态(非敏感):Relay URL、device ID、状态。
CREATE TABLE binding (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    relay_url TEXT NOT NULL DEFAULT '',
    device_id TEXT NOT NULL DEFAULT '',
    -- UNBOUND / PAIRING / PAIRED
    status TEXT NOT NULL DEFAULT 'UNBOUND',
    updated_at TEXT NOT NULL DEFAULT ''
);
INSERT INTO binding (id, relay_url, device_id, status, updated_at)
VALUES (1, '', '', 'UNBOUND', '');

-- 已授权工作区根目录(canonical path)。
CREATE TABLE authorized_workspaces (
    root_path TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    authorized_at TEXT NOT NULL
);

-- 最近 request 回执与 payload 摘要(§15.1/§15.2);正文绝不存。
-- session_key 取原生复合键 (device_id, agent_kind, native_session_id)(§9.1)。
CREATE TABLE request_receipts (
    request_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    agent_kind INTEGER NOT NULL,
    native_session_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    -- RECEIVED/ACCEPTED_BY_BRIDGE/DISPATCHED_TO_CODEX/COMPLETED/REJECTED/OUTCOME_UNKNOWN
    status TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- 下一轮队列:每 session 最多一条(主键唯一约束,§15.3)。
-- prompt 正文只存 Bridge;Relay 只存"有队列"与状态。
CREATE TABLE next_turn_queue (
    device_id TEXT NOT NULL,
    agent_kind INTEGER NOT NULL,
    native_session_id TEXT NOT NULL,
    prompt TEXT NOT NULL,
    after_turn_id TEXT NOT NULL,
    runtime_revision INTEGER NOT NULL,
    -- QUEUED / PAUSED / EMPTY
    status TEXT NOT NULL CHECK (status IN ('QUEUED', 'PAUSED', 'EMPTY')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (device_id, agent_kind, native_session_id)
);

-- capability probe 结果 JSON(schema/能力,无会话内容)与本地 schema 版本。
CREATE TABLE capability_cache (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    probe_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

-- 必要 cursor。
CREATE TABLE cursors (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- 隐私设置(布尔开关)。
CREATE TABLE privacy_settings (
    key TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

-- 临时上传清理记录:只记 data_dir 相对目录与时间,不含文件内容与绝对用户路径。
CREATE TABLE upload_cleanup_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    directory TEXT NOT NULL,
    reason TEXT NOT NULL,
    cleaned_at TEXT NOT NULL
);
