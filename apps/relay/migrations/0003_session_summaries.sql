-- 会话摘要(§18.3):Relay 唯一允许从 PostgreSQL 直接返回的会话数据。
-- 不保存绝对 cwd、消息、输出、Diff 或文件名列表;设备隐私模式开启时标题类字段为空。
CREATE TABLE session_summaries (
    id                 UUID PRIMARY KEY,
    device_id          UUID        NOT NULL REFERENCES devices (id) ON DELETE CASCADE,
    agent_kind         TEXT        NOT NULL,
    native_session_id  TEXT        NOT NULL,
    title              TEXT        NOT NULL DEFAULT '',
    project_display_name TEXT      NOT NULL DEFAULT '',
    current_branch     TEXT        NOT NULL DEFAULT '',
    -- §10 多维状态摘要(稳定枚举名)。
    device_connection  TEXT        NOT NULL DEFAULT 'CONNECTION_OFFLINE',
    device_last_seen_at TIMESTAMPTZ,
    degraded_reason    TEXT        NOT NULL DEFAULT '',
    control_mode       TEXT        NOT NULL DEFAULT 'CONTROL_MODE_UNSPECIFIED',
    compatibility_state TEXT       NOT NULL DEFAULT 'COMPATIBILITY_STATE_UNSPECIFIED',
    active_turn_phase  TEXT        NOT NULL DEFAULT 'ACTIVE_TURN_PHASE_UNSPECIFIED',
    pending_attention_count INT    NOT NULL DEFAULT 0,
    pending_attention_kinds TEXT[] NOT NULL DEFAULT '{}',
    queue_state        TEXT        NOT NULL DEFAULT 'QUEUE_STATE_UNSPECIFIED',
    last_turn_outcome  TEXT        NOT NULL DEFAULT 'LAST_TURN_OUTCOME_UNSPECIFIED',
    runtime_revision   BIGINT      NOT NULL DEFAULT 0,
    last_updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Agent Console 自身偏好(§12):只保存在 Relay,不写回 Codex。
    pinned             BOOLEAN     NOT NULL DEFAULT FALSE,
    muted              BOOLEAN     NOT NULL DEFAULT FALSE,
    archived           BOOLEAN     NOT NULL DEFAULT FALSE,
    UNIQUE (device_id, agent_kind, native_session_id)
);

-- 列表分页(§27.5):稳定 keyset cursor 基于 (last_updated_at DESC, id DESC)。
CREATE INDEX idx_session_summaries_device_updated
    ON session_summaries (device_id, archived, last_updated_at DESC, id DESC);
