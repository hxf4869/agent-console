-- 设备表(§18.1):只保存凭据摘要,不保存明文。
CREATE TABLE devices (
    id               UUID PRIMARY KEY,
    owner_id         UUID        NOT NULL,
    display_name     TEXT        NOT NULL DEFAULT '',
    platform         TEXT        NOT NULL DEFAULT '',
    arch             TEXT        NOT NULL DEFAULT '',
    bridge_version   TEXT        NOT NULL DEFAULT '',
    -- SHA-256(hex) of device credential;唯一索引支撑按摘要认证查找。
    credential_digest TEXT       NOT NULL UNIQUE,
    paired_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at       TIMESTAMPTZ,
    last_seen_at     TIMESTAMPTZ,
    -- §10.2/§10.3 兼容/控制摘要(稳定枚举名,如 CONTROL_MODE_READ_ONLY)。
    compatibility_summary TEXT   NOT NULL DEFAULT 'COMPATIBILITY_STATE_UNSPECIFIED',
    control_summary  TEXT        NOT NULL DEFAULT 'CONTROL_MODE_UNSPECIFIED',
    -- 隐私设置:开启后 Relay 不保存/返回标题等项目信息(§18.1)。
    privacy_hide_titles BOOLEAN  NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_devices_owner ON devices (owner_id);
