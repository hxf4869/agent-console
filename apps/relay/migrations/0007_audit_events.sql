-- 审计事件(§18.7):仅元数据;30 天保留;不保存标题、项目路径、prompt、回答、
-- 审批正文、输出或文件名。由 Relay 单实例内有界维护任务清理(§18)。
CREATE TABLE audit_events (
    id          BIGSERIAL PRIMARY KEY,
    owner_id    UUID NOT NULL,
    device_id   UUID,
    session_id  UUID,
    request_id  TEXT,
    operation   TEXT NOT NULL,
    result      TEXT NOT NULL,
    latency_ms  BIGINT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_events_owner_time ON audit_events (owner_id, created_at DESC);
CREATE INDEX idx_audit_events_created_at ON audit_events (created_at);
