-- 请求回执(§18.4/§15.2):仅元数据;不保存 prompt、answer、approval 说明或 payload 正文。
-- owner_id 用于回执查询 API 的授权过滤(含义不变:回执归属用户)。
CREATE TABLE request_receipts (
    request_id  TEXT PRIMARY KEY,
    owner_id    UUID NOT NULL,
    session_id  UUID REFERENCES session_summaries (id) ON DELETE SET NULL,
    operation   TEXT NOT NULL,
    status      TEXT NOT NULL,
    error_code  TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_request_receipts_session
    ON request_receipts (session_id, created_at DESC);
CREATE INDEX idx_request_receipts_owner_time
    ON request_receipts (owner_id, created_at DESC);
