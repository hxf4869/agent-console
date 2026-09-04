-- Push 订阅存储(§18.5/§24)。endpoint 属于敏感数据,日志不得输出(relay-data 实现)。
CREATE TABLE push_subscriptions (
    id             UUID PRIMARY KEY,
    owner_id       UUID NOT NULL,
    endpoint       TEXT NOT NULL,
    p256dh_key     TEXT NOT NULL,
    auth_key       TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    invalidated_at TIMESTAMPTZ,
    UNIQUE (owner_id, endpoint)
);

CREATE INDEX idx_push_subscriptions_owner ON push_subscriptions (owner_id);
