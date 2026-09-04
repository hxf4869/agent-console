-- 配对挑战(§18.2/§21):challenge 与短码只存摘要;5 分钟过期、单次使用、有限尝试。
CREATE TABLE pairing_challenges (
    id                UUID PRIMARY KEY,
    owner_id          UUID        NOT NULL,
    -- 高熵 32B challenge 与 6 位短码的 SHA-256(hex) 摘要。
    challenge_digest  TEXT        NOT NULL,
    short_code_digest TEXT        NOT NULL,
    -- Bridge 上报的展示信息(设备名/平台/架构/版本)。
    device_name       TEXT,
    platform          TEXT,
    arch              TEXT,
    bridge_version    TEXT,
    expires_at        TIMESTAMPTZ NOT NULL,
    attempt_count     INT         NOT NULL DEFAULT 0,
    registered_at     TIMESTAMPTZ,
    approved_at       TIMESTAMPTZ,
    -- credential 明文只经 pairing 通道返回一次:交付时间戳保证单次交付。
    credential_delivered_at TIMESTAMPTZ,
    consumed_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_pairing_short_code ON pairing_challenges (short_code_digest);
CREATE INDEX idx_pairing_owner_pending ON pairing_challenges (owner_id, consumed_at);
