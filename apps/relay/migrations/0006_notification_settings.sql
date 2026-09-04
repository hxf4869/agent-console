-- 通知设置(§18.6/§24):每用户一行;默认通用文案为代码常量,不落库。
CREATE TABLE notification_settings (
    owner_id    UUID PRIMARY KEY,
    -- 是否允许显示 title(默认不允许)。
    show_title  BOOLEAN NOT NULL DEFAULT FALSE,
    -- 事件开关;与 session mute 的最终组合由发送端计算。
    turn_completed   BOOLEAN NOT NULL DEFAULT TRUE,
    turn_failed      BOOLEAN NOT NULL DEFAULT TRUE,
    turn_interrupted BOOLEAN NOT NULL DEFAULT TRUE,
    waiting_question BOOLEAN NOT NULL DEFAULT TRUE,
    waiting_approval BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
