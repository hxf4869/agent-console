-- Relay 在命令到达 Bridge 前也必须区分“同 request_id 同请求重放”和
-- “同 request_id 不同目标/操作/payload 冲突”(§15.2)。仅保存服务端从
-- Protobuf 请求规范化计算的摘要，不保存 prompt/answer/approval 正文。
ALTER TABLE request_receipts
    ADD COLUMN payload_digest TEXT NOT NULL DEFAULT '';
