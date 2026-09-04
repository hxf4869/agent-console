-- 配对方向改回 Bridge-first(§21):
-- 挑战由未绑定 Bridge 发起注册(此时无 owner),owner_id 在浏览器批准时回填;
-- 设备行与凭据在 claim 单次交付时生成。摘要列(challenge_digest/short_code_digest)、
-- attempt_count(短码尝试上限)与 credential_delivered_at(单次交付标记)沿用既有结构。
ALTER TABLE pairing_challenges ALTER COLUMN owner_id DROP NOT NULL;
