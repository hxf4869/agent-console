//! payload 规范化摘要(§15.1)。
//!
//! 摘要仅用于同 request_id 的去重比较,不用于内容真实性证明;使用固定
//! SHA-256 + hex。此文件不落正文,摘要本身即可入库存放。

/// 计算 payload 的规范化摘要(SHA-256 hex)。
pub fn payload_digest(payload_bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(payload_bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_stable_hex() {
        let a = payload_digest(b"hello");
        let b = payload_digest(b"hello");
        let c = payload_digest(b"world");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn digest_differs_for_different_lengths() {
        assert_ne!(payload_digest(b"abc"), payload_digest(b"abcd"));
    }
}
