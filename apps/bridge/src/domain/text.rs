//! 正文类字段的脱敏包装。
//!
//! 隐私要求(§25.3 日志白名单、§12 不透传未消化 JSON):`Debug` 输出只允许
//! 出现 ID、状态与长度,不允许出现正文全文。所有承载用户可见正文的领域字段
//! 一律使用 [`OutputText`](文本) 或 [`OutputBytes`](字节块) 包装;
//! serde 序列化保持透传(内部 JSON 调试输出需要原文,但 Debug/日志不需要)。

use serde::{Deserialize, Serialize};
use std::fmt;

/// 用户可见文本(消息、问题标题、计划步骤等)。
#[derive(Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OutputText(pub String);

impl OutputText {
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for OutputText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OutputText({} bytes)", self.0.len())
    }
}

impl From<String> for OutputText {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for OutputText {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// 命令/工具输出字节块(§13.2 分块;UTF-8 边界安全)。
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OutputBytes(pub Vec<u8>);

impl OutputBytes {
    pub fn from_text(text: impl Into<String>) -> Self {
        Self(text.into().into_bytes())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// 在 `max` 字节内找一个 UTF-8 边界安全的切分点(不劈开多字节字符)。
    /// 返回 (chunk, rest);`self` 为空时返回 None。
    pub fn split_utf8_safe(&self, max: usize) -> Option<(OutputBytes, OutputBytes)> {
        if self.0.is_empty() || max == 0 {
            return None;
        }
        let mut cut = max.min(self.0.len());
        while cut > 0 && !is_char_boundary(&self.0, cut) {
            cut -= 1;
        }
        if cut == 0 {
            return None;
        }
        Some((
            OutputBytes(self.0[..cut].to_vec()),
            OutputBytes(self.0[cut..].to_vec()),
        ))
    }
}

impl fmt::Debug for OutputBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OutputBytes({} bytes)", self.0.len())
    }
}

/// 字节切片的 UTF-8 字符边界判定(`str::is_char_boundary` 的字节版本)。
fn is_char_boundary(bytes: &[u8], index: usize) -> bool {
    index == 0 || index >= bytes.len() || bytes[index] & 0xC0 != 0b1000_0000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_content() {
        let text = OutputText::new("机密提示词内容-do-not-leak");
        assert_eq!(text.len(), 33);
        let rendered = format!("{text:?}");
        assert!(rendered.contains("33 bytes"), "actual: {rendered}");
        assert!(!rendered.contains("机密"));

        let bytes = OutputBytes::from_text("secret-output-body");
        let rendered = format!("{bytes:?}");
        assert!(rendered.contains("18 bytes"), "actual: {rendered}");
        assert!(!rendered.contains("secret"));
    }

    #[test]
    fn serde_transparent() {
        let text = OutputText::new("ok");
        assert_eq!(serde_json::to_string(&text).unwrap(), "\"ok\"");
        let bytes = OutputBytes::from_text("ab");
        assert_eq!(serde_json::to_string(&bytes).unwrap(), "[97,98]");
    }

    #[test]
    fn split_respects_utf8_boundary() {
        // "中文中文" 每个汉字 3 字节;切 7 字节必须回退到 6。
        let bytes = OutputBytes::from_text("中文中文");
        let (head, tail) = bytes.split_utf8_safe(7).unwrap();
        // 7 字节处落在第 3 个汉字内部,回退到 6 字节边界。
        assert_eq!(head.len(), 6);
        assert_eq!(std::str::from_utf8(head.as_bytes()).unwrap(), "中文");
        assert_eq!(std::str::from_utf8(tail.as_bytes()).unwrap(), "中文");
        assert!(OutputBytes::default().split_utf8_safe(10).is_none());
    }
}
