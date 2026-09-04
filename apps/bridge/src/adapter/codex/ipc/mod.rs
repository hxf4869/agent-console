//! Codex Desktop 私有 IPC:帧编解码、强类型消息、客户端、发现。
//!
//! 协议事实来源:`docs/CODEX-IPC-PROTOCOL.md`(由 asar 逆向 + 真实只读探针得出)。
//! 本模块是协议的唯一边界:原生 JSON 不得越过 [`client`] 的强类型 API 对外暴露。

pub mod client;
pub mod discovery;
pub mod frame;
pub mod messages;
