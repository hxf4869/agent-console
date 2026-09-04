//! Agent Console Protobuf 协议生成 crate。
//!
//! proto 源文件位于仓库根 `proto/agent_console/v1/`,由 proto workstream 维护;
//! 本 crate 只负责编译生成与窄编解码,不包含手写协议逻辑。

/// proto package `agent_console.v1` 的生成类型。
pub mod agent_console {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/agent_console.v1.rs"));
    }
}

/// 便于调用方直接 `use agent_console_protocol::v1::...`。
pub use agent_console::v1;

/// 窄编解码助手:Envelope 二进制编解码、ID 生成与帧大小上限(§17.6)。
pub mod codec;
