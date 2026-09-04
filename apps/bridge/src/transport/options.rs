//! Relay 客户端选项。
//!
//! 连接目标不再由本模块拼路径:WS 地址是 [`crate::config::RelayUrls`] 的
//! 派生产物(`AGENT_CONSOLE_RELAY_URL` = Relay 公网基地址,路径统一为
//! `/agent-console/bridge/ws`),本模块只承载派生结果与运行参数。

use std::time::Duration;

use crate::config::RelayUrls;

/// 客户端选项。默认值来自 config 集中常量(§26.2),测试可整体覆盖。
#[derive(Debug, Clone)]
pub struct RelayClientOptions {
    /// Bridge WebSocket 连接地址([`RelayUrls::bridge_ws_url`] 的派生结果)。
    pub ws_url: String,
    /// Bridge 自身 device_id(填入 Envelope.device_id)。
    pub device_id: String,
    pub heartbeat_interval: Duration,
    pub heartbeat_dead_after: Duration,
    pub reconnect_initial_backoff: Duration,
    pub reconnect_max_backoff: Duration,
    pub reconnect_jitter: Duration,
    pub outbound_capacity: usize,
    pub inbound_capacity: usize,
}

impl RelayClientOptions {
    /// 从已校验的 Relay 基地址派生选项。
    pub fn new(relay: &RelayUrls) -> Self {
        Self {
            ws_url: relay.bridge_ws_url(),
            device_id: String::new(),
            heartbeat_interval: crate::config::HEARTBEAT_INTERVAL,
            heartbeat_dead_after: crate::config::HEARTBEAT_DEAD_AFTER,
            reconnect_initial_backoff: crate::config::RECONNECT_INITIAL_BACKOFF,
            reconnect_max_backoff: crate::config::RECONNECT_MAX_BACKOFF,
            reconnect_jitter: crate::config::RECONNECT_JITTER,
            outbound_capacity: crate::config::OUTBOUND_QUEUE_CAPACITY,
            inbound_capacity: crate::config::INBOUND_QUEUE_CAPACITY,
        }
    }

    pub fn with_device_id(mut self, device_id: impl Into<String>) -> Self {
        self.device_id = device_id.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RelayUrlError;

    #[test]
    fn options_carry_derived_bridge_ws_url() {
        let urls = RelayUrls::parse("https://relay.example.com").unwrap();
        let o = RelayClientOptions::new(&urls).with_device_id("d1");
        assert_eq!(o.ws_url, "wss://relay.example.com/agent-console/bridge/ws");
        assert_eq!(o.device_id, "d1");
        // 本地开发 http 基地址 → ws。
        let local = RelayUrls::parse("http://127.0.0.1:8081").unwrap();
        assert_eq!(
            RelayClientOptions::new(&local).ws_url,
            "ws://127.0.0.1:8081/agent-console/bridge/ws"
        );
        // 历史误用:完整 WebSocket 地址必须在 RelayUrls 校验阶段失败,
        // 不会被原样当作连接目标。
        assert_eq!(
            RelayUrls::parse("wss://h/agent-console/ws").unwrap_err(),
            RelayUrlError::FullWebSocketAddress
        );
    }
}
