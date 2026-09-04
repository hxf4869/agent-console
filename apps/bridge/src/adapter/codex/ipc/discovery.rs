//! Codex Desktop 发现:socket 路径、版本探测、进程存活。
//!
//! 边界(§12):
//! - 只做发现与只读探测;不删除 socket、不启动/终止任何 Codex 进程。
//! - 版本探测使用固定 argv(`codex --version`),不经 shell、不拼接字符串。
//! - 未知版本一律降级只读;写能力由逐版本写能力矩阵决定(§5,
//!   `capabilities::verified_write_probes`),独立于协议/只读兼容版本表。

use std::path::{Path, PathBuf};
use std::time::Duration;

use thiserror::Error;
use tokio::time::timeout;

use super::client::{connect, IpcClientConfig, IpcError};
use super::messages::ClientStatusChangedParams;

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
pub const VERSION_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

/// 协议/只读兼容版本表(CODEX-COMPATIBILITY.md 记录):命中即
/// `CompatibilityState::Verified`(只读协议兼容)。写能力矩阵独立于此表,
/// 见 `crate::capabilities::verified_write_probes`;0.153.1 的写能力已逐操作
/// 真机验证(§9),0.153.0-alpha.5 仅有 fixture 证据(只读兼容)。
pub const VERIFIED_VERSIONS: &[&str] = &["0.153.0-alpha.5", "0.153.1"];

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("codex home directory not found")]
    HomeNotFound,
    #[error("version probe failed: {0}")]
    VersionProbe(String),
    #[error("ipc probe failed: {0}")]
    Ipc(#[from] IpcError),
}

/// 解析默认 socket 路径:
/// 1. `$CODEX_HOME/ipc/ipc.sock`(CODEX_HOME 未设置时为 `~/.codex/ipc/ipc.sock`);
/// 2. 备选 `$(tmpdir)/codex-ipc/ipc-<uid>.sock`(Desktop 在主路径不可写时使用)。
///
/// `explicit` 为 Some 时直接使用(允许上层配置覆盖)。
pub fn socket_path(explicit: Option<PathBuf>, codex_home: Option<PathBuf>) -> Option<PathBuf> {
    if let Some(p) = explicit {
        return Some(p);
    }
    let home = match codex_home {
        Some(h) => h,
        None => default_codex_home()?,
    };
    let primary = home.join("ipc").join("ipc.sock");
    if is_live_socket_path(&primary) {
        return Some(primary);
    }
    if let Some(fallback) = tmpdir_fallback() {
        if is_live_socket_path(&fallback) {
            return Some(fallback);
        }
    }
    // 两个路径都不是 socket 时仍返回主路径:连接层负责报错,由上层安排退避。
    Some(primary)
}

fn default_codex_home() -> Option<PathBuf> {
    if let Some(home) = std::env::var_os("CODEX_HOME") {
        return Some(PathBuf::from(home));
    }
    let home_dir = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())?;
    Some(home_dir.join(".codex"))
}

fn tmpdir_fallback() -> Option<PathBuf> {
    let tmp = std::env::temp_dir().join("codex-ipc");
    #[cfg(unix)]
    {
        let uid = current_uid();
        return Some(tmp.join(format!("ipc-{uid}.sock")));
    }
    #[allow(unreachable_code)]
    {
        Some(tmp.join("ipc.sock"))
    }
}

#[cfg(unix)]
fn current_uid() -> u32 {
    extern "C" {
        fn getuid() -> u32;
    }
    unsafe { getuid() }
}

/// 路径存在且是 socket 文件(不连接、不修改)。
pub fn is_live_socket_path(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        path.exists()
            && std::fs::symlink_metadata(path)
                .map(|m| m.file_type().is_socket())
                .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        path.exists()
    }
}

/// 探测本机 Codex Desktop socket:返回 (路径, 是否当前用户拥有)。
pub fn discover_socket(explicit: Option<PathBuf>) -> Option<(PathBuf, bool)> {
    let path = socket_path(explicit, None)?;
    let owned = socket_owned_by_current_user(&path);
    Some((path, owned))
}

/// socket 文件属主是否为当前用户(Desktop 自身也做同样校验;非当前用户的
/// socket 是安全异常,直接视为不可用)。
pub fn socket_owned_by_current_user(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::symlink_metadata(path)
            .map(|m| m.uid() == current_uid())
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        path.exists()
    }
}

/// 通过固定 argv 探测 codex CLI 版本(不经 shell)。
/// `binary` 缺省时使用 Desktop 内置二进制路径。
pub async fn probe_version(binary: Option<PathBuf>) -> Result<String, DiscoveryError> {
    let bin = binary
        .unwrap_or_else(|| PathBuf::from("/Applications/ChatGPT.app/Contents/Resources/codex"));
    let out = timeout(
        VERSION_COMMAND_TIMEOUT,
        tokio::process::Command::new(&bin).arg("--version").output(),
    )
    .await
    .map_err(|_| DiscoveryError::VersionProbe("timed out".to_string()))?
    .map_err(|e| DiscoveryError::VersionProbe(e.to_string()))?;
    if !out.status.success() {
        return Err(DiscoveryError::VersionProbe(format!(
            "exit status {}",
            out.status
        )));
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if text.is_empty() {
        return Err(DiscoveryError::VersionProbe(
            "empty version output".to_string(),
        ));
    }
    Ok(text)
}

/// 归一化 `codex --version` 输出:strip `codex-cli` 前缀并 trim
/// (输出形如 `codex-cli 0.153.0-alpha.5`)。
pub fn normalized_version(version: &str) -> &str {
    let v = version.trim();
    v.strip_prefix("codex-cli")
        .map(str::trim_start)
        .unwrap_or(v)
}

/// 版本是否命中协议/只读兼容版本表(`VERIFIED_VERSIONS`,决定
/// `CompatibilityState::Verified`;写能力矩阵独立,见
/// `capabilities::verified_write_probes`)。
pub fn version_is_verified(version: &str) -> bool {
    VERIFIED_VERSIONS.contains(&normalized_version(version))
}

/// 一次完整的只读 Desktop 探测:连接 → 握手 → 收集在线 clientType。
/// 只读:只发送 `initialize`,不发送任何写命令。
pub struct DesktopProbe {
    pub socket_path: PathBuf,
    pub version: Option<String>,
    pub client_id: String,
    pub observed_client_types: Vec<String>,
}

pub async fn probe_desktop(config: IpcClientConfig) -> Result<DesktopProbe, DiscoveryError> {
    let version = probe_version(None).await.ok();
    let (client, mut events) = connect(config).await.map_err(DiscoveryError::Ipc)?;
    let mut observed_client_types = Vec::new();
    // 短暂监听 client-status-changed,记录当前在线客户端类型(结构信息,无内容)。
    let _ = timeout(Duration::from_millis(1500), async {
        while let Some(event) = events.recv().await {
            if let super::client::IpcEvent::Broadcast { method, params, .. } = event {
                if method == super::messages::method::CLIENT_STATUS_CHANGED {
                    if let Ok(status) = serde_json::from_value::<ClientStatusChangedParams>(params)
                    {
                        if status.status == "connected" {
                            if let Some(t) = status.client_type {
                                if !observed_client_types.contains(&t) {
                                    observed_client_types.push(t);
                                }
                            }
                        }
                    }
                }
            }
        }
    })
    .await;
    let probe = DesktopProbe {
        socket_path: client.config().socket_path.clone(),
        version,
        client_id: client.client_id().to_string(),
        observed_client_types,
    };
    drop(client);
    Ok(probe)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_path_prefers_primary_then_fallback() {
        let custom_home = std::env::temp_dir().join(format!("ac-home-{}", std::process::id()));
        std::fs::create_dir_all(custom_home.join("ipc")).unwrap();
        let primary = custom_home.join("ipc").join("ipc.sock");
        std::fs::write(&primary, b"").unwrap(); // 普通文件:不是 socket
                                                // 主路径存在(即使不是 socket)时优先返回主路径;连接层负责报错。
        assert_eq!(socket_path(None, Some(custom_home.clone())), Some(primary));
        std::fs::remove_dir_all(custom_home).unwrap();
    }

    #[test]
    fn explicit_path_wins() {
        let explicit = PathBuf::from("/tmp/ac-explicit.sock");
        assert_eq!(socket_path(Some(explicit.clone()), None), Some(explicit));
    }

    #[test]
    fn version_gate_is_closed_for_unknown() {
        assert!(version_is_verified("0.153.0-alpha.5"));
        assert!(version_is_verified("codex-cli 0.153.0-alpha.5"));
        assert!(version_is_verified("0.153.1"));
        assert!(version_is_verified("codex-cli 0.153.1"));
        assert!(!version_is_verified("codex-cli 0.999.0"));
        assert!(!version_is_verified("0.999.0"));
        assert!(!version_is_verified(""));
    }
}
