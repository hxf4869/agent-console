//! 设备配对客户端 v2(§21:Bridge 主动发起方向)。
//!
//! 流程(与 Relay 新配对合同对齐;URL 全部来自
//! [`crate::config::RelayUrls`] 对 `AGENT_CONSOLE_RELAY_URL` 基地址的唯一派生):
//! 1. Bridge 本地生成 32 字节高熵 challenge(base64url,§21 步骤 1);
//! 2. `POST {origin}/agent-console/bridge/pairing/register`
//!    `{challenge, deviceName, platform, arch, bridgeVersion}`(camelCase)→
//!    `{challengeId, shortCode, expiresAt}`;
//! 3. CLI 打印短码与二维码数据(深链 `{origin}/agent-console/pair?code=<短码>`),
//!    轮询(默认 2s,总超时 5 分钟)`POST {origin}/agent-console/bridge/pairing/claim`
//!    `{challengeId, challenge}`:`{status:"pending"}` 继续;
//!    `{status:"ready", deviceId, deviceCredential}` → 停止(明文凭据单次交付);
//! 4. 凭据先入 Keychain,成功后才落绑定状态(§19:凭据不进 SQLite)。
//!
//! 错误分类:429 限流(稍后再试)、410 挑战过期(重新 pair)、409 已消费、
//! 网络错误;凭据/challenge/短码不进日志与错误串(§25.3)。

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use base64::Engine;
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::config::RelayUrls;
use crate::keychain::KeychainStore;
use crate::local_store::{BindingStatus, LocalStore};

/// claim 轮询间隔(§21:挑战 5 分钟有效;轮询不宜过密)。
pub const CLAIM_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// 配对整体超时(= Relay 挑战有效期 5 分钟)。
pub const PAIR_DEADLINE: Duration = Duration::from_secs(5 * 60);

/// 轮询节奏(默认即上述常量;测试注入更短值)。
#[derive(Debug, Clone)]
pub struct PairingOptions {
    pub poll_interval: Duration,
    pub deadline: Duration,
}

impl Default for PairingOptions {
    fn default() -> Self {
        Self {
            poll_interval: CLAIM_POLL_INTERVAL,
            deadline: PAIR_DEADLINE,
        }
    }
}

/// 配对注册结果:短码与二维码数据可立即展示;内部字段供 [`PairingClient::
/// wait_approval`] 使用;Debug 不输出任何字段(challenge/短码不进日志,§25.3)。
pub struct PairingRegistration {
    pub short_code: String,
    /// 二维码数据字符串:深链 `{origin}/agent-console/pair?code=<shortCode>`。
    pub qr_data: String,
    challenge: String,
    challenge_id: String,
}

impl std::fmt::Debug for PairingRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PairingRegistration")
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// wire 类型(camelCase;字段与 Relay 配对合同一一对应)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RegisterBody<'a> {
    challenge: &'a str,
    device_name: &'a str,
    platform: &'a str,
    arch: &'a str,
    bridge_version: &'a str,
}

/// Relay 响应 `{challengeId, shortCode, expiresAt}`;expiresAt 不参与本地
/// 判定(整体超时按 [`PAIR_DEADLINE`]),故不建模。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegisterResponse {
    challenge_id: String,
    short_code: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClaimBody<'a> {
    challenge_id: &'a str,
    challenge: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClaimResponse {
    status: String,
    #[serde(default)]
    device_id: Option<String>,
    #[serde(default)]
    device_credential: Option<String>,
}

// ---------------------------------------------------------------------------
// 客户端
// ---------------------------------------------------------------------------

/// 配对客户端:register / wait_approval 分步暴露,供 headless CLI(先展示
/// 短码再等待)与未来 Tauri shell 复用(§19:生命周期 API 不写死在 CLI)。
pub struct PairingClient {
    http: reqwest::Client,
    urls: RelayUrls,
}

impl std::fmt::Debug for PairingClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PairingClient").finish_non_exhaustive()
    }
}

impl PairingClient {
    /// 解析并校验 Relay 基地址(`AGENT_CONSOLE_RELAY_URL` 合同,含
    /// "完整 WebSocket 地址" 专门提示)。失败即配置错误,不发起网络请求。
    pub fn new(relay_base: &str) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::new(),
            urls: RelayUrls::parse(relay_base)?,
        })
    }

    /// 规范化后的 Relay origin(绑定落库用)。
    pub fn origin(&self) -> &str {
        self.urls.http_origin()
    }

    /// §21 步骤 1-2:本地 challenge → register → `{challengeId, shortCode}`。
    pub async fn register(
        &self,
        device_name: &str,
        bridge_version: &str,
    ) -> anyhow::Result<PairingRegistration> {
        let challenge = new_challenge();
        let resp = self
            .http
            .post(self.urls.pairing_register_url())
            .json(&RegisterBody {
                challenge: &challenge,
                device_name,
                platform: std::env::consts::OS,
                arch: std::env::consts::ARCH,
                bridge_version,
            })
            .send()
            .await
            .context("连接 Relay 失败,请检查 AGENT_CONSOLE_RELAY_URL 与网络")?;
        if !resp.status().is_success() {
            bail!("配对注册被拒绝: {}", status_hint(resp.status()));
        }
        let body: RegisterResponse = resp.json().await.context("配对注册响应不是合法 JSON")?;
        if body.challenge_id.is_empty() || body.short_code.is_empty() {
            bail!("配对注册响应缺少 challengeId/shortCode");
        }
        let qr_data = self.urls.pairing_deep_link(&body.short_code);
        Ok(PairingRegistration {
            short_code: body.short_code,
            qr_data,
            challenge,
            challenge_id: body.challenge_id,
        })
    }

    /// §21 步骤 5-7:轮询 claim 直到批准或超时。
    ///
    /// 返回 `(device_id, credential 明文)`;凭据只在本返回值出现一次,
    /// 调用方必须立即交由 [`bind`] 写入 Keychain,不得记录或持久化明文。
    pub async fn wait_approval(
        &self,
        registration: &PairingRegistration,
        options: PairingOptions,
    ) -> anyhow::Result<(String, String)> {
        let deadline = tokio::time::Instant::now() + options.deadline;
        let (device_id, credential) = loop {
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "配对确认超时({} 秒内未获批准),请重新执行 bridge pair",
                    options.deadline.as_secs()
                );
            }
            tokio::time::sleep(options.poll_interval).await;
            let resp = self
                .http
                .post(self.urls.pairing_claim_url())
                .json(&ClaimBody {
                    challenge_id: &registration.challenge_id,
                    challenge: &registration.challenge,
                })
                .send()
                .await
                .context("轮询配对状态失败(网络错误)")?;
            match resp.status() {
                reqwest::StatusCode::TOO_MANY_REQUESTS => {
                    bail!("配对尝试过于频繁(HTTP 429),请稍后再试");
                }
                reqwest::StatusCode::GONE => {
                    bail!("配对挑战已过期(HTTP 410),请重新执行 bridge pair");
                }
                reqwest::StatusCode::CONFLICT => {
                    bail!("配对挑战已被消费(HTTP 409),请重新执行 bridge pair");
                }
                status if !status.is_success() => {
                    bail!("配对失败: {}", status_hint(status));
                }
                _ => {}
            }
            let body: ClaimResponse = resp.json().await.context("配对状态响应不是合法 JSON")?;
            match body.status.as_str() {
                "pending" => continue,
                "ready" => {
                    let device_id = body
                        .device_id
                        .filter(|s| !s.is_empty())
                        .context("配对批准响应缺少 deviceId")?;
                    let credential = body
                        .device_credential
                        .filter(|s| !s.is_empty())
                        .context("配对批准响应缺少 deviceCredential")?;
                    break (device_id, credential);
                }
                other => bail!("配对状态响应异常(未知 status={other:?})"),
            }
        };
        Ok((device_id, credential))
    }
}

/// §21 步骤 7 + §19:凭据先入 Keychain,成功后才落绑定状态。
/// `origin` 来自 [`PairingClient::origin`]。
pub async fn bind(
    keychain: Arc<dyn KeychainStore>,
    store: &LocalStore,
    origin: &str,
    device_id: &str,
    credential: &str,
) -> anyhow::Result<()> {
    keychain
        .set_device_credential(device_id, credential)
        .await
        .context("写入 Keychain 失败;Bridge 保持未绑定态")?;
    store
        .set_binding(origin, device_id, BindingStatus::Paired)
        .await?;
    Ok(())
}

/// 32 字节高熵 challenge,base64url 无填充(§21 步骤 1)。
pub fn new_challenge() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// 短码大字号横幅(纯文本;短码本身不进日志,只进 pair 命令 stdout)。
pub fn short_code_banner(short_code: &str) -> String {
    let inner: String = short_code
        .chars()
        .map(|c| format!("{c} "))
        .collect::<String>()
        .trim_end()
        .to_owned();
    let width = inner.chars().count() + 8;
    let border = format!("+{}+", "=".repeat(width));
    let blank = format!("|{}|", " ".repeat(width));
    let line = format!("|{}{}{}|", " ".repeat(4), inner, " ".repeat(4));
    [border.clone(), blank.clone(), line, blank, border].join("\n")
}

/// 非 2xx 状态 → 面向用户的稳定提示(不含任何请求细节,§25.3)。
fn status_hint(status: reqwest::StatusCode) -> String {
    match status {
        reqwest::StatusCode::TOO_MANY_REQUESTS => "HTTP 429(尝试过于频繁),请稍后再试".to_owned(),
        reqwest::StatusCode::GONE => "HTTP 410(配对已过期),请重新执行 bridge pair".to_owned(),
        other => format!("HTTP {}", other.as_u16()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PAIRING_CLAIM_PATH, PAIRING_REGISTER_PATH};
    use crate::keychain::InMemoryKeychainStore;
    use axum::routing::post;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // -----------------------------------------------------------------------
    // 纯函数
    // -----------------------------------------------------------------------

    #[test]
    fn challenge_is_base64url_and_unique() {
        let c = new_challenge();
        assert_eq!(c.len(), 43, "32 字节 base64url 无填充应为 43 字符");
        assert!(
            c.chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'),
            "只允许 base64url 字母表: {c}"
        );
        assert_ne!(c, new_challenge(), "每次配对生成新 challenge");
    }

    #[test]
    fn banner_renders_short_code_large() {
        let text = short_code_banner("012345");
        assert!(text.contains("0 1 2 3 4 5"), "actual:\n{text}");
        assert!(text.starts_with('+') && text.contains('|'));
    }

    #[test]
    fn invalid_relay_base_fails_before_network() {
        // 完整 WebSocket 地址 → 专门提示(任务一合同的回归点)。
        let err = PairingClient::new("wss://toolbox.example.com/agent-console/ws")
            .unwrap_err()
            .to_string();
        assert!(err.contains("完整 WebSocket 地址"), "actual: {err}");
        assert!(err.contains("公网基地址"), "actual: {err}");
        assert!(PairingClient::new("ftp://x").is_err());
    }

    // -----------------------------------------------------------------------
    // axum stub:模拟 Relay v2 配对合同
    // -----------------------------------------------------------------------

    async fn spawn_stub(app: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    fn fast_options() -> PairingOptions {
        PairingOptions {
            poll_interval: Duration::from_millis(5),
            deadline: Duration::from_secs(10),
        }
    }

    /// 成功链:register(camelCase 校验)→ claim pending → ready 单次交付 →
    /// bind(Keychain 先、绑定后)。
    #[tokio::test]
    async fn pairs_pending_then_ready_against_relay_v2_stub() {
        let short_code = "024680".to_string();
        let device_id = uuid::Uuid::new_v4().to_string();
        let credential = "pairing-credential-plain-once".to_string();
        let seen_register = std::sync::Mutex::new(None::<serde_json::Value>);
        let seen_register = std::sync::Arc::new(seen_register);
        let seen_claim = std::sync::Mutex::new(None::<serde_json::Value>);
        let seen_claim = std::sync::Arc::new(seen_claim);
        let claim_calls = Arc::new(AtomicUsize::new(0));

        let app = axum::Router::new()
            .route(
                PAIRING_REGISTER_PATH,
                post({
                    let seen = seen_register.clone();
                    move |axum::Json(body): axum::Json<serde_json::Value>| async move {
                        *seen.lock().unwrap() = Some(body);
                        axum::Json(serde_json::json!({
                            "challengeId": "chg-1",
                            "shortCode": short_code,
                            "expiresAt": "2026-09-04T00:05:00Z",
                        }))
                    }
                }),
            )
            .route(
                PAIRING_CLAIM_PATH,
                post({
                    let seen = seen_claim.clone();
                    let calls = claim_calls.clone();
                    let device_id = device_id.clone();
                    let credential = credential.clone();
                    move |axum::Json(body): axum::Json<serde_json::Value>| async move {
                        *seen.lock().unwrap() = Some(body);
                        let n = calls.fetch_add(1, Ordering::SeqCst);
                        if n == 0 {
                            axum::Json(serde_json::json!({ "status": "pending" }))
                        } else {
                            axum::Json(serde_json::json!({
                                "status": "ready",
                                "deviceId": device_id,
                                "deviceCredential": credential,
                            }))
                        }
                    }
                }),
            );
        let base = spawn_stub(app).await;

        let client = PairingClient::new(&base).unwrap();
        let reg = client.register("e2e-mac", "0.1.0").await.unwrap();
        assert_eq!(reg.short_code, "024680");
        assert_eq!(
            reg.qr_data,
            RelayUrls::parse(&base).unwrap().pairing_deep_link("024680")
        );

        // register 请求体:camelCase + challenge 由客户端生成。
        let register_body = seen_register.lock().unwrap().clone().unwrap();
        assert_eq!(register_body["deviceName"], "e2e-mac");
        assert_eq!(register_body["bridgeVersion"], "0.1.0");
        assert_eq!(register_body["platform"], std::env::consts::OS);
        assert_eq!(register_body["arch"], std::env::consts::ARCH);
        let challenge = register_body["challenge"].as_str().unwrap().to_owned();
        assert_eq!(challenge.len(), 43);
        assert!(register_body.get("shortCode").is_none());

        let (paired_device, paired_credential) =
            client.wait_approval(&reg, fast_options()).await.unwrap();
        assert_eq!(paired_device, device_id);
        assert_eq!(paired_credential, credential);
        // claim 请求体回带同一 challenge 与 challengeId。
        let claim_body = seen_claim.lock().unwrap().clone().unwrap();
        assert_eq!(claim_body["challengeId"], "chg-1");
        assert_eq!(claim_body["challenge"], challenge);
        assert!(
            claim_calls.load(Ordering::SeqCst) >= 2,
            "应经历 pending→ready"
        );

        // bind:凭据只在 Keychain;绑定落本地库(§19)。
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(&dir.path().join("data")).await.unwrap();
        let keychain = Arc::new(InMemoryKeychainStore::new());
        bind(
            keychain.clone(),
            &store,
            client.origin(),
            &paired_device,
            &paired_credential,
        )
        .await
        .unwrap();
        assert_eq!(
            keychain.get_device_credential(&device_id).await.unwrap(),
            Some(credential)
        );
        let binding = store.get_binding().await.unwrap();
        assert_eq!(binding.device_id, device_id);
        assert_eq!(binding.status, crate::local_store::BindingStatus::Paired);
        assert_eq!(
            binding.relay_url,
            RelayUrls::parse(&base).unwrap().http_origin()
        );
    }

    #[tokio::test]
    async fn wait_approval_times_out_when_always_pending() {
        let app = axum::Router::new().route(
            PAIRING_CLAIM_PATH,
            post(|| async { axum::Json(serde_json::json!({ "status": "pending" })) }),
        );
        let base = spawn_stub(app).await;
        let client = PairingClient::new(&base).unwrap();
        let reg = PairingRegistration {
            short_code: "111111".to_owned(),
            qr_data: String::new(),
            challenge: new_challenge(),
            challenge_id: "chg-to".to_owned(),
        };
        let err = client
            .wait_approval(
                &reg,
                PairingOptions {
                    poll_interval: Duration::from_millis(5),
                    deadline: Duration::from_millis(60),
                },
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("超时"), "actual: {err}");
        assert!(err.contains("重新执行"), "actual: {err}");
    }

    /// 429 / 410 / 409 的错误分类与提示。
    #[tokio::test]
    async fn wait_approval_maps_rate_limit_expired_and_consumed() {
        for (status, expected) in [(429, "稍后再试"), (410, "过期"), (409, "已被消费")] {
            let app = axum::Router::new().route(
                PAIRING_CLAIM_PATH,
                post(move || async move { axum::http::StatusCode::from_u16(status).unwrap() }),
            );
            let base = spawn_stub(app).await;
            let client = PairingClient::new(&base).unwrap();
            let reg = PairingRegistration {
                short_code: "222222".to_owned(),
                qr_data: String::new(),
                challenge: new_challenge(),
                challenge_id: "chg-x".to_owned(),
            };
            let err = client
                .wait_approval(&reg, fast_options())
                .await
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&status.to_string()) && err.contains(expected),
                "status {status}: actual: {err}"
            );
            // 错误串不含 challenge/challengeId(§25.3)。
            assert!(!err.contains(&reg.challenge_id));
        }
    }

    #[tokio::test]
    async fn register_rejection_is_reported_with_hint() {
        let app = axum::Router::new().route(
            PAIRING_REGISTER_PATH,
            post(|| async { axum::http::StatusCode::TOO_MANY_REQUESTS }),
        );
        let base = spawn_stub(app).await;
        let client = PairingClient::new(&base).unwrap();
        let err = client
            .register("e2e-mac", "0.1.0")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("429") && err.contains("稍后再试"),
            "actual: {err}"
        );
    }

    #[tokio::test]
    async fn register_network_error_is_reported() {
        // 无监听端口 → 连接失败(网络错误分类)。
        let client = PairingClient::new("http://127.0.0.1:1").unwrap();
        let err = client
            .register("e2e-mac", "0.1.0")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("连接 Relay 失败"), "actual: {err}");
    }
}
