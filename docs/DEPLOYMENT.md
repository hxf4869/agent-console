# 部署指南(DEPLOYMENT)

> 状态:**本轮未执行任何部署**。本文档描述可复现的部署拓扑与配置来源;
> 实际发布由部署负责人按此执行。

## 1. 生产拓扑(规格 §6)

```text
                    公网 80/443(仅此入口)
                            │
              ┌─────────────▼──────────────┐
              │  dev-toolbox 网关(Caddy    │   ← 唯一 TLS 终结点与认证入口
              │  或外置 Nginx)             │
              └───┬──────────────┬─────────┘
      同源 HTTPS JSON │              │ 同源 WSS Protobuf / 临时 HTTP 文件流
   forward-auth → api │              │ (直接转发)
              ┌───────▼───┐   ┌──────▼───────────────┐
              │ dev-toolbox│   │ Agent Console Relay  │
              │ API(Go)  │   │ (非 root,只读根 FS)│
              └───────────┘   └──────┬───────────────┘
                        ▲            │
        内部网络:consume │   ┌──────▼───────────────┐
        introspect/verify│   │ Agent Console        │
        (内部 Token)    │   │ PostgreSQL(独立库) │
                        │   └──────────────────────┘
              ┌─────────┴──────────┐
              │ WSS(设备凭据)+   │
              │ producer/consumer  │
              │ 出站 HTTPS         │
       ┌──────┴─────────┐          │
       │ Mac Headless   │◄─ 私有 IPC ── Codex Desktop
       │ Bridge         │◄─ 只读 ──── ~/.codex SQLite
       │ (Keychain 凭据)│
       └────────────────┘
```

信任要点:

- 浏览器只持有 dev-toolbox Cookie、CSRF 与 30 秒一次性 WS ticket;
  Cookie 不转发给 Relay。
- Relay 只信任网关网络(`TRUSTED_PROXY_CIDRS`)携带的身份头
  (`X-Agent-Console-Session-Id` / `-Owner-Id`)。
- Bridge 主动出站连接 Relay(WSS + producer/consumer HTTPS),Mac 不开入站端口。
- 内部端点(`/internal/*`,含 `/internal/agent-console/*`)公网无路由:
  dev-toolbox 网关对 `/internal/*` 一律 404,仅网关自身 forward_auth 与
  内网直连 api 可达。

## 2. Compose 生产用法

```bash
# 网络契约:relay 以 external 方式加入 dev-toolbox 反代所在网络(dev-toolbox
# compose 项目名 dev-toolbox + 网络 frontend,实际网络名 dev-toolbox_frontend),
# 并在该网络上以别名 agent-console-relay 暴露;无需手工 docker network create。
# 部署环境网络名不同时,用 AGENT_CONSOLE_GATEWAY_NETWORK 覆盖。

cp .env.example .env    # 设置 POSTGRES_PASSWORD、RELAY_INTERNAL_TOKEN 等

# 生产 overlay:叠加 compose.yaml 与 compose.production.yaml
docker compose --env-file .env \
  -f deploy/compose.yaml -f deploy/compose.production.yaml up -d
```

`deploy/compose.production.yaml` 已内置(规格 §25.4):

- `relay`:`read_only: true`、`user: "65532:65532"`(distroless nonroot)、
  仅 `tmpfs /tmp` 临时目录;
- **不映射任何宿主机端口**:公网 80/443 只由 gateway 暴露;PostgreSQL 仅在
  compose 内部网络可达;
- relay 服务名保持 `relay`,以网络别名 `agent-console-relay` 加入共享
  `dev-toolbox_frontend` 网络;dev-toolbox 网关上游
  `AGENT_CONSOLE_RELAY_UPSTREAM` 默认 `agent-console-relay:8081`(relay 内部
  监听 8081,不映射宿主机端口);
- 生产 relay 回调 dev-toolbox 内部端点默认走
  `DEVTOOLBOX_INTERNAL_BASE_URL=http://api:8080`(同一共享网络上的
  dev-toolbox api 服务名)。

## 3. 同域路由(唯一合同,Caddy / Nginx)

**生效配置以 dev-toolbox 仓库为准**(`deploy/Caddyfile` 与
`deploy/nginx/external-nginx.conf.template`);agent-console 仓库内的
`deploy/reverse-proxy/*` 是与之同构的参考模板。

唯一公网路由合同(网关只暴露 80/443):

| 公网路径 | 网关鉴权 | 上游 |
|---|---|---|
| `/agent-console/api/*` | `forward_auth` → dev-toolbox `GET /internal/agent-console/auth-sessions/verify`(克隆原请求 Cookie/Origin/X-CSRF-Token + `X-Agent-Console-Method`,Bearer 内部 Token);先删客户端伪造身份头,verify 通过后注入 `X-Agent-Console-Session-Id`/`-Owner-Id`;转发 relay 剥离 Cookie/CSRF/Origin/Authorization | `agent-console-relay:8081` |
| `/agent-console/ws` | 无 forward_auth;一次性 ticket 在 `Sec-WebSocket-Protocol` 由 Relay 握手消费,网关原样转发、不记录;剥 Cookie 与伪造身份头 | 同上 |
| `/agent-console/bridge/ws` | **无 Cookie forward_auth**;Authorization Bearer 设备凭据原样透传,由 Relay 校验 | 同上 |
| `/agent-console/bridge/pairing/register`、`/agent-console/bridge/pairing/claim` | **无 Cookie forward_auth**(Relay 按来源限速);网关层请求体上限 4KiB | 同上 |
| `/agent-console/transfers/*` | **无 forward_auth**(Bridge producer/consumer 无浏览器 Cookie);device credential + 短期 transfer token + 目标设备/方向由 Relay 校验;网关流式不缓冲(`flush_interval -1`) | 同上 |
| `/internal/*`(含 `/internal/agent-console/*`) | **公网无路由**,网关一律 404;仅网关自身 forward_auth 子请求与内网直连 api 可达 | — |
| Relay、PostgreSQL 宿主机端口 | 不映射生产宿主机端口;只有 gateway 暴露 80/443 | — |

Bridge producer/consumer 路径虽经公网 gateway 可达,但鉴权完全由 Relay 完成
(规格 §25.4)。

| 部署模式 | 文件 | 要点 |
|---|---|---|
| Caddy(默认) | `dev-toolbox/deploy/Caddyfile` | 按上表实现:`/agent-console/api/*` forward_auth;`/agent-console/ws` ticket 透传;`/agent-console/bridge/ws`、pairing 两端点、`/agent-console/transfers/*` 均无 Cookie forward_auth(pairing 加 4KiB 请求体上限,transfers 流式);`/internal/*` 一律 404 |
| 外置 Nginx | `dev-toolbox/deploy/nginx/external-nginx.conf.template` | 边缘按上表分段(`/agent-console/api/`、`= /agent-console/ws`、`/agent-console/bridge/`、`/agent-console/transfers/`);边缘不执行 agent-console 鉴权子请求,api 的 forward_auth 由内层 dev-toolbox gateway(Caddy)完成,边缘只剥离伪造身份头并保证 Cookie/Origin/CSRF、Authorization 与 WS 升级头/子协议原样透传 |

agent-console 侧参考模板(与生效配置同构,仅对照用途,勿直接套用):
`deploy/reverse-proxy/caddy.snippet.example`、
`deploy/reverse-proxy/nginx.agent-console.example.conf`。

### 3.1 Bridge 出站地址:`AGENT_CONSOLE_RELAY_URL`

`AGENT_CONSOLE_RELAY_URL` 是 **公网基地址(纯 origin,例如
`https://toolbox.example.com`;允许 http/https/ws/wss,ws/wss 内部规范化;
path 必须为空或 `/`,带 `/agent-console/...` 会被识别为误填完整 WS 地址并报错)**,
Bridge 一切出站路径由 `apps/bridge/src/config/mod.rs` 的 `RelayUrls` 统一派生,
全部 5 条:

| 派生路径 | 用途 | 鉴权 |
|---|---|---|
| `{origin}/agent-console/bridge/ws` | Bridge WSS 长连接(命令/事件/快照) | Bearer 设备凭据 |
| `{origin}/agent-console/bridge/pairing/register` | 配对注册(challenge + 设备信息 → 短码) | challenge 即凭证(Relay 按来源限速) |
| `{origin}/agent-console/bridge/pairing/claim` | 配对轮询领取凭据(单次交付) | 同上 |
| `{origin}/agent-console/transfers/producer/{transferId}` | 文件字节流出站 producer(POST) | 设备凭据 + transfer token |
| `{origin}/agent-console/transfers/consumer/{transferId}` | 上传字节流入站 consumer(GET) | 同上 |

配对二维码深链 `{origin}/agent-console/pair?code=<shortCode>` 亦由该 origin
派生(`RelayUrls::pairing_deep_link`),但它是前端页面地址,不是 Bridge 出站端点。
浏览器流量不走该变量:浏览器使用同源 `/agent-console/ws` 与
`/agent-console/api/*`(Cookie 会话)。

网关 forward-auth 只需注入 `X-Agent-Console-Session-Id` / `X-Agent-Console-Owner-Id`
两个身份头;`X-Agent-Console-Session-Expires` 是可选头(Relay 缺失时跳过本地
预过期检查,过期拦截由 verify 与 WS introspection 负责,见 error-codes.md
登记第 1 条)。

## 4. Secret 注入

| Secret | 注入位置 | 方式 |
|---|---|---|
| `AGENT_CONSOLE_INTERNAL_TOKEN`(≥32 字符) | dev-toolbox API、gateway(Caddy forward_auth 的 Bearer)、Relay(`RELAY_INTERNAL_TOKEN`,同值) | dev-toolbox `deploy/scripts/init-production.sh` 生成 `agent_console_internal_token` Secret;`gateway-entrypoint.sh` 注入 Caddy 环境;Compose 通过 secret/环境注入 Relay;**不进仓库、不进日志** |
| `POSTGRES_PASSWORD`(Relay 库) | compose(dev 可用 `.env`;生产用 secret 注入覆盖) | 独立于 dev-toolbox 数据库凭据 |
| VAPID 私钥 | Relay(`VAPID_PRIVATE_KEY_FILE` 指向的文件,secret 挂载;文件权限仅限运行用户) | 不进仓库与日志;对应公钥 `VAPID_PUBLIC_KEY` 单独下发给前端作 `applicationServerKey`(可选 `VAPID_SUBJECT`) |
| 设备凭据 | 不进服务器侧存储 | 只存在 Mac Keychain(Relay/PostgreSQL 仅存 SHA-256 摘要) |

未配置内部 Token 时 dev-toolbox 内部端点 fail closed(503);未配置 VAPID 时
push 发送禁用(订阅仍可存储)——两者都允许灰度上线。

## 5. 数据库要求

- Relay 使用**独立数据库与独立用户**(例:库 `agent_console`、用户
  `agent_console`);可复用现有 PostgreSQL 实例,但不得读写 dev-toolbox 业务表,
  不得共用用户。
- 生产 compose 中 postgres 不放开任何宿主机端口。
- 迁移由 Relay 启动时自动执行(空库可启动);回滚策略:迁移仅新增表/索引,
  回退镜像版本即可,不自动降级 schema。

## 6. 安全清单(§25.4,上线前逐项核对)

- [ ] Relay 容器非 root(`65532:65532`)、只读根文件系统,仅 `tmpfs /tmp`
- [ ] Relay 与 PostgreSQL 未映射宿主机端口;只有 gateway 暴露 80/443
- [ ] `TRUSTED_PROXY_CIDRS` 设置为 gateway 所在网络(默认 loopback 会让所有请求 401)
- [ ] 网关删除客户端伪造的内部身份头后再注入 verify 结果(Caddyfile 已实现)
- [ ] 浏览器路径(api/ws)的 Cookie/CSRF/Origin/Authorization 不转发给 Relay,
      Relay 响应不设置 Cookie;Bridge 路径(bridge/ws、pairing、transfers)的
      Authorization(设备凭据/transfer token)必须原样透传给 Relay
- [ ] `/internal/*` 公网 404 无路由;bridge/pairing 无 Cookie forward_auth 且
      网关请求体上限 4KiB;transfers 无 forward_auth(鉴权全在 Relay)
- [ ] producer/consumer 数据面同经公网 gateway,但鉴权完全由 Relay 完成
      (device credential + 短期 transfer token + 方向/目标设备校验)
- [ ] 内部 Token、VAPID 私钥、设备凭据不出现在仓库/日志/环境回显中
- [ ] 审计保留 30 天(Relay 维护任务自动清理);日志字段白名单(§25.3)
- [ ] Bridge 以普通用户运行,不请求管理员权限;凭据只入 macOS Keychain

## 7. 明确声明

- 本轮交付止步于可部署的配置与模板:**未部署、未推送、未创建 tag**。
- 生产上线前完成一次端到端冒烟(配对 → 登录 → WS → 快照 → 命令回执 →
  文件预览);同域真实网关链的自动化覆盖见 e2e_gateway 测试。
