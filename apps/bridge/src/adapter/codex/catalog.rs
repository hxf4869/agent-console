//! Codex Desktop SQLite 只读目录(权威规格 §12 SQLite 规则)。
//!
//! - 只读连接(`read_only(true)`);不开 immutable(WAL 活跃时会读坏)、
//!   不改 journal mode、不运行 migration/VACUUM/写 PRAGMA;
//!   busy_timeout 极短 + 查询外层 tokio 超时(默认 2s),不锁 Desktop。
//! - 路径探测:`$CODEX_HOME/state_*.sqlite` 与 `thread_history_*.sqlite`,
//!   版本号取现存最大;不存在 → 对应能力关闭,不 panic。
//! - schema capability probe:先查表/列,缺列只关闭对应能力(§12)。
//! - 会话目录与历史分页带稳定游标;`cwd` 绝不出 adapter
//!   (只输出项目显示名 = projects.name 或 cwd 尾段)。
//! - `fixture` 子模块提供合成 schema fixture 生成器,供本文件测试与 e2e
//!   复用;绝不复制真实数据库。

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use thiserror::Error;
use tokio::time::timeout;

use crate::domain::{ActiveTurnPhase, HistoryEntry, Item, ItemContent, ItemId, Turn, TurnId};

use super::projection;

/// 默认查询超时(§12:查询必须有超时,不能锁住 Desktop)。
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(2);
/// 分页默认值(§27.5:SessionSummary/HistoryPage 默认 50、最大 200)。
pub const DEFAULT_PAGE_SIZE: usize = 50;
pub const MAX_PAGE_SIZE: usize = 200;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("catalog capability unavailable: {0}")]
    Unavailable(String),
    #[error("catalog query timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// Codex home;None → `$CODEX_HOME` 或 `~/.codex`。
    pub codex_home: Option<PathBuf>,
    pub query_timeout: Duration,
    pub default_page_size: usize,
    pub max_page_size: usize,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            codex_home: None,
            query_timeout: DEFAULT_QUERY_TIMEOUT,
            default_page_size: DEFAULT_PAGE_SIZE,
            max_page_size: MAX_PAGE_SIZE,
        }
    }
}

/// catalog 能力(§12 schema probe 结果;不含内容)。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogCapabilities {
    /// 探测到的库文件名(仅文件名,不含绝对路径)。
    pub state_db: Option<String>,
    pub history_db: Option<String>,
    pub sessions_listable: bool,
    pub history_readable: bool,
    pub spawn_edges_readable: bool,
    pub project_names_readable: bool,
}

/// 会话目录条目(§11.1 的 catalog 侧事实;cwd 只以显示名出现)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogThread {
    pub id: String,
    pub title: Option<String>,
    /// projects.name 或 cwd 尾段;绝不是本机绝对路径(§12)。
    pub project_display_name: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub git_branch: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub archived: bool,
    pub agent_nickname: Option<String>,
    pub agent_role: Option<String>,
}

/// 会话列表页(稳定游标:updated_at_ms + id,§27.5)。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SessionListPage {
    pub threads: Vec<CatalogThread>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// 历史页:条目 + turn 状态(状态映射到 ActiveTurnPhase/LastTurnOutcome)。
#[derive(Debug, Clone, Default)]
pub struct CatalogHistoryPage {
    pub entries: Vec<HistoryEntry>,
    pub turns: Vec<Turn>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// Codex SQLite 只读目录句柄。克隆廉价(内部为连接池)。
pub struct CodexCatalog {
    state_pool: Option<SqlitePool>,
    history_pool: Option<SqlitePool>,
    caps: CatalogCapabilities,
    /// threads 表实际存在的列(缺列降级时动态裁剪 SELECT,§12)。
    threads_cols: Vec<String>,
    has_updated_at_ms: bool,
    timeout: Duration,
    default_page_size: usize,
    max_page_size: usize,
}

impl std::fmt::Debug for CodexCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 绝对路径不进 Debug(§25.3)。
        f.debug_struct("CodexCatalog")
            .field("caps", &self.caps)
            .finish_non_exhaustive()
    }
}

impl CodexCatalog {
    /// 打开目录:探测库文件与 schema;缺库/缺列只关闭能力,不 panic。
    pub async fn open(config: CatalogConfig) -> Result<Self, CatalogError> {
        let home = match config.codex_home.clone() {
            Some(home) => home,
            None => default_codex_home()
                .ok_or_else(|| CatalogError::Unavailable("codex home not found".into()))?,
        };
        let state_path = probe_versioned(&home, "state_", ".sqlite");
        let history_path = probe_versioned(&home, "thread_history_", ".sqlite");

        let state_pool = match &state_path {
            Some(path) => Some(open_read_only(path).await?),
            None => None,
        };
        let history_pool = match &history_path {
            Some(path) => Some(open_read_only(path).await?),
            None => None,
        };

        let mut caps = CatalogCapabilities {
            state_db: state_path.as_ref().map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            }),
            history_db: history_path.as_ref().map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            }),
            ..Default::default()
        };
        let mut has_updated_at_ms = false;

        if let Some(pool) = &state_pool {
            let threads_cols = table_columns(pool, "threads").await?;
            let required = ["id", "title", "updated_at"];
            caps.sessions_listable = required
                .iter()
                .all(|c| threads_cols.iter().any(|col| col.as_str() == *c));
            has_updated_at_ms = threads_cols.iter().any(|col| col == "updated_at_ms");
            caps.project_names_readable = table_exists(pool, "projects").await?
                && table_columns(pool, "projects")
                    .await?
                    .iter()
                    .any(|col| col == "name");
            let edges_cols = table_columns(pool, "thread_spawn_edges").await?;
            caps.spawn_edges_readable = ["parent_thread_id", "child_thread_id"]
                .iter()
                .all(|c| edges_cols.iter().any(|col| col.as_str() == *c));
        }
        if let Some(pool) = &history_pool {
            let turns_cols = table_columns(pool, "thread_turns").await?;
            let items_cols = table_columns(pool, "thread_items").await?;
            let turns_ok = ["thread_id", "turn_id", "rollout_ordinal", "status"]
                .iter()
                .all(|c| turns_cols.iter().any(|col| col.as_str() == *c));
            let items_ok = [
                "thread_id",
                "turn_id",
                "item_id",
                "rollout_ordinal",
                "item_type",
                "item_json",
            ]
            .iter()
            .all(|c| items_cols.iter().any(|col| col.as_str() == *c));
            caps.history_readable = turns_ok && items_ok;
        }

        let threads_cols = if let Some(pool) = &state_pool {
            table_columns(pool, "threads").await?
        } else {
            Vec::new()
        };

        Ok(Self {
            state_pool,
            history_pool,
            caps,
            threads_cols,
            has_updated_at_ms,
            timeout: config.query_timeout,
            default_page_size: config.default_page_size.max(1),
            max_page_size: config.max_page_size.max(1),
        })
    }

    pub fn capabilities(&self) -> &CatalogCapabilities {
        &self.caps
    }

    /// 未归档(默认)或全部会话的分页列表;游标 = `updated_at_ms:id`。
    pub async fn list_sessions(
        &self,
        cursor: Option<&str>,
        limit: u32,
        include_archived: bool,
    ) -> Result<SessionListPage, CatalogError> {
        let Some(pool) = self
            .state_pool
            .as_ref()
            .filter(|_| self.caps.sessions_listable)
        else {
            return Err(CatalogError::Unavailable(
                "sessions listing disabled by schema probe".into(),
            ));
        };
        let limit = self.clamp_page_size(limit);
        // 列必须带表前缀:projects 也有 updated_at_ms,JOIN 后会歧义。
        let order_expr = if self.has_updated_at_ms {
            "COALESCE(t.updated_at_ms, t.updated_at * 1000)"
        } else {
            "t.updated_at * 1000"
        };
        let archived_clause = if include_archived {
            ""
        } else {
            "WHERE archived = 0 "
        };
        // projects 表缺失时(缺列降级)不带 JOIN,显示名退回 cwd 尾段。
        let (project_select, project_join) = if self.caps.project_names_readable {
            (
                ", p.name AS project_name",
                "LEFT JOIN projects p ON t.project_id = p.id ",
            )
        } else {
            ("", "")
        };
        // 稳定游标分页:ORDER BY 更新时间 DESC, id DESC;cursor 严格小于。
        // 注意:SQLite 的 WHERE 不能用 SELECT 别名,游标条件复用完整表达式。
        let mut sql = format!(
            "SELECT {}, {order_expr} AS updated_ms{project_select} \
             FROM threads t {project_join}\
             {archived_clause}",
            self.thread_select_cols()
        );
        let mut bind_ms: Option<i64> = None;
        let mut bind_id: Option<String> = None;
        if let Some((ms, id)) = cursor.and_then(parse_list_cursor) {
            let op = if archived_clause.is_empty() {
                "WHERE"
            } else {
                "AND"
            };
            sql.push_str(&format!(
                "{op} ({order_expr} < ? OR ({order_expr} = ? AND t.id < ?)) "
            ));
            bind_ms = Some(ms);
            bind_id = Some(id);
        }
        sql.push_str(&format!(
            "ORDER BY updated_ms DESC, t.id DESC LIMIT {}",
            limit + 1
        ));

        let mut query = sqlx::query(&sql);
        if let Some(ms) = bind_ms {
            query = query
                .bind(ms)
                .bind(ms)
                .bind(bind_id.clone().unwrap_or_default());
        }
        let rows = self.run(pool, query).await?;
        let has_more = rows.len() > limit;
        let mut threads = Vec::new();
        for row in rows.into_iter().take(limit) {
            // 注意:sqlx SQLite 把 NULL 文本解码为 ""(不报错),因此可空列
            // 必须显式按 Option<String> 读取,否则类型推断会选中 String。
            let cwd: Option<String> = row.try_get::<Option<String>, _>("cwd").ok().flatten();
            let project_name: Option<String> = row
                .try_get::<Option<String>, _>("project_name")
                .ok()
                .flatten();
            threads.push(CatalogThread {
                id: row.try_get("id")?,
                title: row.try_get::<Option<String>, _>("title").ok().flatten(),
                // 绝对路径在这里被消化掉:只剩显示名(§12)。
                project_display_name: project_name
                    .or_else(|| cwd.and_then(|c| projection::project_display_from_cwd(&c))),
                model: row.try_get::<Option<String>, _>("model").ok().flatten(),
                reasoning_effort: row
                    .try_get::<Option<String>, _>("reasoning_effort")
                    .ok()
                    .flatten(),
                git_branch: row
                    .try_get::<Option<String>, _>("git_branch")
                    .ok()
                    .flatten(),
                created_at: row
                    .try_get::<Option<i64>, _>("created_at")
                    .ok()
                    .flatten()
                    .and_then(secs_to_datetime),
                updated_at: row
                    .try_get::<Option<i64>, _>("updated_ms")
                    .ok()
                    .flatten()
                    .and_then(ms_to_datetime),
                archived: row.try_get::<i64, _>("archived").unwrap_or(0) != 0,
                agent_nickname: row
                    .try_get::<Option<String>, _>("agent_nickname")
                    .ok()
                    .flatten(),
                agent_role: row
                    .try_get::<Option<String>, _>("agent_role")
                    .ok()
                    .flatten(),
            });
        }
        let next_cursor = if has_more {
            threads.last().map(|t| {
                format!(
                    "{}:{}",
                    t.updated_at.map(|d| d.timestamp_millis()).unwrap_or(0),
                    t.id
                )
            })
        } else {
            None
        };
        Ok(SessionListPage {
            threads,
            next_cursor,
            has_more,
        })
    }

    /// 单会话历史(向前翻页):thread_turns + thread_items 按 rollout_ordinal
    /// 游标从最新向最早读取;单页上限受 clamp。
    pub async fn session_history(
        &self,
        thread_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<CatalogHistoryPage, CatalogError> {
        let Some(pool) = self
            .history_pool
            .as_ref()
            .filter(|_| self.caps.history_readable)
        else {
            return Err(CatalogError::Unavailable(
                "history reading disabled by schema probe".into(),
            ));
        };
        let limit = self.clamp_page_size(limit);
        let before = cursor.and_then(parse_history_cursor).unwrap_or(i64::MAX);
        let sql = format!(
            "SELECT i.turn_id, i.item_id, i.item_type, i.item_json, i.created_at_ms, \
                    i.rollout_ordinal, t.status AS turn_status, t.started_at, t.completed_at, \
                    t.duration_ms \
             FROM thread_items i JOIN thread_turns t \
               ON t.thread_id = i.thread_id AND t.turn_id = i.turn_id \
             WHERE i.thread_id = ? AND i.rollout_ordinal < ? \
             ORDER BY i.rollout_ordinal DESC LIMIT {}",
            limit + 1
        );
        let rows = self
            .run(pool, sqlx::query(&sql).bind(thread_id).bind(before))
            .await?;
        let has_more = rows.len() > limit;
        let mut entries = Vec::new();
        let mut turns_map: std::collections::BTreeMap<String, Turn> = Default::default();
        let mut last_ordinal: Option<i64> = None;
        for row in rows.into_iter().take(limit) {
            let turn_id: String = row.try_get("turn_id")?;
            let item_id: String = row.try_get("item_id")?;
            let item_type: String = row.try_get("item_type")?;
            let item_json: String = row.try_get("item_json")?;
            last_ordinal = Some(row.try_get::<i64, _>("rollout_ordinal")?);
            let parsed: serde_json::Value =
                serde_json::from_str(&item_json).unwrap_or(serde_json::Value::Null);
            let content = if parsed.is_null() {
                ItemContent::Opaque {
                    native_type: item_type.clone(),
                }
            } else {
                projection::parse_item_content(&item_type, &parsed)
            };
            let created = row
                .try_get::<Option<i64>, _>("created_at_ms")
                .ok()
                .flatten()
                .and_then(ms_to_datetime);
            entries.push(HistoryEntry {
                turn: TurnId::native(turn_id.clone()),
                item: Item {
                    item_id: ItemId::native(item_id),
                    turn: Some(TurnId::native(turn_id.clone())),
                    revision: 0,
                    created_at: created,
                    content,
                },
            });
            let status: String = row.try_get("turn_status")?;
            let turn_for_map = turn_id.clone();
            turns_map
                .entry(turn_id)
                .or_insert_with(|| turn_from_status(&turn_for_map, &status, &row));
        }
        let next_cursor = if has_more {
            last_ordinal.map(|o| format!("before:{o}"))
        } else {
            None
        };
        Ok(CatalogHistoryPage {
            entries,
            turns: turns_map.into_values().collect(),
            next_cursor,
            has_more,
        })
    }

    /// 子 Agent 关系(§12:子任务嵌套在父下)。
    pub async fn child_threads(
        &self,
        parent_thread_id: &str,
    ) -> Result<Vec<CatalogThread>, CatalogError> {
        let Some(pool) = self
            .state_pool
            .as_ref()
            .filter(|_| self.caps.spawn_edges_readable)
        else {
            return Err(CatalogError::Unavailable(
                "spawn edges disabled by schema probe".into(),
            ));
        };
        let (project_select, project_join) = if self.caps.project_names_readable {
            (
                ", p.name AS project_name",
                "LEFT JOIN projects p ON t.project_id = p.id ",
            )
        } else {
            ("", "")
        };
        let sql = format!(
            "SELECT {}, {order_expr} AS updated_ms{project_select} \
             FROM thread_spawn_edges e \
             JOIN threads t ON t.id = e.child_thread_id \
             {project_join}\
             WHERE e.parent_thread_id = ? AND e.status = 'open' \
             ORDER BY updated_ms DESC LIMIT 200",
            self.thread_select_cols(),
            order_expr = if self.has_updated_at_ms {
                "COALESCE(t.updated_at_ms, t.updated_at * 1000)"
            } else {
                "t.updated_at * 1000"
            }
        );
        let rows = self
            .run(pool, sqlx::query(&sql).bind(parent_thread_id))
            .await?;
        let mut out = Vec::new();
        for row in rows {
            // 可空列必须显式 Option<String>(sqlx SQLite 把 NULL 文本读成 "")。
            let cwd: Option<String> = row.try_get::<Option<String>, _>("cwd").ok().flatten();
            let project_name: Option<String> = row
                .try_get::<Option<String>, _>("project_name")
                .ok()
                .flatten();
            out.push(CatalogThread {
                id: row.try_get("id")?,
                title: row.try_get::<Option<String>, _>("title").ok().flatten(),
                project_display_name: project_name
                    .or_else(|| cwd.and_then(|c| projection::project_display_from_cwd(&c))),
                model: row.try_get::<Option<String>, _>("model").ok().flatten(),
                reasoning_effort: row
                    .try_get::<Option<String>, _>("reasoning_effort")
                    .ok()
                    .flatten(),
                git_branch: row
                    .try_get::<Option<String>, _>("git_branch")
                    .ok()
                    .flatten(),
                created_at: row
                    .try_get::<Option<i64>, _>("created_at")
                    .ok()
                    .flatten()
                    .and_then(secs_to_datetime),
                updated_at: row
                    .try_get::<Option<i64>, _>("updated_ms")
                    .ok()
                    .flatten()
                    .and_then(ms_to_datetime),
                archived: row.try_get::<i64, _>("archived").unwrap_or(0) != 0,
                agent_nickname: row
                    .try_get::<Option<String>, _>("agent_nickname")
                    .ok()
                    .flatten(),
                agent_role: row
                    .try_get::<Option<String>, _>("agent_role")
                    .ok()
                    .flatten(),
            });
        }
        Ok(out)
    }

    async fn run<'q>(
        &self,
        pool: &SqlitePool,
        query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    ) -> Result<Vec<sqlx::sqlite::SqliteRow>, CatalogError> {
        let timeout_ms = self.timeout.as_millis() as u64;
        // ZERO 超时 = 立即拒绝执行(tokio 的 timeout 只能在 future 真正
        // yield 时打断;inline 完成的 sqlite 查询不会被 0 时限打断)。
        if self.timeout.is_zero() {
            return Err(CatalogError::Timeout { timeout_ms: 0 });
        }
        match timeout(self.timeout, query.fetch_all(pool)).await {
            Ok(result) => result.map_err(CatalogError::Sql),
            Err(_) => Err(CatalogError::Timeout { timeout_ms }),
        }
    }

    /// threads 列选择:按 probe 结果裁剪可选列(缺列降级,§12)。
    /// 必需列(id/title/updated_at)由 sessions_listable 保证。
    fn thread_select_cols(&self) -> String {
        let has = |name: &str| self.threads_cols.iter().any(|c| c == name);
        let mut parts: Vec<String> = vec!["t.id".into(), "t.title".into()];
        for optional in [
            "cwd",
            "archived",
            "git_branch",
            "model",
            "reasoning_effort",
            "agent_nickname",
            "agent_role",
            "created_at",
            "updated_at",
        ] {
            if has(optional) {
                parts.push(format!("t.{optional}"));
            }
        }
        parts.join(", ")
    }

    fn clamp_page_size(&self, limit: u32) -> usize {
        let limit = limit as usize;
        if limit == 0 {
            self.default_page_size
        } else {
            limit.min(self.max_page_size)
        }
    }
}

// ---------------------------------------------------------------------------
// 内部:连接、探测与解析
// ---------------------------------------------------------------------------

fn default_codex_home() -> Option<PathBuf> {
    if let Some(home) = std::env::var_os("CODEX_HOME") {
        return Some(PathBuf::from(home));
    }
    let home = std::env::var_os("HOME").map(PathBuf::from)?;
    Some(home.join(".codex"))
}

/// 探测 `home/<prefix><N><suffix>`:返回 N 最大的现存文件(§12 版本号可变)。
fn probe_versioned(home: &Path, prefix: &str, suffix: &str) -> Option<PathBuf> {
    let mut best: Option<(u32, PathBuf)> = None;
    let entries = std::fs::read_dir(home).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(rest) = name.strip_prefix(prefix) else {
            continue;
        };
        let Some(number) = rest.strip_suffix(suffix) else {
            continue;
        };
        if let Ok(n) = number.parse::<u32>() {
            if best.as_ref().map(|(b, _)| n > *b).unwrap_or(true) {
                best = Some((n, entry.path()));
            }
        }
    }
    best.map(|(_, path)| path)
}

/// 只读连接:read_only + 极短 busy_timeout;不动 journal mode、不用 immutable
/// (WAL 活跃时 immutable 会读到不一致快照)。
async fn open_read_only(path: &Path) -> Result<SqlitePool, CatalogError> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .busy_timeout(Duration::from_millis(150));
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await?;
    Ok(pool)
}

async fn table_exists(pool: &SqlitePool, table: &str) -> Result<bool, CatalogError> {
    let row =
        sqlx::query("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table)
            .fetch_one(pool)
            .await?;
    Ok(row.try_get::<i64, _>("n").unwrap_or(0) > 0)
}

async fn table_columns(pool: &SqlitePool, table: &str) -> Result<Vec<String>, CatalogError> {
    let rows = sqlx::query(&format!("SELECT name FROM pragma_table_info('{table}')"))
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.try_get::<String, _>("name").ok())
        .collect())
}

/// 列表游标:`<updated_at_ms>:<id>`。
fn parse_list_cursor(cursor: &str) -> Option<(i64, String)> {
    let (ms, id) = cursor.split_once(':')?;
    Some((ms.parse().ok()?, id.to_string()))
}

/// 历史游标:`before:<rollout_ordinal>`。
fn parse_history_cursor(cursor: &str) -> Option<i64> {
    cursor.strip_prefix("before:")?.parse().ok()
}

fn ms_to_datetime(ms: i64) -> Option<DateTime<Utc>> {
    if ms <= 0 {
        return None;
    }
    Utc.timestamp_millis_opt(ms).single()
}

fn secs_to_datetime(secs: i64) -> Option<DateTime<Utc>> {
    if secs <= 0 {
        return None;
    }
    Utc.timestamp_opt(secs, 0).single()
}

/// turn 状态 → 领域 Turn(§10.4/§10.7 映射)。
fn turn_from_status(turn_id: &str, status: &str, row: &sqlx::sqlite::SqliteRow) -> Turn {
    let outcome = projection::map_turn_outcome(Some(status));
    let phase = if status == "inProgress" {
        ActiveTurnPhase::Running
    } else {
        ActiveTurnPhase::Idle
    };
    Turn {
        turn_id: TurnId::native(turn_id),
        created_at: None,
        updated_at: row
            .try_get::<Option<i64>, _>("completed_at")
            .ok()
            .flatten()
            .and_then(ms_to_datetime)
            .or_else(|| {
                row.try_get::<Option<i64>, _>("started_at")
                    .ok()
                    .flatten()
                    .and_then(ms_to_datetime)
            }),
        phase,
        outcome,
    }
}

// ---------------------------------------------------------------------------
// fixture:合成 schema(供本文件测试与 e2e 复用;绝不复制真实库)
// ---------------------------------------------------------------------------

pub mod fixture {
    use super::*;

    /// fixture 库路径对。
    #[derive(Debug, Clone)]
    pub struct FixturePaths {
        pub state_db: PathBuf,
        pub history_db: PathBuf,
    }

    /// 在 `dir` 下创建与真实 schema 同构的合成库并插入脱敏数据。
    ///
    /// 合成内容:3 个会话(fixture-alpha 主任务 / fixture-archived 已归档 /
    /// fixture-child 子任务),2 个 turn,覆盖全部已验证 item_type + 一个未知
    /// 类型;所有文本均为 "fixture-*" 占位。
    pub async fn create_catalog_fixture(dir: &Path) -> anyhow::Result<FixturePaths> {
        tokio::fs::create_dir_all(dir).await?;
        let state_db = dir.join("state_5.sqlite");
        let history_db = dir.join("thread_history_1.sqlite");
        let state = open_writable(&state_db).await?;
        let history = open_writable(&history_db).await?;

        sqlx::query(
            "CREATE TABLE projects (
                id TEXT PRIMARY KEY, name TEXT NOT NULL,
                metadata TEXT NOT NULL DEFAULT '{}', position INTEGER NOT NULL DEFAULT 0,
                created_at_ms INTEGER NOT NULL DEFAULT 0, updated_at_ms INTEGER NOT NULL DEFAULT 0
             )",
        )
        .execute(&state)
        .await?;
        sqlx::query(
            "CREATE TABLE threads (
                id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                cwd TEXT NOT NULL, title TEXT NOT NULL,
                archived INTEGER NOT NULL DEFAULT 0,
                git_branch TEXT, model TEXT, reasoning_effort TEXT,
                agent_nickname TEXT, agent_role TEXT,
                created_at_ms INTEGER, updated_at_ms INTEGER, project_id TEXT
             )",
        )
        .execute(&state)
        .await?;
        sqlx::query(
            "CREATE TABLE thread_spawn_edges (
                parent_thread_id TEXT NOT NULL,
                child_thread_id TEXT NOT NULL PRIMARY KEY,
                status TEXT NOT NULL
             )",
        )
        .execute(&state)
        .await?;

        sqlx::query("INSERT INTO projects (id, name, updated_at_ms) VALUES ('proj-1', 'fixture-project', 1000)")
            .execute(&state)
            .await?;
        let threads: &[(&str, &str, &str, i64, i64, i64, Option<&str>)] = &[
            // (id, title, cwd, created_ms, updated_ms, archived, project)
            (
                "11111111-1111-4111-8111-111111111111",
                "fixture-alpha",
                "/tmp/fixture-alpha",
                1_000,
                9_000,
                0,
                Some("proj-1"),
            ),
            (
                "22222222-2222-4222-8222-222222222222",
                "fixture-archived",
                "/tmp/fixture-archived",
                2_000,
                3_000,
                1,
                None,
            ),
            (
                "33333333-3333-4333-8333-333333333333",
                "fixture-child",
                "/tmp/fixture-child",
                4_000,
                5_000,
                0,
                None,
            ),
        ];
        for (id, title, cwd, created, updated, archived, project) in threads {
            sqlx::query(
                "INSERT INTO threads (id, created_at, updated_at, cwd, title, archived, \
                 git_branch, model, reasoning_effort, agent_nickname, agent_role, \
                 created_at_ms, updated_at_ms, project_id) \
                 VALUES (?, ?, ?, ?, ?, ?, 'fixture-branch', 'gpt-5.3-fixture', 'medium', \
                 NULL, NULL, ?, ?, ?)",
            )
            .bind(id)
            .bind(created / 1000)
            .bind(updated / 1000)
            .bind(cwd)
            .bind(title)
            .bind(*archived)
            .bind(created)
            .bind(updated)
            .bind(project)
            .execute(&state)
            .await?;
        }
        sqlx::query(
            "INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status) \
             VALUES ('11111111-1111-4111-8111-111111111111', \
             '33333333-3333-4333-8333-333333333333', 'open')",
        )
        .execute(&state)
        .await?;

        // ---- 历史库:2 个 turn,覆盖 item 类型映射 ----
        sqlx::query(
            "CREATE TABLE thread_turns (
                thread_id TEXT NOT NULL, turn_id TEXT NOT NULL,
                rollout_ordinal INTEGER NOT NULL, status TEXT NOT NULL,
                error_json TEXT, started_at INTEGER, completed_at INTEGER,
                duration_ms INTEGER,
                PRIMARY KEY (thread_id, turn_id)
             )",
        )
        .execute(&history)
        .await?;
        sqlx::query(
            "CREATE TABLE thread_items (
                thread_id TEXT NOT NULL, turn_id TEXT NOT NULL, item_id TEXT NOT NULL,
                rollout_ordinal INTEGER NOT NULL, created_at_ms INTEGER NOT NULL,
                item_json TEXT NOT NULL, item_type TEXT NOT NULL DEFAULT '',
                updated_at_ordinal INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (thread_id, turn_id, item_id)
             )",
        )
        .execute(&history)
        .await?;

        let main = "11111111-1111-4111-8111-111111111111";
        sqlx::query(
            "INSERT INTO thread_turns (thread_id, turn_id, rollout_ordinal, status, \
             started_at, completed_at, duration_ms) VALUES (?, 'turn-f1', 10, 'completed', \
             1000, 2000, 1000)",
        )
        .bind(main)
        .execute(&history)
        .await?;
        sqlx::query(
            "INSERT INTO thread_turns (thread_id, turn_id, rollout_ordinal, status, \
             started_at, completed_at, duration_ms) VALUES (?, 'turn-f2', 20, 'inProgress', \
             3000, NULL, NULL)",
        )
        .bind(main)
        .execute(&history)
        .await?;

        let items: &[(i64, &str, &str, serde_json::Value)] = &[
            (
                11,
                "item-u1",
                "userMessage",
                serde_json::json!({
                    "id": "item-u1", "content": [{"type": "text", "text": "fixture-input"}]
                }),
            ),
            (
                12,
                "item-r1",
                "reasoning",
                serde_json::json!({
                    "id": "item-r1", "summary": "fixture-reasoning", "content": "hidden"
                }),
            ),
            (
                13,
                "item-c1",
                "commandExecution",
                serde_json::json!({
                    "id": "item-c1", "command": "fixture-cmd", "status": "completed",
                    "aggregatedOutput": "fixture-output\n", "exitCode": 0, "durationMs": 42
                }),
            ),
            (
                14,
                "item-a1",
                "agentMessage",
                serde_json::json!({
                    "id": "item-a1", "text": "fixture-reply", "phase": "final_answer"
                }),
            ),
            (
                21,
                "item-f1",
                "fileChange",
                serde_json::json!({
                    "id": "item-f1",
                    "changes": [{"path": "fixture.txt", "kind": "modified"}],
                    "status": "completed"
                }),
            ),
            (
                22,
                "item-s1",
                "subAgentActivity",
                serde_json::json!({
                    "id": "item-s1", "kind": "fixture-subagent",
                    "agentThreadId": "33333333-3333-4333-8333-333333333333", "status": "completed"
                }),
            ),
            (
                23,
                "item-m1",
                "mcpToolCall",
                serde_json::json!({
                    "id": "item-m1", "server": "fixture-server", "tool": "fixture_tool",
                    "status": "completed", "durationMs": 7
                }),
            ),
            (
                24,
                "item-x1",
                "brandNewType",
                serde_json::json!({"id": "item-x1"}),
            ),
        ];
        for (ordinal, item_id, item_type, json) in items {
            sqlx::query(
                "INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, \
                 created_at_ms, item_json, item_type) VALUES (?, 'turn-f1', ?, ?, ?, ?, ?)",
            )
            .bind(main)
            .bind(item_id)
            .bind(ordinal)
            .bind(1000 + ordinal)
            .bind(json.to_string())
            .bind(item_type)
            .execute(&history)
            .await?;
        }
        // turn-f2:进行中的一条命令(部分输出;§10.4 RUNNING 形态)。
        sqlx::query(
            "INSERT INTO thread_items (thread_id, turn_id, item_id, rollout_ordinal, \
             created_at_ms, item_json, item_type) VALUES (?, 'turn-f2', ?, 21, 3000, ?, ?)",
        )
        .bind(main)
        .bind("item-live-1")
        .bind(
            serde_json::json!({
                "id": "item-live-1", "command": "fixture-live-cmd", "status": "inProgress",
                "aggregatedOutput": "partial\n"
            })
            .to_string(),
        )
        .bind("commandExecution")
        .execute(&history)
        .await?;
        state.close().await;
        history.close().await;
        Ok(FixturePaths {
            state_db,
            history_db,
        })
    }

    /// 降级 fixture:threads 缺 model/reasoning_effort/agent_nickname/
    /// agent_role/project_id/projects 表,验证缺列只关闭对应能力。
    pub async fn create_degraded_fixture(dir: &Path) -> anyhow::Result<FixturePaths> {
        tokio::fs::create_dir_all(dir).await?;
        let state_db = dir.join("state_5.sqlite");
        let history_db = dir.join("thread_history_1.sqlite");
        let state = open_writable(&state_db).await?;
        let history = open_writable(&history_db).await?;
        sqlx::query(
            "CREATE TABLE threads (
                id TEXT PRIMARY KEY, created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL, cwd TEXT NOT NULL, title TEXT NOT NULL,
                archived INTEGER NOT NULL DEFAULT 0
             )",
        )
        .execute(&state)
        .await?;
        sqlx::query(
            "INSERT INTO threads (id, created_at, updated_at, cwd, title, archived) \
             VALUES ('dd-1', 1, 2, '/tmp/fixture-degraded', 'fixture-degraded', 0)",
        )
        .execute(&state)
        .await?;
        sqlx::query(
            "CREATE TABLE thread_turns (thread_id TEXT NOT NULL, turn_id TEXT NOT NULL, \
             rollout_ordinal INTEGER NOT NULL, status TEXT NOT NULL, PRIMARY KEY (thread_id, turn_id))",
        )
        .execute(&history)
        .await?;
        sqlx::query(
            "CREATE TABLE thread_items (thread_id TEXT NOT NULL, turn_id TEXT NOT NULL, \
             item_id TEXT NOT NULL, rollout_ordinal INTEGER NOT NULL, item_json TEXT NOT NULL, \
             item_type TEXT NOT NULL DEFAULT '', PRIMARY KEY (thread_id, turn_id, item_id))",
        )
        .execute(&history)
        .await?;
        state.close().await;
        history.close().await;
        Ok(FixturePaths {
            state_db,
            history_db,
        })
    }

    async fn open_writable(path: &Path) -> anyhow::Result<SqlitePool> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        Ok(pool)
    }
}
