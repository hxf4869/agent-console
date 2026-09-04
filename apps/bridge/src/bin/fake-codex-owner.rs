//! fake-codex-owner headless bin:独立进程的假 Codex Desktop(路由器 +
//! owner),供 CodexAdapter 集成测试与无 UI e2e 使用(§13.4/§29.4)。
//!
//! 薄壳:读取命令行参数与脚本 JSON,把实际服务器实现委托给
//! [`bridge::adapter::codex::fake_owner::run_server`](library 入口,进程内
//! spawn 供无 UI e2e 复用)。行为契约与脚本格式(含 backgroundCommands 扩展)
//! 见该模块文档。

use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("用法: fake-codex-owner <socket_path> <script_json_path>");
        std::process::exit(2);
    }
    let socket_path = PathBuf::from(&args[1]);
    let script_path = PathBuf::from(&args[2]);

    let script_text = std::fs::read_to_string(&script_path).unwrap_or_else(|err| {
        eprintln!("[fake-owner] 读取脚本失败: {err}");
        std::process::exit(2);
    });
    let script: serde_json::Value = serde_json::from_str(&script_text).unwrap_or_else(|err| {
        eprintln!("[fake-owner] 脚本不是合法 JSON: {err}");
        std::process::exit(2);
    });

    if let Err(err) = bridge::adapter::codex::fake_owner::run_server(socket_path, script).await {
        eprintln!("[fake-owner] server error: {err}");
        std::process::exit(1);
    }
}
