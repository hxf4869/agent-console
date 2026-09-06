#!/usr/bin/env bash
# ZCode Hook 原型本机测试安装(仅限已授权的联调;全部可恢复)。
#
# 行为:
#   1. 确认 helper 二进制(默认 target/debug/bridge,可用 BRIDGE_BIN 覆盖)。
#   2. 生成已替换 /ABSOLUTE/PATH/ 的插件副本到 ~/.agent-console/zcode-test/plugin。
#   3. 在 ~/.zcode/cli/config.json 注册用户级测试 hooks + agent-console MCP。
#      管理边界(只动本项目的条目):
#      - 本项目 hook 识别标记: type=process 且 args 含 "zcode-hook"
#        (本项目 helper 专属子命令);同一条目内用户自己的 hook 不受影响。
#      - 本项目 MCP 识别标记: mcp.servers["agent-console"] 且
#        args==["mcp-stdio"]。同名条目若非该形态(用户自己配的),拒绝安装
#        并退出(退出码 1),不写任何配置、不覆盖。
#      - 幂等:重复安装不产生重复条目。
#   4. 把被修改键的原值(hooks 键是否存在 / hooks.enabled)记入安装清单
#      ~/.agent-console/zcode-test/install-manifest.json(与 .bak 时间戳备份
#      分开);restore.sh 按清单精确还原。
# 恢复: integrations/zcode/scripts/restore.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# scripts/ -> zcode/ -> integrations/ -> 仓库根
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
ZCODE_CONFIG="${ZCODE_CONFIG:-$HOME/.zcode/cli/config.json}"
PLUGIN_COPY_ROOT="${PLUGIN_COPY_ROOT:-$HOME/.agent-console/zcode-test}"
MANIFEST="$PLUGIN_COPY_ROOT/install-manifest.json"

if [ ! -x "$REPO_ROOT/target/debug/bridge" ] && [ -z "${BRIDGE_BIN:-}" ]; then
  echo "[install] 未找到 target/debug/bridge,先构建…"
  (cd "$REPO_ROOT" && cargo build -p bridge)
fi
BIN="${BRIDGE_BIN:-$REPO_ROOT/target/debug/bridge}"
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"
echo "[install] helper: $BIN"

# ---- 1. 插件副本(占位符替换) ----
mkdir -p "$PLUGIN_COPY_ROOT"
rm -rf "$PLUGIN_COPY_ROOT/plugin"
cp -R "$REPO_ROOT/integrations/zcode/plugin" "$PLUGIN_COPY_ROOT/plugin"
python3 - "$PLUGIN_COPY_ROOT/plugin/hooks/hooks.json" "$BIN" <<'PY'
import json, sys
path, bin_path = sys.argv[1], sys.argv[2]
with open(path) as f:
    data = json.load(f)
text = json.dumps(data)
text = text.replace("/ABSOLUTE/PATH/bridge", bin_path)
with open(path, "w") as f:
    f.write(text)
print(f"[install] 插件副本已写入并替换占位符: {path}")
PY

# ---- 2. 用户配置 hooks + MCP(备份/防重复/不覆盖他人条目/记录原值) ----
python3 - "$ZCODE_CONFIG" "$BIN" "$MANIFEST" <<'PY'
import json, os, shutil, sys, time

config_path, bin_path, manifest_path = sys.argv[1], sys.argv[2], sys.argv[3]

HOOK_MARKER = "zcode-hook"
MCP_NAME = "agent-console"
MCP_ARGS = ["mcp-stdio"]
EVENTS = ["PermissionRequest", "SessionStart", "UserPromptSubmit",
          "PostToolUse", "PostToolUseFailure", "Stop"]


def is_our_hook(hook):
    return (isinstance(hook, dict) and hook.get("type") == "process"
            and HOOK_MARKER in (hook.get("args") or []))


def is_our_mcp(server):
    return isinstance(server, dict) and (server.get("args") or []) == MCP_ARGS


with open(config_path) as f:
    config = json.load(f)

# ---- 冲突检测:在任何写入之前完成 ----
mcp_servers = (config.get("mcp") or {}).get("servers") or {}
existing_mcp = mcp_servers.get(MCP_NAME)
if existing_mcp is not None and not is_our_mcp(existing_mcp):
    print(f'[install] 拒绝安装:mcp.servers["{MCP_NAME}"] 已存在且非本项目管理'
          f"(command={existing_mcp.get('command')!r} args={existing_mcp.get('args')!r})。")
    print("[install] 为不覆盖你的配置,本次未做任何修改。"
          "如确需注册,请先在 ZCode 配置中移除或改名该条目后重试。")
    sys.exit(1)

hooks_raw = config.get("hooks")
if hooks_raw is not None and not isinstance(hooks_raw, dict):
    print(f"[install] 拒绝安装:hooks 配置类型异常({type(hooks_raw).__name__}),不做修改。")
    sys.exit(1)

# ---- 备份(时间戳快照,与清单互补;清单才是 restore 的还原依据) ----
stamp = time.strftime("%Y%m%d_%H%M%S")
backup = f"{config_path}.bak.ac-zcode-{stamp}"
shutil.copy2(config_path, backup)
print(f"[install] 备份: {backup}")

# ---- 原值清单:重复安装时保留第一次记录的最初原值 ----
previous = None
if os.path.exists(manifest_path):
    try:
        with open(manifest_path) as f:
            previous = json.load(f)
        print("[install] 已有安装清单,保留最初原值(重复安装幂等)")
    except ValueError:
        previous = None

hooks_previous = (previous or {}).get("hooks_previous") or {
    "present": "hooks" in config,
    "enabled": hooks_raw.get("enabled") if isinstance(hooks_raw, dict) else None,
}
manifest = {
    "version": 1,
    "installed_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    "helper": bin_path,
    "config_path": config_path,
    "hooks_previous": hooks_previous,
}

# ---- 修改 hooks:只替换本项目条目,用户条目/用户 hook 原样保留 ----
hooks = config.get("hooks")
if not isinstance(hooks, dict):
    hooks = {}
events = hooks.setdefault("events", {})
for event in EVENTS:
    entries = events.get(event) or []
    kept = []
    for entry in entries:
        entry_hooks = entry.get("hooks") or []
        if not any(is_our_hook(h) for h in entry_hooks):
            kept.append(entry)  # 用户条目,原样
            continue
        remaining = [h for h in entry_hooks if not is_our_hook(h)]
        if remaining:  # 混合条目:只移除本项目的 hook,保留用户的
            entry["hooks"] = remaining
            kept.append(entry)
        # 纯本项目条目:丢弃(下方重新追加当前 helper 的新条目)
    kept.append({
        "hooks": [{
            "type": "process",
            "command": bin_path,
            "args": [HOOK_MARKER],
            "timeoutMs": 60000 if event == "PermissionRequest" else 10000,
        }]
    })
    events[event] = kept
hooks["enabled"] = True
config["hooks"] = hooks

# ---- MCP:能走到这里说明同名条目要么不存在、要么本就是本项目形态 ----
mcp = config.setdefault("mcp", {}).setdefault("servers", {})
mcp[MCP_NAME] = {"type": "stdio", "command": bin_path, "args": list(MCP_ARGS)}

# ---- 写入:先清单后配置(清单写失败则配置保持原样) ----
os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
with open(manifest_path, "w") as f:
    json.dump(manifest, f, indent=2, ensure_ascii=False)
    f.write("\n")

with open(config_path, "w") as f:
    json.dump(config, f, indent=2, ensure_ascii=False)
    f.write("\n")

print(f"[install] 已注册:用户级 hooks(6 事件)+ agent-console MCP")
print(f"[install] 原值已记入清单: {manifest_path}"
      f" (hooks_previous={json.dumps(hooks_previous)})")
print("[install] 生效条件:ZCode 新会话(配置按会话快照取得)")
print("[install] 注意:插件方式与配置方式不要同时启用(双卡片)")
PY

echo "[install] 完成。恢复执行: $SCRIPT_DIR/restore.sh"
