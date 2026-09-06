#!/usr/bin/env bash
# 恢复 install.sh 对本机 ZCode 配置的改动(只动本项目拥有的条目):
#   - hooks:从各事件条目中精细移除本项目 hook(识别标记: type=process 且
#     args 含 "zcode-hook");用户同一条目内的 hook 原样保留;纯本项目条目
#     整条移除;事件为空则删除该事件。
#   - hooks.enabled/hooks 键:按安装清单
#     ~/.agent-console/zcode-test/install-manifest.json 还原安装前原值
#     (存在性 + enabled)。旧版脚本安装无清单时,从最早的"安装前"
#     .bak.ac-zcode-* 备份(不含本项目条目者)推断原值;推不出则不动该键
#     并提示对照备份。
#   - mcp.servers["agent-console"]:仅当条目为本项目形态
#     (args==["mcp-stdio"])时移除;用户自己配置的同名条目一律不动。
# 有改动时先做时间戳备份再写文件;无改动不写。插件副本目录一并删除。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZCODE_CONFIG="${ZCODE_CONFIG:-$HOME/.zcode/cli/config.json}"
PLUGIN_COPY_ROOT="${PLUGIN_COPY_ROOT:-$HOME/.agent-console/zcode-test}"

[ -f "$ZCODE_CONFIG" ] || { echo "[restore] 未找到 $ZCODE_CONFIG,无需恢复"; exit 0; }

python3 - "$ZCODE_CONFIG" "$PLUGIN_COPY_ROOT" <<'PY'
import glob, json, os, shutil, sys, time

config_path, plugin_root = sys.argv[1], sys.argv[2]
manifest_path = os.path.join(plugin_root, "install-manifest.json")

HOOK_MARKER = "zcode-hook"
MCP_NAME = "agent-console"
MCP_ARGS = ["mcp-stdio"]


def is_our_hook(hook):
    return (isinstance(hook, dict) and hook.get("type") == "process"
            and HOOK_MARKER in (hook.get("args") or []))


def is_our_mcp(server):
    return isinstance(server, dict) and (server.get("args") or []) == MCP_ARGS


def entry_has_our_hook(entry):
    return any(is_our_hook(h) for h in ((entry or {}).get("hooks") or []))


def backup_state_has_our_hooks(old_config):
    hooks = old_config.get("hooks")
    if not isinstance(hooks, dict):
        return False
    events = hooks.get("events")
    if not isinstance(events, dict):
        return False
    return any(
        entry_has_our_hook(entry)
        for entries in events.values()
        if isinstance(entries, list)
        for entry in entries
    )


def infer_hooks_previous_from_backups(config_path):
    """旧版脚本安装没有清单:从文件名最早的、不含本项目条目的
    .bak.ac-zcode-* 备份推断安装前 hooks 状态。"""
    base = os.path.basename(config_path)
    pattern = os.path.join(os.path.dirname(config_path), f"{base}.bak.ac-zcode-*")
    for cand in sorted(glob.glob(pattern)):
        name = os.path.basename(cand)
        if "restore" in name:
            continue
        try:
            with open(cand) as f:
                old = json.load(f)
        except (OSError, ValueError):
            continue
        if backup_state_has_our_hooks(old):
            continue  # 已是安装后状态,不是安装前快照
        hooks = old.get("hooks")
        return {
            "present": isinstance(hooks, dict),
            "enabled": hooks.get("enabled") if isinstance(hooks, dict) else None,
        }
    return None


with open(config_path) as f:
    config = json.load(f)

manifest = None
if os.path.exists(manifest_path):
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
    except ValueError:
        manifest = None

hook_removed = False
hooks_restored = False
mcp_removed = False

# 1) 移除本项目 hook 条目(精细到 hook 级)
hooks = config.get("hooks")
if isinstance(hooks, dict):
    events = hooks.get("events")
    if isinstance(events, dict):
        for event in list(events):
            entries = events.get(event) or []
            kept = []
            for entry in entries:
                if not entry_has_our_hook(entry):
                    kept.append(entry)  # 用户条目,原样
                    continue
                remaining = [h for h in (entry.get("hooks") or [])
                             if not is_our_hook(h)]
                if remaining:
                    entry["hooks"] = remaining  # 混合条目:只留用户的 hook
                    kept.append(entry)
                hook_removed = True
            if kept:
                events[event] = kept
            else:
                del events[event]
        # 事件清空后移除空 events 键,让下方"只剩 enabled"判定成立
        if isinstance(hooks.get("events"), dict) and not hooks["events"]:
            hooks.pop("events", None)

    # 2) 还原 hooks.enabled / hooks 键存在性
    hooks_previous = (manifest or {}).get("hooks_previous")
    if hooks_previous is None:
        hooks_previous = infer_hooks_previous_from_backups(config_path)
    if hooks_previous is not None:
        events_after = hooks.get("events")
        only_ours_left = (not isinstance(events_after, dict) or not events_after) \
            and set(hooks.keys()) <= {"enabled"}
        if not hooks_previous.get("present"):
            # 安装前无 hooks 键:恢复后只剩空壳则整体删除
            if only_ours_left:
                config.pop("hooks", None)
            else:
                hooks.pop("enabled", None)
        else:
            prev_enabled = hooks_previous.get("enabled")
            if prev_enabled is None:
                hooks.pop("enabled", None)
            else:
                hooks["enabled"] = prev_enabled
        hooks_restored = True
    else:
        print("[restore] 未找到安装清单,也无法从 .bak.ac-zcode-* 备份推断"
              "安装前 hooks 状态;hooks.enabled 未改动。"
              "如需还原请对照 ~/.zcode/cli/config.json.bak.ac-zcode-* 手工处理。")

# 3) 移除本项目形态的 agent-console MCP;用户同名条目不动
mcp = config.get("mcp")
if isinstance(mcp, dict):
    servers = mcp.get("servers")
    if isinstance(servers, dict) and is_our_mcp(servers.get(MCP_NAME)):
        del servers[MCP_NAME]
        mcp_removed = True
        # 因本项目而变空的层级一并删除,回到安装前形态(空 dict 无用户数据)
        if not servers:
            mcp.pop("servers", None)
        if not mcp:
            config.pop("mcp", None)

if not (hook_removed or hooks_restored or mcp_removed):
    print("[restore] 未发现本项目注册(已是干净状态)")
    sys.exit(0)

backup = f"{config_path}.bak.ac-zcode-restore-{time.strftime('%Y%m%d_%H%M%S')}"
shutil.copy2(config_path, backup)
print(f"[restore] 备份: {backup}")

with open(config_path, "w") as f:
    json.dump(config, f, indent=2, ensure_ascii=False)
    f.write("\n")

parts = []
if hook_removed:
    parts.append("本项目 hooks 条目")
if hooks_restored:
    parts.append("hooks.enabled/hooks 键原值")
if mcp_removed:
    parts.append("agent-console MCP")
print("[restore] 已恢复: " + "、".join(parts))
PY

# python 部分失败(非 0)时 set -e 直接退出,不会执行下方清理。
if [ -d "$PLUGIN_COPY_ROOT" ]; then
  rm -rf "$PLUGIN_COPY_ROOT"
  echo "[restore] 已删除插件副本目录与安装清单: $PLUGIN_COPY_ROOT"
fi
echo "[restore] 完成。若曾通过 Settings UI 启用插件,请在界面禁用/移除。"
