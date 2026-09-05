#!/usr/bin/env bash
# Remove the per-user macOS Bridge service and installed binary. Pairing data is
# preserved unless --purge-data is explicitly supplied.
set -euo pipefail

LABEL="com.hxf.agent-console.bridge"
USER_ID="$(id -u)"
SERVICE_TARGET="gui/$USER_ID/$LABEL"
INSTALL_ROOT="$HOME/Library/Application Support/com.hxf.agent-console"
BIN_PATH="$INSTALL_ROOT/bin/bridge"
PLIST_PATH="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_DIR="$HOME/Library/Logs/com.hxf.agent-console"
STDOUT_LOG="$LOG_DIR/bridge.stdout.log"
STDERR_LOG="$LOG_DIR/bridge.stderr.log"
DEFAULT_DATA_DIR="$HOME/Library/Application Support/agent-console"
DATA_DIR=""
CONFIGURED_DATA_DIR=""
PURGE_DATA=false

usage() {
  printf '%s\n' \
    "用法: $0 [选项]" \
    "" \
    "选项:" \
    "  --purge-data        同时清除 Keychain 绑定和 Bridge 本地数据" \
    "  --data-dir <目录>   指定要清除的数据目录（仅与 --purge-data 一起使用）" \
    "  -h, --help          显示帮助"
}

die() {
  printf '错误: %s\n' "$*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --purge-data)
      PURGE_DATA=true
      shift
      ;;
    --data-dir)
      [[ $# -ge 2 && -n "$2" ]] || die "--data-dir 缺少参数"
      DATA_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "未知参数: $1"
      ;;
  esac
done

[[ "$(uname -s)" == "Darwin" ]] || die "该卸载器仅支持 macOS"
[[ "$PURGE_DATA" == true || -z "$DATA_DIR" ]] || die "--data-dir 只能与 --purge-data 一起使用"

SERVICE_LOADED=false
if launchctl print "$SERVICE_TARGET" >/dev/null 2>&1; then
  SERVICE_LOADED=true
fi

if [[ -e "$PLIST_PATH" ]]; then
  EXISTING_LABEL="$(/usr/bin/plutil -extract Label raw -o - "$PLIST_PATH" 2>/dev/null || true)"
  EXISTING_PROGRAM="$(/usr/bin/plutil -extract ProgramArguments.0 raw -o - "$PLIST_PATH" 2>/dev/null || true)"
  EXISTING_BRIDGE="$(/usr/bin/plutil -extract ProgramArguments.7 raw -o - "$PLIST_PATH" 2>/dev/null || true)"
  [[ "$EXISTING_LABEL" == "$LABEL" && "$EXISTING_PROGRAM" == "/usr/bin/env" && "$EXISTING_BRIDGE" == "$BIN_PATH" ]] || \
    die "目标 LaunchAgent 不属于本卸载器，拒绝删除: $PLIST_PATH"
  CONFIGURED_DATA_ARGUMENT="$(/usr/bin/plutil -extract ProgramArguments.6 raw -o - "$PLIST_PATH" 2>/dev/null || true)"
  case "$CONFIGURED_DATA_ARGUMENT" in
    AGENT_CONSOLE_DATA_DIR=*) CONFIGURED_DATA_DIR="${CONFIGURED_DATA_ARGUMENT#AGENT_CONSOLE_DATA_DIR=}" ;;
    *) die "LaunchAgent 缺少可识别的数据目录配置" ;;
  esac
  if [[ -n "$DATA_DIR" && -n "$CONFIGURED_DATA_DIR" && "$DATA_DIR" != "$CONFIGURED_DATA_DIR" ]]; then
    die "--data-dir 与已安装 LaunchAgent 的数据目录不一致"
  fi
  DATA_DIR="${DATA_DIR:-$CONFIGURED_DATA_DIR}"
elif [[ "$SERVICE_LOADED" == true ]]; then
  die "launchd 中有同名服务但缺少预期 plist，拒绝卸载: $SERVICE_TARGET"
fi

if [[ "$SERVICE_LOADED" == true ]]; then
  launchctl bootout "$SERVICE_TARGET"
fi

if [[ "$PURGE_DATA" == true ]]; then
  DATA_DIR="${DATA_DIR:-$DEFAULT_DATA_DIR}"
  [[ "$DATA_DIR" == /* ]] || die "--data-dir 必须是绝对路径"
  [[ -x "$BIN_PATH" ]] || die "找不到已安装 Bridge，无法安全清除 Keychain 绑定"
  [[ -d "$DATA_DIR" ]] || die "数据目录不存在，无法确定并清除对应 Keychain 绑定"

  DATA_PARENT="$(cd "$(dirname "$DATA_DIR")" && pwd -P)"
  DATA_NAME="$(basename "$DATA_DIR")"
  [[ "$DATA_NAME" != "." && "$DATA_NAME" != ".." ]] || die "拒绝清除不安全的数据目录"
  DATA_DIR="$DATA_PARENT/$DATA_NAME"
  case "$DATA_DIR" in
    "$HOME"/*) ;;
    *) die "--purge-data 只允许清除当前用户主目录内的数据" ;;
  esac

  env AGENT_CONSOLE_DATA_DIR="$DATA_DIR" "$BIN_PATH" unpair --confirm
fi

/bin/rm -f "$PLIST_PATH" "$BIN_PATH" "$STDOUT_LOG" "$STDERR_LOG"
/bin/rmdir "$INSTALL_ROOT/bin" "$INSTALL_ROOT" "$LOG_DIR" 2>/dev/null || true

if [[ "$PURGE_DATA" == true ]]; then
  /bin/rm -rf -- "$DATA_DIR"
  printf 'Bridge、LaunchAgent、Keychain 绑定与本地数据均已清除。\n'
else
  printf '%s\n' \
    "Bridge 与 LaunchAgent 已卸载。" \
    "本地数据和 Keychain 绑定已保留；重新安装后可继续使用原绑定。"
fi
