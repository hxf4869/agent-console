#!/usr/bin/env bash
# Build or install the macOS Bridge, pair it when needed, and register a
# per-user launchd service. No administrator privileges are required.
set -euo pipefail

LABEL="com.hxf.agent-console.bridge"
USER_ID="$(id -u)"
LAUNCH_DOMAIN="gui/$USER_ID"
SERVICE_TARGET="$LAUNCH_DOMAIN/$LABEL"
INSTALL_ROOT="$HOME/Library/Application Support/com.hxf.agent-console"
BIN_DIR="$INSTALL_ROOT/bin"
BIN_PATH="$BIN_DIR/bridge"
PLIST_PATH="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_DIR="$HOME/Library/Logs/com.hxf.agent-console"
STDOUT_LOG="$LOG_DIR/bridge.stdout.log"
STDERR_LOG="$LOG_DIR/bridge.stderr.log"
DEFAULT_DATA_DIR="$HOME/Library/Application Support/agent-console"
DATA_DIR="${AGENT_CONSOLE_DATA_DIR:-$DEFAULT_DATA_DIR}"
RELAY_URL="${AGENT_CONSOLE_RELAY_URL:-}"
BINARY_SOURCE=""
PAIR_MODE="if-needed"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

usage() {
  printf '%s\n' \
    "用法: $0 --relay-url <公网基地址> [选项]" \
    "" \
    "选项:" \
    "  --relay-url <URL>    Relay 公网基地址，例如 https://toolbox.example.com" \
    "  --binary <文件>      安装已有 Bridge 二进制；缺省时从当前仓库 release 构建" \
    "  --data-dir <目录>    Bridge 数据目录；缺省为 ~/Library/Application Support/agent-console" \
    "  --force-pair         即使已有绑定也重新配对" \
    "  --no-pair            不发起配对；仅在已有绑定时启动服务" \
    "  -h, --help           显示帮助"
}

die() {
  printf '错误: %s\n' "$*" >&2
  exit 1
}

require_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 缺少参数"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --relay-url)
      require_value "$@"
      RELAY_URL="$2"
      shift 2
      ;;
    --binary)
      require_value "$@"
      BINARY_SOURCE="$2"
      shift 2
      ;;
    --data-dir)
      require_value "$@"
      DATA_DIR="$2"
      shift 2
      ;;
    --force-pair)
      PAIR_MODE="always"
      shift
      ;;
    --no-pair)
      PAIR_MODE="never"
      shift
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

[[ "$(uname -s)" == "Darwin" ]] || die "该安装器仅支持 macOS"
[[ -n "$RELAY_URL" ]] || die "请用 --relay-url 或 AGENT_CONSOLE_RELAY_URL 提供 Relay 公网基地址"
[[ "$DATA_DIR" == /* ]] || die "--data-dir 必须是绝对路径"
USER_TMPDIR="$(/usr/bin/getconf DARWIN_USER_TEMP_DIR 2>/dev/null || true)"
USER_TMPDIR="${USER_TMPDIR:-/tmp}"
/bin/mkdir -p "$DATA_DIR"
DATA_DIR="$(cd "$DATA_DIR" && pwd -P)"
case "$DATA_DIR" in
  "$HOME"/*) ;;
  *) die "--data-dir 必须位于当前用户主目录内" ;;
esac

if [[ -z "$BINARY_SOURCE" ]]; then
  command -v cargo >/dev/null 2>&1 || die "找不到 cargo，无法从源码构建 Bridge"
  CARGO_TARGET_ROOT="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
  if [[ "$CARGO_TARGET_ROOT" != /* ]]; then
    CARGO_TARGET_ROOT="$REPO_ROOT/$CARGO_TARGET_ROOT"
  fi
  printf '正在构建 Bridge release 二进制…\n'
  (
    cd "$REPO_ROOT"
    cargo build --release --locked -p bridge --bin bridge
  )
  BINARY_SOURCE="$CARGO_TARGET_ROOT/release/bridge"
elif [[ "$BINARY_SOURCE" != /* ]]; then
  BINARY_SOURCE="$(cd "$(dirname "$BINARY_SOURCE")" && pwd -P)/$(basename "$BINARY_SOURCE")"
fi

[[ -f "$BINARY_SOURCE" && -x "$BINARY_SOURCE" ]] || die "Bridge 二进制不可执行: $BINARY_SOURCE"

SERVICE_LOADED=false
if launchctl print "$SERVICE_TARGET" >/dev/null 2>&1; then
  SERVICE_LOADED=true
fi

if [[ -e "$PLIST_PATH" ]]; then
  EXISTING_LABEL="$(/usr/bin/plutil -extract Label raw -o - "$PLIST_PATH" 2>/dev/null || true)"
  EXISTING_PROGRAM="$(/usr/bin/plutil -extract ProgramArguments.0 raw -o - "$PLIST_PATH" 2>/dev/null || true)"
  EXISTING_BRIDGE="$(/usr/bin/plutil -extract ProgramArguments.7 raw -o - "$PLIST_PATH" 2>/dev/null || true)"
  [[ "$EXISTING_LABEL" == "$LABEL" && "$EXISTING_PROGRAM" == "/usr/bin/env" && "$EXISTING_BRIDGE" == "$BIN_PATH" ]] || \
    die "目标 LaunchAgent 已存在但不属于本安装器: $PLIST_PATH"
elif [[ "$SERVICE_LOADED" == true ]]; then
  die "launchd 中已有同名服务但缺少预期 plist，拒绝覆盖: $SERVICE_TARGET"
fi

/bin/mkdir -p "$BIN_DIR" "$(dirname "$PLIST_PATH")" "$LOG_DIR"
/bin/chmod 700 "$INSTALL_ROOT" "$BIN_DIR" "$LOG_DIR" "$DATA_DIR"
/usr/bin/touch "$STDOUT_LOG" "$STDERR_LOG"
/bin/chmod 600 "$STDOUT_LOG" "$STDERR_LOG"

BINARY_TMP="$BIN_PATH.new.$$"
PLIST_TMP="$(mktemp "${TMPDIR:-/tmp}/agent-console-bridge.XXXXXX")"
cleanup() {
  /bin/rm -f "$BINARY_TMP" "$PLIST_TMP"
}
trap cleanup EXIT

/usr/bin/install -m 0755 "$BINARY_SOURCE" "$BINARY_TMP"

/usr/bin/plutil -create xml1 "$PLIST_TMP"
/usr/bin/plutil -insert Label -string "$LABEL" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments -array "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.0 -string "/usr/bin/env" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.1 -string "-i" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.2 -string "HOME=$HOME" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.3 -string "TMPDIR=$USER_TMPDIR" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.4 -string "PATH=/usr/bin:/bin:/usr/sbin:/sbin" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.5 -string "RUST_LOG=info" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.6 -string "AGENT_CONSOLE_DATA_DIR=$DATA_DIR" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.7 -string "$BIN_PATH" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.8 -string "run" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.9 -string "--relay-url" "$PLIST_TMP"
/usr/bin/plutil -insert ProgramArguments.10 -string "$RELAY_URL" "$PLIST_TMP"
/usr/bin/plutil -insert RunAtLoad -bool true "$PLIST_TMP"
/usr/bin/plutil -insert KeepAlive -bool true "$PLIST_TMP"
/usr/bin/plutil -insert ProcessType -string "Background" "$PLIST_TMP"
/usr/bin/plutil -insert ThrottleInterval -integer 10 "$PLIST_TMP"
/usr/bin/plutil -insert StandardOutPath -string "$STDOUT_LOG" "$PLIST_TMP"
/usr/bin/plutil -insert StandardErrorPath -string "$STDERR_LOG" "$PLIST_TMP"
/usr/bin/plutil -lint "$PLIST_TMP" >/dev/null

if [[ "$SERVICE_LOADED" == true ]]; then
  launchctl bootout "$SERVICE_TARGET"
fi

/bin/mv -f "$BINARY_TMP" "$BIN_PATH"
/bin/mv -f "$PLIST_TMP" "$PLIST_PATH"
/bin/chmod 600 "$PLIST_PATH"

DOCTOR_OUTPUT="$(env \
  AGENT_CONSOLE_DATA_DIR="$DATA_DIR" \
  AGENT_CONSOLE_RELAY_URL="$RELAY_URL" \
  "$BIN_PATH" doctor 2>&1)"
BOUND=false
if printf '%s\n' "$DOCTOR_OUTPUT" | /usr/bin/grep -q '^绑定: Paired device_id='; then
  BOUND=true
fi

if [[ "$PAIR_MODE" == "always" || ( "$PAIR_MODE" == "if-needed" && "$BOUND" == false ) ]]; then
  printf 'Bridge 尚未绑定，开始设备配对。\n'
  env \
    AGENT_CONSOLE_DATA_DIR="$DATA_DIR" \
    "$BIN_PATH" pair --relay-url "$RELAY_URL"
  BOUND=true
fi

if [[ "$BOUND" == false ]]; then
  printf '%s\n' \
    "Bridge 已安装，但因 --no-pair 且当前未绑定，未启动 LaunchAgent。" \
    "完成配对后重新运行本安装器即可启动。" \
    "二进制: $BIN_PATH" \
    "LaunchAgent: $PLIST_PATH"
  exit 0
fi

launchctl enable "$SERVICE_TARGET"
launchctl bootstrap "$LAUNCH_DOMAIN" "$PLIST_PATH"
launchctl print "$SERVICE_TARGET" >/dev/null

printf '%s\n' \
  "Bridge 安装完成并已由 launchd 常驻。" \
  "二进制: $BIN_PATH" \
  "LaunchAgent: $PLIST_PATH" \
  "日志: $STDOUT_LOG / $STDERR_LOG"
