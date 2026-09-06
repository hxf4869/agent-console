#!/usr/bin/env bash
# install.sh / restore.sh 副本级全流程测试。
#
# 全部在临时目录内进行(模拟 HOME 布局),绝不触碰真实
# ~/.zcode/cli/config.json。运行结束后自清理临时文件。
#
# 覆盖场景:
#   1. hooks.enabled=false → install 后 true → restore 回到 false(用户 hook 保留)
#   2. 用户自有 agent-console MCP → install 拒绝(exit!=0,配置零改动) → restore 原样
#   3. 用户自有 PermissionRequest hook(独立条目 + 混合条目)→ install 共存 → restore 只删本项目
#   4. 重复安装两次 → 无重复条目、清单保留最初原值 → restore 干净
#   5. 干净配置 install → restore 与原文件语义等价(JSON 语义;格式相同时逐字节)
#   6. 旧版脚本安装状态模拟(无清单,有安装前 .bak 备份)→ 新版 restore 正确还原
#      (即当前真实配置的恢复方式)
#
# 用法: bash integrations/zcode/scripts/test-install-restore.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL="$SCRIPT_DIR/install.sh"
RESTORE="$SCRIPT_DIR/restore.sh"

TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/ac-zcode-test.XXXXXX")"
trap 'rm -rf "$TMPROOT"' EXIT

# 桩 helper:install/restore 只需要可执行路径,不实际运行
STUB_BIN="$TMPROOT/bin/stub-bridge"
mkdir -p "$(dirname "$STUB_BIN")"
printf '#!/usr/bin/env sh\nexit 0\n' > "$STUB_BIN"
chmod +x "$STUB_BIN"

FAILURES=0

# 断言:pyassert <json文件> <python表达式(可用 c=已加载config)> <描述>
pyassert() {
  local file="$1" expr="$2" desc="$3"
  if python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    c = json.load(f)
sys.exit(0 if ($expr) else 1)
" "$file" 2>/dev/null; then
    echo "    PASS: $desc"
  else
    echo "    FAIL: $desc"
    FAILURES=$((FAILURES + 1))
  fi
}

# 断言两文件逐字节相同
assert_bytes_equal() {
  if cmp -s "$1" "$2"; then
    echo "    PASS: $3 (逐字节相同)"
  else
    echo "    FAIL: $3 (逐字节不同)"
    FAILURES=$((FAILURES + 1))
  fi
}

# 断言两文件 JSON 语义等价
assert_json_equal() {
  if python3 -c "
import json, sys
with open(sys.argv[1]) as f: a = json.load(f)
with open(sys.argv[2]) as f: b = json.load(f)
sys.exit(0 if a == b else 1)
" "$1" "$2" 2>/dev/null; then
    echo "    PASS: $3 (JSON 语义等价)"
  else
    echo "    FAIL: $3 (JSON 语义不等价)"
    FAILURES=$((FAILURES + 1))
  fi
}

# 建一个场景目录,echo 出 config 路径
#   <case>/home/.zcode/cli/config.json ; <case>/home/.agent-console/
new_case() {
  local case_dir="$TMPROOT/$1"
  mkdir -p "$case_dir/home/.zcode/cli" "$case_dir/home/.agent-console"
  echo "$case_dir/home/.zcode/cli/config.json"
}

run_install() { # <config> <home_dir>  ; 返回 install 退出码
  ZCODE_CONFIG="$1" \
  PLUGIN_COPY_ROOT="$2/.agent-console/zcode-test" \
  BRIDGE_BIN="$STUB_BIN" \
  bash "$INSTALL" >"$2/install.log" 2>&1
  local rc=$?
  [ $rc -ne 0 ] && sed 's/^/      | /' "$2/install.log"
  return $rc
}

run_restore() { # <config> <home_dir>  ; 返回 restore 退出码
  ZCODE_CONFIG="$1" \
  PLUGIN_COPY_ROOT="$2/.agent-console/zcode-test" \
  bash "$RESTORE" >"$2/restore.log" 2>&1
  local rc=$?
  sed 's/^/      | /' "$2/restore.log"
  return $rc
}

home_dir_of() { dirname "$(dirname "$(dirname "$1")")"; }
# config -> cli -> .zcode -> home

write_config() { # <config> <JSON 文本>
  python3 -c "
import json, sys
with open(sys.argv[1], 'w') as f:
    f.write(json.dumps(json.loads(sys.argv[2]), indent=2, ensure_ascii=False) + '\n')
" "$1" "$2"
}

echo "=== 场景 1: hooks.enabled=false → install=true → restore=false ==="
CFG="$(new_case s1)"; D="$(home_dir_of "$CFG")"
write_config "$CFG" '{"hooks": {"enabled": false, "events": {"PermissionRequest": [{"hooks": [{"type": "process", "command": "/bin/echo", "args": ["user-hook"]}]}]}}, "model": "test"}'
cp "$CFG" "$D/before.json"
run_install "$CFG" "$D" && echo "    install OK"
pyassert "$CFG" 'c["hooks"]["enabled"] is True' 'install 后 hooks.enabled=true'
pyassert "$CFG" 'any(h.get("args")==["user-hook"] for e in c["hooks"]["events"]["PermissionRequest"] for h in e["hooks"])' '用户 hook 条目 install 后保留'
pyassert "$CFG" 'any(h.get("args")==["zcode-hook"] for e in c["hooks"]["events"]["PermissionRequest"] for h in e["hooks"])' '本项目 hook 已注册'
pyassert "$D/.agent-console/zcode-test/install-manifest.json" 'c["hooks_previous"]=={"present": True, "enabled": False}' '清单记录原值 enabled=false'
run_restore "$CFG" "$D"
pyassert "$CFG" 'c["hooks"]["enabled"] is False' 'restore 后 hooks.enabled 回到 false'
pyassert "$CFG" 'sum(1 for e in c["hooks"]["events"]["PermissionRequest"] for h in e["hooks"] if h.get("args")==["user-hook"])==1' '用户 hook 条目 restore 后原样保留'
pyassert "$CFG" 'not any(h.get("args")==["zcode-hook"] for e in c["hooks"]["events"].get("PermissionRequest", []) for h in e["hooks"])' '本项目 hook 已移除'

echo "=== 场景 2: 用户自有 agent-console MCP → install 拒绝,配置零改动 ==="
CFG="$(new_case s2)"; D="$(home_dir_of "$CFG")"
write_config "$CFG" '{"mcp": {"servers": {"agent-console": {"type": "stdio", "command": "/usr/bin/my-own-console", "args": ["serve"]}}}, "other": 1}'
cp "$CFG" "$D/before.json"
if run_install "$CFG" "$D"; then
  echo "    FAIL: install 应拒绝(退出码非 0)"
  FAILURES=$((FAILURES + 1))
else
  echo "    PASS: install 拒绝(退出码非 0),未覆盖用户 MCP"
fi
assert_bytes_equal "$CFG" "$D/before.json" 'install 拒绝后配置零改动'
run_restore "$CFG" "$D"
assert_bytes_equal "$CFG" "$D/before.json" 'restore 后配置仍零改动'
pyassert "$CFG" 'c["mcp"]["servers"]["agent-console"]["command"]=="/usr/bin/my-own-console"' '用户自有 agent-console MCP 原样保留'

echo "=== 场景 3: 用户自有 PermissionRequest hook(独立+混合条目)共存 ==="
CFG="$(new_case s3)"; D="$(home_dir_of "$CFG")"
write_config "$CFG" '{"hooks": {"enabled": true, "events": {"PermissionRequest": [{"hooks": [{"type": "process", "command": "/bin/echo", "args": ["user-solo"]}]}, {"hooks": [{"type": "process", "command": "/bin/echo", "args": ["user-mixed"]}, {"type": "process", "command": "/old/place/bridge", "args": ["zcode-hook"], "timeoutMs": 10000}]}]}}}'
cp "$CFG" "$D/before.json"
run_install "$CFG" "$D" && echo "    install OK"
pyassert "$CFG" 'sum(1 for e in c["hooks"]["events"]["PermissionRequest"] for h in e["hooks"] if h.get("args")==["user-solo"])==1' '独立用户条目 install 后保留'
pyassert "$CFG" 'sum(1 for e in c["hooks"]["events"]["PermissionRequest"] for h in e["hooks"] if h.get("args")==["user-mixed"])==1' '混合条目中用户 hook install 后保留'
pyassert "$CFG" 'sum(1 for e in c["hooks"]["events"]["PermissionRequest"] for h in e["hooks"] if h.get("args")==["zcode-hook"])==1' 'install 后每请求恰 1 个本项目 hook(无叠加)'
run_restore "$CFG" "$D"
pyassert "$CFG" 'sum(1 for e in c["hooks"]["events"]["PermissionRequest"] for h in e["hooks"] if h.get("args") in (["user-solo"],["user-mixed"]))==2' 'restore 后两个用户 hook 均原样保留'
pyassert "$CFG" 'not any(h.get("args")==["zcode-hook"] for e in c["hooks"]["events"].get("PermissionRequest", []) for h in e["hooks"])' 'restore 后本项目 hook 全部移除(含安装前残留)'
# 预期终态 = 安装前状态剔除其中历史残留的本项目 hook
python3 -c "
import json, sys
expected = {'hooks': {'enabled': True, 'events': {'PermissionRequest': [
    {'hooks': [{'type': 'process', 'command': '/bin/echo', 'args': ['user-solo']}]},
    {'hooks': [{'type': 'process', 'command': '/bin/echo', 'args': ['user-mixed']}]}]}}}
open(sys.argv[1], 'w').write(json.dumps(expected, indent=2) + '\n')
" "$D/expected.json"
assert_json_equal "$CFG" "$D/expected.json" 'restore 后恰为预期终态(仅剩用户条目,无 mcp 残留)'

echo "=== 场景 4: 重复安装两次 → 无重复条目,restore 干净 ==="
CFG="$(new_case s4)"; D="$(home_dir_of "$CFG")"
write_config "$CFG" '{"mcp": {"servers": {"serena": {"command": "x"}}}}'
cp "$CFG" "$D/before.json"
run_install "$CFG" "$D" && run_install "$CFG" "$D" && echo "    install x2 OK"
pyassert "$CFG" 'all(sum(1 for e in c["hooks"]["events"][ev] for h in e["hooks"] if h.get("args")==["zcode-hook"])==1 for ev in c["hooks"]["events"])' '每个事件恰 1 个本项目 hook(重复安装无叠加)'
pyassert "$CFG" 'sum(1 for k in c["mcp"]["servers"] if k=="agent-console")==1' 'agent-console MCP 恰 1 个(无双卡片)'
pyassert "$D/.agent-console/zcode-test/install-manifest.json" 'c["hooks_previous"]=={"present": False, "enabled": None}' '清单保留第一次的最初原值(未重置)'
run_restore "$CFG" "$D"
assert_json_equal "$CFG" "$D/before.json" 'restore 后与安装前 JSON 语义等价'
pyassert "$CFG" 'not any(h.get("args")==["zcode-hook"] for e in c.get("hooks", {}).get("events", {}).values() for h in e)' '无残留 zcode-hook 条目'

echo "=== 场景 5: 干净配置 install → restore 语义等价 ==="
CFG="$(new_case s5)"; D="$(home_dir_of "$CFG")"
write_config "$CFG" '{"model": "glm-5", "mcp": {"servers": {"context7": {"command": "npx"}}}}'
cp "$CFG" "$D/before.json"
run_install "$CFG" "$D" && echo "    install OK"
pyassert "$CFG" '"hooks" in c and c["hooks"]["enabled"] is True' 'install 写入 hooks'
run_restore "$CFG" "$D"
assert_json_equal "$CFG" "$D/before.json" 'restore 后与原文件 JSON 语义等价(判据:json.load 相等)'
if cmp -s "$CFG" "$D/before.json"; then
  echo "    (信息: 本例格式一致,恰好逐字节等价)"
else
  echo "    (信息: 语义等价但字节不同——键序/空白差异,ZCode 按 JSON 解析不受影响)"
fi

echo "=== 场景 6: 旧版脚本安装状态(无清单)→ 新版 restore 按安装前备份还原 ==="
echo "--- 6a. 安装前无 hooks 键(当前真实配置的形态) ---"
CFG="$(new_case s6a)"; D="$(home_dir_of "$CFG")"
write_config "$D/.zcode/cli/config.json.bak.ac-zcode-20260906_010101" '{"mcp": {"servers": {"serena": {"command": "x"}}}, "plugins": {}}'
write_config "$CFG" '{"mcp": {"servers": {"serena": {"command": "x"}, "agent-console": {"type": "stdio", "command": "'"$STUB_BIN"'", "args": ["mcp-stdio"]}}}, "plugins": {}, "hooks": {"enabled": true, "events": {"Stop": [{"hooks": [{"type": "process", "command": "'"$STUB_BIN"'", "args": ["zcode-hook"], "timeoutMs": 10000}]}]}}}'
run_restore "$CFG" "$D"
assert_json_equal "$CFG" "$D/.zcode/cli/config.json.bak.ac-zcode-20260906_010101" 'restore 后与安装前备份语义等价(hooks 键整体消失)'
echo "--- 6b. 安装前 hooks.enabled=false 且有用户 hooks ---"
CFG="$(new_case s6b)"; D="$(home_dir_of "$CFG")"
write_config "$D/.zcode/cli/config.json.bak.ac-zcode-20260906_010102" '{"hooks": {"enabled": false, "events": {"Stop": [{"hooks": [{"type": "process", "command": "/bin/echo", "args": ["user-stop"]}]}]}}}'
write_config "$CFG" '{"mcp": {"servers": {"agent-console": {"type": "stdio", "command": "'"$STUB_BIN"'", "args": ["mcp-stdio"]}}}, "hooks": {"enabled": true, "events": {"Stop": [{"hooks": [{"type": "process", "command": "/bin/echo", "args": ["user-stop"]}]}, {"hooks": [{"type": "process", "command": "'"$STUB_BIN"'", "args": ["zcode-hook"], "timeoutMs": 10000}]}]}}}'
run_restore "$CFG" "$D"
pyassert "$CFG" 'c["hooks"]["enabled"] is False' 'restore 后 hooks.enabled 回到备份中的 false'
pyassert "$CFG" 'sum(1 for e in c["hooks"]["events"]["Stop"] for h in e["hooks"] if h.get("args")==["user-stop"])==1' '用户 hook 保留'
pyassert "$CFG" 'not ((c.get("mcp") or {}).get("servers") or {}).get("agent-console")' '本项目形态 agent-console MCP 已移除'

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "全部场景通过。临时目录已清理: $TMPROOT"
  exit 0
else
  echo "失败断言数: $FAILURES"
  exit 1
fi
