#!/usr/bin/env bash
# =============================================================================
# OpenAPI 契约轻量检查(不建框架;只依赖 ruby stdlib YAML 与 grep/sed/comm)
#
# 对比:
#   1) contracts/openapi.yaml 中的 method+path 集合(ruby YAML 解析);
#   2) 代码路由清单:
#      - relay:grep 各 src 下 .route("/path", get/put/post/patch/delete(...))
#        (lib.rs 根级、devices/sessions/transfers/push/audit 各模块;
#        format! 拼接的 transfers 路径按常量展开);
#      - dev-toolbox:apps/api/internal/auth/agent_console.go 的
#        router.Handle("METHOD /path")。
#
# 输出与退出码:
#   FAIL:openapi 中存在但代码路由中不存在的操作 → 退出 1
#   WARN:代码公网端点在 openapi 中缺失(/internal/* 内部端点除外)→ 退出 0
#   全部一致 → 退出 0
# =============================================================================
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPENAPI="$REPO_ROOT/contracts/openapi.yaml"
RELAY_SRC="$REPO_ROOT/apps/relay/src"
DEVTOOLBOX_GO="${AGENT_CONSOLE_DEVTOOLBOX_GO:-$REPO_ROOT/../dev-toolbox/apps/api/internal/auth/agent_console.go}"
DEVTOOLBOX_OWNED_OPS="$(printf '%s\n' \
  'post /api/v1/agent-console/ws-tickets' \
  'post /internal/agent-console/ws-tickets/consume' \
  'post /internal/agent-console/auth-sessions/introspect' \
  'get /internal/agent-console/auth-sessions/verify' | sort)"

# --- 1) openapi.yaml 的 method+path 集合 -------------------------------------
openapi_ops="$(ruby -ryaml -e '
  doc = YAML.load_file(ARGV[0])
  doc["paths"].each do |path, item|
    next unless item.is_a?(Hash)
    item.each_key do |m|
      next unless %w[get post put patch delete head options trace].include?(m)
      puts "#{m} #{path}"
    end
  end
' "$OPENAPI" | sort | sed '/^$/d')"

# --- 2) 代码路由清单 ----------------------------------------------------------
# relay:逐个 .route( 调用,按括号深度截取整个调用片段,片段内第一个字符串字面量
# 即路径,方法词以 "get(" 形式出现。
# 前缀映射(白名单,依据 lib.rs 装配):devices/sessions/push/audit 与
# transfers::browser_routes 的模块内路径经 nest("/agent-console/api") 挂载;
# lib.rs 根级路由与 transfers::device_routes(format! 拼接)本身就是完整路径。
# 参数名对齐 openapi:axum 源码 {request_id}/{transfer_id} 与配对 challenges
# 的 {id} 在契约中分别登记为 {requestId}/{transferId}/{challengeId}。
relay_routes="$(ruby -e '
  src = ""
  Dir.glob(File.join(ARGV[0], "**/*.rs")).sort.each do |f|
    src << File.read(f) << "\n"
  end
  i = 0
  while (anchor = src.index(".route(", i))
    seg = ""
    depth = 0
    opened = false
    src[anchor..-1].each_char do |c|
      seg << c
      depth += 1 if c == "("
      opened = true if depth > 0
      depth -= 1 if c == ")"
      break if opened && depth == 0
    end
    if seg =~ /"([^"]*)"/
      path = Regexp.last_match(1)
      path = path.gsub("{PRODUCER_PATH_PREFIX}", "/agent-console/transfers/producer/")
      path = path.gsub("{CONSUMER_PATH_PREFIX}", "/agent-console/transfers/consumer/")
      path = path.gsub("{{transfer_id}}", "{transfer_id}")
      # nest("/agent-console/api") 前缀:模块内相对路径补全
      unless path.start_with?("/agent-console", "/health", "/internal")
        path = "/agent-console/api#{path}"
      end
      # 路径参数名与 openapi.yaml 对齐
      path = path.gsub("/pairing/challenges/{id}", "/pairing/challenges/{challengeId}")
      path = path.gsub("{request_id}", "{requestId}")
      path = path.gsub("{transfer_id}", "{transferId}")
      seg.scan(/\b(get|post|put|patch|delete|head|options)\(/).flatten.uniq.each do |m|
        puts "#{m} #{path}"
      end
    end
    i = anchor + seg.length
    i += 1 if i >= src.length
    break if i >= src.length
  end
' "$RELAY_SRC" | sed '/^$/d')"

# dev-toolbox:router.Handle("METHOD /path", ...)。独立 checkout（CI）没有
# sibling 仓库时，仍检查全部 Relay 路由，并确认 4 个外部边界操作仍在契约中；
# 本地存在 dev-toolbox 时自动升级为完整 39 路由核对。
if [[ -f "$DEVTOOLBOX_GO" ]]; then
  toolbox_routes="$(grep -oE 'Handle\("[A-Za-z]+ [^"]+"' "$DEVTOOLBOX_GO" \
    | sed -E 's/^Handle\("([A-Za-z]+) ([^"]+)".*$/\1 \2/' \
    | awk '{print tolower($1), $2}' | sed '/^$/d')"
  expected_openapi_ops="$openapi_ops"
  scope_label="relay + dev-toolbox"
else
  toolbox_routes=""
  expected_openapi_ops="$(comm -23 \
    <(printf '%s\n' "$openapi_ops" | sort) \
    <(printf '%s\n' "$DEVTOOLBOX_OWNED_OPS" | sort))"
  scope_label="relay（dev-toolbox 源码不在当前 checkout）"
fi

code_routes="$(printf '%s\n%s\n' "$relay_routes" "$toolbox_routes" | sort -u | sed '/^$/d')"

# --- 3) 对比 ------------------------------------------------------------------
echo "== openapi 操作数:$(printf '%s\n' "$openapi_ops" | wc -l | tr -d ' ')"
echo "== 代码路由数($scope_label,去重):$(printf '%s\n' "$code_routes" | wc -l | tr -d ' ')"

fail=0

# FAIL:openapi 有,代码没有
missing_in_code="$(comm -23 <(printf '%s\n' "$expected_openapi_ops" | sort) <(printf '%s\n' "$code_routes" | sort))"
if [ -n "$missing_in_code" ]; then
  fail=1
  echo "FAIL: openapi 中存在但代码路由中不存在的操作:"
  printf '%s\n' "$missing_in_code" | sed 's/^/  /'
else
  echo "PASS: 当前检查范围内的 openapi 操作都能对应到代码路由"
fi

if [[ ! -f "$DEVTOOLBOX_GO" ]]; then
  missing_devtoolbox_contract="$(comm -23 \
    <(printf '%s\n' "$DEVTOOLBOX_OWNED_OPS" | sort) \
    <(printf '%s\n' "$openapi_ops" | sort))"
  if [[ -n "$missing_devtoolbox_contract" ]]; then
    fail=1
    echo "FAIL: dev-toolbox 外部边界操作在 openapi 中缺失:"
    printf '%s\n' "$missing_devtoolbox_contract" | sed 's/^/  /'
  else
    echo "PASS: dev-toolbox 的 4 个外部边界操作均已登记；实现核对需提供 sibling 仓库"
  fi
fi

# WARN:代码有,openapi 缺(internal 内部端点除外)
missing_in_openapi="$(comm -13 <(printf '%s\n' "$openapi_ops" | sort) <(printf '%s\n' "$code_routes" | sort) \
  | grep -v ' /internal/' || true)"
if [ -n "$missing_in_openapi" ]; then
  echo "WARN: 代码公网端点在 openapi 中缺失(internal 端点已豁免):"
  printf '%s\n' "$missing_in_openapi" | sed 's/^/  /'
else
  echo "PASS: 代码全部公网端点都已登记进 openapi(internal 端点除外)"
fi

if [ "$fail" -eq 1 ]; then
  exit 1
fi
exit 0
