#!/usr/bin/env bash
# 一键再生成 Agent Console 协议代码(§17.1:proto 冻结后由本脚本保持双语言同步)。
#
# 用法:仓库根执行 ./scripts/gen-protobuf.sh
#
# 前置条件:
#   * protoc 在 PATH(本机 /opt/homebrew/bin/protoc);
#   * 已在仓库根执行过 `pnpm install`(提供 @bufbuild/protoc-gen-es);
#   * Rust: cargo 在 PATH,或已 source ~/.cargo/env。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PROTO_FILES=$(find proto -name '*.proto' | sort)
if [[ -z "$PROTO_FILES" ]]; then
  echo "错误:$ROOT/proto 下没有 .proto 文件" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 1) TypeScript:protoc + protoc-gen-es(pnpm 包内 .bin 下的插件入口)。
#    生成 packages/protocol-ts/src/generated/**/*_pb.ts(target=ts)。
# ---------------------------------------------------------------------------
command -v protoc >/dev/null || { echo "错误:找不到 protoc" >&2; exit 1; }
ES_PLUGIN="$(pnpm -C packages/protocol-ts bin)/protoc-gen-es"
[[ -x "$ES_PLUGIN" ]] || {
  echo "错误:找不到 protoc-gen-es,请先在仓库根执行 pnpm install" >&2
  exit 1
}

mkdir -p packages/protocol-ts/src/generated
# shellcheck disable=SC2086
protoc -I proto \
  --plugin="protoc-gen-es=$ES_PLUGIN" \
  --es_out=packages/protocol-ts/src/generated \
  --es_opt=target=ts \
  --es_opt=import_extension=.js \
  $PROTO_FILES
echo "[ok] TypeScript 已生成:packages/protocol-ts/src/generated/"

# ---------------------------------------------------------------------------
# 2) Rust:cargo build 触发 crates/protocol/build.rs(protox + prost)。
# ---------------------------------------------------------------------------
if ! command -v cargo >/dev/null 2>&1; then
  if [[ -x "$HOME/.cargo/bin/cargo" ]]; then
    CARGO="$HOME/.cargo/bin/cargo"
  else
    echo "错误:找不到 cargo(尝试 source ~/.cargo/env)" >&2
    exit 1
  fi
else
  CARGO=cargo
fi
"$CARGO" build -p agent_console_protocol
echo "[ok] Rust 已生成并编译:crates/protocol(OUT_DIR include)"

# ---------------------------------------------------------------------------
# 3) 提示人工 diff 检查(字段号冻结,§17.1:任何 .pb.ts / 生成 Rust 的 diff
#    都必须人工确认没有复用或改动既有字段号)。
# ---------------------------------------------------------------------------
echo
echo "请检查生成代码 diff(字段号一旦发布即冻结,不得复用):"
git status --porcelain -- proto packages/protocol-ts/src/generated crates/protocol || true
