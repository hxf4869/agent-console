// @agent-console/protocol 统一出口。
//
// 生成的类型与 Schema 按源 proto 文件分组导出;运行时编解码入口在 ./codec。
// 生成代码由 `scripts/gen-protobuf.sh` 再生成,禁止手工编辑 src/generated/。

export * from "./codec.js";

export * from "./generated/agent_console/v1/common_pb.js";
export * from "./generated/agent_console/v1/session_pb.js";
export * from "./generated/agent_console/v1/runtime_pb.js";
export * from "./generated/agent_console/v1/events_pb.js";
export * from "./generated/agent_console/v1/commands_pb.js";
export * from "./generated/agent_console/v1/transfers_pb.js";
export * from "./generated/agent_console/v1/envelope_pb.js";
