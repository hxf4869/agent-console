// Agent Console Protobuf 窄编解码入口(浏览器侧)。
//
// 只做传输层职责:Envelope 二进制编/解码、帧上限、ID 生成。
// 不包含 WebSocket 客户端、store、UI 或任何业务逻辑(§7:生成协议包禁止
// 混入前端产品代码)。

import { fromBinary, toBinary } from "@bufbuild/protobuf";
import type { Envelope } from "./generated/agent_console/v1/envelope_pb.js";
import { EnvelopeSchema } from "./generated/agent_console/v1/envelope_pb.js";

/** 当前协议主版本,与 Rust 侧 codec::PROTOCOL_VERSION 一致。 */
export const PROTOCOL_VERSION = 1;

/** 结构化 Protobuf 单帧上限:1 MiB(§17.6)。 */
export const MAX_FRAME_BYTES = 1024 * 1024;

/** 输出流单块默认上限:64 KiB;分块必须在 UTF-8 边界切分(§13.2)。 */
export const MAX_OUTPUT_CHUNK_BYTES = 64 * 1024;

/** 帧超过 MAX_FRAME_BYTES 时抛出。 */
export class FrameTooLargeError extends Error {
  readonly size: number;
  readonly max: number;

  constructor(size: number, max: number) {
    super(`frame too large: ${size} bytes (max ${max})`);
    this.name = "FrameTooLargeError";
    this.size = size;
    this.max = max;
  }
}

/** 编码 Envelope 为二进制帧;超过上限时抛出 FrameTooLargeError。 */
export function encodeEnvelope(envelope: Envelope): Uint8Array {
  const frame = toBinary(EnvelopeSchema, envelope);
  if (frame.length > MAX_FRAME_BYTES) {
    throw new FrameTooLargeError(frame.length, MAX_FRAME_BYTES);
  }
  return frame;
}

/** 解码二进制帧为 Envelope;超过上限时抛出 FrameTooLargeError。 */
export function decodeEnvelope(frame: Uint8Array): Envelope {
  if (frame.length > MAX_FRAME_BYTES) {
    throw new FrameTooLargeError(frame.length, MAX_FRAME_BYTES);
  }
  return fromBinary(EnvelopeSchema, frame);
}

/** 取 WebCrypto 的 randomUUID(不引入 DOM 类型依赖)。 */
function randomUuid(): string {
  const c = (
    globalThis as unknown as {
      crypto?: { randomUUID?: () => string };
    }
  ).crypto;
  if (typeof c?.randomUUID !== "function") {
    throw new Error("crypto.randomUUID is unavailable in this environment");
  }
  return c.randomUUID();
}

/** 生成新的 message_id(UUID v4)。 */
export function newMessageId(): string {
  return randomUuid();
}

/** 生成新的 correlation_id(UUID v4);请求方也可直接复用 request_id。 */
export function newCorrelationId(): string {
  return randomUuid();
}
