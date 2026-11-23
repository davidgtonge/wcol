/** WebSocket protocol. One logical graph edge is one unidirectional signal stream. */

export const MAX_EDGE_ID_BYTES = 256;
export const MAX_PAYLOAD_BYTES = 64 * 1024;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

export type ClientControl =
  | { type: "subscribe"; edge: string }
  | { type: "unsubscribe"; edge: string }
  | { type: "publish"; edge: string }
  | { type: "finish"; edge: string }
  | { type: "abort"; edge: string; reason: string };

export type ServerControl =
  | { type: "registered" }
  | { type: "subscribed"; edge: string }
  | { type: "ready"; edge: string }
  | { type: "finished"; edge: string }
  | { type: "end"; edge: string }
  | { type: "error"; edge: string; reason: string };

export type Control = ClientControl | ServerControl;

const isRecord = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v);

export function validEdge(edge: unknown): edge is string {
  return typeof edge === "string" && edge.length > 0 &&
    encoder.encode(edge).byteLength <= MAX_EDGE_ID_BYTES &&
    !/[\u0000-\u001f]/.test(edge);
}

export function treeSignalEdge(fromTaskId: number, toTaskId: number): string {
  return `t${fromTaskId}->t${toTaskId}`;
}

export function hashSignalEdge(reducerTaskId: number, scanIndex: number): string {
  return `r${reducerTaskId}:s${scanIndex}`;
}

export function parseControl(value: string): Control {
  if (value.length > 2048) throw new Error("Control message too large");
  const raw: unknown = JSON.parse(value);
  if (!isRecord(raw) || typeof raw.type !== "string") throw new Error("Invalid control message");
  if (raw.type === "registered") return { type: "registered" };
  if (!validEdge(raw.edge)) throw new Error("Missing or invalid edge ID");
  const edge = raw.edge;
  switch (raw.type) {
    case "subscribe": case "unsubscribe": case "publish": case "finish":
    case "subscribed": case "ready": case "finished": case "end":
      return { type: raw.type, edge };
    case "abort": case "error":
      if (typeof raw.reason !== "string") throw new Error("Missing error reason");
      return { type: raw.type, edge, reason: raw.reason.slice(0, 256) };
    default:
      throw new Error(`Unknown control message: ${raw.type}`);
  }
}

export function encodeFrame(edge: string, bytes: Uint8Array): Uint8Array {
  if (!validEdge(edge)) throw new Error("Invalid edge ID");
  if (bytes.byteLength > MAX_PAYLOAD_BYTES) throw new Error("Signal frame too large");
  const name = encoder.encode(edge);
  const result = new Uint8Array(2 + name.byteLength + bytes.byteLength);
  new DataView(result.buffer, result.byteOffset, result.byteLength).setUint16(0, name.byteLength, false);
  result.set(name, 2);
  result.set(bytes, 2 + name.byteLength);
  return result;
}

export function decodeFrame(frame: ArrayBuffer | Uint8Array): { edge: string; bytes: Uint8Array } {
  const data = frame instanceof Uint8Array ? frame : new Uint8Array(frame);
  if (data.length < 3 || data.length > 2 + MAX_EDGE_ID_BYTES + MAX_PAYLOAD_BYTES) {
    throw new Error("Invalid signal frame length");
  }
  const edgeLength = new DataView(data.buffer, data.byteOffset, data.byteLength).getUint16(0, false);
  if (edgeLength === 0 || edgeLength > MAX_EDGE_ID_BYTES || data.length < 2 + edgeLength) {
    throw new Error("Invalid signal frame header");
  }
  const edge = decoder.decode(data.subarray(2, 2 + edgeLength));
  if (!validEdge(edge)) throw new Error("Invalid signal frame edge");
  const bytes = data.subarray(2 + edgeLength);
  if (bytes.byteLength > MAX_PAYLOAD_BYTES) throw new Error("Signal frame too large");
  return { edge, bytes };
}

export function asError(cause: unknown): Error {
  return cause instanceof Error ? cause : new Error(String(cause));
}
