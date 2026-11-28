/** A resident graph: loaded once into a stream pool, then driven by host commands. */
import type { PoolWasm } from "./wasm-stream.ts";

export type ResidentMemoryStats = {
  readonly limitBytes: number;
  readonly featureBytes: number;
  readonly loadPeakBytes: number;
  readonly normalizePeakBytes: number;
  readonly roundPeakBytes: number;
  readonly wasmMemoryBytes: number;
};

export async function readResidentMemory(
  request: ResidentGraph["request"],
  wasm: Pick<PoolWasm, "memory">,
): Promise<ResidentMemoryStats> {
  const bytes = await request(3, new Uint8Array());
  if (
    bytes.length !== 44 ||
    new TextDecoder().decode(bytes.subarray(0, 4)) !== "WRM1"
  ) {
    throw new Error("invalid resident memory report");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const value = (index: number) => Number(view.getBigUint64(4 + index * 8, true));
  return {
    limitBytes: value(0),
    featureBytes: value(1),
    loadPeakBytes: value(2),
    normalizePeakBytes: value(3),
    roundPeakBytes: value(4),
    wasmMemoryBytes: wasm.memory.buffer.byteLength,
  };
}

export type ResidentGraph = {
  readonly metadata: Uint8Array;
  memory(): Promise<ResidentMemoryStats>;
  request(kind: number, bytes: Uint8Array): Promise<Uint8Array>;
  release(): Promise<void>;
};
