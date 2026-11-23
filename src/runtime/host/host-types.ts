import type { HostBytes } from "./host-bytes.ts";
import type { WcolStreamWasm } from "./wasm-stream.ts";
import { wasmMemoryU8 } from "./wasm.ts";
import type { R2OutputBucket } from "../../../cloudflare/src/r2.ts";
import type { PoolInputLatch } from "./pool-streams.ts";
import type { ReadAtCallback, StreamPool } from "./stream-pool.ts";
import type { ResidentGraph } from "./resident-graph.ts";
import type { StreamRunStats } from "./wasm-stream.ts";

export type WcolHost = {
  readAt?(source: number, offset: number, length: number): Uint8Array | Promise<Uint8Array>;
  readPort?(port: number, maxLength: number): Promise<Uint8Array | null | "absent">;
  abortOutputs?(): Promise<void>;
  writePort(port: number, bytes: Uint8Array): void | Promise<void>;
  openPort?(port: number): void | Promise<void>;
  closePort?(port: number): void | Promise<void>;
  yield(): Promise<void>;
  spillR2Bucket?: R2OutputBucket;
  /**
   * Prefix for spill run object keys. Rust names runs per partition only, so
   * without a per-task prefix every job and attempt writes the same keys.
   */
  spillKeyPrefix?: string;
};

export type RunWcolEngineOptions = {
  wasm: WcolStreamWasm;
  plan: Uint8Array;
  host: WcolHost;
  scratchBytes?: number;
  /** Tasks sharing this isolate's memory, this one included; see the stream pool's `graph`. */
  budgetShare?: number;
  /** Counts the host's edge, spill-upload and spill read-ahead bytes. */
  hostBytes?: HostBytes;
  /** Cap peak resident feature allocations, including normalization overlap. */
  residentMemoryLimitBytes?: number;
  /** Ingress / egress mailbox counts for the stream pool. */
  layout: { inputs: number; outputs: number };
  inputSizeKiB?: number;
  outputSizeKiB?: number;
  /** Binds to the pool synchronously when it opens (for scan edges that may arrive before wasm load). */
  inputLatch?: PoolInputLatch;
  /** Called synchronously right after the pool is created. */
  onPoolReady?: (pool: StreamPool) => void;
  /** Takes ownership of a planned resident graph after its initial load. */
  onResidentReady?: (graph: ResidentGraph) => void;
  readAt?: ReadAtCallback;
  yieldToEventLoop?: () => Promise<void>;
  onProgress?: (info: {
    pollIterations: number;
    inputBytes: number;
    outputBytes: number;
    wasmMemoryBytes: number;
    spillRuns?: number;
    spillGroupsFlushed?: number;
    spillPeakLiveBytes?: number;
    spillPeakInternerBytes?: number;
    spillBoundaries?: SpillBoundary[];
    residentGroups?: number;
    residentGroupBytes?: number;
  }) => void;
};

export type SpillBoundary = {
  run: number;
  wasmBefore: number;
  wasmAfter: number;
  liveGroups: number;
  liveTableBefore: number;
  tableAfter: number;
  indexCapAfter: number;
  internerBefore: number;
  internerAfter: number;
  sortCap: number;
  pageBytes: number;
  pageCap: number;
  liveAfter: number;
  peakAfter: number;
  liveBefore: number;
  serializeBytes: number;
  appendBufCap: number;
  ioEncodeCap: number;
  hostPendingOps: number;
  sqliteWriteBytes: number;
  hostPendingSqlBytes: number;
  rustQueuedSpillBytes: number;
  rustInflightSpillBytes: number;
  liveSerializedPeak: number;
  liveSqlAcked: number;
  mergeScanBegins: number;
  mergeSteps: number;
};

export type DictTraffic = {
  entriesSent: number;
  bytesSent: number;
  entriesReferenced: number;
  bytesReferenced: number;
  bytesRetained: number;
  bytesReleased: number;
  peakRetained: number;
  maxStreamBytes: number;
  streamsReleased: number;
  activeDicts: number;
  peakActiveDicts: number;
  capacityRetained: number;
  peakCapacity: number;
};

export type MemGrowthSample = {
  phase: "scan" | "wcs_ingress" | "hash_aggregate" | "spill_write" | "spill_merge" | "unset";
  wasmLinearBytes: number;
  rustLiveBytes: number;
  rustPeakBytes: number;
  groupTableBytes: number;
  stringInternerBytes: number;
  spillRunsCommitted: number;
  spillPendingWriteBytes: number;
  spillMergeBufferBytes: number;
  wcsPendingBytes: number;
  activeInputStreams: number;
};

const MEM_GROWTH_PHASES = ["unset", "scan", "wcs_ingress", "hash_aggregate", "spill_write", "spill_merge"] as const;

export function readMemGrowth(wasm: WcolStreamWasm): MemGrowthSample[] | undefined {
  if (!wasm.wcs_mem_growth_ptr || !wasm.wcs_mem_growth_len) return undefined;
  const len = Number(wasm.wcs_mem_growth_len());
  if (!len) return [];
  const ptr = Number(wasm.wcs_mem_growth_ptr());
  const bytes = wasmMemoryU8(wasm, ptr, len).slice();
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const stride = 11 * 8;
  const rows: MemGrowthSample[] = [];
  for (let offset = 0; offset + stride <= bytes.byteLength; offset += stride) {
    const word = (index: number) => Number(view.getBigUint64(offset + index * 8, true));
    const phaseIndex = word(0);
    rows.push({
      phase: MEM_GROWTH_PHASES[phaseIndex] ?? "unset",
      wasmLinearBytes: word(1),
      rustLiveBytes: word(2),
      rustPeakBytes: word(3),
      groupTableBytes: word(4),
      stringInternerBytes: word(5),
      spillRunsCommitted: word(6),
      spillPendingWriteBytes: word(7),
      spillMergeBufferBytes: word(8),
      wcsPendingBytes: word(9),
      activeInputStreams: word(10),
    });
  }
  return rows;
}

export function readDictTraffic(wasm: WcolStreamWasm): DictTraffic | undefined {
  if (!wasm.wcs_dict_traffic_ptr || !wasm.wcs_dict_traffic_len) return undefined;
  const len = Number(wasm.wcs_dict_traffic_len());
  const ptr = Number(wasm.wcs_dict_traffic_ptr());
  if (len < 72 || !ptr) return undefined;
  const bytes = wasmMemoryU8(wasm, ptr, len);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const word = (index: number) => (len >= (index + 1) * 8 ? Number(view.getBigUint64(index * 8, true)) : 0);
  return {
    entriesSent: word(0),
    bytesSent: word(1),
    entriesReferenced: word(2),
    bytesReferenced: word(3),
    bytesRetained: word(4),
    bytesReleased: word(5),
    peakRetained: word(6),
    maxStreamBytes: word(7),
    streamsReleased: word(8),
    activeDicts: word(9),
    peakActiveDicts: word(10),
    capacityRetained: word(11),
    peakCapacity: word(12),
  };
}

const SPILL_BOUNDARY_WORDS = 27;
const SPILL_BOUNDARY_WORDS_V23 = 23;

function spillBoundaryStride(byteLength: number): number {
  const current = SPILL_BOUNDARY_WORDS * 4;
  const v23 = SPILL_BOUNDARY_WORDS_V23 * 4;
  if (byteLength >= current && byteLength % current === 0) return current;
  if (byteLength >= v23 && byteLength % v23 === 0) return v23;
  return current;
}

export function readSpillBoundaries(wasm: WcolStreamWasm): SpillBoundary[] | undefined {
  if (!wasm.wcs_spill_boundary_len || !wasm.wcs_spill_boundary_ptr) return undefined;
  const len = Number(wasm.wcs_spill_boundary_len());
  if (!len) return [];
  const ptr = Number(wasm.wcs_spill_boundary_ptr());
  const bytes = wasmMemoryU8(wasm, ptr, len).slice();
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const stride = spillBoundaryStride(bytes.byteLength);
  const wordsInRecord = stride / 4;
  const rows: SpillBoundary[] = [];
  for (let offset = 0; offset + stride <= bytes.byteLength; offset += stride) {
    const word = (index: number) =>
      index < wordsInRecord ? view.getUint32(offset + index * 4, true) : 0;
    rows.push({
      run: word(0),
      wasmBefore: word(1),
      wasmAfter: word(2),
      liveGroups: word(3),
      liveTableBefore: word(4),
      tableAfter: word(5),
      indexCapAfter: word(6),
      internerBefore: word(7),
      internerAfter: word(8),
      sortCap: word(9),
      pageBytes: word(10),
      pageCap: word(11),
      liveAfter: word(12),
      peakAfter: word(13),
      liveBefore: word(14),
      serializeBytes: word(15),
      appendBufCap: word(16),
      ioEncodeCap: word(17),
      hostPendingOps: word(18),
      sqliteWriteBytes: word(19),
      hostPendingSqlBytes: word(20),
      rustQueuedSpillBytes: word(21),
      rustInflightSpillBytes: word(22),
      liveSerializedPeak: word(23),
      liveSqlAcked: word(24),
      mergeScanBegins: word(25),
      mergeSteps: word(26),
    });
  }
  return rows;
}

export type { StreamRunStats };
