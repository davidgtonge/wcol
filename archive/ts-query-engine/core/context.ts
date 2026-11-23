import type { ByteSource } from "../io/byte-source.ts";
import type { WasmBindings } from "../wasm/wasm.ts";
import type { ColumnInfo, DictsMap, HeaderInfo, RuntimeInitBytes, WorkerRuntimeKind } from "./types.ts";

export type WcolContext = {
  source: ByteSource;
  wasm: WasmBindings;
  runtime: number;
  header: HeaderInfo;
  columnInfoCache: Map<number, ColumnInfo>;
  columnNameCache: Map<number, string>;
  dicts: DictsMap;
  init?: RuntimeInitBytes;
  workerPool?: {
    kind: WorkerRuntimeKind;
    size: number;
    pool: unknown;
  };
  queryChain?: Promise<void>;
};
