import type { QueryResult } from "./core/types.ts";
import { ByteSource, HttpRangeSource, NodeFileSource } from "./io/byte-source.ts";
import { addStage, stageSync } from "./stage-time.ts";

const littleEndian = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;
let aggregatePack: "value" | "buffer" = "buffer";

/** `value` allocates one 8-byte array per integer. `buffer` copies the column in one write. */
export function setAggregatePack(mode: "value" | "buffer"): void {
  aggregatePack = mode;
}

let injectedWasm: Uint8Array | WebAssembly.Module | null = null;

/** Durable Objects pass the compiled module. Node reads the release artifact when this is unset. */
export function setParquetWasm(wasm: Uint8Array | WebAssembly.Module): void {
  injectedWasm = wasm;
  wasmPromise = null;
  parquetMemory = null;
  parquetExports = null;
}

type Exports = {
  memory: WebAssembly.Memory;
  parquet_alloc(len: number): number;
  parquet_dealloc(ptr: number, len: number): void;
  parquet_alloc_stats(out: number): void;
  parquet_footer_span(fileSize: bigint, tail: number, outStart: number, outLen: number): number;
  parquet_describe(footer: number, footerLen: number, out: number, outCap: number, outLen: number): number;
  parquet_plan_sql(input: number, inputLen: number, out: number, outCap: number, outLen: number): number;
  parquet_scan_open(
    bytes: number,
    len: number,
    physical: number,
    typeLength: number,
    codec: number,
    maxDef: number,
    maxRep: number,
    err: number,
    errCap: number,
    errLen: number,
  ): number;
  parquet_scan_next(handle: number, maxRows: number, out: number, outCap: number, outLen: number): number;
  parquet_scan_close(handle: number): void;
  parquet_exec_decoded(input: number, inputLen: number, out: number, outCap: number, outLen: number): number;
  parquet_plan_rows(input: number, inputLen: number, out: number, outCap: number, outLen: number): number;
  parquet_select_decoded(input: number, inputLen: number, out: number, outCap: number, outLen: number): number;
  parquet_engine_open(program: number, len: number, err: number, errCap: number, errLen: number): number;
  parquet_engine_next(handle: number, action: number): number;
  parquet_engine_supply(handle: number, bytes: number, len: number): number;
  parquet_engine_release(handle: number): void;
  parquet_engine_close(handle: number): void;
};

export type EngineAction = {
  kind: number;
  offset: bigint;
  length: number;
  rowGroup: number;
  rows: number;
  bytes: Uint8Array;
};

export type ParquetColumn = {
  name: string;
  kind: "number" | "text";
  physical: number;
  typeLength: number;
  maxDef: number;
  maxRep: number;
};

export type ParquetLeaf = {
  name: string;
  physical: number;
  typeLength: number;
  maxDef: number;
  maxRep: number;
};

export type ParquetChunk = {
  leaf: number;
  codec: number;
  numValues: bigint;
  start: bigint;
  compressedSize: bigint;
  /** Set when the describe buffer includes per-chunk dictionary flags (future WPQ revision). */
  hasDictionary?: boolean;
};

export type ParquetPlan = {
  rows: bigint;
  leaves: ParquetLeaf[];
  chunks: ParquetChunk[];
  /** One `num_rows` per row group, from the footer rather than a column's value count. */
  groupRows: number[];
};

export type ParquetAggregate = {
  sum: number | bigint;
  min: number | bigint;
  max: number | bigint;
  count: bigint;
  nulls: bigint;
  bytes: boolean;
};

let wasmPromise: Promise<Exports> | null = null;
let parquetMemory: WebAssembly.Memory | null = null;
let parquetExports: Exports | null = null;
let parquetTurn: Promise<void> = Promise.resolve();

/** Byte length of the shared Parquet Wasm memory, or null before the module is instantiated. */
export function parquetWasmMemoryBytes(): number | null {
  const bytes = parquetMemory?.buffer.byteLength;
  return typeof bytes === "number" ? bytes : null;
}

/** Live, peak, and Wasm-memory bytes around the last page and the last cleared row group. */
export function parquetAllocSnapshot(): string | null {
  const wasm = parquetExports;
  if (!wasm) return null;
  const ptr = wasm.parquet_alloc(36);
  try {
    wasm.parquet_alloc_stats(ptr);
    const view = new Uint32Array(wasm.memory.buffer, ptr, 9);
    const sample = (offset: number): string => `${view[offset]}/${view[offset + 1]}/${view[offset + 2]}`;
    return `before ${sample(0)} after ${sample(3)} clear ${sample(6)}`;
  } finally {
    wasm.parquet_dealloc(ptr, 36);
  }
}

/** One shared Wasm memory. Reads stay outside this lock; alloc and decode stay inside it. */
function withParquetWasm<T>(fn: () => Promise<T> | T): Promise<T> {
  const queued = performance.now();
  const run = parquetTurn.then(() => {
    addStage("wasmQueue", performance.now() - queued);
    const started = performance.now();
    return Promise.resolve(fn()).finally(() => addStage("wasmHold", performance.now() - started));
  });
  parquetTurn = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}

function nodeSpecifier(name: string): string {
  return ["node", name].join(":");
}

async function readWasmFile(): Promise<Uint8Array> {
  const fs = (await import(nodeSpecifier("fs/promises"))) as { readFile(path: string): Promise<Uint8Array> };
  const path = (await import(nodeSpecifier("path"))) as { dirname(path: string): string; resolve(...parts: string[]): string };
  const url = (await import(nodeSpecifier("url"))) as { fileURLToPath(url: string): string };
  const wasmPath = path.resolve(
    path.dirname(url.fileURLToPath(import.meta.url)),
    "../../rust/target/wasm32-unknown-unknown/release/wcol_parquet.wasm",
  );
  return fs.readFile(wasmPath);
}

function loadWasm(): Promise<Exports> {
  wasmPromise ??= (async () => {
    let compiled: Exports;
    if (injectedWasm instanceof WebAssembly.Module) {
      const instantiated = await WebAssembly.instantiate(injectedWasm, {});
      compiled = instantiated.exports as Exports;
    } else {
      const bytes = injectedWasm ?? (await readWasmFile());
      const { instance } = await WebAssembly.instantiate(bytes, {});
      compiled = instance.exports as Exports;
    }
    parquetMemory = compiled.memory;
    parquetExports = compiled;
    return compiled;
  })();
  return wasmPromise;
}

function statusText(code: number): string {
  if (code === 0) return "ok";
  if (code === 1) return "output buffer is too small";
  if (code === -2) return "unsupported parquet column";
  if (code === -3) return "invalid parquet footer";
  if (code === -4) return "parquet page could not be decoded";
  return `parquet status ${code}`;
}

function view(wasm: Exports): DataView {
  return new DataView(wasm.memory.buffer);
}

function allocCopy(wasm: Exports, bytes: Uint8Array): number {
  const ptr = wasm.parquet_alloc(bytes.length);
  new Uint8Array(wasm.memory.buffer, ptr, bytes.length).set(bytes);
  return ptr;
}

function readPlan(bytes: Uint8Array): ParquetPlan {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.length < 16 || String.fromCharCode(...bytes.subarray(0, 4)) !== "WPQ1") {
    throw new Error("parquet description is invalid");
  }
  let at = 4;
  const rows = view.getBigInt64(at, true);
  at += 8;
  const leafCount = view.getUint32(at, true);
  at += 4;
  const leaves: ParquetLeaf[] = [];
  const names = new TextDecoder("utf-8", { fatal: true });
  for (let i = 0; i < leafCount; i++) {
    const nameLen = view.getUint16(at, true);
    at += 2;
    const name = names.decode(bytes.subarray(at, at + nameLen));
    at += nameLen;
    const physical = view.getInt32(at, true);
    at += 4;
    const typeLength = view.getInt32(at, true);
    at += 4;
    const maxDef = bytes[at];
    const maxRep = bytes[at + 1];
    at += 2;
    if (maxDef === undefined || maxRep === undefined) throw new Error("truncated parquet leaf");
    leaves.push({ name, physical, typeLength, maxDef, maxRep });
  }
  const chunkCount = view.getUint32(at, true);
  at += 4;
  const chunks: ParquetChunk[] = [];
  for (let i = 0; i < chunkCount; i++) {
    const leaf = view.getUint32(at, true);
    at += 4;
    const codec = view.getInt32(at, true);
    at += 4;
    const numValues = view.getBigInt64(at, true);
    at += 8;
    const start = view.getBigUint64(at, true);
    at += 8;
    const compressedSize = view.getBigUint64(at, true);
    at += 8;
    chunks.push({ leaf, codec, numValues, start, compressedSize });
  }
  const groupRows: number[] = [];
  if (at < bytes.length) {
    const groupCount = view.getUint32(at, true);
    at += 4;
    for (let group = 0; group < groupCount; group += 1) {
      const count = view.getBigInt64(at, true);
      at += 8;
      groupRows.push(safeNumber(count, "row group rows"));
    }
  }
  if (at !== bytes.length) throw new Error("parquet description has trailing bytes");
  return { rows, leaves, chunks, groupRows };
}

class Bytes {
  private parts: Uint8Array[] = [];

  u8(value: number): void {
    this.parts.push(Uint8Array.of(value));
  }

  u16(value: number): void {
    const out = new Uint8Array(2);
    new DataView(out.buffer).setUint16(0, value, true);
    this.parts.push(out);
  }

  u32(value: number): void {
    const out = new Uint8Array(4);
    new DataView(out.buffer).setUint32(0, value, true);
    this.parts.push(out);
  }

  u64(value: number): void {
    const out = new Uint8Array(8);
    new DataView(out.buffer).setBigUint64(0, BigInt(value), true);
    this.parts.push(out);
  }

  i64(value: bigint): void {
    const out = new Uint8Array(8);
    new DataView(out.buffer).setBigInt64(0, value, true);
    this.parts.push(out);
  }

  i32(value: number): void {
    const out = new Uint8Array(4);
    new DataView(out.buffer).setInt32(0, value, true);
    this.parts.push(out);
  }

  bytes(value: Uint8Array): void {
    this.u32(value.byteLength);
    this.parts.push(value);
  }

  bytesRaw(value: Uint8Array): void {
    this.parts.push(value);
  }

  text(value: string): void {
    this.bytes(new TextEncoder().encode(value));
  }

  name(value: string): void {
    const encoded = new TextEncoder().encode(value);
    this.u16(encoded.byteLength);
    this.parts.push(encoded);
  }

  finish(): Uint8Array {
    const length = this.parts.reduce((sum, part) => sum + part.byteLength, 0);
    const out = new Uint8Array(length);
    let at = 0;
    for (const part of this.parts) {
      out.set(part, at);
      at += part.byteLength;
    }
    return out;
  }
}

function writeCatalog(out: Bytes, columns: ParquetColumn[]): void {
  out.u32(columns.length);
  for (const column of columns) {
    out.u8(column.kind === "text" ? 1 : 0);
    out.i32(column.physical);
    out.i32(column.typeLength);
    out.u8(column.maxDef);
    out.u8(column.maxRep);
    out.name(column.name);
  }
}

function requestSql(sql: string, columns: ParquetColumn[]): Uint8Array {
  const out = new Bytes();
  out.text(sql);
  writeCatalog(out, columns);
  return out.finish();
}

async function callBytes(
  input: Uint8Array,
  invoke: (wasm: Exports, inputPtr: number, outPtr: number, outCap: number, lenPtr: number) => number,
): Promise<Uint8Array> {
  return withParquetWasm(async () => {
    const wasm = await loadWasm();
    const inputPtr = allocCopy(wasm, input);
    const lenPtr = wasm.parquet_alloc(4);
    let cap = 256;
    let outPtr = wasm.parquet_alloc(cap);
    try {
      for (;;) {
        const code = invoke(wasm, inputPtr, outPtr, cap, lenPtr);
        const needed = view(wasm).getUint32(lenPtr, true);
        if (code === 1 || code === -3) {
          wasm.parquet_dealloc(outPtr, cap);
          cap = Math.max(needed, cap * 2);
          outPtr = wasm.parquet_alloc(cap);
          continue;
        }
        if (code === -2) {
          const message = new TextDecoder().decode(new Uint8Array(wasm.memory.buffer, outPtr, needed));
          throw new Error(message || "parquet query failed");
        }
        if (code !== 0) throw new Error(statusText(code));
        return new Uint8Array(wasm.memory.buffer, outPtr, needed).slice();
      }
    } finally {
      wasm.parquet_dealloc(outPtr, cap);
      wasm.parquet_dealloc(lenPtr, 4);
      wasm.parquet_dealloc(inputPtr, input.byteLength);
    }
  });
}

const ACTION_BYTES = 32;

/** Opens a Parquet engine. The program is the SQL, catalog, and the ranges it may read. */
export async function engineOpen(program: Uint8Array): Promise<number> {
  return withParquetWasm(async () => {
    const wasm = await loadWasm();
    const ptr = allocCopy(wasm, program);
    const errPtr = wasm.parquet_alloc(512);
    const errLen = wasm.parquet_alloc(4);
    try {
      const handle = wasm.parquet_engine_open(ptr, program.byteLength, errPtr, 512, errLen);
      if (handle <= 0) {
        const needed = view(wasm).getUint32(errLen, true);
        const message = new TextDecoder().decode(new Uint8Array(wasm.memory.buffer, errPtr, Math.min(needed, 512)));
        throw new Error(message || statusText(handle));
      }
      return handle;
    } finally {
      wasm.parquet_dealloc(ptr, program.byteLength);
      wasm.parquet_dealloc(errPtr, 512);
      wasm.parquet_dealloc(errLen, 4);
    }
  });
}

/** One step. Any frame bytes are copied out before the Wasm turn ends. */
export async function engineNext(handle: number): Promise<EngineAction> {
  return withParquetWasm(async () => {
    const wasm = await loadWasm();
    const out = wasm.parquet_alloc(ACTION_BYTES);
    try {
      const code = wasm.parquet_engine_next(handle, out);
      if (code !== 0) throw new Error(statusText(code));
      const data = new DataView(wasm.memory.buffer, out, ACTION_BYTES);
      const kind = data.getUint32(0, true);
      const offset = data.getBigUint64(4, true);
      const length = data.getUint32(12, true);
      const ptr = data.getUint32(16, true);
      const byteLen = data.getUint32(20, true);
      const rowGroup = data.getUint32(24, true);
      const rows = data.getUint32(28, true);
      const bytes = byteLen > 0 ? new Uint8Array(wasm.memory.buffer, ptr, byteLen).slice() : new Uint8Array();
      wasm.parquet_engine_release(handle);
      return { kind, offset, length, rowGroup, rows, bytes };
    } finally {
      wasm.parquet_dealloc(out, ACTION_BYTES);
    }
  });
}

/** Copies a range into the engine. Returns false when the engine rejected it. */
export async function engineSupply(handle: number, bytes: Uint8Array): Promise<boolean> {
  return withParquetWasm(async () => {
    const wasm = await loadWasm();
    const ptr = allocCopy(wasm, bytes);
    try {
      return wasm.parquet_engine_supply(handle, ptr, bytes.byteLength) === 0;
    } finally {
      wasm.parquet_dealloc(ptr, bytes.byteLength);
    }
  });
}

export async function engineClose(handle: number): Promise<void> {
  await withParquetWasm(async () => {
    const wasm = await loadWasm();
    wasm.parquet_engine_close(handle);
  });
}

export async function planParquetSql(
  sql: string,
  columns: ParquetColumn[],
): Promise<{ columns: number[]; grouped: boolean }> {
  const input = requestSql(sql, columns);
  const encoded = await callBytes(input, (wasm, inputPtr, outPtr, outCap, lenPtr) =>
    wasm.parquet_plan_sql(inputPtr, input.byteLength, outPtr, outCap, lenPtr),
  );
  return readPlanSql(encoded);
}

export async function planParquetRows(
  sql: string,
  columns: ParquetColumn[],
): Promise<{ columns: number[]; grouped: boolean }> {
  const input = requestSql(sql, columns);
  const encoded = await callBytes(input, (wasm, inputPtr, outPtr, outCap, lenPtr) =>
    wasm.parquet_plan_rows(inputPtr, input.byteLength, outPtr, outCap, lenPtr),
  );
  return readPlanSql(encoded);
}

export type DecodedIntegerColumn = {
  column: number;
  values: BigInt64Array;
  nulls: Uint8Array;
};

export async function execDecodedBatch(
  sql: string,
  columns: ParquetColumn[],
  decoded: DecodedIntegerColumn[],
): Promise<Uint8Array> {
  const rows = decoded[0]?.values.length ?? 0;
  const out = new Bytes();
  out.text(sql);
  writeCatalog(out, columns);
  out.u32(rows);
  out.u32(decoded.length);
  for (const column of decoded) {
    if (column.values.length !== rows || column.nulls.length !== rows) {
      throw new Error("decoded columns have different lengths");
    }
    out.u32(column.column);
    stageSync("aggPack", () => {
      if (aggregatePack === "buffer" && littleEndian) {
        out.bytesRaw(new Uint8Array(column.values.buffer, column.values.byteOffset, column.values.byteLength));
        return;
      }
      for (let row = 0; row < rows; row += 1) out.i64(column.values[row]!);
    });
    out.bytesRaw(column.nulls);
  }
  const input = out.finish();
  return callBytes(input, (wasm, inputPtr, outPtr, outCap, lenPtr) =>
    stageSync("aggWasm", () => wasm.parquet_exec_decoded(inputPtr, input.byteLength, outPtr, outCap, lenPtr)),
  );
}

export async function execSelectBatch(
  sql: string,
  columns: ParquetColumn[],
  decoded: DecodedIntegerColumn[],
): Promise<DecodedIntegerColumn[]> {
  const rows = decoded[0]?.values.length ?? 0;
  const out = new Bytes();
  out.text(sql);
  writeCatalog(out, columns);
  out.u32(rows);
  out.u32(decoded.length);
  for (const column of decoded) {
    if (column.values.length !== rows || column.nulls.length !== rows) {
      throw new Error("decoded columns have different lengths");
    }
    out.u32(column.column);
    for (let row = 0; row < rows; row += 1) out.i64(column.values[row]!);
    out.bytesRaw(column.nulls);
  }
  const input = out.finish();
  const encoded = await callBytes(input, (wasm, inputPtr, outPtr, outCap, lenPtr) =>
    wasm.parquet_select_decoded(inputPtr, input.byteLength, outPtr, outCap, lenPtr),
  );
  const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
  const kept = view.getUint32(0, true);
  const count = view.getUint32(4, true);
  let at = 8;
  const selected: DecodedIntegerColumn[] = [];
  for (let index = 0; index < count; index += 1) {
    const column = view.getUint32(at, true);
    at += 4;
    const values = new BigInt64Array(kept);
    for (let row = 0; row < kept; row += 1) {
      values[row] = view.getBigInt64(at, true);
      at += 8;
    }
    const nulls = encoded.subarray(at, at + kept);
    at += kept;
    selected.push({ column, values, nulls: nulls.slice() });
  }
  if (at !== encoded.byteLength) throw new Error("selected batch has trailing bytes");
  return selected;
}

export type IntegerBatch = { values: BigInt64Array; nulls: Uint8Array };

export type NumericScan = {
  next(maxRows: number): Promise<IntegerBatch | null>;
  close(): Promise<void>;
};

const SCAN_CHUNK_LIMIT = 8 * 1024 * 1024;

/** Decode one integer column chunk into bounded batches. The chunk bytes are released after open. */
export async function openNumericScan(
  bytes: Uint8Array,
  column: { physical: number; typeLength: number; codec: number; maxDef: number; maxRep: number },
): Promise<NumericScan> {
  if (bytes.byteLength > SCAN_CHUNK_LIMIT) {
    throw new Error(`column chunk of ${bytes.byteLength} bytes exceeds the scan buffer`);
  }
  const handle = await withParquetWasm(async () => {
    const wasm = await loadWasm();
    const ptr = allocCopy(wasm, bytes);
    const errPtr = wasm.parquet_alloc(256);
    const errLen = wasm.parquet_alloc(4);
    try {
      const opened = stageSync("decodeOpen", () => wasm.parquet_scan_open(
        ptr,
        bytes.byteLength,
        column.physical,
        column.typeLength,
        column.codec,
        column.maxDef,
        column.maxRep,
        errPtr,
        256,
        errLen,
      ));
      if (opened <= 0) {
        const needed = view(wasm).getUint32(errLen, true);
        const message = new TextDecoder().decode(new Uint8Array(wasm.memory.buffer, errPtr, Math.min(needed, 256)));
        throw new Error(message || statusText(opened));
      }
      return opened;
    } finally {
      wasm.parquet_dealloc(ptr, bytes.byteLength);
      wasm.parquet_dealloc(errPtr, 256);
      wasm.parquet_dealloc(errLen, 4);
    }
  });
  let closed = false;
  return {
    async next(maxRows: number) {
      if (closed) return null;
      return withParquetWasm(async () => {
        const wasm = await loadWasm();
        const lenPtr = wasm.parquet_alloc(4);
        let cap = 256;
        let outPtr = wasm.parquet_alloc(cap);
        try {
          for (;;) {
            const code = stageSync("decodeWasm", () => wasm.parquet_scan_next(handle, maxRows, outPtr, cap, lenPtr));
            const needed = view(wasm).getUint32(lenPtr, true);
            if (code === 1) {
              wasm.parquet_dealloc(outPtr, cap);
              cap = Math.max(needed, cap * 2);
              outPtr = wasm.parquet_alloc(cap);
              continue;
            }
            if (code === 2) return null;
            if (code === -2) {
              const message = new TextDecoder().decode(new Uint8Array(wasm.memory.buffer, outPtr, Math.min(needed, cap)));
              throw new Error(message || "parquet page could not be decoded");
            }
            if (code !== 0) throw new Error(statusText(code));
            const copied = stageSync("decodeCopy", () => {
              const data = new DataView(wasm.memory.buffer, outPtr, needed);
              const rows = data.getUint32(0, true);
              const values = new BigInt64Array(rows);
              for (let row = 0; row < rows; row += 1) values[row] = data.getBigInt64(4 + row * 8, true);
              const nulls = new Uint8Array(rows);
              nulls.set(new Uint8Array(wasm.memory.buffer, outPtr + 4 + rows * 8, rows));
              return { values, nulls };
            });
            return copied;
          }
        } finally {
          wasm.parquet_dealloc(outPtr, cap);
          wasm.parquet_dealloc(lenPtr, 4);
        }
      });
    },
    async close() {
      if (closed) return;
      closed = true;
      await withParquetWasm(async () => {
        const wasm = await loadWasm();
        wasm.parquet_scan_close(handle);
      });
    },
  };
}


export type ParquetPartialQuery = {
  query: QueryResult;
  rows: number;
  sums: Record<string, number>;
};

function readPlanSql(bytes: Uint8Array): { columns: number[]; grouped: boolean } {
  const data = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = data.getUint32(0, true);
  const columns: number[] = [];
  let at = 4;
  for (let index = 0; index < count; index += 1) {
    columns.push(data.getUint32(at, true));
    at += 4;
  }
  const grouped = bytes[at];
  if (grouped === undefined) throw new Error("parquet plan is truncated");
  return { columns, grouped: grouped !== 0 };
}


function safeNumber(value: bigint, label: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`${label} does not fit in a JavaScript number`);
  }
  return Number(value);
}

function isHttp(location: string): boolean {
  return location.startsWith("https://") || location.startsWith("http://");
}

/** Range-read a Parquet footer and column layout. */
export class ParquetFile {
  private constructor(
    private readonly source: ByteSource,
    private readonly release: () => Promise<void>,
    readonly plan: ParquetPlan,
    readonly etag: string | null,
  ) {}

  static async open(location: string): Promise<ParquetFile> {
    if (isHttp(location)) {
      const source = new HttpRangeSource(location, { rangeOnly: true });
      const size = await source.contentLength();
      const plan = await describe(source, size);
      return new ParquetFile(source, async () => {}, plan, source.validator());
    }
    const fs = (await import(nodeSpecifier("fs/promises"))) as { stat(path: string): Promise<{ size: number }> };
    const info = await fs.stat(location);
    if (!Number.isSafeInteger(info.size)) {
      throw new Error("parquet file is larger than this reader supports");
    }
    const source = new NodeFileSource(location, { cacheMaxBytes: 0 });
    try {
      const plan = await describe(source, info.size);
      return new ParquetFile(source, () => source.close(), plan, null);
    } catch (error) {
      await source.close();
      throw error;
    }
  }


  async close(): Promise<void> {
    await this.release();
  }
}

async function describe(source: ByteSource, fileSize: number): Promise<ParquetPlan> {
  if (fileSize < 12) throw new Error(statusText(-3));
  const tail = await source.read(fileSize - 8, 8);
  if (tail.length !== 8) throw new Error(statusText(-3));
  const span = await withParquetWasm(async () => {
    const wasm = await loadWasm();
    const tailPtr = allocCopy(wasm, tail);
    const spanPtr = wasm.parquet_alloc(16);
    try {
      const code = wasm.parquet_footer_span(BigInt(fileSize), tailPtr, spanPtr, spanPtr + 8);
      if (code !== 0) throw new Error(statusText(code));
      return {
        start: safeNumber(view(wasm).getBigUint64(spanPtr, true), "footer start"),
        len: safeNumber(view(wasm).getBigUint64(spanPtr + 8, true), "footer length"),
      };
    } finally {
      wasm.parquet_dealloc(spanPtr, 16);
      wasm.parquet_dealloc(tailPtr, tail.length);
    }
  });
  if (span.len > 32 * 1024 * 1024) throw new Error(`parquet footer of ${span.len} bytes is unreasonably large`);
  const footer = await source.read(span.start, span.len);
  if (footer.length !== span.len) throw new Error(statusText(-3));
  return withParquetWasm(async () => {
    const wasm = await loadWasm();
    const footerPtr = allocCopy(wasm, footer);
    const lenPtr = wasm.parquet_alloc(4);
    try {
      let describedCode = wasm.parquet_describe(footerPtr, footer.length, 0, 0, lenPtr);
      if (describedCode !== 1) throw new Error(statusText(describedCode));
      const needed = view(wasm).getUint32(lenPtr, true);
      const outPtr = wasm.parquet_alloc(needed);
      try {
        describedCode = wasm.parquet_describe(footerPtr, footer.length, outPtr, needed, lenPtr);
        if (describedCode !== 0) throw new Error(statusText(describedCode));
        const described = new Uint8Array(wasm.memory.buffer, outPtr, needed).slice();
        return readPlan(described);
      } finally {
        wasm.parquet_dealloc(outPtr, needed);
      }
    } finally {
      wasm.parquet_dealloc(lenPtr, 4);
      wasm.parquet_dealloc(footerPtr, footer.length);
    }
  });
}
