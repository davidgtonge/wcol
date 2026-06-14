import { open, readFile, stat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { QueryPlan } from "../../apps/explorer/demo/query/plan-types.ts";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "../..");

export type QueryResult = {
  rows: bigint[] | number[];
  projection?: {
    columns: { name: string; colId: number; kind: number }[];
    data: unknown[];
  } | null;
  groups?: {
    keys: (bigint | number)[];
    keys2?: (bigint | number)[];
    keyInfo?: { colId: number; physicalType: number; flags: number }[];
    values: { sum: number; count: number }[][];
  } | null;
  aggregates?: Record<string, unknown>;
};

export type WcolFileHandle = {
  header: { totalRows: bigint | number; ncols: number; nchunks: number };
  query(plan: QueryPlan, opts?: { workers?: number }): Promise<QueryResult>;
  getColumnInfo(colId: number): Promise<{ physicalType: number; flags: number }>;
  getColumnDictValue(colId: number, dictId: number | bigint): Promise<string | undefined>;
  getRuntimeDictValue(colId: number, poolId: number): Promise<string>;
};

type Runtime = {
  WcolFile: {
    open(source: unknown): Promise<WcolFileHandle>;
  };
  buildPlan: (spec: Record<string, unknown>) => QueryPlan;
  ByteSource: new () => { read(offset: number, len: number): Promise<Uint8Array> };
};

let runtimePromise: Promise<Runtime> | null = null;

/** Patch fetch so SIMD wasm loads from disk (Node cannot fetch file:// wasm). */
async function loadRuntime(): Promise<Runtime> {
  if (runtimePromise) return runtimePromise;
  runtimePromise = (async () => {
    const wasmPath = join(ROOT, "dist/browser/wasm/wcol_wasm.simd.wasm");
    const wasmBytes = await readFile(wasmPath);
    const origFetch = globalThis.fetch;
    globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).includes("wcol_wasm.simd.wasm")) {
        return {
          ok: true,
          status: 200,
          arrayBuffer: async () =>
            wasmBytes.buffer.slice(wasmBytes.byteOffset, wasmBytes.byteOffset + wasmBytes.byteLength),
        } as Response;
      }
      return origFetch(input, init);
    };
    return import("../../dist/browser/main.js") as Promise<Runtime>;
  })();
  return runtimePromise;
}

class NodeFileSource {
  constructor(private readonly path: string) {}
  async read(offset: number, len: number): Promise<Uint8Array> {
    const fh = await open(this.path, "r");
    try {
      const buf = Buffer.alloc(len);
      const { bytesRead } = await fh.read(buf, 0, len, offset);
      return new Uint8Array(buf.buffer, buf.byteOffset, bytesRead);
    } finally {
      await fh.close();
    }
  }
}

export async function defaultCratesFixture(): Promise<string> {
  if (process.env.WCOL_CRATES_FILE) return resolve(process.env.WCOL_CRATES_FILE);
  const candidates = [
    join(ROOT, "apps/explorer/demo/data/crates_versions.wcol"),
    join(ROOT, "data/crates_versions.wcol"),
    join(ROOT, "apps/explorer/apps/explorer/dist/browser/data/crates_versions.wcol"),
  ];
  for (const path of candidates) {
    if (await fixtureExists(path)) return path;
  }
  return candidates[0];
}

export async function defaultDepsFixture(): Promise<string> {
  if (process.env.WCOL_DEPS_FILE) return resolve(process.env.WCOL_DEPS_FILE);
  const candidates = [
    join(ROOT, "apps/explorer/demo/data/crates_dependencies.wcol"),
    join(ROOT, "data/crates_dependencies.wcol"),
    join(ROOT, "apps/explorer/dist/browser/data/crates_dependencies.wcol"),
  ];
  for (const path of candidates) {
    if (await fixtureExists(path)) return path;
  }
  return candidates[0];
}

export async function defaultCategoriesFixture(): Promise<string> {
  if (process.env.WCOL_CATEGORIES_FILE) return resolve(process.env.WCOL_CATEGORIES_FILE);
  const candidates = [
    join(ROOT, "apps/explorer/demo/data/crates_categories.wcol"),
    join(ROOT, "data/crates_categories.wcol"),
    join(ROOT, "apps/explorer/dist/browser/data/crates_categories.wcol"),
  ];
  for (const path of candidates) {
    if (await fixtureExists(path)) return path;
  }
  return candidates[0];
}

export async function defaultMaintainersFixture(): Promise<string> {
  if (process.env.WCOL_MAINTAINERS_FILE) return resolve(process.env.WCOL_MAINTAINERS_FILE);
  const candidates = [
    join(ROOT, "apps/explorer/demo/data/crate_maintainers.wcol"),
    join(ROOT, "data/crate_maintainers.wcol"),
    join(ROOT, "apps/explorer/dist/browser/data/crate_maintainers.wcol"),
  ];
  for (const path of candidates) {
    if (await fixtureExists(path)) return path;
  }
  return candidates[0];
}

export async function defaultTrendsFixture(): Promise<string> {
  if (process.env.WCOL_TRENDS_FILE) return resolve(process.env.WCOL_TRENDS_FILE);
  const candidates = [
    join(ROOT, "apps/explorer/demo/data/version_downloads_daily.wcol"),
    join(ROOT, "data/version_downloads_daily.wcol"),
    join(ROOT, "apps/explorer/dist/browser/data/version_downloads_daily.wcol"),
  ];
  for (const path of candidates) {
    if (await fixtureExists(path)) return path;
  }
  return candidates[0];
}

export async function defaultTrendsCrate30dFixture(): Promise<string> {
  if (process.env.WCOL_TRENDS_CRATE_30D_FILE) return resolve(process.env.WCOL_TRENDS_CRATE_30D_FILE);
  const candidates = [
    join(ROOT, "apps/explorer/demo/data/trends_crate_downloads_30d.wcol"),
    join(ROOT, "data/trends_crate_downloads_30d.wcol"),
  ];
  for (const path of candidates) {
    if (await fixtureExists(path)) return path;
  }
  return candidates[0];
}

export async function defaultTrendsSerdeVersionsFixture(): Promise<string> {
  if (process.env.WCOL_TRENDS_SERDE_VERSIONS_FILE) {
    return resolve(process.env.WCOL_TRENDS_SERDE_VERSIONS_FILE);
  }
  const candidates = [
    join(ROOT, "apps/explorer/demo/data/trends_serde_version_downloads.wcol"),
    join(ROOT, "data/trends_serde_version_downloads.wcol"),
  ];
  for (const path of candidates) {
    if (await fixtureExists(path)) return path;
  }
  return candidates[0];
}

export async function resolveTrendsQueryFixture(
  query: { rollup?: "crate_downloads_30d" | "serde_version_downloads" }
): Promise<string> {
  if (query.rollup === "crate_downloads_30d") return defaultTrendsCrate30dFixture();
  if (query.rollup === "serde_version_downloads") return defaultTrendsSerdeVersionsFixture();
  return defaultTrendsFixture();
}

export async function defaultHitsFixture(): Promise<string> {
  if (process.env.WCOL_HITS_FILE) return resolve(process.env.WCOL_HITS_FILE);
  const candidates = [
    join(ROOT, "apps/explorer/demo/data/hits_subset_500k.wcol"),
    join(ROOT, "data/hits_subset_500k.wcol"),
    join(ROOT, "apps/explorer/dist/browser/data/hits_subset_500k.wcol"),
  ];
  for (const path of candidates) {
    if (await fixtureExists(path)) return path;
  }
  return candidates[0];
}

export async function fixtureExists(path: string): Promise<boolean> {
  try {
    const s = await stat(path);
    return s.isFile() && s.size > 0;
  } catch {
    return false;
  }
}

export async function openCratesFile(path = defaultCratesFixture()): Promise<WcolFileHandle> {
  const { WcolFile, ByteSource } = await loadRuntime();
  class Source extends ByteSource {
    constructor(private readonly filePath: string) {
      super();
    }
    read(offset: number, len: number) {
      return new NodeFileSource(this.filePath).read(offset, len);
    }
  }
  return WcolFile.open(new Source(path));
}

export async function buildPlan(spec: Record<string, unknown>): Promise<QueryPlan> {
  const { buildPlan: bp } = await loadRuntime();
  return bp(spec);
}

export async function runPlan(
  file: WcolFileHandle,
  plan: QueryPlan,
  workers = Number(process.env.WCOL_QUERY_WORKERS ?? 1)
): Promise<{ result: QueryResult; ms: number }> {
  const t0 = performance.now();
  const result = await file.query(plan, { workers });
  return { result, ms: performance.now() - t0 };
}

export async function groupLabels(file: WcolFileHandle, result: QueryResult, limit = 5): Promise<string[]> {
  const g = result.groups;
  if (!g?.keys?.length) return [];
  const { resolveGroupKey } = await import("../../apps/explorer/demo/data/resolve-values.ts");
  const out: string[] = [];
  for (let i = 0; i < Math.min(limit, g.keys.length); i += 1) {
    out.push(await resolveGroupKey(file, g.keyInfo?.[0], g.keys[i]));
  }
  return out;
}

export async function projectionRows(
  file: WcolFileHandle,
  result: QueryResult,
  limit = 5
): Promise<Record<string, string | number | boolean | null>[]> {
  const proj = result.projection;
  if (!proj) return [];
  const { resolveProjectionCell } = await import("../../apps/explorer/demo/data/resolve-values.ts");
  const rows: Record<string, string | number | boolean | null>[] = [];
  const n = Math.min(limit, result.rows?.length ?? 0);
  for (let r = 0; r < n; r += 1) {
    const row: Record<string, string | number | boolean | null> = {};
    for (let c = 0; c < proj.columns.length; c += 1) {
      const meta = proj.columns[c];
      const col = proj.data[c] as {
        kind: number;
        values: ArrayLike<number | bigint | boolean>;
        nulls: Uint8Array;
      };
      row[meta.name] = await resolveProjectionCell(file, meta, col, r);
    }
    rows.push(row);
  }
  return rows;
}

export function rowCount(result: QueryResult): number {
  if (result.groups?.keys?.length) return result.groups.keys.length;
  return result.rows?.length ?? 0;
}
