import { glob, stat } from "node:fs/promises";
import path from "node:path";

import { HttpRangeSource } from "./io/byte-source.ts";
import {
  planParquetRows,
  planParquetSql,
  ParquetFile,
  type ParquetPlan,
} from "./parquet.ts";

export type { ParquetColumn } from "./parquet.ts";
import { CLOUDFLARE_MAX_OUTGOING, chunkSlice, type StreamNode } from "../../archive/legacy-ts-stream/types.ts";

/** Files at or above this size are split from their footer. Smaller files are one task. */
export const DEFAULT_FOOTER_BYTES = 32 * 1024 * 1024;

export type ParquetScanChunk = {
  column: number;
  start: number;
  compressedSize: number;
  codec: number;
  /** When true, Wasm decode preserves dictionary ids for this column. */
  dictEncoded?: boolean;
};

export type ParquetScanGroup = {
  rows: number;
  /** Footer row-group index. Present when the planner read `num_rows` from the row group. */
  rowGroup?: number;
  chunks: ParquetScanChunk[];
};

export type ParquetNode = {
  url?: string;
  sql: string;
  columns: ParquetColumn[];
  concurrency?: number;
  /** Row groups this scan owns. Absent when the scan opens the file and reads its own footer. */
  groups?: ParquetScanGroup[];
  fileSize?: number;
  etag?: string;
  batchRows?: number;
  /** How many row-group range reads stay in flight. `1` waits for each read. */
  prefetch?: number;
};

export type ParquetJobPlan = {
  nodes: StreamNode[];
  files: number;
  scans: number;
  /** Files whose footer the planner read in order to assign row groups. */
  footerFiles: number;
};

export function assignChunks<T>(items: T[], tasks: number): T[][] {
  if (items.length === 0) return [];
  const count = Math.min(Math.max(1, tasks), items.length);
  const groups: T[][] = [];
  for (let index = 0; index < count; index += 1) {
    const range = chunkSlice(items.length, count, index);
    groups.push(items.slice(range.start, range.end));
  }
  return groups;
}

function isHttp(location: string): boolean {
  return location.startsWith("https://") || location.startsWith("http://");
}

async function enumerate(sources: string[]): Promise<string[]> {
  const files: string[] = [];
  for (const source of sources) {
    if (isHttp(source)) {
      files.push(source);
      continue;
    }
    const info = await stat(source);
    if (info.isDirectory()) {
      const matches = await glob("**/*.parquet", { cwd: source });
      matches.sort();
      for (const match of matches) files.push(path.join(source, match));
      continue;
    }
    files.push(source);
  }
  if (files.length === 0) throw new Error("no parquet files");
  return files;
}

async function byteSize(location: string): Promise<number> {
  if (!isHttp(location)) {
    const info = await stat(location);
    return info.size;
  }
  const source = new HttpRangeSource(location, { rangeOnly: true });
  return source.contentLength();
}

function safe(value: bigint, label: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`${label} does not fit in a JavaScript number`);
  }
  return Number(value);
}

function buildGraph(scans: ParquetNode[]): StreamNode[] {
  const head = scans[0];
  if (!head) throw new Error("no parquet tasks");
  const shared = { sql: head.sql, columns: head.columns };
  let mergeSeq = 0;
  const scanNodes: StreamNode[] = scans.map((task, index) => ({
    id: `scan${index}`,
    op: "scan-parquet",
    parquet: task,
  }));
  const assemble = (ids: string[]): StreamNode[] => {
    if (ids.length <= CLOUDFLARE_MAX_OUTGOING) {
      return [{ id: "reduce", op: "reduce-parquet", inputs: ids, parquet: shared }];
    }
    const mids: string[] = [];
    const nodes: StreamNode[] = [];
    for (let offset = 0; offset < ids.length; offset += CLOUDFLARE_MAX_OUTGOING) {
      const group = ids.slice(offset, offset + CLOUDFLARE_MAX_OUTGOING);
      const id = `merge${mergeSeq}`;
      mergeSeq += 1;
      mids.push(id);
      nodes.push({ id, op: "merge-parquet", inputs: group, parquet: shared });
    }
    return [...nodes, ...assemble(mids)];
  };
  return [...scanNodes, ...assemble(scanNodes.map((node) => node.id))];
}

export function columnsFromPlan(plan: ParquetPlan): ParquetColumn[] {
  return plan.leaves.map((leaf) => ({
    name: leaf.name,
    kind: leaf.physical === 6 || leaf.physical === 7 ? "text" : "number",
    physical: leaf.physical,
    typeLength: leaf.typeLength,
    maxDef: leaf.maxDef,
    maxRep: leaf.maxRep,
  }));
}

function sameSchema(plan: ParquetPlan, columns: ParquetColumn[]): void {
  if (plan.leaves.length !== columns.length) throw new Error("parquet files have different columns");
  for (let index = 0; index < columns.length; index += 1) {
    const leaf = plan.leaves[index];
    const column = columns[index];
    if (!leaf || !column || leaf.name !== column.name || leaf.physical !== column.physical) {
      throw new Error(`parquet schema mismatch at ${column?.name ?? index}`);
    }
  }
}

/** One entry per row group. `needed` lists the column indexes the query reads. */
export function rowGroups(plan: ParquetPlan, needed: number[]): ParquetScanGroup[] {
  const width = plan.leaves.length;
  if (width === 0 || plan.chunks.length % width !== 0) throw new Error("parquet row groups are incomplete");
  const groups: ParquetScanGroup[] = [];
  for (let start = 0; start < plan.chunks.length; start += width) {
    const slice = plan.chunks.slice(start, start + width);
    const sample = slice[0];
    const groupIndex = groups.length;
    const declared = plan.groupRows[groupIndex];
    if (plan.groupRows.length > 0 && declared === undefined) {
      throw new Error(`row group ${groupIndex} has no num_rows`);
    }
    const rows = declared ?? (sample ? safe(sample.numValues, "row group rows") : 0);
    const chunks: ParquetScanChunk[] = [];
    for (const column of needed) {
      const chunk = slice[column];
      if (!chunk || chunk.leaf !== column) throw new Error("parquet row group is missing a column");
      chunks.push({
        column,
        start: safe(chunk.start, "column chunk start"),
        compressedSize: safe(chunk.compressedSize, "column chunk size"),
        codec: chunk.codec,
        dictEncoded: chunk.hasDictionary ?? false,
      });
    }
    groups.push({ rows, rowGroup: groupIndex, chunks });
  }
  return groups;
}

/**
 * Plan `sql` against the Parquet columns, then cut the row groups those columns
 * touch into scan tasks. A small file stays one scan and reads its own footer.
 */
export async function planParquetJob(options: {
  sources: string[];
  sql: string;
  /** How many scan tasks a large file is cut into. Small files stay one task each. */
  tasks?: number;
  /** Read the footer and split when a file is at least this many bytes. `0` always splits. */
  footerBytes?: number;
  concurrency?: number;
}): Promise<ParquetJobPlan> {
  const files = await enumerate(options.sources);
  const tasks = Math.max(1, options.tasks ?? 8);
  const footerBytes = options.footerBytes ?? DEFAULT_FOOTER_BYTES;
  const concurrency = options.concurrency ?? 8;
  const first = await ParquetFile.open(files[0]!);
  const columns = columnsFromPlan(first.plan);
  const planned = await planParquetSql(options.sql, columns);
  const scans: ParquetNode[] = [];
  let footerFiles = 0;
  const opened = new Map<string, ParquetFile>([[files[0]!, first]]);
  try {
    for (const file of files) {
      const size = await byteSize(file);
      const split = size >= footerBytes;
      let fileHandle = opened.get(file);
      if (split && !fileHandle) {
        fileHandle = await ParquetFile.open(file);
        opened.set(file, fileHandle);
        sameSchema(fileHandle.plan, columns);
      }
      if (!fileHandle) {
        scans.push({ url: file, sql: options.sql, columns, concurrency });
        continue;
      }
      if (split) footerFiles += 1;
      const groups = rowGroups(fileHandle.plan, planned.columns);
      const pieces = split ? assignChunks(groups, tasks) : [groups];
      if (pieces.length === 0) {
        scans.push({ url: file, sql: options.sql, columns, concurrency, groups: [] });
        continue;
      }
      for (const piece of pieces) {
        scans.push({ url: file, sql: options.sql, columns, concurrency, groups: piece });
      }
    }
  } finally {
    await Promise.all([...opened.values()].map((file) => file.close()));
  }
  return {
    nodes: buildGraph(scans),
    files: files.length,
    scans: scans.length,
    footerFiles,
  };
}

export type ParquetStreamTask = {
  id: string;
  rowGroups: number[];
  bytes: number;
};

export type ParquetStreamPlan = {
  nodes: StreamNode[];
  fileSize: number;
  etag: string | null;
  rowGroups: number;
  columns: string[];
  compressedBytes: number;
  rangeRequests: number;
  tasks: ParquetStreamTask[];
};

function groupBytes(group: ParquetScanGroup): number {
  return group.chunks.reduce((sum, chunk) => sum + chunk.compressedSize, 0);
}

/** Pack row groups onto workers by compressed bytes of the columns the query reads. */
export function assignByBytes(groups: ParquetScanGroup[], workers: number): ParquetScanGroup[][] {
  if (groups.length === 0) return [];
  const count = Math.min(Math.max(1, workers), groups.length);
  const bins: Array<{ bytes: number; items: Array<{ index: number; group: ParquetScanGroup }> }> = [];
  for (let index = 0; index < count; index += 1) bins.push({ bytes: 0, items: [] });
  const ranked = groups.map((group, index) => ({ group, index, bytes: groupBytes(group) }));
  ranked.sort((left, right) => right.bytes - left.bytes || left.index - right.index);
  for (const item of ranked) {
    let best = 0;
    for (let index = 1; index < bins.length; index += 1) {
      if (bins[index]!.bytes < bins[best]!.bytes) best = index;
    }
    bins[best]!.items.push({ index: item.index, group: item.group });
    bins[best]!.bytes += item.bytes;
  }
  return bins
    .filter((bin) => bin.items.length > 0)
    .map((bin) => bin.items.sort((left, right) => left.index - right.index).map((item) => item.group));
}

/** Parquet → WCOL projection stream (bounded ingest memory; no SQL in Rust). */
export function streamIngestGraph(scans: ParquetNode[]): StreamNode[] {
  const head = scans[0];
  if (!head) throw new Error("no parquet tasks");
  const shared = { sql: head.sql, columns: head.columns };
  const scanNodes: StreamNode[] = scans.map((task, index) => ({
    id: `scan${index}`,
    op: "ingest-parquet-stream",
    parquet: task,
  }));
  const aggIds = scanNodes.map((node) => node.id);
  let mergeSeq = 0;
  const assemble = (ids: string[]): StreamNode[] => {
    if (ids.length <= CLOUDFLARE_MAX_OUTGOING) {
      return [{ id: "reduce", op: "reduce-parquet", inputs: ids, parquet: shared }];
    }
    const mids: string[] = [];
    const nodes: StreamNode[] = [];
    for (let offset = 0; offset < ids.length; offset += CLOUDFLARE_MAX_OUTGOING) {
      const group = ids.slice(offset, offset + CLOUDFLARE_MAX_OUTGOING);
      const id = `merge${mergeSeq}`;
      mergeSeq += 1;
      mids.push(id);
      nodes.push({ id, op: "merge-parquet", inputs: group, parquet: shared });
    }
    return [...nodes, ...assemble(mids)];
  };
  return [...scanNodes, ...assemble(aggIds)];
}

function streamGraph(scans: ParquetNode[]): StreamNode[] {
  const head = scans[0];
  if (!head) throw new Error("no parquet tasks");
  const shared = { sql: head.sql, columns: head.columns };
  const scanNodes: StreamNode[] = scans.map((task, index) => ({
    id: `scan${index}`,
    op: "scan-parquet-fold",
    parquet: task,
  }));
  const aggIds = scanNodes.map((node) => node.id);
  let mergeSeq = 0;
  const assemble = (ids: string[]): StreamNode[] => {
    if (ids.length <= CLOUDFLARE_MAX_OUTGOING) {
      return [{ id: "reduce", op: "reduce-parquet", inputs: ids, parquet: shared }];
    }
    const mids: string[] = [];
    const nodes: StreamNode[] = [];
    for (let offset = 0; offset < ids.length; offset += CLOUDFLARE_MAX_OUTGOING) {
      const group = ids.slice(offset, offset + CLOUDFLARE_MAX_OUTGOING);
      const id = `merge${mergeSeq}`;
      mergeSeq += 1;
      mids.push(id);
      nodes.push({ id, op: "merge-parquet", inputs: group, parquet: shared });
    }
    return [...nodes, ...assemble(mids)];
  };
  return [...scanNodes, ...assemble(aggIds)];
}

function projectionGraph(scans: ParquetNode[], delayMs?: number): StreamNode[] {
  const head = scans[0];
  if (!head) throw new Error("no parquet tasks");
  const shared = { sql: head.sql, columns: head.columns };
  const nodes: StreamNode[] = [];
  scans.forEach((task, index) => {
    const scan = `scan${index}`;
    const filter = `filter${index}`;
    const transform = `transform${index}`;
    const count = `count${index}`;
    nodes.push({ id: scan, op: "scan-parquet-rows", parquet: task });
    nodes.push({ id: filter, op: "filter-rows", inputs: [scan], parquet: shared });
    nodes.push({ id: transform, op: "transform", inputs: [filter] });
    nodes.push({ id: count, op: "count", inputs: [transform], delayMs });
  });
  return nodes;
}

/**
 * Footer-only plan. An aggregate scan decodes, filters, and groups inside one
 * Wasm engine and emits a partial. A row scan emits projection frames for a
 * downstream node.
 */
export async function planParquetStream(options: {
  url: string;
  sql: string;
  parallelism?: number;
  maxRowGroups?: number;
  batchRows?: number;
  shape?: "aggregate" | "rows";
  delayMs?: number;
  prefetch?: number;
}): Promise<ParquetStreamPlan> {
  const file = await ParquetFile.open(options.url);
  try {
    const columns = columnsFromPlan(file.plan);
    const planned = options.shape === "rows"
      ? await planParquetRows(options.sql, columns)
      : await planParquetSql(options.sql, columns);
    const selected = planned.columns.map((index) => columns[index]?.name ?? String(index));
    for (const index of planned.columns) {
      const column = columns[index];
      if (!column) throw new Error(`query references missing column ${index}`);
      if (column.kind === "text" || column.physical === 6 || column.physical === 7) {
        throw new Error(`string batches are not implemented (${column.name})`);
      }
      if (column.physical === 4 || column.physical === 5) {
        throw new Error(`float batches are not implemented (${column.name})`);
      }
    }
    let groups = rowGroups(file.plan, planned.columns);
    if (options.maxRowGroups !== undefined) groups = groups.slice(0, options.maxRowGroups);
    const packed = assignByBytes(groups, options.parallelism ?? 32);
    const fileSize = await byteSize(options.url);
    const scans: ParquetNode[] = packed.map((piece) => ({
      url: options.url,
      sql: options.sql,
      columns,
      groups: piece,
      fileSize,
      etag: file.etag ?? undefined,
      batchRows: options.batchRows,
      concurrency: 2,
      prefetch: options.prefetch,
    }));
    const tasks = scans.map((scan, index) => ({
      id: `scan${index}`,
      rowGroups: (scan.groups ?? []).map((group) => group.rowGroup ?? -1),
      bytes: (scan.groups ?? []).reduce((sum, group) => sum + groupBytes(group), 0),
    }));
    const compressedBytes = tasks.reduce((sum, task) => sum + task.bytes, 0);
    return {
      nodes: scans.length === 0 ? [] : options.shape === "rows" ? projectionGraph(scans, options.delayMs) : streamGraph(scans),
      fileSize,
      etag: file.etag,
      rowGroups: file.plan.groupRows.length || file.plan.chunks.length / columns.length,
      columns: selected,
      compressedBytes,
      rangeRequests: groups.reduce((sum, group) => sum + group.chunks.length, 0),
      tasks,
    };
  } finally {
    await file.close();
  }
}
