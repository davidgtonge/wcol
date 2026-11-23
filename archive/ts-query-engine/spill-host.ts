/**
 * Synchronous DO SQLite spill host (one op per Wasm round-trip).
 * Composite (key_blob, seq) keyset pages — no cursor held across await.
 */

import {
  SPILL_OP_APPEND,
  SPILL_OP_BEGIN,
  SPILL_OP_END,
  SPILL_OP_SCAN,
  tableName,
} from "./spill-wire.ts";

export type SpillSql = {
  exec(query: string, ...bindings: unknown[]): unknown;
  /** DO `storage.transactionSync` — one commit around many INSERT statements. */
  transactionSync?(fn: () => void): void;
};

/** Result of setting or reading `PRAGMA cache_size`. Unknown pragmas are silent no-ops. */
export type SqliteCacheProbe = {
  requested: number | null;
  reported: number | null;
  accepted: boolean;
  error?: string;
};

function firstNumeric(row: unknown): number | null {
  if (row == null || typeof row !== "object") return null;
  const rec = row as Record<string, unknown>;
  const value = rec.cache_size ?? rec.Cache_size ?? Object.values(rec)[0];
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "string" && value.length > 0 && Number.isFinite(Number(value))) {
    return Number(value);
  }
  return null;
}

function rowsFromExec(out: unknown): unknown[] {
  if (out == null) return [];
  if (typeof out === "object" && out !== null && "one" in out && typeof (out as { one: () => unknown }).one === "function") {
    try {
      const row = (out as { one: () => unknown }).one();
      return row == null ? [] : [row];
    } catch {
      return [];
    }
  }
  if (typeof out === "object" && out !== null && "toArray" in out && typeof (out as { toArray: () => unknown[] }).toArray === "function") {
    try {
      return (out as { toArray: () => unknown[] }).toArray();
    } catch {
      return [];
    }
  }
  if (typeof (out as Iterable<unknown>)[Symbol.iterator] === "function") {
    return [...(out as Iterable<unknown>)];
  }
  return [];
}

export function readSqliteCacheSize(sql: SpillSql): number | null {
  try {
    const rows = rowsFromExec(sql.exec("PRAGMA cache_size"));
    for (const row of rows) {
      const value = firstNumeric(row);
      if (value != null) return value;
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * Request a SQLite page-cache size and read it back. `accepted` is true only when
 * the reported value equals `pages`. Cloudflare may ignore undocumented pragmas.
 */
export function applySqliteCache(sql: SpillSql, pages: number): SqliteCacheProbe {
  if (!Number.isInteger(pages) || pages === 0) {
    return { requested: pages, reported: readSqliteCacheSize(sql), accepted: false, error: "cache_size must be a non-zero integer" };
  }
  try {
    sql.exec(`PRAGMA cache_size = ${pages}`);
  } catch (error) {
    return {
      requested: pages,
      reported: readSqliteCacheSize(sql),
      accepted: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
  const reported = readSqliteCacheSize(sql);
  return { requested: pages, reported, accepted: reported === pages };
}

/**
 * Durable Object SQLite: max 100 bound parameters per query.
 * Each spill row binds (key_blob, partial), so one INSERT can hold at most 50 rows.
 * Larger logical batches are a transaction of several 50-row statements, not a bigger INSERT.
 */
export const SPILL_INSERT_MAX_BOUND_ROWS = 50;
export const SPILL_INSERT_CHUNK_ROWS = SPILL_INSERT_MAX_BOUND_ROWS;

export type SpillHostRequest = {
  op: number;
  session: bigint;
  resumeStarted: boolean;
  resumeKey: Uint8Array;
  resumeSeq: bigint;
  maxRows: number;
  maxBytes: number;
  appendBytes: Uint8Array;
};

function decodeAppendBatch(bytes: Uint8Array): Array<{ key: Uint8Array; partial: Uint8Array }> {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let off = 0;
  const rowCount = view.getUint32(off, true);
  off += 4;
  const rows: Array<{ key: Uint8Array; partial: Uint8Array }> = [];
  for (let i = 0; i < rowCount; i++) {
    const keyLen = view.getUint32(off, true);
    off += 4;
    const key = bytes.slice(off, off + keyLen);
    off += keyLen;
    const partialLen = view.getUint32(off, true);
    off += 4;
    const partial = bytes.slice(off, off + partialLen);
    off += partialLen;
    rows.push({ key, partial });
  }
  return rows;
}

function encodeScanPage(
  rows: Array<{ seq: number; key: Uint8Array; partial: Uint8Array }>,
  exhausted: boolean,
): Uint8Array {
  let size = 4;
  for (const row of rows) {
    size += 8 + 4 + row.key.byteLength + 4 + row.partial.byteLength;
  }
  size += 1;
  const out = new Uint8Array(size);
  const view = new DataView(out.buffer);
  let off = 0;
  view.setUint32(off, rows.length, true);
  off += 4;
  for (const row of rows) {
    view.setBigInt64(off, BigInt(row.seq), true);
    off += 8;
    view.setUint32(off, row.key.byteLength, true);
    off += 4;
    out.set(row.key, off);
    off += row.key.byteLength;
    view.setUint32(off, row.partial.byteLength, true);
    off += 4;
    out.set(row.partial, off);
    off += row.partial.byteLength;
  }
  out[off] = exhausted ? 1 : 0;
  return out;
}

export type SpillTelemetry = {
  begin: number;
  append: number;
  appendRows: number;
  appendBytes: number;
  scan: number;
  scanRows: number;
  end: number;
};

export function emptySpillTelemetry(): SpillTelemetry {
  return {
    begin: 0,
    append: 0,
    appendRows: 0,
    appendBytes: 0,
    scan: 0,
    scanRows: 0,
    end: 0,
  };
}

/** Wrap SQLite and record spill op counts for smoke tests and dispatch traces. */
export function trackedSpillSql(sql: SpillSql, telemetry: SpillTelemetry): SpillSql {
  return {
    transactionSync: sql.transactionSync
      ? (fn: () => void) => sql.transactionSync!(fn)
      : undefined,
    exec: (query: string, ...bindings: unknown[]) => {
      if (
        query.includes("CREATE TABLE IF NOT EXISTS wcol_spill_rows") ||
        query.includes("CREATE TABLE IF NOT EXISTS spill_")
      ) {
        telemetry.begin += 1;
      } else if (query.startsWith("INSERT INTO wcol_spill_rows") || query.startsWith("INSERT INTO spill_")) {
        telemetry.append += 1;
        telemetry.appendRows += Math.floor(bindings.length / 2);
        for (const binding of bindings) {
          if (binding instanceof Uint8Array) telemetry.appendBytes += binding.byteLength;
        }
      } else if (
        (query.includes("FROM wcol_spill_rows") || query.includes("FROM spill_")) &&
        query.includes("ORDER BY key_blob")
      ) {
        telemetry.scan += 1;
      } else if (
        query.startsWith("DELETE FROM wcol_spill_rows") ||
        query.startsWith("DROP TABLE IF EXISTS spill_")
      ) {
        telemetry.end += 1;
      }
      const out = sql.exec(query, ...bindings);
      if (
        (query.includes("FROM wcol_spill_rows") || query.includes("FROM spill_")) &&
        query.includes("ORDER BY key_blob")
      ) {
        if (out && typeof (out as Iterable<unknown>)[Symbol.iterator] === "function") {
          const items = [...(out as Iterable<unknown>)];
          telemetry.scanRows += items.length;
          return items;
        }
      }
      return out;
    },
  };
}

/** Drop leftover opcode spill tables and clear the byte-port table. */
export function cleanupSpillTables(sql: SpillSql): number {
  type NameRow = { name: string };
  const tables = sql.exec(
    `SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'spill_%'`,
  ) as Iterable<NameRow> | null;
  let dropped = 0;
  if (tables) {
    for (const row of tables) {
      sql.exec(`DROP TABLE IF EXISTS ${row.name}`);
      dropped += 1;
    }
  }
  try {
    sql.exec(`DELETE FROM wcol_spill_rows`);
  } catch {
    // The byte-port table is created on the first spill open.
  }
  return dropped;
}

export function insertSpillRows(
  sql: SpillSql,
  table: string,
  rows: Array<{ key: Uint8Array; partial: Uint8Array }>,
  chunkRows: number = SPILL_INSERT_CHUNK_ROWS,
): void {
  const size = Math.max(1, chunkRows);
  for (let offset = 0; offset < rows.length; offset += size) {
    const chunk = rows.slice(offset, offset + size);
    const placeholders = chunk.map(() => "(?, ?)").join(", ");
    const bindings: unknown[] = [];
    for (const row of chunk) bindings.push(row.key, row.partial);
    sql.exec(`INSERT INTO ${table} (key_blob, partial) VALUES ${placeholders}`, ...bindings);
  }
}

function withSpillTx(sql: SpillSql, fn: () => void): void {
  if (sql.transactionSync) sql.transactionSync(fn);
  else fn();
}

/** Runs one spill op synchronously inside the DO (finish the SQL cursor before returning). */
export function runSpillHostOp(sql: SpillSql, req: SpillHostRequest): Uint8Array {
  const table = tableName(req.session);
  if (req.op === SPILL_OP_BEGIN) {
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${table} (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        key_blob BLOB NOT NULL,
        partial BLOB NOT NULL
      )`,
    );
    sql.exec(`CREATE INDEX IF NOT EXISTS ${table}_key ON ${table} (key_blob, seq)`);
    return new Uint8Array(0);
  }
  if (req.op === SPILL_OP_APPEND) {
    withSpillTx(sql, () =>
      insertSpillRows(sql, table, decodeAppendBatch(req.appendBytes), SPILL_INSERT_CHUNK_ROWS),
    );
    return new Uint8Array(0);
  }
  if (req.op === SPILL_OP_SCAN) {
    type Row = { seq: number; key_blob: ArrayBuffer; partial: ArrayBuffer };
    const cursor = !req.resumeStarted
      ? (sql.exec(
          `SELECT seq, key_blob, partial FROM ${table} ORDER BY key_blob, seq LIMIT ?1`,
          req.maxRows,
        ) as Iterable<Row>)
      : (sql.exec(
          `SELECT seq, key_blob, partial FROM ${table}
           WHERE (key_blob, seq) > (?1, ?2)
           ORDER BY key_blob, seq
           LIMIT ?3`,
          req.resumeKey,
          Number(req.resumeSeq),
          req.maxRows,
        ) as Iterable<Row>);
    const raw: Array<{ seq: number; key: Uint8Array; partial: Uint8Array }> = [];
    for (const row of cursor) {
      raw.push({
        seq: row.seq,
        key: new Uint8Array(row.key_blob),
        partial: new Uint8Array(row.partial),
      });
      if (raw.length >= req.maxRows) break;
    }
    const sqlCount = raw.length;
    let used = 0;
    const collected: typeof raw = [];
    for (const row of raw) {
      const rowBytes = row.key.byteLength + row.partial.byteLength;
      if (collected.length > 0 && used + rowBytes > req.maxBytes) {
        break;
      }
      collected.push(row);
      used += rowBytes;
    }
    const exhausted = collected.length === 0 || sqlCount < req.maxRows;
    return encodeScanPage(collected, exhausted);
  }
  if (req.op === SPILL_OP_END) {
    sql.exec(`DROP TABLE IF EXISTS ${table}`);
    return new Uint8Array(0);
  }
  throw new Error(`unknown spill op ${req.op}`);
}
