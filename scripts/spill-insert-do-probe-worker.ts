import { DurableObject } from "cloudflare:workers";

export class SpillInsertProbe extends DurableObject {
  insertChunk(rows: number): { ok: boolean; rows: number; bindings: number; error?: string } {
    const sql = this.ctx.storage.sql;
    sql.exec(`CREATE TABLE IF NOT EXISTS spill_probe (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      key_blob BLOB NOT NULL,
      partial BLOB NOT NULL
    )`);
    const placeholders = Array.from({ length: rows }, () => "(?, ?)").join(", ");
    const bindings: Uint8Array[] = [];
    for (let i = 0; i < rows; i++) {
      bindings.push(Uint8Array.of(i & 255, (i >> 8) & 255), Uint8Array.of(1, 0, 0, 0));
    }
    try {
      sql.exec(`INSERT INTO spill_probe (key_blob, partial) VALUES ${placeholders}`, ...bindings);
      return { ok: true, rows, bindings: bindings.length };
    } catch (error) {
      return {
        ok: false,
        rows,
        bindings: bindings.length,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  timedInsert(total: number, chunk: number): { ok: boolean; chunk: number; total: number; ms: number; error?: string } {
    const sql = this.ctx.storage.sql;
    sql.exec(`DROP TABLE IF EXISTS spill_probe`);
    sql.exec(`CREATE TABLE spill_probe (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      key_blob BLOB NOT NULL,
      partial BLOB NOT NULL
    )`);
    const started = performance.now();
    try {
      this.ctx.storage.transactionSync(() => {
        for (let offset = 0; offset < total; offset += chunk) {
          const n = Math.min(chunk, total - offset);
          const placeholders = Array.from({ length: n }, () => "(?, ?)").join(", ");
          const bindings: Uint8Array[] = [];
          for (let i = 0; i < n; i++) {
            const id = offset + i;
            bindings.push(Uint8Array.of(id & 255, (id >> 8) & 255), Uint8Array.of(1));
          }
          sql.exec(`INSERT INTO spill_probe (key_blob, partial) VALUES ${placeholders}`, ...bindings);
        }
      });
      return { ok: true, chunk, total, ms: Math.round((performance.now() - started) * 10) / 10 };
    } catch (error) {
      return {
        ok: false,
        chunk,
        total,
        ms: Math.round((performance.now() - started) * 10) / 10,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }
}

export default {
  async fetch(request: Request, env: { PROBE: DurableObjectNamespace }): Promise<Response> {
    const id = env.PROBE.idFromName("spill-insert-probe");
    const stub = env.PROBE.get(id) as DurableObjectStub & SpillInsertProbe;
    const url = new URL(request.url);
    if (url.pathname === "/probe") {
      const rows = Number(url.searchParams.get("rows") ?? "50");
      return Response.json(await stub.insertChunk(rows));
    }
    if (url.pathname === "/timed") {
      const chunk = Number(url.searchParams.get("chunk") ?? "50");
      const total = Number(url.searchParams.get("total") ?? "5000");
      return Response.json(await stub.timedInsert(total, chunk));
    }
    return new Response("not found", { status: 404 });
  },
};
