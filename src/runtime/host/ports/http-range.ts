import type { FileHandle } from "node:fs/promises";

function nodeFsPromisesSpecifier(): string {
  return ["node", "fs/promises"].join(":");
}

function nodeHttpSpecifier(protocol: string): string {
  return ["node", protocol === "https:" ? "https" : "http"].join(":");
}

function isNode(): boolean {
  return typeof process !== "undefined" && typeof process.versions?.node === "string";
}

type NodeIncoming = {
  statusCode?: number;
  headers: Record<string, string | string[] | undefined>;
  destroy(): void;
  on(event: "data", listener: (chunk: Buffer) => void): void;
  on(event: "end", listener: () => void): void;
  on(event: "error", listener: (error: Error) => void): void;
};

type NodeHttpModule = {
  Agent: new (options: { keepAlive: boolean; maxSockets: number }) => unknown;
  request(options: Record<string, unknown>, callback: (response: NodeIncoming) => void): {
    reusedSocket: boolean;
    on(event: "error" | "socket", listener: (error: Error) => void): void;
    end(): void;
  };
};

const agents = new Map<string, unknown>();

async function keepAliveAgent(protocol: string): Promise<unknown> {
  const key = protocol === "https:" ? "https" : "http";
  let agent = agents.get(key);
  if (!agent) {
    const mod = (await import(nodeHttpSpecifier(protocol))) as NodeHttpModule;
    agent = new mod.Agent({ keepAlive: true, maxSockets: 64 });
    agents.set(key, agent);
  }
  return agent;
}

async function nodeRequest(
  url: string,
  method: string,
  headers: Record<string, string>,
  bufferStatuses: number[],
): Promise<{
  status: number;
  headers: Record<string, string | string[] | undefined>;
  body: Uint8Array;
  headerMs: number;
  bodyMs: number;
}> {
  const target = new URL(url);
  const mod = (await import(nodeHttpSpecifier(target.protocol))) as NodeHttpModule;
  const agent = await keepAliveAgent(target.protocol);
  const started = performance.now();
  return new Promise((resolve, reject) => {
    const request = mod.request(
      {
        protocol: target.protocol,
        hostname: target.hostname,
        port: target.port || undefined,
        path: `${target.pathname}${target.search}`,
        method,
        agent,
        headers: { "Accept-Encoding": "identity", ...headers },
      },
      (response) => {
        const headerMs = performance.now() - started;
        void headerMs;
        const status = response.statusCode ?? 0;
        if (!bufferStatuses.includes(status)) {
          response.on("error", () => {});
          response.destroy();
          resolve({ status, headers: response.headers, body: new Uint8Array(), headerMs, bodyMs: 0 });
          return;
        }
        const bodyStarted = performance.now();
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(chunk));
        response.on("end", () => {
          const bodyMs = performance.now() - bodyStarted;
          void bodyMs;
          resolve({ status, headers: response.headers, body: new Uint8Array(Buffer.concat(chunks)), headerMs, bodyMs });
        });
        response.on("error", reject);
      },
    );
    request.on("socket", () => {
      void request.reusedSocket;
    });
    request.on("error", reject);
    request.end();
  });
}

function retryDelay(headers: Record<string, string | string[] | undefined>, attempt: number): number {
  const after = Number(headerValue(headers, "retry-after"));
  if (Number.isFinite(after) && after >= 0) return Math.min(after * 1000, 2000);
  return Math.min(50 * 2 ** attempt, 800);
}

function pause(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function acceptRange(
  offset: number,
  length: number,
  headers: Record<string, string | string[] | undefined>,
  body: Uint8Array,
  remember: (headers: Record<string, string | string[] | undefined>) => void,
): Uint8Array {
  const match = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(headerValue(headers, "content-range") ?? "");
  if (!match) throw new Error("content-range is missing");
  const start = Number(match[1]);
  const end = Number(match[2]);
  if (start !== offset || end !== offset + length - 1 || body.byteLength !== length) {
    throw new Error(`content-range ${match[0]} does not match bytes ${offset}-${offset + length - 1}`);
  }
  remember(headers);
  return body;
}

function headerValue(headers: Record<string, string | string[] | undefined>, name: string): string | null {
  const value = headers[name];
  if (Array.isArray(value)) return value[0] ?? null;
  return value ?? null;
}

/** Browser demo / local serve: fetch whole file once when under this size (bytes). */
const HTTP_EAGER_MAX_BYTES = 256 * 1024 * 1024;
const RANGE_TIMEOUT_MS = 20_000;

export type RangeTiming = {
  headerSumMs: number;
  headerMaxMs: number;
  bodySumMs: number;
  ranges: number;
};

function rangeSignal(): AbortSignal {
  return AbortSignal.timeout(RANGE_TIMEOUT_MS);
}

async function stopBody(body: ReadableStream<Uint8Array> | null): Promise<void> {
  if (!body) return;
  await Promise.race([
    body.cancel().then(() => undefined, () => undefined),
    new Promise<void>((resolve) => setTimeout(resolve, 500)),
  ]);
}

export abstract class ByteSource {
  abstract read(offset: number, length: number): Promise<Uint8Array>;
}

export class LocalFileSource extends ByteSource {
  constructor(private file: File) {
    super();
  }

  async read(offset: number, length: number): Promise<Uint8Array> {
    const slice = this.file.slice(offset, offset + length);
    const buffer = await slice.arrayBuffer();
    return new Uint8Array(buffer);
  }
}

export class HttpRangeSource extends ByteSource {
  private url: string;
  private headers: Record<string, string>;
  private fullBuffer: Uint8Array | null;
  private initPromise: Promise<void> | null;
  private eagerDisabled: boolean;
  private rangeOnly: boolean;
  private tailCache: { offset: number; bytes: Uint8Array } | null;
  private etag: string | null;
  private timing: RangeTiming = { headerSumMs: 0, headerMaxMs: 0, bodySumMs: 0, ranges: 0 };

  private nodeTransport: boolean;

  constructor(
    url: string,
    options: { headers?: Record<string, string>; rangeOnly?: boolean; transport?: "node" | "fetch"; etag?: string | null } = {},
  ) {
    super();
    this.url = url;
    this.headers = options.headers ?? {};
    this.fullBuffer = null;
    this.initPromise = null;
    this.rangeOnly = options.rangeOnly === true;
    this.nodeTransport = options.transport === "fetch" ? false : isNode();
    this.eagerDisabled = this.rangeOnly;
    this.tailCache = null;
    this.etag = options.etag ?? null;
  }

  validator(): string | null {
    return this.etag;
  }

  rangeTimings(): RangeTiming {
    return {
      headerSumMs: Math.round(this.timing.headerSumMs * 10) / 10,
      headerMaxMs: Math.round(this.timing.headerMaxMs * 10) / 10,
      bodySumMs: Math.round(this.timing.bodySumMs * 10) / 10,
      ranges: this.timing.ranges,
    };
  }

  private noteTiming(headerMs: number, bodyMs: number): void {
    this.timing.ranges += 1;
    this.timing.headerSumMs += headerMs;
    this.timing.bodySumMs += bodyMs;
    if (headerMs > this.timing.headerMaxMs) this.timing.headerMaxMs = headerMs;
  }

  async contentLength(): Promise<number> {
    if (this.nodeTransport) {
      let lastStatus = 0;
      for (let attempt = 0; attempt < 4; attempt += 1) {
        const response = await nodeRequest(this.url, "GET", { ...this.headers, Range: "bytes=-8" }, [206, 429]);
        lastStatus = response.status;
        this.noteTiming(response.headerMs, response.bodyMs);
        if (response.status === 206) {
          const match = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(headerValue(response.headers, "content-range") ?? "");
          if (!match) throw new Error("content-range is missing");
          const size = Number(match[3]);
          if (!Number.isSafeInteger(size) || size < 0) {
            throw new Error("content-length is not a safe file size");
          }
          this.noteValidator(response.headers);
          this.tailCache = { offset: Number(match[1]), bytes: response.body };
          return size;
        }
        await pause(retryDelay(response.headers, attempt));
      }
      throw new Error(`range request was ignored (status ${lastStatus})`);
    }
    const started = performance.now();
    let head: Response;
    try {
      head = await fetch(this.url, { method: "HEAD", headers: this.headers, signal: rangeSignal() });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new Error(`HEAD failed: ${message}`);
    }
    this.noteTiming(performance.now() - started, 0);
    if (!head.ok) throw new Error(`HEAD failed with status ${head.status}`);
    const etag = head.headers.get("etag");
    if (etag) this.noteValidator({ etag });
    const len = head.headers.get("content-length");
    if (len == null) throw new Error("content-length is missing");
    const size = Number(len);
    if (!Number.isSafeInteger(size) || size < 0) {
      throw new Error("content-length is not a safe file size");
    }
    return size;
  }

  async read(offset: number, length: number): Promise<Uint8Array> {
    if (this.tailCache && offset === this.tailCache.offset && length === this.tailCache.bytes.length) {
      return this.tailCache.bytes;
    }
    await this.ensureBuffer();
    if (this.fullBuffer) {
      return this.fullBuffer.subarray(offset, offset + length);
    }

    const rangeHeader = `bytes=${offset}-${offset + length - 1}`;
    if (this.nodeTransport) {
      const response = await this.requestRange(rangeHeader, offset, length);
      if (response.status === 206) {
        return acceptRange(offset, length, response.headers, response.body, (headers) => this.noteValidator(headers));
      }
      if (this.rangeOnly) throw new Error(`range request was ignored (status ${response.status})`);
      if (response.status === 200) {
        this.fullBuffer = response.body;
        return response.body.subarray(offset, offset + length);
      }
      throw new Error(`Range read failed with status ${response.status}`);
    }
    let lastStatus = 0;
    let lastLength = "";
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const started = performance.now();
      let response: Response;
      try {
        response = await fetch(this.url, {
          cache: "no-store",
          headers: { "Accept-Encoding": "identity", ...this.headers, Range: rangeHeader },
          signal: rangeSignal(),
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        throw new Error(`range ${rangeHeader} failed: ${message}`);
      }
      const headerMs = performance.now() - started;
      lastStatus = response.status;
      lastLength = response.headers.get("content-length") ?? "";
      if (response.status === 206) {
        const buffer = new Uint8Array(await response.arrayBuffer());
        const bodyMs = performance.now() - started - headerMs;
        this.noteTiming(headerMs, bodyMs);
        void headerMs;
        void bodyMs;
        const headers: Record<string, string | string[] | undefined> = {};
        response.headers.forEach((value, key) => {
          headers[key] = value;
        });
        return acceptRange(offset, length, headers, buffer, (next) => this.noteValidator(next));
      }
      await stopBody(response.body);
      if (response.status !== 429 && response.status < 500 && response.status !== 200) break;
      const retryAfter = response.headers.get("retry-after");
      await pause(retryDelay(retryAfter ? { "retry-after": retryAfter } : {}, attempt));
    }
    if (this.rangeOnly || lastStatus !== 200) {
      throw new Error(`range request was ignored (status ${lastStatus}, content-length ${lastLength || "missing"})`);
    }
    throw new Error(`Range read failed with status ${lastStatus}`);
  }

  private noteValidator(headers: Record<string, string | string[] | undefined>): void {
    const etag = headerValue(headers, "etag");
    if (!etag) return;
    if (this.etag && this.etag !== etag) throw new Error("parquet file changed during the scan");
    this.etag = etag;
  }

  private async requestRange(
    rangeHeader: string,
    offset: number,
    length: number,
  ): Promise<{ status: number; headers: Record<string, string | string[] | undefined>; body: Uint8Array }> {
    let last: Error | null = null;
    for (let attempt = 0; attempt < 4; attempt += 1) {
      try {
        const response = await nodeRequest(
          this.url,
          "GET",
          { ...this.headers, Range: rangeHeader },
          this.rangeOnly ? [206, 429] : [200, 206, 429],
        );
        this.noteTiming(response.headerMs, response.bodyMs);
        if (response.status === 429 || response.status >= 500) {
          last = new Error(`range request failed with status ${response.status}`);
          await pause(retryDelay(response.headers, attempt));
          continue;
        }
        return response;
      } catch (err) {
        last = err instanceof Error ? err : new Error(String(err));
        if (last.message.startsWith("content-range") || last.message.startsWith("parquet file changed")) throw last;
        await pause(retryDelay({}, attempt));
      }
    }
    throw last ?? new Error(`range read failed at ${offset}+${length}`);
  }

  /** Prefer one full download for demo-sized files; avoids thousands of Range requests per query. */
  private async ensureBuffer(): Promise<void> {
    if (this.fullBuffer || this.eagerDisabled) return;
    if (!this.initPromise) {
      this.initPromise = this.tryEagerFetch();
    }
    await this.initPromise;
  }

  private async tryEagerFetch(): Promise<void> {
    let size: number | null = null;
    try {
      const head = await fetch(this.url, { method: "HEAD", headers: this.headers });
      if (head.ok) {
        const len = head.headers.get("content-length");
        if (len) size = Number(len);
      }
    } catch {
      // HEAD often unsupported; fall through to GET
    }

    if (size != null && size > HTTP_EAGER_MAX_BYTES) {
      this.eagerDisabled = true;
      return;
    }

    try {
      const response = await fetch(this.url, { headers: this.headers });
      if (!response.ok) {
        this.eagerDisabled = true;
        return;
      }
      const buffer = new Uint8Array(await response.arrayBuffer());
      if (size != null && buffer.byteLength !== size) {
        // size mismatch — keep range path
        this.eagerDisabled = true;
        return;
      }
      if (buffer.byteLength > HTTP_EAGER_MAX_BYTES) {
        this.eagerDisabled = true;
        return;
      }
      this.fullBuffer = buffer;
    } catch {
      this.eagerDisabled = true;
    }
  }
}

export class MemoryByteSource extends ByteSource {
  constructor(private bytes: Uint8Array) {
    super();
  }

  async read(offset: number, length: number): Promise<Uint8Array> {
    const start = Math.max(0, offset);
    const end = Math.min(this.bytes.byteLength, start + length);
    if (end <= start) return new Uint8Array(0);
    return this.bytes.slice(start, end);
  }
}

export class NodeFileSource extends ByteSource {
  private filePath: string;
  private fileHandle: FileHandle | null;
  private cacheMaxBytes: number;
  private cacheUsedBytes: number;
  private cacheMap: Map<string, Uint8Array>;
  private cacheOrder: string[];

  constructor(filePath: string, options?: { cacheMaxBytes?: number }) {
    super();
    this.filePath = filePath;
    this.fileHandle = null;
    this.cacheMaxBytes = options?.cacheMaxBytes ?? resolveNodeCacheBytes();
    this.cacheUsedBytes = 0;
    this.cacheMap = new Map();
    this.cacheOrder = [];
  }

  private async ensureOpen(): Promise<void> {
    if (!this.fileHandle) {
      const fs = await import(nodeFsPromisesSpecifier());
      this.fileHandle = await fs.open(this.filePath, "r");
    }
  }

  async read(offset: number, length: number): Promise<Uint8Array> {
    const cacheKey = `${offset}:${length}`;
    const cached = this.cacheMap.get(cacheKey);
    if (cached) {
      return cached;
    }

    await this.ensureOpen();
    const buffer = new Uint8Array(length);
    const { bytesRead } = await this.fileHandle!.read(buffer, 0, length, offset);
    const out = buffer.slice(0, bytesRead);
    this.addToCache(cacheKey, out);
    return out;
  }

  async close(): Promise<void> {
    if (this.fileHandle) {
      await this.fileHandle.close();
      this.fileHandle = null;
    }
  }

  private addToCache(key: string, bytes: Uint8Array): void {
    if (this.cacheMaxBytes <= 0 || bytes.byteLength > this.cacheMaxBytes) {
      return;
    }
    if (this.cacheMap.has(key)) {
      return;
    }
    while (this.cacheUsedBytes + bytes.byteLength > this.cacheMaxBytes) {
      const evictKey = this.cacheOrder.shift();
      if (!evictKey) {
        break;
      }
      const evicted = this.cacheMap.get(evictKey);
      if (evicted) {
        this.cacheUsedBytes -= evicted.byteLength;
      }
      this.cacheMap.delete(evictKey);
    }
    this.cacheMap.set(key, bytes);
    this.cacheOrder.push(key);
    this.cacheUsedBytes += bytes.byteLength;
  }
}

function resolveNodeCacheBytes(): number {
  const defaultMb = 1024;
  try {
    const raw = typeof process !== "undefined" ? process.env?.WCOL_TS_PAGE_CACHE_MB : undefined;
    const parsed = raw ? Number.parseInt(raw, 10) : defaultMb;
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return 0;
    }
    return parsed * 1024 * 1024;
  } catch {
    return defaultMb * 1024 * 1024;
  }
}
