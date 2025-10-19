import type { FileHandle } from "node:fs/promises";

function nodeFsPromisesSpecifier(): string {
  return ["node", "fs/promises"].join(":");
}

/** Browser demo / local serve: fetch whole file once when under this size (bytes). */
const HTTP_EAGER_MAX_BYTES = 256 * 1024 * 1024;

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

  constructor(url: string, options: { headers?: Record<string, string> } = {}) {
    super();
    this.url = url;
    this.headers = options.headers ?? {};
    this.fullBuffer = null;
    this.initPromise = null;
    this.eagerDisabled = false;
  }

  async read(offset: number, length: number): Promise<Uint8Array> {
    await this.ensureBuffer();
    if (this.fullBuffer) {
      return this.fullBuffer.subarray(offset, offset + length);
    }

    const rangeHeader = `bytes=${offset}-${offset + length - 1}`;
    const response = await fetch(this.url, {
      headers: { ...this.headers, Range: rangeHeader },
    });

    if (response.status === 206) {
      const buffer = await response.arrayBuffer();
      return new Uint8Array(buffer);
    }

    if (response.status === 200) {
      const buffer = new Uint8Array(await response.arrayBuffer());
      this.fullBuffer = buffer;
      return buffer.subarray(offset, offset + length);
    }

    throw new Error(`Range read failed with status ${response.status}`);
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

export class NodeFileSource extends ByteSource {
  private filePath: string;
  private fileHandle: FileHandle | null;
  private cacheMaxBytes: number;
  private cacheUsedBytes: number;
  private cacheMap: Map<string, Uint8Array>;
  private cacheOrder: string[];

  constructor(filePath: string) {
    super();
    this.filePath = filePath;
    this.fileHandle = null;
    this.cacheMaxBytes = resolveNodeCacheBytes();
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
