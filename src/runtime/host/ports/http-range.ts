/**
 * One object behind an http(s) URL, read by byte ranges. Under Node the request goes through
 * a keep-alive `node:http(s)` agent; elsewhere through `fetch`. Both share one retry policy.
 */

type Headers = Record<string, string | string[] | undefined>;

/** A 206 carries the body; any other status carries none. */
type RangeResponse = { status: number; headers: Headers; body: Uint8Array };

const RANGE_TIMEOUT_MS = 20_000;
const MAX_ATTEMPTS = 4;

function isNode(): boolean {
  return typeof process !== "undefined" && typeof process.versions?.node === "string";
}

function nodeHttpSpecifier(protocol: string): string {
  return ["node", protocol === "https:" ? "https" : "http"].join(":");
}

type NodeIncoming = {
  statusCode?: number;
  headers: Headers;
  destroy(): void;
  on(event: "data", listener: (chunk: Uint8Array) => void): void;
  on(event: "end", listener: () => void): void;
  on(event: "error", listener: (error: Error) => void): void;
};

type NodeHttpModule = {
  Agent: new (options: { keepAlive: boolean; maxSockets: number }) => unknown;
  request(options: Record<string, unknown>, callback: (response: NodeIncoming) => void): {
    on(event: "error", listener: (error: Error) => void): void;
    end(): void;
  };
};

const agents = new Map<string, Promise<{ mod: NodeHttpModule; agent: unknown }>>();

function keepAliveAgent(protocol: string): Promise<{ mod: NodeHttpModule; agent: unknown }> {
  const specifier = nodeHttpSpecifier(protocol);
  let entry = agents.get(specifier);
  if (!entry) {
    entry = (import(specifier) as Promise<NodeHttpModule>).then((mod) => ({
      mod,
      agent: new mod.Agent({ keepAlive: true, maxSockets: 64 }),
    }));
    agents.set(specifier, entry);
  }
  return entry;
}

function joinChunks(chunks: readonly Uint8Array[]): Uint8Array {
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0));
  let at = 0;
  for (const chunk of chunks) {
    out.set(chunk, at);
    at += chunk.byteLength;
  }
  return out;
}

/** One ranged GET over `node:http(s)`. Network errors and the timeout reject. */
async function nodeGet(url: string, headers: Record<string, string>): Promise<RangeResponse> {
  const target = new URL(url);
  const { mod, agent } = await keepAliveAgent(target.protocol);
  return new Promise((resolve, reject) => {
    const request = mod.request(
      {
        protocol: target.protocol,
        hostname: target.hostname,
        port: target.port || undefined,
        path: `${target.pathname}${target.search}`,
        method: "GET",
        agent,
        headers,
        signal: AbortSignal.timeout(RANGE_TIMEOUT_MS),
      },
      (response) => {
        const status = response.statusCode ?? 0;
        if (status !== 206) {
          response.on("error", () => undefined);
          response.destroy();
          resolve({ status, headers: response.headers, body: new Uint8Array() });
          return;
        }
        const chunks: Uint8Array[] = [];
        response.on("data", (chunk) => chunks.push(chunk));
        response.on("end", () => resolve({ status, headers: response.headers, body: joinChunks(chunks) }));
        response.on("error", reject);
      },
    );
    request.on("error", reject);
    request.end();
  });
}

/** One ranged GET over `fetch`. Network errors and the timeout reject. */
async function fetchGet(url: string, headers: Record<string, string>): Promise<RangeResponse> {
  const response = await fetch(url, { cache: "no-store", headers, signal: AbortSignal.timeout(RANGE_TIMEOUT_MS) });
  const record: Headers = {};
  response.headers.forEach((value, key) => {
    record[key] = value;
  });
  if (response.status !== 206) {
    await stopBody(response.body);
    return { status: response.status, headers: record, body: new Uint8Array() };
  }
  return { status: response.status, headers: record, body: new Uint8Array(await response.arrayBuffer()) };
}

async function stopBody(body: ReadableStream<Uint8Array> | null): Promise<void> {
  if (!body) return;
  await Promise.race([
    body.cancel().then(() => undefined, () => undefined),
    new Promise<void>((resolve) => setTimeout(resolve, 500)),
  ]);
}

function headerValue(headers: Headers, name: string): string | null {
  const value = headers[name];
  if (Array.isArray(value)) return value[0] ?? null;
  return value ?? null;
}

function retryDelay(headers: Headers, attempt: number): number {
  const after = Number(headerValue(headers, "retry-after") ?? Number.NaN);
  if (Number.isFinite(after) && after >= 0) return Math.min(after * 1000, 2000);
  return Math.min(50 * 2 ** attempt, 800);
}

function pause(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Retry a request on a network error, a timeout, 429 or 5xx, with backoff (`Retry-After`
 * when the server sends one). Any other status is the caller's answer.
 */
async function withRetry(what: string, send: () => Promise<RangeResponse>): Promise<RangeResponse> {
  let lastError: unknown;
  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
    let headers: Headers = {};
    try {
      const response = await send();
      if (response.status !== 429 && response.status < 500) return response;
      headers = response.headers;
      lastError = new Error(`${what} failed with status ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    if (attempt + 1 < MAX_ATTEMPTS) await pause(retryDelay(headers, attempt));
  }
  const reason = lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(`${what} failed after ${MAX_ATTEMPTS} attempts: ${reason}`, { cause: lastError });
}

/** A non-206 answer to a range request: 200 means the server ignored the range. */
function rangeRefused(what: string, status: number): Error {
  return status === 200
    ? new Error(`${what}: range request was ignored (status 200)`)
    : new Error(`${what} failed with status ${status}`);
}

const CONTENT_RANGE = /^bytes (\d+)-(\d+)\/(\d+)$/;

function contentRange(headers: Headers): { start: number; end: number; size: number } {
  const match = CONTENT_RANGE.exec(headerValue(headers, "content-range") ?? "");
  if (!match) throw new Error("content-range is missing");
  const size = Number(match[3]);
  if (!Number.isSafeInteger(size)) throw new Error("content-range size is not a safe file size");
  return { start: Number(match[1]), end: Number(match[2]), size };
}

export class HttpRangeSource {
  private readonly url: string;
  private readonly headers: Record<string, string>;
  private readonly get: (url: string, headers: Record<string, string>) => Promise<RangeResponse>;
  /** The 8-byte tail `contentLength` read, so the footer length costs no second request. */
  private tail: { offset: number; bytes: Uint8Array } | null = null;
  private etag: string | null;

  /** `rangeOnly` is the only mode; the option is accepted until callers stop passing it. */
  constructor(url: string, options: { headers?: Record<string, string>; rangeOnly?: true; etag?: string | null } = {}) {
    this.url = url;
    this.headers = options.headers ?? {};
    this.get = isNode() ? nodeGet : fetchGet;
    this.etag = options.etag ?? null;
  }

  validator(): string | null {
    return this.etag;
  }

  /** Object size, from a suffix range of its last 8 bytes (kept for the footer read). */
  async contentLength(): Promise<number> {
    const what = `size of ${this.url}`;
    const response = await this.request("bytes=-8", what);
    if (response.status !== 206) throw rangeRefused(what, response.status);
    const { start, end, size } = contentRange(response.headers);
    if (end - start + 1 !== response.body.byteLength) throw new Error(`${what}: content-range does not match the body`);
    this.noteValidator(response.headers);
    this.tail = { offset: start, bytes: response.body };
    return size;
  }

  async read(offset: number, length: number): Promise<Uint8Array> {
    if (this.tail && offset === this.tail.offset && length === this.tail.bytes.byteLength) {
      return this.tail.bytes;
    }
    const range = `bytes=${offset}-${offset + length - 1}`;
    const what = `range ${range} of ${this.url}`;
    const response = await this.request(range, what);
    if (response.status !== 206) throw rangeRefused(what, response.status);
    const { start, end } = contentRange(response.headers);
    if (start !== offset || end !== offset + length - 1 || response.body.byteLength !== length) {
      throw new Error(`${what}: got content-range bytes ${start}-${end} and ${response.body.byteLength} bytes`);
    }
    this.noteValidator(response.headers);
    return response.body;
  }

  private request(range: string, what: string): Promise<RangeResponse> {
    const headers = { "Accept-Encoding": "identity", ...this.headers, Range: range };
    return withRetry(what, () => this.get(this.url, headers));
  }

  private noteValidator(headers: Headers): void {
    const etag = headerValue(headers, "etag");
    if (!etag) return;
    if (this.etag && this.etag !== etag) throw new Error("parquet file changed during the scan");
    this.etag = etag;
  }
}
