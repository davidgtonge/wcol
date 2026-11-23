/**
 * The parts of the Workers Durable Object API this code uses, for the root type check.
 * The full runtime types (`@cloudflare/workers-types`) redefine `fetch`, `Response` and other
 * DOM and Node globals that tests and scripts also use, so they cannot share one program.
 * Like `workers-stub.ts`, this describes the runtime; it is never bundled.
 */

interface DurableObjectId {
  toString(): string;
}

interface DurableObjectTransaction {
  get(key: string): Promise<unknown>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<boolean>;
  setAlarm(scheduledTime: number | Date): Promise<void>;
}

interface DurableObjectStorage {
  get(key: string): Promise<unknown>;
  put(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<boolean>;
  deleteAll(): Promise<void>;
  setAlarm(scheduledTime: number | Date, options?: { allowUnconfirmed?: boolean }): Promise<void>;
  deleteAlarm(): Promise<void>;
  getAlarm(): Promise<number | null>;
  list(options?: { prefix?: string }): Promise<Map<string, unknown>>;
  transaction<T>(run: (txn: DurableObjectTransaction) => Promise<T>): Promise<T>;
  readonly sql: { exec(query: string, ...bindings: unknown[]): Iterable<Record<string, unknown>> };
}

interface DurableObjectState {
  readonly id: DurableObjectId;
  readonly storage: DurableObjectStorage;
  waitUntil(promise: Promise<unknown>): void;
  /** Reset the object: in-flight requests fail and the next one gets a fresh instance. */
  abort(reason?: string): void;
  /** Present when the class has a `containers` entry. */
  readonly container?: Container;
}

/** A Durable Object's own container (`durable_object` scheduling policy). */
interface Container {
  readonly running: boolean;
  readonly images: Record<string, string | undefined>;
  start(options: {
    image: string;
    instance?: "lite" | "basic" | "standard-1" | "standard-2" | "standard-3" | "standard-4";
    enableInternet: boolean;
    env?: Record<string, string>;
  }): void;
  monitor(): Promise<void>;
  destroy(error?: unknown): Promise<void>;
  setInactivityTimeout(durationMs: number): Promise<void>;
  getTcpPort(port: number): { fetch(url: string, init?: RequestInit): Promise<Response> };
}

/** The console page, built by `scripts/build-workers.ts` and bundled as text. */
declare module "*.html" {
  const text: string;
  export default text;
}

/** A Worker bundle imports its Wasm as a compiled module (wrangler `CompiledWasm` rule). */
declare module "*.wasm" {
  const compiled: WebAssembly.Module;
  export default compiled;
}

interface DurableObjectNamespace {
  idFromName(name: string): DurableObjectId;
  get(id: DurableObjectId, options?: { locationHint?: string }): unknown;
}

/** Workers' constant-time comparison (not in the DOM lib). */
interface SubtleCrypto {
  timingSafeEqual(a: ArrayBufferView | ArrayBuffer, b: ArrayBufferView | ArrayBuffer): boolean;
}
