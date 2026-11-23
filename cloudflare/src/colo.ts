/**
 * The Cloudflare data centre this isolate runs in, from the trace endpoint, asked once per
 * isolate. Tasks report it so the console can show where a job ran.
 */
let colo: Promise<string> | undefined;

export function coloOnce(): Promise<string> {
  colo ??= fetch("https://www.cloudflare.com/cdn-cgi/trace")
    .then((response) => response.text())
    .then((text) => /^colo=(\w+)$/m.exec(text)?.[1] ?? "unknown")
    .catch(() => "unknown");
  return colo;
}

let isolate: string | undefined;

/**
 * A random id per isolate, made on first use (Workers forbid random values at global
 * scope). Durable Objects of one class can share an isolate, one thread between them; equal
 * ids in the logs show which objects took turns on the same CPU.
 */
export function isolateId(): string {
  isolate ??= crypto.randomUUID().slice(0, 8);
  return isolate;
}
