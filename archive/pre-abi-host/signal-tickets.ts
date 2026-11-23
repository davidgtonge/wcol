/** Short-lived, per-task capability for the WebSocket upgrade. Only a trusted scheduler signs. */
export interface WorkerTicket {
  queryId: string;
  taskId: string;
  /** Monotonically increasing fencing generation assigned by the coordinator. */
  epoch: number;
  /** Expiration in Unix seconds; checked at connection time. */
  exp: number;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const ID = /^[\w.:-]{1,128}$/;

function assertTicket(value: unknown): asserts value is WorkerTicket {
  if (typeof value !== "object" || value === null) throw new Error("Invalid ticket");
  const t = value as Record<string, unknown>;
  if (typeof t.queryId !== "string" || !ID.test(t.queryId) ||
      typeof t.taskId !== "string" || !ID.test(t.taskId) ||
      typeof t.epoch !== "number" || !Number.isSafeInteger(t.epoch) || t.epoch < 0 ||
      typeof t.exp !== "number" || !Number.isSafeInteger(t.exp)) {
    throw new Error("Invalid ticket");
  }
}

function base64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return globalThis.btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function fromBase64Url(s: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/.test(s)) throw new Error("Invalid base64url");
  const padded = s.replace(/-/g, "+").replace(/_/g, "/");
  const binary = globalThis.atob(padded);
  return Uint8Array.from(binary, (c) => c.charCodeAt(0));
}

async function signingKey(secret: string): Promise<CryptoKey> {
  if (secret.length < 32) throw new Error("Signing secret must be at least 32 characters");
  return crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign", "verify"]);
}

export async function signTicket(secret: string, ticket: WorkerTicket): Promise<string> {
  assertTicket(ticket);
  const body = base64Url(encoder.encode(JSON.stringify(ticket)));
  const mac = new Uint8Array(await crypto.subtle.sign("HMAC", await signingKey(secret), encoder.encode(body)));
  return `${body}.${base64Url(mac)}`;
}

export async function verifyTicket(secret: string, token: string): Promise<WorkerTicket> {
  if (token.length > 1024) throw new Error("Ticket too long");
  const [body, signature, extra] = token.split(".");
  if (!body || !signature || extra !== undefined) throw new Error("Malformed ticket");
  const verified = await crypto.subtle.verify(
    "HMAC", await signingKey(secret), fromBase64Url(signature), encoder.encode(body),
  );
  if (!verified) throw new Error("Invalid ticket signature");
  const decoded: unknown = JSON.parse(decoder.decode(fromBase64Url(body)));
  assertTicket(decoded);
  if (decoded.exp <= Math.floor(Date.now() / 1000)) throw new Error("Expired ticket");
  return decoded;
}
