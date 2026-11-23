/**
 * Format-agnostic R2 multipart proxy. Each request is one binding call.
 * The body is stored as a part; this worker does not parse it.
 */

interface R2UploadedPart {
  partNumber: number;
  etag: string;
}

interface R2MultipartUpload {
  readonly uploadId: string;
  uploadPart(partNumber: number, value: ArrayBuffer | ArrayBufferView): Promise<R2UploadedPart>;
  complete(uploadedParts: R2UploadedPart[]): Promise<unknown>;
  abort(): Promise<void>;
}

interface R2Bucket {
  createMultipartUpload(key: string): Promise<R2MultipartUpload>;
  resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload;
}

interface Env {
  OUTPUT: R2Bucket;
  WRITE_TOKEN: string;
}

const KEY = /^[A-Za-z0-9._-]{1,128}$/;
const MAX_PART_BYTES = 8 * 1024 * 1024;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!authorized(request, env.WRITE_TOKEN)) {
      return json({ error: "unauthorized" }, 401);
    }
    const url = new URL(request.url);
    try {
      if (request.method === "POST" && url.pathname === "/uploads") {
        const body = (await request.json()) as { key?: unknown };
        const key = requireKey(body.key);
        const upload = await env.OUTPUT.createMultipartUpload(key);
        return json({ uploadId: upload.uploadId });
      }
      if (request.method === "PUT" && url.pathname === "/parts") {
        const key = requireKey(request.headers.get("x-object-key"));
        const uploadId = requireText(request.headers.get("x-upload-id"), "upload id");
        const partNumber = requirePartNumber(request.headers.get("x-part-number"));
        const bytes = await request.arrayBuffer();
        if (bytes.byteLength === 0 || bytes.byteLength > MAX_PART_BYTES) {
          return json({ error: "part size out of range" }, 400);
        }
        const uploaded = await env.OUTPUT.resumeMultipartUpload(key, uploadId).uploadPart(
          partNumber,
          bytes,
        );
        return json({ etag: uploaded.etag, partNumber: uploaded.partNumber });
      }
      if (request.method === "POST" && url.pathname === "/complete") {
        const body = (await request.json()) as {
          key?: unknown;
          uploadId?: unknown;
          parts?: unknown;
        };
        const key = requireKey(body.key);
        const uploadId = requireText(body.uploadId, "upload id");
        const parts = requireParts(body.parts);
        await env.OUTPUT.resumeMultipartUpload(key, uploadId).complete(parts);
        return json({ ok: true });
      }
      if (request.method === "POST" && url.pathname === "/abort") {
        const body = (await request.json()) as { key?: unknown; uploadId?: unknown };
        const key = requireKey(body.key);
        const uploadId = requireText(body.uploadId, "upload id");
        await env.OUTPUT.resumeMultipartUpload(key, uploadId).abort();
        return json({ ok: true });
      }
      return json({ error: "not found" }, 404);
    } catch (error) {
      const message = error instanceof Error ? error.message : "r2 request failed";
      return json({ error: message.slice(0, 200) }, 500);
    }
  },
};

function authorized(request: Request, token: string): boolean {
  if (!token) return false;
  const header = request.headers.get("authorization") ?? "";
  const prefix = "Bearer ";
  if (!header.startsWith(prefix)) return false;
  return timingSafeEqual(header.slice(prefix.length), token);
}

function timingSafeEqual(left: string, right: string): boolean {
  const a = new TextEncoder().encode(left);
  const b = new TextEncoder().encode(right);
  if (a.byteLength !== b.byteLength) return false;
  let diff = 0;
  for (let i = 0; i < a.byteLength; i++) diff |= a[i]! ^ b[i]!;
  return diff === 0;
}

function requireKey(value: unknown): string {
  if (typeof value !== "string" || !KEY.test(value)) throw new Error("invalid object key");
  return value;
}

function requireText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0 || value.length > 1024) {
    throw new Error(`invalid ${label}`);
  }
  return value;
}

function requirePartNumber(value: string | null): number {
  const partNumber = Number(value);
  if (!Number.isInteger(partNumber) || partNumber < 1 || partNumber > 10_000) {
    throw new Error("invalid part number");
  }
  return partNumber;
}

function requireParts(value: unknown): R2UploadedPart[] {
  if (!Array.isArray(value) || value.length === 0 || value.length > 10_000) {
    throw new Error("invalid part list");
  }
  return value.map((part, index) => {
    const record = part as { partNumber?: unknown; etag?: unknown };
    const partNumber = record.partNumber;
    const etag = record.etag;
    if (partNumber !== index + 1) throw new Error("part list is not ordered");
    if (typeof etag !== "string" || etag.length === 0 || etag.length > 512) {
      throw new Error("invalid part etag");
    }
    return { partNumber, etag };
  });
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}
