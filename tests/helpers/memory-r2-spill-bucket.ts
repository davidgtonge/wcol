/**
 * In-memory R2 bucket for local spill tests (put, multipart, ranged get).
 */

import type { R2OutputBucket } from "../../cloudflare/src/r2.ts";

export type MemoryR2SpillBucket = R2OutputBucket & {
  objects: Map<string, Uint8Array>;
  puts: number;
  gets: number;
  bytesUploaded: number;
  bytesDownloaded: number;
};

export function createMemoryR2SpillBucket(): MemoryR2SpillBucket {
  const objects = new Map<string, Uint8Array>();
  let puts = 0;
  let gets = 0;
  let bytesUploaded = 0;
  let bytesDownloaded = 0;
  const uploads = new Map<string, { key: string; parts: Map<number, Uint8Array> }>();

  function concat(parts: Uint8Array[]): Uint8Array {
    const out = new Uint8Array(parts.reduce((n, part) => n + part.byteLength, 0));
    let at = 0;
    for (const part of parts) {
      out.set(part, at);
      at += part.byteLength;
    }
    return out;
  }

  async function readStream(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
    const reader = stream.getReader();
    const parts: Uint8Array[] = [];
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value?.byteLength) parts.push(value.slice());
    }
    return concat(parts);
  }

  return {
    objects,
    get puts() {
      return puts;
    },
    get gets() {
      return gets;
    },
    get bytesUploaded() {
      return bytesUploaded;
    },
    get bytesDownloaded() {
      return bytesDownloaded;
    },
    async put(key, value) {
      puts += 1;
      const body =
        value instanceof ReadableStream
          ? await readStream(value)
          : value instanceof Uint8Array
            ? value.slice()
            : new TextEncoder().encode(String(value));
      objects.set(key, body);
      bytesUploaded += body.byteLength;
      return { httpEtag: `mem-${puts}`, size: body.byteLength };
    },
    async get(key, options) {
      gets += 1;
      const whole = objects.get(key);
      if (!whole) return null;
      const range = options?.range;
      let offset = 0;
      let length = whole.byteLength;
      if (range?.offset != null) {
        offset = range.offset;
        length = range.length ?? whole.byteLength - offset;
      } else if (range?.length != null) {
        length = range.length;
      }
      if (offset >= whole.byteLength) {
        return { body: null, arrayBuffer: async () => new ArrayBuffer(0), text: async () => "" };
      }
      length = Math.min(length, whole.byteLength - offset);
      const slice = whole.slice(offset, offset + length);
      bytesDownloaded += slice.byteLength;
      return {
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(slice);
            controller.close();
          },
        }),
        arrayBuffer: async () => slice.buffer.slice(slice.byteOffset, slice.byteOffset + slice.byteLength),
        text: async () => new TextDecoder().decode(slice),
      };
    },
    async createMultipartUpload(key) {
      const uploadId = `mem-upload-${uploads.size + 1}`;
      uploads.set(uploadId, { key, parts: new Map() });
      return this.resumeMultipartUpload(key, uploadId);
    },
    resumeMultipartUpload(key, uploadId) {
      const open = () => {
        const upload = uploads.get(uploadId);
        if (!upload || upload.key !== key) throw new Error(`no multipart upload ${uploadId}`);
        return upload;
      };
      return {
        uploadId,
        async uploadPart(partNumber, value) {
          puts += 1;
          open().parts.set(partNumber, value.slice());
          bytesUploaded += value.byteLength;
          return { partNumber, etag: `mem-part-${partNumber}` };
        },
        async complete(uploaded) {
          const { parts } = open();
          const bodies = uploaded.map(({ partNumber }) => {
            const part = parts.get(partNumber);
            if (!part) throw new Error(`multipart part ${partNumber} was never uploaded`);
            return part;
          });
          const sizes = bodies.slice(0, -1).map((part) => part.byteLength);
          if (sizes.some((size) => size !== sizes[0] || size < 5 * 1024 * 1024)) {
            throw new Error("multipart parts before the last must be equal and at least 5 MiB");
          }
          objects.set(key, concat(bodies));
          uploads.delete(uploadId);
          return {};
        },
        async abort() {
          uploads.delete(uploadId);
        },
      };
    },
  };
}
