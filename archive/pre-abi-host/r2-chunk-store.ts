import type { ChunkStore } from "./chunk-stream.ts";
import type { R2OutputBucket } from "./r2-binding-session.ts";

/** Whole-object R2 adapter. Each value already has a known length. */
export function r2ChunkStore(bucket: R2OutputBucket): ChunkStore {
  return {
    async put(key, data, signal) {
      if (signal.aborted) {
        throw signal.reason instanceof Error ? signal.reason : new Error("PUT aborted");
      }
      await bucket.put(key, data);
    },
    async get(key, signal) {
      if (signal.aborted) {
        throw signal.reason instanceof Error ? signal.reason : new Error("GET aborted");
      }
      const obj = await bucket.get(key);
      if (!obj) throw new Error(`R2 object missing: ${key}`);
      return new Uint8Array(await obj.arrayBuffer());
    },
  };
}
