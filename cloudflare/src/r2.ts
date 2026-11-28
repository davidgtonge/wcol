/**
 * Bucket, key, and one multipart upload. Ports call this. Job publication does not.
 */

import type { R2MultipartSession } from "../../src/runtime/host/ports/r2-write.ts";
import type { R2OutputBucket } from "../../src/runtime/host/ports/r2-bucket.ts";

export type { R2MultipartUpload, R2OutputBucket, R2UploadedPart } from "../../src/runtime/host/ports/r2-bucket.ts";

/** One object key. Part bodies are the bytes the caller already staged. */
export function r2BindingSession(bucket: R2OutputBucket, key: string): R2MultipartSession {
  return {
    async create(objectKey) {
      if (objectKey !== key) throw new Error("r2 session key mismatch");
      const upload = await bucket.createMultipartUpload(key);
      return upload.uploadId;
    },
    async uploadPart(uploadId, partNumber, body) {
      const uploaded = await bucket.resumeMultipartUpload(key, uploadId).uploadPart(partNumber, body);
      return uploaded.etag;
    },
    async complete(uploadId, parts) {
      await bucket.resumeMultipartUpload(key, uploadId).complete(parts);
    },
    async abort(uploadId) {
      await bucket.resumeMultipartUpload(key, uploadId).abort();
    },
  };
}

/** The R2 binding a job names as `bucket`. Workers have one: `OUTPUT`. */
export type R2BindingName = "output";

export type R2Env = {
  OUTPUT?: R2OutputBucket;
};

export function parseR2BindingName(value: unknown): R2BindingName | null {
  return value === "output" ? value : null;
}

/** One path segment: no empty, `.` or `..` segments, so a key cannot escape its prefix. */
const SEGMENT = /^[A-Za-z0-9._=%-]{1,255}$/;

/**
 * An R2 object key the Workers read or write: `/`-separated segments of letters, digits and
 * `._=%-` (partition paths use `=` and percent-encoding), at most 1024 bytes, no leading `/`.
 */
export function validateObjectKey(key: string): boolean {
  if (key.length === 0 || key.length > 1024) return false;
  return key.split("/").every((segment) => SEGMENT.test(segment) && segment !== "." && segment !== "..");
}
