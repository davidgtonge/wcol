import type { R2UploadedPart } from "./r2-bucket.ts";

/**
 * Format-agnostic multipart sink. It stores bytes and part receipts.
 * Callers decide when the byte stream is finished; this type does not
 * inspect page headers, schemas, or row groups.
 */

export const R2_PART_SIZE = 8 * 1024 * 1024;
/** R2's smallest non-final part. */
export const R2_MIN_PART_SIZE = 5 * 1024 * 1024;
export const R2_MAX_PARTS = 10_000;
/** Backoff before the second part attempt; it doubles for each one after. */
const PART_RETRY_MS = 100;

export type { R2UploadedPart };

/**
 * One R2 multipart upload. Implementations talk to a bucket; they do not parse payloads.
 * A part's `body` is a view of the writer's staging buffer, valid until `uploadPart` settles.
 */
export interface R2MultipartSession {
  create(key: string): Promise<string>;
  uploadPart(uploadId: string, partNumber: number, body: Uint8Array): Promise<string>;
  complete(uploadId: string, parts: R2UploadedPart[]): Promise<void>;
  abort(uploadId: string): Promise<void>;
}

export type R2MultipartSnapshot = {
  stagedBytes: number;
  inFlightBytes: number;
  parts: number;
  bytesAccepted: number;
  peakAccountedBytes: number;
};

export type ByteCharge = {
  charge(bytes: number): void;
  release(bytes: number): void;
};

/**
 * One staging buffer (8 MiB unless the caller picks another part size) and one part upload
 * at a time. `write` resolves only after those bytes fit in the buffer, so a caller that
 * awaits it stops producing while a part is uploading.
 */
export class R2MultipartWritePort {
  private readonly buffer: Uint8Array;
  private stagingCharged: boolean;
  private filled = 0;
  private partNumber = 1;
  private readonly parts: R2UploadedPart[] = [];
  private uploadId: string | null = null;
  private closed = false;
  private failed = false;
  private inFlightBytes = 0;
  private bytesAccepted = 0;
  private peakAccountedBytes = 0;

  constructor(
    private readonly session: R2MultipartSession,
    private readonly key: string,
    private readonly maxAttempts = 3,
    private readonly account?: ByteCharge,
    private readonly partBytes = R2_PART_SIZE,
  ) {
    if (partBytes < R2_MIN_PART_SIZE) throw new Error(`multipart parts must be at least ${R2_MIN_PART_SIZE} bytes`);
    this.buffer = new Uint8Array(partBytes);
    this.stagingCharged = false;
    if (account) {
      account.charge(partBytes);
      this.stagingCharged = true;
    }
  }

  snapshot(): R2MultipartSnapshot {
    return {
      stagedBytes: this.filled,
      inFlightBytes: this.inFlightBytes,
      parts: this.parts.length,
      bytesAccepted: this.bytesAccepted,
      peakAccountedBytes: this.peakAccountedBytes,
    };
  }

  async write(chunk: Uint8Array): Promise<void> {
    this.assertOpen();
    let offset = 0;
    while (offset < chunk.length) {
      if (this.filled === this.partBytes) await this.flushFullPart();
      const n = Math.min(this.partBytes - this.filled, chunk.length - offset);
      this.buffer.set(chunk.subarray(offset, offset + n), this.filled);
      this.filled += n;
      offset += n;
      this.bytesAccepted += n;
      this.noteAccounted();
    }
    if (this.filled === this.partBytes) await this.flushFullPart();
  }

  /** Publish the object. The final part may be shorter than the others. */
  async finish(): Promise<void> {
    this.assertOpen();
    if (this.filled > 0) {
      const body = this.buffer.subarray(0, this.filled);
      this.filled = 0;
      await this.putPart(body);
    }
    if (!this.uploadId || this.parts.length === 0) {
      throw new Error("multipart upload received no bytes");
    }
    const uploadId = this.uploadId;
    try {
      await this.session.complete(uploadId, this.parts.slice());
    } catch (error) {
      await this.safeAbort();
      throw error;
    }
    this.uploadId = null;
    this.closed = true;
    this.releaseStaging();
  }

  async abort(): Promise<void> {
    this.failed = true;
    await this.safeAbort();
    this.releaseStaging();
  }

  /** Writes wait for this, so the staging buffer is not refilled while its part uploads. */
  private async flushFullPart(): Promise<void> {
    this.filled = 0;
    await this.putPart(this.buffer.subarray(0, this.partBytes));
  }

  private async putPart(body: Uint8Array): Promise<void> {
    if (this.partNumber > R2_MAX_PARTS) {
      this.failed = true;
      await this.safeAbort();
      throw new Error(`multipart upload exceeded ${R2_MAX_PARTS} parts`);
    }
    if (!this.uploadId) this.uploadId = await this.session.create(this.key);
    const number = this.partNumber;
    this.inFlightBytes = body.byteLength;
    this.noteAccounted();
    let charged = false;
    try {
      this.account?.charge(body.byteLength);
      charged = this.account != null;
      let lastError: unknown;
      for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
        if (attempt > 1) await new Promise((resolve) => setTimeout(resolve, PART_RETRY_MS * 2 ** (attempt - 2)));
        try {
          const etag = await this.session.uploadPart(this.uploadId, number, body);
          this.parts.push({ partNumber: number, etag });
          this.partNumber += 1;
          this.inFlightBytes = 0;
          this.noteAccounted();
          return;
        } catch (error) {
          lastError = error;
          // A TypeError or RangeError is a bad call, not a transient R2 failure.
          if (error instanceof TypeError || error instanceof RangeError) break;
        }
      }
      this.inFlightBytes = 0;
      this.failed = true;
      await this.safeAbort();
      this.releaseStaging();
      throw lastError instanceof Error ? lastError : new Error("multipart part upload failed");
    } finally {
      if (charged) this.account?.release(body.byteLength);
    }
  }

  private releaseStaging(): void {
    if (!this.stagingCharged) return;
    this.stagingCharged = false;
    this.account?.release(this.partBytes);
  }

  private async safeAbort(): Promise<void> {
    const uploadId = this.uploadId;
    this.uploadId = null;
    if (!uploadId) return;
    try {
      await this.session.abort(uploadId);
    } catch {
      // The original failure remains the caller's error.
    }
  }

  private assertOpen(): void {
    if (this.closed || this.failed) throw new Error("multipart upload is closed");
  }

  private noteAccounted(): void {
    const accounted = this.filled + this.inFlightBytes;
    if (accounted > this.peakAccountedBytes) this.peakAccountedBytes = accounted;
  }
}
