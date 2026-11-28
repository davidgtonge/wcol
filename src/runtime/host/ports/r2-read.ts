/**
 * One R2 object as range reads. Rust chooses the offset and length; this port fetches that
 * range, pinned to the object's etag when the plan recorded one.
 */

export type R2RangeBucket = {
  get(
    key: string,
    options?: {
      range?: { offset?: number; length?: number; suffix?: number };
      onlyIf?: { etagMatches?: string; etagDoesNotMatch?: string };
    },
  ): Promise<{ body?: ReadableStream<Uint8Array> | null; arrayBuffer(): Promise<ArrayBuffer> } | null>;
};

/** Bounded range read from a single object key. */
export class R2RangeSource {
  constructor(
    private readonly bucket: R2RangeBucket,
    private readonly key: string,
    private readonly etag?: string,
  ) {}

  /** Not loaded ahead: always null, so callers read through `read`. */
  readIfLoaded(_offset: number, _length: number): null {
    return null;
  }

  async read(offset: number, length: number): Promise<Uint8Array> {
    if (length <= 0) return new Uint8Array(0);
    const obj = await this.bucket.get(this.key, {
      range: { offset, length },
      onlyIf: this.etag ? { etagMatches: this.etag } : undefined,
    });
    // A failed `onlyIf` returns the object's metadata without a body.
    if (!obj || obj.body == null) throw new Error(`R2 object missing or changed: ${this.key}`);
    const buf = new Uint8Array(await obj.arrayBuffer());
    if (buf.byteLength !== length) {
      throw new Error(
        `R2 range length mismatch for ${this.key} @${offset}: got ${buf.byteLength}, expected ${length}`,
      );
    }
    return buf;
  }
}
