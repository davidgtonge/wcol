/**
 * One R2 or HTTP-shaped object as ticketed range reads.
 * Rust chooses the offset and length. This port fetches that range and returns the bytes.
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

export type R2RangeSourceOptions = {
  /**
   * Load the object once and serve ranges from that copy. celld's local store
   * deadlocks overlapping range gets from two Durable Objects; a whole-object
   * load (especially a single-flight cache) avoids that.
   */
  loadWholeObject?: () => Promise<Uint8Array>;
};

/** Bounded range read from a single object key. */
export class R2RangeSource {
  private whole: Uint8Array | null = null;

  constructor(
    private readonly bucket: R2RangeBucket,
    private readonly key: string,
    private readonly etag?: string,
    private readonly options?: R2RangeSourceOptions,
  ) {}

  private sliceWhole(offset: number, length: number): Uint8Array {
    const whole = this.whole;
    if (!whole) throw new Error(`R2 object was not loaded: ${this.key}`);
    if (offset < 0 || offset + length > whole.byteLength) {
      throw new Error(
        `R2 range length mismatch for ${this.key} @${offset}: object ${whole.byteLength}, expected ${length}`,
      );
    }
    return whole.slice(offset, offset + length);
  }

  /** Sync range after `loadWholeObject`. Null until that load finishes. */
  readIfLoaded(offset: number, length: number): Uint8Array | null {
    if (!this.whole) return null;
    if (length <= 0) return new Uint8Array(0);
    return this.sliceWhole(offset, length);
  }

  /** Fetch the object once so later `readIfLoaded` / `read` calls stay in memory. */
  async preload(): Promise<void> {
    if (this.whole || !this.options?.loadWholeObject) return;
    this.whole = await this.options.loadWholeObject();
  }

  async read(offset: number, length: number): Promise<Uint8Array> {
    if (length <= 0) return new Uint8Array(0);
    if (this.options?.loadWholeObject) {
      await this.preload();
      return this.sliceWhole(offset, length);
    }
    const obj = await this.bucket.get(this.key, {
      range: { offset, length },
      onlyIf: this.etag ? { etagMatches: this.etag } : undefined,
    });
    if (!obj || obj.body === null) throw new Error(`R2 object missing or changed: ${this.key}`);
    const buf = new Uint8Array(await obj.arrayBuffer());
    if (buf.byteLength !== length) {
      throw new Error(
        `R2 range length mismatch for ${this.key} @${offset}: got ${buf.byteLength}, expected ${length}`,
      );
    }
    return buf;
  }
}
