/** WCS1 byte-stream helpers (transport only — no query semantics). */

const WCS_MAGIC = 0x3153_4357;
const WCS_HEADER_BYTES = 32;
const WCS_MAX_PAYLOAD = 32 * 1024 * 1024;
export const FRAME_PARTIAL = 4;
const FRAME_END = 5;

/**
 * Incremental WCS1 reader: feed chunks as they arrive, get back each `FRAME_PARTIAL` body as
 * soon as its frame is complete. Holds at most one frame, so a reducer can merge partials
 * without buffering the whole stream.
 */
export class WcsPartialFrameReader {
  private readonly header = new Uint8Array(WCS_HEADER_BYTES);
  private headerFill = 0;
  private payload: Uint8Array | null = null;
  private payloadFill = 0;
  private frameType = 0;
  private sequence = 0;
  private sawEnd = false;
  private partials = 0;

  /** Consume `chunk`; returns the partial bodies it completed, in stream order. */
  push(chunk: Uint8Array): Uint8Array[] {
    const done: Uint8Array[] = [];
    let at = 0;
    while (at < chunk.byteLength) {
      if (!this.payload) {
        const take = Math.min(WCS_HEADER_BYTES - this.headerFill, chunk.byteLength - at);
        this.header.set(chunk.subarray(at, at + take), this.headerFill);
        this.headerFill += take;
        at += take;
        if (this.headerFill < WCS_HEADER_BYTES) break;
        this.payload = new Uint8Array(this.beginFrame());
        this.payloadFill = 0;
      }
      const take = Math.min(this.payload.byteLength - this.payloadFill, chunk.byteLength - at);
      this.payload.set(chunk.subarray(at, at + take), this.payloadFill);
      this.payloadFill += take;
      at += take;
      if (this.payloadFill === this.payload.byteLength) {
        if (this.frameType === FRAME_PARTIAL) {
          done.push(this.payload);
          this.partials += 1;
        } else if (this.frameType === FRAME_END) {
          this.sawEnd = true;
        }
        this.payload = null;
        this.headerFill = 0;
      }
    }
    return done;
  }

  /** Check the stream ended on a frame boundary after `FRAME_END` and carried a partial. */
  finish(): void {
    if (this.headerFill > 0 && !this.payload) throw new Error("truncated WCS1 header");
    if (this.payload) throw new Error("truncated WCS1 payload");
    if (!this.sawEnd) throw new Error("WCS1 stream missing END");
    if (this.partials === 0) throw new Error("reduce received no partials");
  }

  /** Validate the buffered header; returns the payload length. */
  private beginFrame(): number {
    const view = new DataView(this.header.buffer);
    if (view.getUint32(0, true) !== WCS_MAGIC) throw new Error("invalid WCS1 magic");
    if (this.header[4] !== 1) throw new Error("unsupported WCS1 version");
    this.frameType = this.header[5]!;
    const payloadLen = view.getUint32(8, true);
    if (payloadLen > WCS_MAX_PAYLOAD) throw new Error("WCS1 payload exceeds limit");
    if (view.getUint32(24, true) !== this.sequence) {
      throw new Error("WCS1 frame sequence is not contiguous");
    }
    this.sequence = (this.sequence + 1) >>> 0;
    return payloadLen;
  }
}

/** Last FRAME_PARTIAL payload in a WCS1 byte stream (`PWCL`). */
export function lastPartialPayload(wire: Uint8Array): Uint8Array | null {
  const view = new DataView(wire.buffer, wire.byteOffset, wire.byteLength);
  let offset = 0;
  let last: Uint8Array | null = null;
  while (offset + WCS_HEADER_BYTES <= wire.byteLength) {
    if (view.getUint32(offset, true) !== WCS_MAGIC) break;
    const frameType = wire[offset + 5]!;
    const payloadLen = view.getUint32(offset + 8, true);
    const end = offset + WCS_HEADER_BYTES + payloadLen;
    if (end > wire.byteLength) break;
    if (frameType === FRAME_PARTIAL) {
      last = wire.subarray(offset + WCS_HEADER_BYTES, end);
    }
    offset = end;
  }
  return last;
}
