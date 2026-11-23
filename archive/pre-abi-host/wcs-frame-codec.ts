/** Minimal WCS1 frame encoding for transport benchmarks (no LZ4). */

import {
  createBoundedBytePipe,
  type BoundedBytePipeMetrics,
} from "../../src/runtime/host/ports/rpc.ts";

export const WCS_MAGIC = 0x3153_4357;
export const WCS_HEADER_BYTES = 32;
export const FRAME_DATA_BATCH = 3;
export const FRAME_PARTIAL = 4;
export const FRAME_END = 5;

export function encodeWcsFrame(type: number, payload: Uint8Array, sequence: number): Uint8Array {
  const out = new Uint8Array(WCS_HEADER_BYTES + payload.byteLength);
  const view = new DataView(out.buffer);
  view.setUint32(0, WCS_MAGIC, true);
  out[4] = 1;
  out[5] = type;
  view.setUint32(8, payload.byteLength, true);
  view.setUint32(24, sequence, true);
  out.set(payload, WCS_HEADER_BYTES);
  return out;
}

/** Readable stream of valid WCS1 DATA_BATCH + END frames (~`totalPayloadBytes` in batch bodies). */
export function wcs1SyntheticReadable(
  totalPayloadBytes: number,
  batchPayloadBytes = 64 * 1024,
): ReadableStream<Uint8Array> {
  let payloadSent = 0;
  let sequence = 0;
  let closed = false;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (closed) return;
      if (payloadSent >= totalPayloadBytes) {
        closed = true;
        controller.enqueue(encodeWcsFrame(FRAME_END, new Uint8Array(0), sequence++));
        controller.close();
        return;
      }
      const n = Math.min(batchPayloadBytes, totalPayloadBytes - payloadSent);
      const body = new Uint8Array(n);
      for (let i = 0; i < n; i++) body[i] = (payloadSent + i) & 0xff;
      payloadSent += n;
      controller.enqueue(encodeWcsFrame(FRAME_DATA_BATCH, body, sequence++));
    },
  });
}

/** Readable stream of WCS1 PARTIAL + END frames (synthetic PWCL-sized bodies). */
export function wcs1PartialSyntheticReadable(
  totalPayloadBytes: number,
  batchPayloadBytes = 4 * 1024,
): ReadableStream<Uint8Array> {
  let payloadSent = 0;
  let sequence = 0;
  let closed = false;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (closed) return;
      if (payloadSent >= totalPayloadBytes) {
        closed = true;
        controller.enqueue(encodeWcsFrame(FRAME_END, new Uint8Array(0), sequence++));
        controller.close();
        return;
      }
      const n = Math.min(batchPayloadBytes, totalPayloadBytes - payloadSent);
      const body = new Uint8Array(n);
      body[0] = 0x50; // PWCL magic byte for visual sanity
      body[1] = 0x57;
      for (let i = 2; i < n; i++) body[i] = (payloadSent + i) & 0xff;
      payloadSent += n;
      controller.enqueue(encodeWcsFrame(FRAME_PARTIAL, body, sequence++));
    },
  });
}

export function wcs1PartialBoundedResponse(
  totalPayloadBytes: number,
  bufferCap: number,
  batchPayloadBytes = 4 * 1024,
): Response {
  const { readable } = startWcs1PartialBoundedPipe(totalPayloadBytes, bufferCap, batchPayloadBytes);
  return new Response(readable, {
    headers: {
      "content-type": "application/octet-stream",
      "x-wcol-wcs1-partial-bytes": String(totalPayloadBytes),
    },
  });
}

export function startWcs1PartialBoundedPipe(
  totalPayloadBytes: number,
  bufferCap: number,
  batchPayloadBytes = 4 * 1024,
): { readable: ReadableStream<Uint8Array>; whenDone: Promise<BoundedBytePipeMetrics> } {
  return startWcs1BoundedPipeFromReadable(
    wcs1PartialSyntheticReadable(totalPayloadBytes, batchPayloadBytes),
    bufferCap,
  );
}

function startWcs1BoundedPipeFromReadable(
  source: ReadableStream<Uint8Array>,
  bufferCap: number,
): { readable: ReadableStream<Uint8Array>; whenDone: Promise<BoundedBytePipeMetrics> } {
  const pipe = createBoundedBytePipe(bufferCap);
  const whenDone = source.pipeTo(pipe.writable).then(
    () => pipe.metrics(),
    () => pipe.metrics(),
  );
  return { readable: pipe.readable, whenDone };
}

export function startWcs1BoundedPipe(
  totalPayloadBytes: number,
  bufferCap: number,
  batchPayloadBytes = 64 * 1024,
): { readable: ReadableStream<Uint8Array>; whenDone: Promise<BoundedBytePipeMetrics> } {
  return startWcs1BoundedPipeFromReadable(
    wcs1SyntheticReadable(totalPayloadBytes, batchPayloadBytes),
    bufferCap,
  );
}

export function wcs1BoundedResponse(
  totalPayloadBytes: number,
  bufferCap: number,
  batchPayloadBytes = 64 * 1024,
): Response {
  const { readable } = startWcs1BoundedPipe(totalPayloadBytes, bufferCap, batchPayloadBytes);
  return new Response(readable, {
    headers: {
      "content-type": "application/octet-stream",
      "x-wcol-wcs1-bytes": String(totalPayloadBytes),
    },
  });
}

/** Rust stream plan: WCS1 guard + byte copy (see `open_plan` in stream/core.rs). */
export const PLAN_BYTE_PIPE = 0xbe;
export const WCS_VALIDATE_AND_COPY_PLAN = new Uint8Array([PLAN_BYTE_PIPE, 1, 0]);

/** Passthrough only (GraphHost WorkerShard wire often ends after PARTIAL, without END). */
export const BYTE_COPY_ONLY_PLAN = new Uint8Array([PLAN_BYTE_PIPE, 0]);
