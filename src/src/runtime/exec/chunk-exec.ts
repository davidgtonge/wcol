import {
  CHUNK_SPAN_BYTES,
  INDEX_ENTRY_BYTES,
  PAGE_REQ_WORDS
} from "../core/constants.ts";
import { decodePageRequests, packPages } from "../io/pages.ts";
import { callStatus, readOutBytes, withArray, withBytes, withTwoBuffers } from "../wasm/helpers.ts";
import { freeWasm, readFromWasm } from "../wasm/wasm.ts";
import type { WcolContext } from "../core/context.ts";
import type { PageRequest, PageRequestList } from "../core/types.ts";

export type ChunkPayload = {
  chunkId: number;
  descs: Uint32Array;
  data: Uint8Array;
};

function readChunkSpan(spanBytes: Uint8Array): { indexOffset: number; indexCompLen: number } {
  const v = new DataView(spanBytes.buffer, spanBytes.byteOffset, spanBytes.byteLength);
  if (spanBytes.byteLength >= 12) {
    return { indexOffset: v.getUint32(4, true) * 2 ** 32 + v.getUint32(0, true), indexCompLen: v.getUint32(8, true) };
  }
  return { indexOffset: v.getUint32(0, true), indexCompLen: v.getUint32(4, true) };
}

async function requiredPages(
  ctx: WcolContext,
  plan: number,
  chunkId: number,
  indexBytes: Uint8Array,
  hasFilters: boolean
): Promise<PageRequestList> {
  const rawLen = ctx.header.ncols * INDEX_ENTRY_BYTES;
  return withBytes(ctx.wasm, indexBytes, (ptr, len) => {
    let outLen = 4096;
    for (;;) {
      const outPtr = ctx.wasm.alloc(outLen);
      const count = ctx.wasm.exports.plan_required_pages(
        ctx.runtime,
        plan,
        chunkId,
        ptr,
        len,
        rawLen,
        outPtr,
        outLen
      );
      if (count < 0) {
        freeWasm(ctx.wasm, outPtr, outLen);
        const needed = -count;
        if (needed <= outLen) throw new Error(`WASM call failed (${count})`);
        outLen = needed;
        continue;
      }
      if (count === 0 && hasFilters) {
        freeWasm(ctx.wasm, outPtr, outLen);
        return Object.assign([], { skip: true }) as PageRequestList;
      }
      const bytes = readFromWasm(ctx.wasm, outPtr, count * PAGE_REQ_WORDS * 4);
      freeWasm(ctx.wasm, outPtr, outLen);
      return decodePageRequests(bytes);
    }
  });
}

const MATERIALIZE_INDEX_CACHE_MISS = -20;

async function decodeMaterializePageRequests(
  ctx: WcolContext,
  outPtr: number,
  outLen: number,
  count: number
): Promise<PageRequestList> {
  if (count === 0) {
    freeWasm(ctx.wasm, outPtr, outLen);
    return [] as PageRequestList;
  }
  const bytes = readFromWasm(ctx.wasm, outPtr, count * PAGE_REQ_WORDS * 4);
  freeWasm(ctx.wasm, outPtr, outLen);
  return decodePageRequests(bytes);
}

async function requiredPagesMaterializeCached(
  ctx: WcolContext,
  plan: number,
  chunkId: number
): Promise<PageRequestList | null> {
  let outLen = 4096;
  for (;;) {
    const outPtr = ctx.wasm.alloc(outLen);
    const count = ctx.wasm.exports.plan_materialize_required_pages_cached(
      ctx.runtime,
      plan,
      chunkId,
      outPtr,
      outLen
    );
    if (count === MATERIALIZE_INDEX_CACHE_MISS) {
      freeWasm(ctx.wasm, outPtr, outLen);
      return null;
    }
    if (count < 0) {
      freeWasm(ctx.wasm, outPtr, outLen);
      const needed = -count;
      if (needed <= outLen) throw new Error(`WASM call failed (${count})`);
      outLen = needed;
      continue;
    }
    return decodeMaterializePageRequests(ctx, outPtr, outLen, count);
  }
}

async function requiredPagesMaterialize(
  ctx: WcolContext,
  plan: number,
  chunkId: number,
  indexBytes: Uint8Array
): Promise<PageRequestList> {
  const rawLen = ctx.header.ncols * INDEX_ENTRY_BYTES;
  return withBytes(ctx.wasm, indexBytes, (ptr, len) => {
    let outLen = 4096;
    for (;;) {
      const outPtr = ctx.wasm.alloc(outLen);
      const count = ctx.wasm.exports.plan_materialize_required_pages(
        ctx.runtime,
        plan,
        chunkId,
        ptr,
        len,
        rawLen,
        outPtr,
        outLen
      );
      if (count < 0) {
        freeWasm(ctx.wasm, outPtr, outLen);
        const needed = -count;
        if (needed <= outLen) throw new Error(`WASM call failed (${count})`);
        outLen = needed;
        continue;
      }
      return decodeMaterializePageRequests(ctx, outPtr, outLen, count);
    }
  });
}

/** required_pages → read pages → packed payload; null if pruned by filters. */
export async function loadChunkPayload(
  ctx: WcolContext,
  plan: number,
  chunkId: number,
  hasFilters: boolean
): Promise<ChunkPayload | null> {
  const spanBytes = await readOutBytes(
    ctx.wasm,
    (outPtr, outLen) => ctx.wasm.exports.runtime_chunk_index_span(ctx.runtime, chunkId, outPtr, outLen),
    CHUNK_SPAN_BYTES
  );
  const { indexOffset, indexCompLen } = readChunkSpan(spanBytes);
  const indexBytes = await ctx.source.read(indexOffset, indexCompLen);
  const pageRequests = await requiredPages(ctx, plan, chunkId, indexBytes, hasFilters);
  if (pageRequests.skip) return null;
  if (!pageRequests.length) {
    return { chunkId, descs: new Uint32Array(0), data: new Uint8Array(0) };
  }
  const pagePayloads = await Promise.all(pageRequests.map((req) => ctx.source.read(req.offset, req.compLen)));
  const { descs, data } = packPages(pageRequests, pagePayloads);
  return { chunkId, descs, data };
}

/** Load only SELECT projection pages for late materialize. */
export async function loadChunkPayloadForMaterialize(
  ctx: WcolContext,
  plan: number,
  chunkId: number
): Promise<ChunkPayload | null> {
  let pageRequests = await requiredPagesMaterializeCached(ctx, plan, chunkId);
  if (pageRequests === null) {
    const spanBytes = await readOutBytes(
      ctx.wasm,
      (outPtr, outLen) => ctx.wasm.exports.runtime_chunk_index_span(ctx.runtime, chunkId, outPtr, outLen),
      CHUNK_SPAN_BYTES
    );
    const { indexOffset, indexCompLen } = readChunkSpan(spanBytes);
    const indexBytes = await ctx.source.read(indexOffset, indexCompLen);
    pageRequests = await requiredPagesMaterialize(ctx, plan, chunkId, indexBytes);
  }
  if (!pageRequests.length) {
    return { chunkId, descs: new Uint32Array(0), data: new Uint8Array(0) };
  }
  const pagePayloads = await Promise.all(pageRequests.map((req) => ctx.source.read(req.offset, req.compLen)));
  const { descs, data } = packPages(pageRequests, pagePayloads);
  return { chunkId, descs, data };
}

export function materializeChunkOnPlan(
  ctx: WcolContext,
  plan: number,
  payload: ChunkPayload,
  localRows: Uint32Array,
  dstRows: Uint32Array
): void {
  const run = (
    descPtr: number,
    descWords: number,
    dataPtr: number,
    dataLen: number
  ) =>
    withArray(ctx.wasm, localRows, (localPtr, localLen) =>
      withArray(ctx.wasm, dstRows, (dstPtr, dstLen) =>
        callStatus(
          ctx.wasm.exports.plan_materialize_chunk(
            ctx.runtime,
            plan,
            payload.chunkId,
            descPtr,
            descWords,
            dataPtr,
            dataLen,
            localPtr,
            localLen,
            dstPtr,
            dstLen
          )
        )
      )
    );

  if (payload.descs.byteLength) {
    withTwoBuffers(ctx.wasm, payload.descs, payload.data, (descPtr, descLen, dataPtr, dataLen) =>
      run(descPtr, descLen, dataPtr, dataLen)
    );
  } else {
    run(0, 0, 0, 0);
  }
}

export function execChunkOnPlan(ctx: WcolContext, plan: number, payload: ChunkPayload): void {
  if (payload.descs.byteLength) {
    withTwoBuffers(ctx.wasm, payload.descs, payload.data, (descPtr, descLen, dataPtr, dataLen) =>
      callStatus(
        ctx.wasm.exports.plan_exec_chunk(ctx.runtime, plan, payload.chunkId, descPtr, descLen, dataPtr, dataLen)
      )
    );
  } else {
    callStatus(ctx.wasm.exports.plan_exec_chunk(ctx.runtime, plan, payload.chunkId, 0, 0, 0, 0));
  }
}
