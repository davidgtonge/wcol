import { PAGE_EXEC_WORDS, PAGE_REQ_WORDS } from "../core/constants.ts";
import { toBytes } from "../wasm/wasm.ts";
import type { ByteInput } from "../wasm/wasm.ts";
import type { PageRequest } from "../core/types.ts";

function combineU64(low: number, high: number): number {
  return high * 2 ** 32 + low;
}

export function decodePageRequests(bytes: Uint8Array): PageRequest[] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = bytes.byteLength / (PAGE_REQ_WORDS * 4);
  const requests: PageRequest[] = [];
  let offset = 0;
  for (let i = 0; i < count; i += 1) {
    const kind = view.getUint32(offset, true);
    offset += 4;
    const colId = view.getUint32(offset, true);
    offset += 4;
    const pageOffsetLow = view.getUint32(offset, true);
    offset += 4;
    const pageOffsetHigh = view.getUint32(offset, true);
    offset += 4;
    const compLen = view.getUint32(offset, true);
    offset += 4;
    const rawLen = view.getUint32(offset, true);
    offset += 4;
    requests.push({ kind, colId, offset: combineU64(pageOffsetLow, pageOffsetHigh), compLen, rawLen });
  }
  return requests;
}

export function packPages(
  requests: PageRequest[],
  payloads: ByteInput[]
): { descs: Uint32Array; data: Uint8Array } {
  let total = 0;
  for (const payload of payloads) {
    total += toBytes(payload).byteLength;
  }
  const data = new Uint8Array(total);
  const descs = new Uint32Array(requests.length * PAGE_EXEC_WORDS);

  let dataOffset = 0;
  let descOffset = 0;
  for (let i = 0; i < requests.length; i += 1) {
    const req = requests[i];
    const payload = toBytes(payloads[i]);
    data.set(payload, dataOffset);

    descs[descOffset] = req.kind;
    descs[descOffset + 1] = req.colId;
    descs[descOffset + 2] = dataOffset;
    descs[descOffset + 3] = req.compLen;
    descs[descOffset + 4] = req.rawLen;

    dataOffset += payload.byteLength;
    descOffset += PAGE_EXEC_WORDS;
  }

  return { descs, data };
}
