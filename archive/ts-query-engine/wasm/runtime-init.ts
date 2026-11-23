import type { ByteSource } from "../io/byte-source.ts";
import { HEADER_BYTES, HEADER_FLAG_DICT_COMPRESSED, HEADER_INFO_BYTES } from "../core/constants.ts";
import { decompressDictBytes } from "../schema/dicts.ts";
import { parseHeaderInfo } from "../io/header.ts";
import { callStatus, readOutBytes, withBytes } from "./helpers.ts";
import type { WasmBindings } from "./wasm.ts";
import type { HeaderInfo, RuntimeInitBytes } from "../core/types.ts";

function toNumber(value: number | bigint): number {
  return typeof value === "bigint" ? Number(value) : value;
}

/** Read file bytes needed to initialize a runtime (header, schema, toc, optional dicts). */
export async function loadRuntimeInitBytes(
  wasm: WasmBindings,
  source: ByteSource
): Promise<RuntimeInitBytes> {
  const headerBytes = await source.read(0, HEADER_BYTES);
  const header = parseHeaderInfo(headerBytes);

  const schemaBytes = await source.read(toNumber(header.schemaOff), toNumber(header.schemaLen));
  const tocWidth = 8;
  const tocBytes = await source.read(toNumber(header.indexOff), header.nchunks * tocWidth);

  let dictBytes: Uint8Array | undefined;
  if (header.dictLen) {
    dictBytes = await source.read(toNumber(header.dictOff), toNumber(header.dictLen));
    if (header.flags & HEADER_FLAG_DICT_COMPRESSED) {
      dictBytes = decompressDictBytes(wasm, dictBytes, toNumber(header.dictRawLen));
    }
  }

  return { header: headerBytes, schema: schemaBytes, toc: tocBytes, dicts: dictBytes };
}

/** Create a WASM runtime from preloaded init bytes (main thread or workers). */
export async function bindRuntimeFromInitBytes(
  wasm: WasmBindings,
  init: RuntimeInitBytes
): Promise<{ runtime: number; header: HeaderInfo }> {
  const runtime = wasm.exports.create_runtime();
  withBytes(wasm, init.header, (ptr, len) => callStatus(wasm.exports.runtime_set_header(runtime, ptr, len)));
  withBytes(wasm, init.schema, (ptr, len) => callStatus(wasm.exports.runtime_set_schema(runtime, ptr, len)));
  withBytes(wasm, init.toc, (ptr, len) => callStatus(wasm.exports.runtime_set_toc(runtime, ptr, len)));
  if (init.dicts?.byteLength) {
    withBytes(wasm, init.dicts, (ptr, len) => callStatus(wasm.exports.runtime_set_dicts(runtime, ptr, len)));
  }
  const header = parseHeaderInfo(
    await readOutBytes(
      wasm,
      (outPtr, outLen) => wasm.exports.runtime_header_info(runtime, outPtr, outLen),
      HEADER_INFO_BYTES
    )
  );
  return { runtime, header };
}
