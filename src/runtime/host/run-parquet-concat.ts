import { runWcolEngine } from "./run-wcol-engine.ts";
import { R2RangeSource, type R2RangeBucket } from "./ports/r2-read.ts";
import { ParquetUploadSink } from "./parquet-upload-sink.ts";
import type { R2MultipartSession } from "./ports/r2-write.ts";
import type { WcolStreamWasm } from "./wasm-stream.ts";

/** Run an opcode-11 concat program: ReadAt sources are committed shard keys in order. */
export async function runConcatProgram(options: {
  wasm: WcolStreamWasm;
  program: Uint8Array;
  bucket: R2RangeBucket;
  sourceKeys: string[];
  outputKey: string;
  writeSession: R2MultipartSession;
}): Promise<number> {
  const readers = options.sourceKeys.map((key) => {
    const range = new R2RangeSource(options.bucket, key);
    return (offset: number, length: number) => range.read(offset, length);
  });
  const sink = new ParquetUploadSink(options.writeSession, options.outputKey);
  try {
    await runWcolEngine({
      wasm: options.wasm,
      plan: options.program,
      scratchBytes: 1024 * 1024,
      layout: { inputs: 0, outputs: 1 },
      readAt: (source, offset, length) => readers[source]!(Number(offset), length),
      host: {
        readAt: (source, offset, length) => readers[source]!(offset, length),
        writePort: sink.writePort,
        yield: () => new Promise((resolve) => setTimeout(resolve, 0)),
      },
    });
    return await sink.finish();
  } catch (error) {
    await sink.abort(error);
    throw error;
  }
}
