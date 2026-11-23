import { R2MultipartWritePort, type R2MultipartSession } from "./ports/r2-write.ts";

/** Streaming Parquet bytes to one R2 object via multipart upload. */
export class ParquetUploadSink {
  private readonly port: R2MultipartWritePort;
  private bytes = 0;

  constructor(session: R2MultipartSession, objectKey: string) {
    this.port = new R2MultipartWritePort(session, objectKey);
  }

  writePort = async (_port: number, chunk: Uint8Array): Promise<void> => {
    await this.port.write(chunk);
    this.bytes += chunk.byteLength;
  };

  async finish(): Promise<number> {
    await this.port.finish();
    return this.bytes;
  }

  async abort(_error: unknown): Promise<void> {
    await this.port.abort();
  }

  /** Completed multipart parts, including the short tail after `finish`. */
  parts(): number {
    return this.port.snapshot().parts;
  }
}

/** Shard metadata written only after the multipart upload has completed. */
export type ParquetShardPart = {
  objectKey: string;
  bytes: number;
  taskId: number;
  parts: number;
  partitionValues: string[];
};

/**
 * Complete the R2 upload, then build the shard receipt.
 * A failed `complete` throws, so the caller has no receipt to record.
 */
export async function finishParquetShard(
  sink: ParquetUploadSink,
  objectKey: string,
  taskId: number,
): Promise<ParquetShardPart> {
  const bytes = await sink.finish();
  return {
    objectKey,
    bytes,
    taskId,
    parts: sink.parts(),
    partitionValues: [],
  };
}

/** Upload a complete Parquet blob via multipart (attempt shards and small outputs). */
export async function uploadParquetBuffer(
  session: R2MultipartSession,
  objectKey: string,
  bytes: Uint8Array,
): Promise<number> {
  const sink = new ParquetUploadSink(session, objectKey);
  try {
    if (bytes.byteLength > 0) await sink.writePort(0, bytes);
    return await sink.finish();
  } catch (error) {
    await sink.abort(error);
    throw error;
  }
}
