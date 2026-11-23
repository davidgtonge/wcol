/**
 * Splits a partitioned Parquet sink's output into objects. The engine frames its files
 * (`rust/wcol-engine/src/formats/parquet/partitioned.rs`); this reads only the framing:
 *
 *   OPEN  u8 1, u32 slot, u32 len, path   a file under `path` (`Name=value/...`)
 *   DATA  u8 2, u32 slot, u32 len, bytes  its next bytes
 *   CLOSE u8 3, u32 slot, u64 rows        it is complete
 *
 * Each file streams to its own multipart upload with 5 MiB parts, so the engine's limit on
 * open files bounds the staging memory here too.
 */
import { R2_MIN_PART_SIZE, type R2MultipartSession } from "./ports/r2-write.ts";
import { ParquetUploadSink } from "./parquet-upload-sink.ts";

const FRAME_OPEN = 1;
const FRAME_DATA = 2;
const FRAME_CLOSE = 3;

/** One object an export task wrote: key, size, rows if counted, and its encoded path values. */
export type ExportObject = { objectKey: string; bytes: number; rows?: number; partitionValues: string[] };

/** What an export task writes through: one Parquet object, or Hive files. */
export type ExportSink = {
  writePort(port: number, chunk: Uint8Array): Promise<void>;
  finish(): Promise<ExportObject[]>;
  abort(error: unknown): Promise<void>;
};

/**
 * The sink for one export task. `objectKey` is the task attempt's object; Hive files go in
 * that attempt's directory instead, as `<path>/part-<slot>.parquet`.
 */
export function exportSink(
  layout: "single_file" | "partitioned" | "hive",
  session: (objectKey: string) => R2MultipartSession,
  objectKey: string,
): ExportSink {
  if (layout === "hive") {
    const dir = objectKey.slice(0, objectKey.lastIndexOf("/"));
    return new HiveUploadSink(session, (path, slot) => `${dir}/${path}/part-${slot}.parquet`);
  }
  const sink = new ParquetUploadSink(session(objectKey), objectKey);
  return {
    writePort: sink.writePort,
    finish: async () => [{ objectKey, bytes: await sink.finish(), partitionValues: [] }],
    abort: (error) => sink.abort(error),
  };
}

type Open = { sink: ParquetUploadSink; objectKey: string; path: string };

/** Header bytes after the tag: slot plus a length (OPEN, DATA) or a row count (CLOSE). */
const HEADER = { [FRAME_OPEN]: 8, [FRAME_DATA]: 8, [FRAME_CLOSE]: 12 } as const;

export class HiveUploadSink implements ExportSink {
  private readonly open = new Map<number, Open>();
  private readonly done: ExportObject[] = [];
  /** The frame header being read: tag first, then its fixed fields. */
  private readonly header = new Uint8Array(13);
  private headerAt = 0;
  /** Bytes still to come of the current OPEN path or DATA payload. */
  private payloadLeft = 0;
  /** The OPEN path being read, and the slot it opens. */
  private path: Uint8Array | null = null;
  private pathSlot = 0;
  /** The slot the current DATA payload belongs to. */
  private dataSlot = 0;

  /**
   * `session(key)` opens an upload for one object; `objectKey(path, slot)` names the file
   * for a slot under its partition path.
   */
  constructor(
    private readonly session: (objectKey: string) => R2MultipartSession,
    private readonly objectKey: (path: string, slot: number) => string,
  ) {}

  writePort = async (_port: number, chunk: Uint8Array): Promise<void> => {
    let at = 0;
    while (at < chunk.length) {
      if (this.payloadLeft > 0) {
        const n = Math.min(this.payloadLeft, chunk.length - at);
        await this.payload(chunk.subarray(at, at + n));
        this.payloadLeft -= n;
        at += n;
        if (this.payloadLeft === 0) await this.payloadEnd();
        continue;
      }
      this.header[this.headerAt++] = chunk[at++]!;
      const tag = this.header[0]!;
      const need = HEADER[tag as keyof typeof HEADER];
      if (need === undefined) throw new Error(`partitioned parquet stream: unknown frame ${tag}`);
      if (this.headerAt === need + 1) await this.headerEnd(tag);
    }
  };

  /** Every file the sink wrote. Fails if a file was opened and never closed. */
  async finish(): Promise<ExportObject[]> {
    if (this.open.size > 0 || this.headerAt > 0 || this.payloadLeft > 0) {
      throw new Error("partitioned parquet stream ended inside a file");
    }
    return this.done.slice();
  }

  async abort(error: unknown): Promise<void> {
    await Promise.all([...this.open.values()].map((file) => file.sink.abort(error)));
    this.open.clear();
  }

  private async headerEnd(tag: number): Promise<void> {
    const view = new DataView(this.header.buffer);
    const slot = view.getUint32(1, true);
    this.headerAt = 0;
    if (tag === FRAME_CLOSE) {
      const file = this.file(slot);
      this.open.delete(slot);
      const bytes = await file.sink.finish();
      this.done.push({
        objectKey: file.objectKey,
        bytes,
        rows: Number(view.getBigUint64(5, true)),
        partitionValues: file.path.split("/").map((segment) => segment.slice(segment.indexOf("=") + 1)),
      });
      return;
    }
    const len = view.getUint32(5, true);
    if (tag === FRAME_OPEN) {
      if (this.open.has(slot)) throw new Error(`partitioned parquet stream reopened slot ${slot}`);
      this.path = new Uint8Array(len);
      this.pathSlot = slot;
    } else {
      this.file(slot);
      this.dataSlot = slot;
    }
    this.payloadLeft = len;
    if (len === 0) await this.payloadEnd();
  }

  private async payload(bytes: Uint8Array): Promise<void> {
    if (this.path) {
      this.path.set(bytes, this.path.length - this.payloadLeft);
      return;
    }
    await this.file(this.dataSlot).sink.writePort(0, bytes);
  }

  private async payloadEnd(): Promise<void> {
    if (!this.path) return;
    const path = new TextDecoder().decode(this.path);
    this.path = null;
    const objectKey = this.objectKey(path, this.pathSlot);
    this.open.set(this.pathSlot, {
      sink: new ParquetUploadSink(this.session(objectKey), objectKey, R2_MIN_PART_SIZE),
      objectKey,
      path,
    });
  }

  private file(slot: number): Open {
    const file = this.open.get(slot);
    if (!file) throw new Error(`partitioned parquet stream wrote to slot ${slot}, which is not open`);
    return file;
  }
}
