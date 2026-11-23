import fs from "node:fs";

/** A stream that yields `chunks` one per pull, then closes. */
export function readableFrom(chunks: readonly Uint8Array[]): ReadableStream<Uint8Array> {
  let index = 0;
  return new ReadableStream({
    pull(controller) {
      const chunk = chunks[index];
      if (chunk === undefined) {
        controller.close();
        return;
      }
      controller.enqueue(chunk);
      index += 1;
    },
  });
}

/** Range reads of a local file, each opening and closing its own handle. */
export function fileRangeReader(file: string): (offset: number, length: number) => Promise<Uint8Array> {
  return async (offset, length) => {
    const fh = await fs.promises.open(file, "r");
    try {
      const out = new Uint8Array(length);
      await fh.read(out, 0, length, offset);
      return out;
    } finally {
      await fh.close();
    }
  };
}
