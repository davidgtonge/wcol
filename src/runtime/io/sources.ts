import { ByteSource, LocalFileSource, HttpRangeSource } from "./byte-source.ts";

export function normalizeSource(source: ByteSource | File | string): ByteSource {
  if (source instanceof ByteSource) {
    return source;
  }
  if (typeof File !== "undefined" && source instanceof File) {
    return new LocalFileSource(source);
  }
  if (typeof source === "string") {
    return new HttpRangeSource(source);
  }
  throw new Error("Unsupported ByteSource");
}
