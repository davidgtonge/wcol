/** The narrow R2 binding surface the host uses: puts, ranged gets, multipart uploads, cleanup. */

export type R2UploadedPart = { partNumber: number; etag: string };

export type R2MultipartUpload = {
  readonly uploadId: string;
  uploadPart(partNumber: number, value: Uint8Array): Promise<R2UploadedPart>;
  complete(uploadedParts: R2UploadedPart[]): Promise<unknown>;
  abort(): Promise<void>;
};

export type R2ObjectBody = {
  size?: number;
  etag?: string;
  httpEtag?: string;
  /** Absent when an `onlyIf` condition failed: R2 then returns the metadata only. */
  body?: ReadableStream<Uint8Array> | null;
  customMetadata?: Record<string, string>;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
};

export type R2GetOptions = {
  range?: { offset?: number; length?: number; suffix?: number };
  onlyIf?: { etagMatches?: string; etagDoesNotMatch?: string };
};

export type R2PutOptions = {
  onlyIf?: { etagDoesNotMatch?: string; etagMatches?: string };
  customMetadata?: Record<string, string>;
  httpMetadata?: { contentType?: string };
};

/** R2 binding surface used by export, spill and compaction. Bytes are not interpreted. */
export type R2OutputBucket = {
  createMultipartUpload(key: string, options?: R2PutOptions): Promise<R2MultipartUpload>;
  resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload;
  put(
    key: string,
    value: string | Uint8Array | ReadableStream,
    options?: R2PutOptions,
  ): Promise<{
    etag?: string;
    httpEtag?: string;
    size?: number;
    customMetadata?: Record<string, string>;
  } | null>;
  get(key: string, options?: R2GetOptions): Promise<R2ObjectBody | null>;
  head?(key: string): Promise<R2ObjectBody | null>;
  list?(options: { prefix?: string; cursor?: string; limit?: number }): Promise<{
    objects: { key: string; uploaded?: Date }[];
    truncated?: boolean;
    cursor?: string;
  }>;
  delete?(keys: string | string[]): Promise<void>;
};
