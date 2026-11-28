import { readWasmLastError, wasmMemoryU8 } from "./wasm.ts";
import { plannerWasm, type WcolStreamWasm } from "./wasm-stream.ts";

export type KMeansWireConfig = {
  dimensions: number;
  clusters: number;
  maxIterations: number;
  tolerance: number;
};

function withWasmInput<T>(
  wasm: WcolStreamWasm,
  parts: readonly Uint8Array[],
  use: (ptr: number, lengths: readonly number[]) => T,
): T {
  const lengths = parts.map((part) => part.byteLength);
  const total = lengths.reduce((sum, length) => sum + length, 0);
  const ptr = wasm.wcs_alloc(total);
  if (!ptr) throw new Error("wcs_alloc failed");
  try {
    let at = ptr;
    for (const [index, part] of parts.entries()) {
      wasmMemoryU8(wasm, at, part.byteLength).set(part);
      at += lengths[index]!;
    }
    return use(ptr, lengths);
  } finally {
    wasm.wcs_dealloc(ptr, total);
  }
}

function copyWasmOutput(wasm: WcolStreamWasm, length: number): Uint8Array {
  return wasmMemoryU8(wasm, plannerWasm(wasm, "K-means output").wcol_iterative_kmeans_output_ptr(), length).slice();
}

export function mergeResidentKMeansMoments(
  wasm: WcolStreamWasm,
  left: Uint8Array,
  right: Uint8Array,
  dimensions: number,
): Uint8Array {
  const fn = plannerWasm(wasm, "wcol_iterative_kmeans_merge_moments").wcol_iterative_kmeans_merge_moments;
  return withWasmInput(wasm, [left, right], (ptr, lengths) => {
    const length = fn(ptr, lengths[0]!, ptr + lengths[0]!, lengths[1]!, dimensions);
    if (!length) throw new Error(readWasmLastError(wasm, "K-means moments merge failed"));
    return copyWasmOutput(wasm, length);
  });
}

export function prepareResidentKMeans(
  wasm: WcolStreamWasm,
  moments: Uint8Array,
  config: KMeansWireConfig & { seed: number },
): { state: Uint8Array; normalization: Uint8Array } {
  const fn = plannerWasm(wasm, "wcol_iterative_kmeans_prepare").wcol_iterative_kmeans_prepare;
  return withWasmInput(wasm, [moments], (ptr, [length]) => {
    const outputLength = fn(
      ptr, length!, config.dimensions, config.clusters, config.maxIterations,
      config.tolerance, BigInt(Math.trunc(config.seed)),
    );
    if (!outputLength) throw new Error(readWasmLastError(wasm, "K-means preparation failed"));
    const packed = copyWasmOutput(wasm, outputLength);
    const view = new DataView(packed.buffer, packed.byteOffset, packed.byteLength);
    if (packed.byteLength < 12 || new TextDecoder().decode(packed.subarray(0, 4)) !== "WKP1") {
      throw new Error("invalid K-means preparation response");
    }
    const stateLength = view.getUint32(4, true);
    const normalizationLengthAt = 8 + stateLength;
    if (normalizationLengthAt + 4 > packed.byteLength) throw new Error("truncated K-means preparation response");
    const normalizationLength = view.getUint32(normalizationLengthAt, true);
    if (normalizationLengthAt + 4 + normalizationLength !== packed.byteLength) throw new Error("invalid K-means preparation response length");
    return {
      state: packed.slice(8, normalizationLengthAt),
      normalization: packed.slice(normalizationLengthAt + 4),
    };
  });
}

export function decodeResidentKMeansNormalization(bytes: Uint8Array, dimensions: number): { means: number[]; scales: number[] } {
  if (bytes.byteLength !== 8 + dimensions * 16 || new TextDecoder().decode(bytes.subarray(0, 4)) !== "WKN1") {
    throw new Error("invalid K-means normalization metadata");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(4, true) !== dimensions) throw new Error("K-means normalization dimensions mismatch");
  const values = Array.from({ length: dimensions * 2 }, (_, index) => view.getFloat64(8 + index * 8, true));
  if (values.some((value) => !Number.isFinite(value))) throw new Error("invalid K-means normalization values");
  return { means: values.slice(0, dimensions), scales: values.slice(dimensions) };
}

export function mergeResidentKMeansPartials(
  wasm: WcolStreamWasm,
  left: Uint8Array,
  right: Uint8Array,
  config: KMeansWireConfig,
): Uint8Array {
  const fn = plannerWasm(wasm, "wcol_iterative_kmeans_merge").wcol_iterative_kmeans_merge;
  return withWasmInput(wasm, [left, right], (ptr, lengths) => {
    const length = fn(
      ptr, lengths[0]!, ptr + lengths[0]!, lengths[1]!,
      config.dimensions,
      config.clusters,
      config.maxIterations,
      config.tolerance,
    );
    if (!length)
      throw new Error(readWasmLastError(wasm, "K-means partial merge failed"));
    return copyWasmOutput(wasm, length);
  });
}

export function updateResidentKMeans(
  wasm: WcolStreamWasm,
  state: Uint8Array,
  partial: Uint8Array,
  config: KMeansWireConfig,
): { state: Uint8Array; converged: boolean; inertia: number } {
  const fn = plannerWasm(wasm, "wcol_iterative_kmeans_update").wcol_iterative_kmeans_update;
  return withWasmInput(wasm, [state, partial], (ptr, lengths) => {
    const length = fn(
      ptr, lengths[0]!, ptr + lengths[0]!, lengths[1]!,
      config.dimensions,
      config.clusters,
      config.maxIterations,
      config.tolerance,
    );
    if (!length)
      throw new Error(readWasmLastError(wasm, "K-means global update failed"));
    const bytes = copyWasmOutput(wasm, length);
    if (
      bytes.byteLength < 17 ||
      new TextDecoder().decode(bytes.subarray(0, 4)) !== "WKR1"
    )
      throw new Error("invalid K-means update response");
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const stateLength = view.getUint32(5, true);
    if (bytes.byteLength !== 17 + stateLength)
      throw new Error("invalid K-means update state length");
    return {
      state: bytes.slice(9, 9 + stateLength),
      converged: bytes[4] === 1,
      inertia: view.getFloat64(9 + stateLength, true),
    };
  });
}

export function residentKMeansCentroids(
  wasm: WcolStreamWasm,
  state: Uint8Array,
  config: KMeansWireConfig,
): number[] {
  const fn = plannerWasm(wasm, "wcol_iterative_kmeans_centroids_json").wcol_iterative_kmeans_centroids_json;
  return withWasmInput(wasm, [state], (ptr, [length]) => {
    const outputLength = fn(ptr, length!, config.dimensions, config.clusters, config.maxIterations, config.tolerance);
    if (!outputLength) throw new Error(readWasmLastError(wasm, "K-means artifact serialization failed"));
    const artifact = JSON.parse(new TextDecoder().decode(copyWasmOutput(wasm, outputLength))) as { centroids?: unknown };
    if (!Array.isArray(artifact.centroids) || !artifact.centroids.every((value) => typeof value === "number" && Number.isFinite(value))) {
      throw new Error("invalid K-means centroid artifact");
    }
    return artifact.centroids as number[];
  });
}

