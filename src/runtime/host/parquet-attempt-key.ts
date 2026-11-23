/** Canonical attempt object keys (matches Rust `attempt_object_key`). */

export function taskIdHex(taskId: number): string {
  const view = new DataView(new ArrayBuffer(8));
  view.setBigUint64(0, BigInt(taskId >>> 0), true);
  return [...new Uint8Array(view.buffer)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export function attemptObjectKeyJobs(
  jobId: string,
  taskId: number,
  attempt: number,
  fileName = "part.parquet",
): string {
  return `_jobs/${jobId}/${taskIdHex(taskId)}/${attempt}/${fileName}`;
}

export function finalizerObjectKeyJobs(jobId: string, attempt: number, fileName = "result.parquet"): string {
  return `_jobs/${jobId}/finalizer/${attempt}/${fileName}`;
}
