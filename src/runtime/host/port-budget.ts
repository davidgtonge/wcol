/**
 * Per-port buffer sizes for a task with `ports` streams. Each kind of buffer has a fixed
 * total, split across the ports: widening a job to more scans or reducers shrinks each
 * port's buffers instead of growing the isolate's memory. Sizes are powers of two between
 * a floor and the size every port had before the split (still used up to 64 ports).
 */

const KiB = 1024;

export type PortBudget = { readonly totalBytes: number; readonly minBytes: number; readonly maxBytes: number };

/** Stream pool input rings: a reducer's scan edges. */
export const INPUT_RINGS: PortBudget = { totalBytes: 4096 * KiB, minBytes: 16 * KiB, maxBytes: 64 * KiB };
/** Stream pool output slots: a scan's partition outputs. */
export const OUTPUT_SLOTS: PortBudget = { totalBytes: 8192 * KiB, minBytes: 32 * KiB, maxBytes: 128 * KiB };
/**
 * A reducer's read-ahead on each scan edge, in front of its input ring. The RPC body arrives
 * in 4 KiB chunks; unbuffered, an edge moved about a chunk per round trip. Buffered at
 * 64 KiB, Q33 reducers took in 15 MB/s instead of 9.
 */
export const EDGE_READ_AHEAD: PortBudget = { totalBytes: 4096 * KiB, minBytes: 16 * KiB, maxBytes: 64 * KiB };

export function portBytes(budget: PortBudget, ports: number): number {
  const share = budget.totalBytes / Math.max(1, ports);
  const pow2 = 2 ** Math.floor(Math.log2(Math.max(1, share)));
  return Math.min(budget.maxBytes, Math.max(budget.minBytes, pow2));
}

export function portKiB(budget: PortBudget, ports: number): number {
  return portBytes(budget, ports) / KiB;
}
