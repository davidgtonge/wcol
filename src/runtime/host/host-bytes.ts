/**
 * Bytes one task's TS host holds outside Wasm linear memory, by owner. Workers expose no
 * heap statistics, so these are the buffers this host allocates and frees itself: what the
 * isolate's memory limit sees beyond Wasm, less the runtime's own RPC and stream buffers.
 */
export type HostByteOwner =
  /** Scan edge bytes received over RPC and not yet copied into Wasm. */
  | "edges"
  /** Copies of spill run parts while they upload to R2. */
  | "spillUploads"
  /** Spill run ranges read ahead of the merge. */
  | "spillPrefetch";

const OWNERS: readonly HostByteOwner[] = ["edges", "spillUploads", "spillPrefetch"];

export type HostBytesSnapshot = {
  readonly current: Readonly<Record<HostByteOwner, number>>;
  readonly peak: Readonly<Record<HostByteOwner, number>>;
  /** Peak of the sum, which can be below the sum of the per-owner peaks. */
  readonly peakTotal: number;
};

export type HostBytes = {
  /** Positive when the host takes bytes, negative when it lets them go. */
  add(owner: HostByteOwner, bytes: number): void;
  total(): number;
  snapshot(): HostBytesSnapshot;
};

const zeros = (): Record<HostByteOwner, number> =>
  Object.fromEntries(OWNERS.map((owner) => [owner, 0])) as Record<HostByteOwner, number>;

export function createHostBytes(): HostBytes {
  const current = zeros();
  const peak = zeros();
  let total = 0;
  let peakTotal = 0;
  return {
    add(owner, bytes) {
      current[owner] += bytes;
      total += bytes;
      if (current[owner] > peak[owner]) peak[owner] = current[owner];
      if (total > peakTotal) peakTotal = total;
    },
    total: () => total,
    snapshot: () => ({ current: { ...current }, peak: { ...peak }, peakTotal }),
  };
}
