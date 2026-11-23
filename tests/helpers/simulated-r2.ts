/**
 * R2 as a scan node sees it: each read waits a latency, then transfers over that node's
 * link at a fixed bandwidth (one link per scan, transfers in order), optionally under a
 * cap shared by every scan. Deterministic: jitter comes from a seeded generator.
 */

import type { ReadAt } from "../../src/runtime/coordinator/plan-dispatch.ts";

export type SimulatedR2 = {
  /** Time to first byte, in ms. */
  latencyMs: number;
  /** Latency varies uniformly by ± this fraction. */
  jitter: number;
  /** One scan node's bandwidth, MB/s. */
  scanMBps: number;
  /** All scans together, MB/s; 0 for no shared cap. */
  totalMBps: number;
  seed: number;
};

export type SimulatedRead = { taskId: number; issuedAt: number; doneAt: number; bytes: number };

export function simulatedR2(config: SimulatedR2, log: SimulatedRead[] = []) {
  let state = config.seed >>> 0 || 1;
  const random = () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return (state >>> 0) / 0x1_0000_0000;
  };
  let sharedFreeAt = 0;
  const links = new Map<number, number>();
  const transferMs = (bytes: number, mbps: number) => (mbps > 0 ? (bytes / (mbps * 1e6)) * 1000 : 0);
  return {
    log,
    wrap(taskId: number, read: ReadAt): ReadAt {
      return async (offset, length) => {
        const issuedAt = performance.now();
        const bytes = await read(offset, length);
        const firstByte = issuedAt + config.latencyMs * (1 + config.jitter * (2 * random() - 1));
        let done = Math.max(firstByte, links.get(taskId) ?? 0) + transferMs(bytes.byteLength, config.scanMBps);
        if (config.totalMBps > 0) {
          done = Math.max(done, sharedFreeAt + transferMs(bytes.byteLength, config.totalMBps));
          sharedFreeAt = done;
        }
        links.set(taskId, done);
        const wait = done - performance.now();
        if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
        log.push({ taskId, issuedAt, doneAt: performance.now(), bytes: bytes.byteLength });
        return bytes;
      };
    },
  };
}
