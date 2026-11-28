/**
 * Miniflare Durable Object for session-fence.test.ts.
 * An older cleanup awaits a macrotask, then tries to release an id
 * that a newer execution already replaced.
 */

import { DurableObject } from "cloudflare:workers";
import { beginNodeExecution, releaseIfOwner, type NodeExecution, type SessionEnd } from "../cloudflare/src/node/session.ts";

type FenceEnv = {
  FENCE: {
    idFromName(name: string): unknown;
    get(id: unknown): {
      exercise(): Promise<FenceResult>;
    };
  };
};

type FenceResult = {
  lateReleased: boolean;
  currentId: number | null;
  lastReason: string | null;
};

function macrotask(): Promise<void> {
  const scheduler = (globalThis as { scheduler?: { wait?: (delay: number) => Promise<void> } }).scheduler;
  if (typeof scheduler?.wait === "function") return scheduler.wait(0);
  return new Promise((resolve) => {
    setTimeout(resolve, 0);
  });
}

export class SessionFence extends DurableObject<FenceEnv> {
  async exercise(): Promise<FenceResult> {
    let current: NodeExecution | null = beginNodeExecution(null, 1, { jobId: "j", executionAttempt: 1 });
    const oldId = current.id;
    // Assigned inside a callback, so the annotation keeps TypeScript from narrowing it to null.
    let lastEnd = null as SessionEnd | null;
    const release = (executionId: number, reason: string): boolean => {
      const released = releaseIfOwner(current, executionId, reason);
      if (!released.ended) return false;
      lastEnd = released.ended;
      current = released.current;
      return true;
    };
    const late = (async () => {
      await macrotask();
      return release(oldId, "stream settled");
    })();
    release(oldId, "abortHashAttempt");
    current = beginNodeExecution(current, 2, { jobId: "j", executionAttempt: 2 });
    const lateReleased = await late;
    return {
      lateReleased,
      currentId: current?.id ?? null,
      lastReason: lastEnd?.reason ?? null,
    };
  }
}

export default {
  async fetch(_request: Request, env: FenceEnv): Promise<Response> {
    const stub = env.FENCE.get(env.FENCE.idFromName("fence"));
    return Response.json(await stub.exercise());
  },
};
