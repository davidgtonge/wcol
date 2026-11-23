/**
 * Node tasks running in this isolate, and the memory they hold.
 *
 * The runtime can place several node Durable Objects in one isolate (we have seen two
 * reducers share one), and they then share its 128 MB. Module scope is shared by exactly
 * those objects, so this registry is module state on purpose: it is the only place that
 * sees every task in the isolate. Tasks join when they start and leave in `finally`.
 */

export type TaskRole = "scan" | "reducer";

/** One task's memory right now. */
export type TaskMemory = {
  readonly wasmBytes: number;
  /** Bytes the TS host holds outside Wasm (see `HostBytes`). */
  readonly hostBytes: number;
};

type Tenant = { readonly role: TaskRole; readonly taskId: number; readonly memory: () => TaskMemory };

const tenants = new Set<Tenant>();

export type IsolateLoad = {
  readonly tasks: number;
  readonly reducers: number;
  readonly scans: number;
  readonly wasmBytes: number;
  readonly hostBytes: number;
};

/** Register a running task. The returned function removes it; call it exactly once. */
export function joinIsolate(role: TaskRole, taskId: number, memory: () => TaskMemory): () => void {
  const tenant: Tenant = { role, taskId, memory };
  tenants.add(tenant);
  return () => {
    tenants.delete(tenant);
  };
}

export function isolateLoad(): IsolateLoad {
  let reducers = 0;
  let wasmBytes = 0;
  let hostBytes = 0;
  for (const tenant of tenants) {
    if (tenant.role === "reducer") reducers += 1;
    const memory = tenant.memory();
    wasmBytes += memory.wasmBytes;
    hostBytes += memory.hostBytes;
  }
  return { tasks: tenants.size, reducers, scans: tenants.size - reducers, wasmBytes, hostBytes };
}

/** The most tasks, and the most memory, this isolate held while one task ran. */
export type IsolatePeak = { readonly tasks: number; readonly bytes: number; readonly at: IsolateLoad };

export function createIsolatePeak(): { observe(): void; peak(): IsolatePeak } {
  let at = isolateLoad();
  let tasks = at.tasks;
  let bytes = at.wasmBytes + at.hostBytes;
  return {
    observe() {
      const load = isolateLoad();
      tasks = Math.max(tasks, load.tasks);
      if (load.wasmBytes + load.hostBytes > bytes) {
        bytes = load.wasmBytes + load.hostBytes;
        at = load;
      }
    },
    peak: () => ({ tasks, bytes, at }),
  };
}
