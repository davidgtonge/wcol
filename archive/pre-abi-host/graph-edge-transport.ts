import type { ExecutionEdge } from "../../src/runtime/coordinator/execution-graph.ts";
import type { TaskIdentity } from "./task-identity.ts";
import { createBoundedBytePipe } from "../../src/runtime/host/ports/rpc.ts";
import type { SharedBytePool } from "./shared-byte-pool.ts";

export type EdgeOpenContext = {
  edge: ExecutionEdge;
  producer: TaskIdentity;
};

export type TaskWcsEgressIdentity = {
  jobId: string;
  epoch: number;
  taskId: string;
  attempt: number;
};

export type DoRpcEdgeOpener = {
  openTaskWcsEgress(identity: TaskWcsEgressIdentity): Promise<{ body: ReadableStream<Uint8Array> }>;
};

export function edgeKey(from: number, to: number, attempt: number): string {
  return `${from}->${to}@${attempt}`;
}

export function startInProcessEdge(
  bufferCap = 256 * 1024,
  pool?: SharedBytePool,
): {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
} {
  const cap = bufferCap > 0 ? bufferCap : 256 * 1024;
  const pipe = createBoundedBytePipe({ maxBufferedBytes: cap, pool });
  return { readable: pipe.readable, writable: pipe.writable };
}

/** Authoritative transport from the physical plan edge descriptor. */
export async function openEdgeWcsStream(
  ctx: EdgeOpenContext,
  deps: {
    inProcess?: () => ReadableStream<Uint8Array>;
    rpc?: DoRpcEdgeOpener;
    fetch?: (url: string, init?: RequestInit) => Promise<Response>;
    fetchUrl?: string;
  },
): Promise<ReadableStream<Uint8Array>> {
  switch (ctx.edge.transport) {
    case "in_process": {
      if (!deps.inProcess) throw new Error("in_process edge missing local pipe");
      return deps.inProcess();
    }
    case "do_rpc_wcs": {
      if (!deps.rpc) throw new Error("do_rpc_wcs edge missing RPC opener");
      const handle = await deps.rpc.openTaskWcsEgress({
        jobId: ctx.producer.queryId,
        epoch: 0,
        taskId: String(ctx.producer.taskId),
        attempt: ctx.producer.attempt,
      });
      return handle.body;
    }
    case "do_fetch_wcs": {
      if (!deps.fetch || !deps.fetchUrl) throw new Error("do_fetch_wcs edge missing fetch URL");
      const response = await deps.fetch(deps.fetchUrl);
      if (!response.ok || !response.body) {
        throw new Error(`edge fetch ${response.status}`);
      }
      return response.body;
    }
    default:
      throw new Error("unsupported edge transport");
  }
}
