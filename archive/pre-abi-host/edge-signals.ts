/**
 * Coordinator-routed edge signals. Tickets are signed only on the job Durable Object;
 * nodes fetch a ticket then open one WebSocket per task.
 */

import { SignalClient } from "./signal-client.ts";
import { hashSignalEdge, treeSignalEdge } from "./signal-wire.ts";
import type { ByteTransport, GraphTaskEnv } from "../../cloudflare/src/node/node.ts";

export type SignalRoute = "p2p" | "coordinator";

export type SignalCoordinator = {
  issueSignalTicket(params: { jobId: string; taskId: string; epoch: number }): Promise<{ ticket: string }>;
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
};

export function parseSignalRoute(bytes: ByteTransport, value: unknown): SignalRoute {
  if (value === "coordinator") {
    if (bytes !== "r2") {
      throw new Error("WCOL_EDGE_SIGNALS=coordinator requires WCOL_EDGE_BYTES=r2");
    }
    return "coordinator";
  }
  return "p2p";
}

export function signalTaskName(taskId: number | "coordinator"): string {
  return taskId === "coordinator" ? "coordinator" : `t${taskId}`;
}

export function payloadSignalEdge(fromTaskId: number, consumerTaskId?: number): string {
  return consumerTaskId == null ? `t${fromTaskId}->coordinator` : treeSignalEdge(fromTaskId, consumerTaskId);
}

export { hashSignalEdge, treeSignalEdge };

export function coordinatorStub(env: GraphTaskEnv, jobId: string): SignalCoordinator {
  if (!env.WCOL_COORDINATOR) {
    throw new Error("WCOL_COORDINATOR binding is missing for coordinator signals");
  }
  return env.WCOL_COORDINATOR.get(env.WCOL_COORDINATOR.idFromName(jobId)) as unknown as SignalCoordinator;
}

export async function connectTaskSignals(
  env: GraphTaskEnv,
  jobId: string,
  epoch: number,
  taskId: number | "coordinator",
  ticket?: string,
): Promise<SignalClient> {
  const stub = coordinatorStub(env, jobId);
  const name = signalTaskName(taskId);
  const token = ticket ?? (await stub.issueSignalTicket({ jobId, taskId: name, epoch })).ticket;
  return SignalClient.connect({
    url: `https://coordinator/q/${jobId}/ws`,
    ticket: token,
    createSocket: async (url) => {
      const response = await stub.fetch(url, { headers: { Upgrade: "websocket" } });
      const socket = response.webSocket;
      if (response.status !== 101 || !socket) {
        throw new Error(`signal router upgrade failed: ${response.status}`);
      }
      socket.accept();
      return socket;
    },
  });
}
