import type { QueryPlan, RuntimeInitBytes } from "../core/types.ts";

export type WorkerTaskPayload = {
  chunkId: number;
  descs: Uint32Array;
  data: Uint8Array;
};

export type WorkerPartialResult = {
  chunkId: number;
  rows: Uint8Array;
  aggs: Uint8Array;
  groups: Uint8Array;
  rowCandidates?: Uint8Array;
};

export type WorkerInitMessage = RuntimeInitBytes & {
  type: "init";
};

export type WorkerPlanMessage = {
  type: "plan";
  planSpec?: QueryPlan;
  sql?: string;
};

export type WorkerTaskMessage = WorkerTaskPayload & {
  type: "task";
  taskId: number;
};

export type WorkerReadyMessage = {
  type: "ready";
};

export type WorkerPlannedMessage = {
  type: "planned";
};

export type WorkerResultMessage = {
  type: "result";
  taskId: number;
  result: WorkerPartialResult;
};

export type WorkerErrorMessage = {
  type: "error";
  error: string;
};

export type WorkerInboundMessage = WorkerInitMessage | WorkerPlanMessage | WorkerTaskMessage;
export type WorkerOutboundMessage =
  | WorkerReadyMessage
  | WorkerPlannedMessage
  | WorkerResultMessage
  | WorkerErrorMessage;

export function taskTransferables(task: WorkerTaskPayload): ArrayBuffer[] {
  const transfer: ArrayBuffer[] = [];
  if (task.descs.byteLength) {
    transfer.push(task.descs.buffer as ArrayBuffer);
  }
  if (task.data.byteLength) {
    transfer.push(task.data.buffer as ArrayBuffer);
  }
  return transfer;
}

export function resultTransferables(result: WorkerPartialResult): ArrayBuffer[] {
  const transfer: ArrayBuffer[] = [];
  if (result.rows.byteLength) {
    transfer.push(result.rows.buffer as ArrayBuffer);
  }
  if (result.aggs.byteLength) {
    transfer.push(result.aggs.buffer as ArrayBuffer);
  }
  if (result.groups.byteLength) {
    transfer.push(result.groups.buffer as ArrayBuffer);
  }
  if (result.rowCandidates?.byteLength) {
    transfer.push(result.rowCandidates.buffer as ArrayBuffer);
  }
  return transfer;
}
