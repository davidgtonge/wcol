import test from "node:test";
import assert from "node:assert/strict";
import { createWorkerCore } from "../src/runtime/workers/core.ts";
import type { WasmBindings } from "../src/runtime/wasm/wasm.ts";
import type { HeaderInfo, QueryPlan } from "../src/runtime/core/types.ts";
import type { WorkerOutboundMessage } from "../src/runtime/workers/protocol.ts";

function createMockHeader(): HeaderInfo {
  return {
    version: 6,
    flags: 0,
    ncols: 1,
    nchunks: 1,
    rowsPerChunk: 1,
    totalRows: 1,
    schemaOff: 0,
    schemaLen: 0,
    indexOff: 0,
    indexLen: 0,
    dictOff: 0,
    dictLen: 0,
    dataOff: 0,
    dictRawLen: 0
  };
}

function createMockWasm(): WasmBindings {
  const memory = new Uint8Array(1024 * 1024);
  let ptr = 0;
  let nextPlan = 1;
  return {
    exports: {
      create_runtime: () => 1,
      create_plan: () => {
        nextPlan += 1;
        return nextPlan;
      },
      destroy_plan: () => undefined,
      plan_reset_results: () => undefined,
      plan_apply_sql: () => 0,
      plan_exec_chunk: () => 0,
      plan_row_order_by_len: () => 0,
      plan_group_key_count: () => 0,
      plan_limit: () => 0,
      plan_set_limit: () => 0,
      plan_finalize_rows: () => 0,
      runtime_set_header: () => 0,
      runtime_set_schema: () => 0,
      runtime_set_toc: () => 0,
      runtime_set_dicts: () => 0
    } as unknown as WasmBindings["exports"],
    alloc: (size: number) => {
      const out = ptr;
      ptr += Math.max(size, 1);
      return out;
    },
    free: () => undefined,
    memoryU8: () => memory
  };
}

test("worker-core emits lifecycle messages for init, plan, task", async () => {
  const messages: WorkerOutboundMessage[] = [];
  const transfers: number[] = [];
  const core = createWorkerCore({
    postMessage: (msg, transfer) => {
      messages.push(msg);
      transfers.push(transfer?.length ?? 0);
    },
    deps: {
      loadWasm: async () => createMockWasm(),
      bindRuntime: async () => ({ runtime: 1, header: createMockHeader() }),
      applyPlan: async (_ctx, _plan: QueryPlan, _handle: number) => undefined,
      readRowBytes: async () => new Uint8Array([1, 2, 3]),
      readRowCandidateBytes: async () => new Uint8Array(0),
      readAggregateBytes: async () => new Uint8Array([4]),
      readGroupBytes: async () => new Uint8Array([5, 6])
    }
  });

  await core.onMessage({
    type: "init",
    header: new Uint8Array([1]),
    schema: new Uint8Array([2]),
    toc: new Uint8Array([3])
  });
  await core.onMessage({
    type: "plan",
    planSpec: { limit: 10 }
  });
  await core.onMessage({
    type: "task",
    taskId: 7,
    chunkId: 0,
    descs: new Uint32Array([1, 2, 3, 4, 5]),
    data: new Uint8Array([9, 8])
  });

  assert.equal(messages[0]?.type, "ready");
  assert.equal(messages[1]?.type, "planned");
  assert.equal(messages[2]?.type, "result");
  if (messages[2]?.type === "result") {
    assert.equal(messages[2].taskId, 7);
    assert.equal(messages[2].result.chunkId, 0);
    assert.deepEqual(Array.from(messages[2].result.rows), [1, 2, 3]);
    assert.deepEqual(Array.from(messages[2].result.aggs), [4]);
    assert.deepEqual(Array.from(messages[2].result.groups), [5, 6]);
  }
  assert.equal(transfers[2], 3);
});

test("worker-core returns error for plan before init", async () => {
  const messages: WorkerOutboundMessage[] = [];
  const core = createWorkerCore({
    postMessage: (msg) => {
      messages.push(msg);
    },
    deps: {
      loadWasm: async () => createMockWasm(),
      bindRuntime: async () => ({ runtime: 1, header: createMockHeader() })
    }
  });

  await core.onMessage({ type: "plan", sql: "SELECT 1" });
  assert.equal(messages.length, 1);
  assert.equal(messages[0]?.type, "error");
});

test("worker-core returns error for task before init", async () => {
  const messages: WorkerOutboundMessage[] = [];
  const core = createWorkerCore({
    postMessage: (msg) => {
      messages.push(msg);
    },
    deps: {
      loadWasm: async () => createMockWasm(),
      bindRuntime: async () => ({ runtime: 1, header: createMockHeader() })
    }
  });

  await core.onMessage({ type: "task", taskId: 1, chunkId: 0, descs: new Uint32Array(0), data: new Uint8Array(0) });
  assert.equal(messages.length, 1);
  assert.equal(messages[0]?.type, "error");
});

test("worker-core returns error for invalid message payload", async () => {
  const messages: WorkerOutboundMessage[] = [];
  const core = createWorkerCore({
    postMessage: (msg) => {
      messages.push(msg);
    }
  });

  await core.onMessage({ bad: true });
  assert.equal(messages.length, 1);
  assert.equal(messages[0]?.type, "error");
});
