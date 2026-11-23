/**
 * Miniflare-only Durable Object with the hash-shuffle RPC surface and no Wasm.
 * Production jobs use {@link WcolNode} in wcol-node.ts.
 */

import { DurableObject } from "cloudflare:workers";
import { graphSlotObjectName } from "../../cloudflare/src/node.ts";
import { pushPartitionedScan } from "./hash-exchange.ts";
import { createSharedBytePool } from "./shared-byte-pool.ts";
import {
  createReducerIngressSession,
  type ReducerIngressSession,
} from "../../src/runtime/host/scan-edge-session.ts";
import type {
  AcceptScanEdgeParams,
  HashAttemptParams,
  HashScanDestination,
  PrepareReducerParams,
  PushHashFramesParams,
} from "../../cloudflare/src/node.ts";

export type HashFanoutEnv = {
  WCOL_NODE: DurableObjectNamespace;
};

function nodeStub(env: HashFanoutEnv, jobId: string, attempt: number, slot: number) {
  return env.WCOL_NODE.get(env.WCOL_NODE.idFromName(graphSlotObjectName(jobId, attempt, slot)));
}

export class WcolNode extends DurableObject<HashFanoutEnv> {
  private session: ReducerIngressSession | null = null;

  async prepareReducer(params: PrepareReducerParams) {
    if (
      this.session &&
      (this.session.jobId !== params.jobId || this.session.executionAttempt !== params.executionAttempt)
    ) {
      this.session.abort(new Error("superseded hash attempt"));
      this.session = null;
    }
    this.session ??= createReducerIngressSession({
      jobId: params.jobId,
      executionAttempt: params.executionAttempt,
      expectedScans: params.expectedScans,
    });
    return {
      ready: true as const,
      invokedSlots: [params.slot],
      body: new ReadableStream<Uint8Array>({
        start(controller) {
          controller.close();
        },
      }),
    };
  }

  async acceptScanEdge(params: AcceptScanEdgeParams) {
    if (!this.session) throw new Error("reducer ingress session is missing");
    return this.session.acceptScanEdge(params.scanIndex, params.stream);
  }

  async waitConsumeReady(params: HashAttemptParams) {
    if (params.scanIndex == null) throw new Error("waitConsumeReady needs scanIndex");
    if (!this.session) throw new Error("reducer ingress session is missing");
    this.session.assertAttempt(params.jobId, params.executionAttempt);
    return this.session.waitConsumeReady(params.scanIndex);
  }

  async completeScan(params: HashAttemptParams) {
    if (params.scanIndex == null) throw new Error("completeScan needs scanIndex");
    if (!this.session) throw new Error("reducer ingress session is missing");
    this.session.assertAttempt(params.jobId, params.executionAttempt);
    this.session.completeScan(params.scanIndex);
  }

  async sealReducer(params: HashAttemptParams) {
    if (!this.session) throw new Error("reducer ingress session is missing");
    this.session.assertAttempt(params.jobId, params.executionAttempt);
    this.session.seal();
  }

  async abortHashAttempt(params: HashAttemptParams & { error?: string }) {
    this.session?.assertAttempt(params.jobId, params.executionAttempt);
    this.session?.abort(new Error(params.error ?? "hash attempt aborted"));
    this.session = null;
  }

  async readScanEdge(params: HashAttemptParams) {
    if (params.scanIndex == null) throw new Error("readScanEdge needs scanIndex");
    if (!this.session) throw new Error("reducer ingress session is missing");
    this.session.assertAttempt(params.jobId, params.executionAttempt);
    const stream = await this.session.waitForStream(params.scanIndex);
    const parts: Uint8Array[] = [];
    const reader = stream.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      parts.push(value);
    }
    const out = new Uint8Array(parts.reduce((n, p) => n + p.byteLength, 0));
    let at = 0;
    for (const part of parts) {
      out.set(part, at);
      at += part.byteLength;
    }
    return out;
  }

  async pushHashFrames(params: PushHashFramesParams) {
    let index = 0;
    const input = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (index >= params.chunks.length) {
          controller.close();
          return;
        }
        controller.enqueue(params.chunks[index]!);
        index += 1;
      },
    });
    const destinations = [...params.destinations].sort((a, b) => a.partition - b.partition);
    const env = this.env;
    return pushPartitionedScan({
      input,
      partitions: destinations.length,
      openingConcurrency: params.openingConcurrency,
      maxPendingWrites: params.maxActiveEgress,
      pool: createSharedBytePool(params.demuxBytes ?? 2 * 1024 * 1024),
      accept: async (partition, stream) => {
        const dest = destinations[partition] as HashScanDestination | undefined;
        if (!dest) throw new Error(`hash scan missing destination ${partition}`);
        const stub = nodeStub(env, params.jobId, params.executionAttempt, dest.slot) as unknown as {
          acceptScanEdge(args: AcceptScanEdgeParams): Promise<{ accepted: true }>;
        };
        await stub.acceptScanEdge({
          jobId: params.jobId,
          executionAttempt: params.executionAttempt,
          scanTaskId: params.scanTaskId,
          scanIndex: params.scanIndex,
          partition,
          stream,
        });
      },
    });
  }
}

export default {
  fetch() {
    return new Response("hash-fanout-test", { status: 200 });
  },
};
