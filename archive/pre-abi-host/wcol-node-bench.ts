import {
  handleWcsBenchConsumePartialWcs,
  handleWcsBenchConsumeShardWcs,
  handleWcsBenchConsumeWcs,
  handleWcsBenchDrain,
  handleWcsBenchMetrics,
  handleWcsBenchPartialWcsSource,
  handleWcsBenchRpcPipe,
  handleWcsBenchShardWcsSource,
  handleWcsBenchSource,
  handleWcsBenchSourceWcs,
  handleWcsIngressShardRpc,
  openRawByteEgressRpc,
  openShardWcsEgressRpc,
  type RawByteEgressParams,
  type ShardWcsEgressParams,
} from "./wcs-bench-node.ts";
import {
  WCS_BENCH_RPC_PIPE,
  WCS_EGRESS_METRICS,
  WCS_EGRESS_SHARD,
  WCS_INGRESS_COPY,
  WCS_INGRESS_SHARD,
  WCS_INGRESS_SHARD_RPC,
} from "./inter-do-wcs-paths.ts";
import { WcolNode as WcolNodeProd } from "../../cloudflare/src/wcol-node.ts";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/** Production graph node plus WCS transport benchmark routes (dev / bench Worker only). */
export class WcolNode extends WcolNodeProd {
  private benchProducerCtx() {
    return {
      storage: this.nodeState.storage,
      waitUntil: (p: Promise<unknown>) => this.nodeState.waitUntil(p),
    };
  }

  /** Benchmark: native RPC byte stream (parity with `POST` `WCS_EGRESS_SHARD`). */
  async openInterDoShardWcsEgress(params: ShardWcsEgressParams) {
    return openShardWcsEgressRpc(this.benchProducerCtx(), params);
  }

  /** Benchmark: native RPC raw bytes (bounded pipe smoke). */
  async openInterDoRawByteEgress(params: RawByteEgressParams) {
    return openRawByteEgressRpc(this.benchProducerCtx(), params);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (url.pathname === "/wcs/bench/source" && request.method === "GET") {
        return await handleWcsBenchSource(request, this.benchProducerCtx());
      }
      if (url.pathname === "/wcs/bench/metrics" && request.method === "GET") {
        return await handleWcsBenchMetrics(request, this.benchProducerCtx());
      }
      if (url.pathname === WCS_EGRESS_METRICS && request.method === "GET") {
        return await handleWcsBenchMetrics(request, this.benchProducerCtx());
      }
      if (url.pathname === "/wcs/bench/drain" && request.method === "POST") {
        return await handleWcsBenchDrain(request, this.nodeEnv);
      }
      if (url.pathname === "/wcs/bench/wcs-source" && request.method === "GET") {
        return await handleWcsBenchSourceWcs(request, this.benchProducerCtx());
      }
      if (url.pathname === "/wcs/bench/shard-wcs-source" && request.method === "POST") {
        return await handleWcsBenchShardWcsSource(request, this.benchProducerCtx());
      }
      if (url.pathname === WCS_EGRESS_SHARD && request.method === "POST") {
        return await handleWcsBenchShardWcsSource(request, this.benchProducerCtx());
      }
      if (url.pathname === "/wcs/bench/partial-wcs-source" && request.method === "GET") {
        return await handleWcsBenchPartialWcsSource(request, this.benchProducerCtx());
      }
      if (url.pathname === "/wcs/bench/consume-wcs" && request.method === "POST") {
        return await handleWcsBenchConsumeWcs(request, this.nodeEnv);
      }
      if (url.pathname === WCS_INGRESS_COPY && request.method === "POST") {
        return await handleWcsBenchConsumeWcs(request, this.nodeEnv);
      }
      if (url.pathname === "/wcs/bench/consume-shard-wcs" && request.method === "POST") {
        return await handleWcsBenchConsumeShardWcs(request, this.nodeEnv);
      }
      if (url.pathname === WCS_INGRESS_SHARD && request.method === "POST") {
        return await handleWcsBenchConsumeShardWcs(request, this.nodeEnv);
      }
      if (url.pathname === WCS_INGRESS_SHARD_RPC && request.method === "POST") {
        return await handleWcsIngressShardRpc(request, this.nodeEnv);
      }
      if (url.pathname === WCS_BENCH_RPC_PIPE && request.method === "POST") {
        return await handleWcsBenchRpcPipe(request, this.nodeEnv);
      }
      if (url.pathname === "/wcs/bench/consume-partial-wcs" && request.method === "POST") {
        return await handleWcsBenchConsumePartialWcs(request, this.nodeEnv);
      }
      return json({ error: "not found" }, 404);
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      return json({ error: error.message }, 500);
    }
  }
}
