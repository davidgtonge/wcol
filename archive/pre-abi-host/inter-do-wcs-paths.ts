/**
 * Stable inter-DO WCS route names (Milestone F). Bench paths remain for CI compatibility.
 */

/** Producer: bounded GraphHost WorkerShard WCS stream. */
export const WCS_EGRESS_SHARD = "/wcs/egress/shard";
export const WCS_EGRESS_SHARD_BENCH = "/wcs/bench/shard-wcs-source";

/** Producer: pipe metrics for backpressure checks. */
export const WCS_EGRESS_METRICS = "/wcs/egress/metrics";
export const WCS_EGRESS_METRICS_BENCH = "/wcs/bench/metrics";

/** Consumer: validate+copy WCS1 from remote producer DO. */
export const WCS_INGRESS_COPY = "/wcs/ingress/copy";
export const WCS_INGRESS_COPY_BENCH = "/wcs/bench/consume-wcs";

/** Consumer: GraphHost shard wire through Rust guard. */
export const WCS_INGRESS_SHARD = "/wcs/ingress/shard";
export const WCS_INGRESS_SHARD_BENCH = "/wcs/bench/consume-shard-wcs";

/** Worker orchestration (consumer DO). */
export const WCS_STREAM_SHARD = "/wcs/stream/shard";
export const WCS_STREAM_SHARD_BENCH = "/wcs/bench/shard-wcs";

/** Same as `WCS_STREAM_SHARD` but producer egress uses Workers RPC stream handle. */
export const WCS_STREAM_SHARD_RPC = "/wcs/stream/shard-rpc";
export const WCS_INGRESS_SHARD_RPC = "/wcs/ingress/shard-rpc";
export const WCS_BENCH_RPC_PIPE = "/wcs/bench/rpc-pipe";
