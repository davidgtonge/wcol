#!/usr/bin/env node
/**
 * Hit local wrangler dev (or WORKER_URL) and print wasm load + plan timings.
 */
const base = process.env.WORKER_URL ?? "http://127.0.0.1:8787";

async function main() {
  const cold = await post(`${base}/plan/bench`);
  console.log("--- first request (cold isolate wasm load) ---");
  print(cold);

  const warm = await post(`${base}/plan/bench`);
  console.log("--- second request (warm) ---");
  print(warm);
}

async function post(url) {
  const res = await fetch(url, { method: "POST" });
  const body = await res.json();
  if (!res.ok) {
    console.error(body);
    process.exit(1);
  }
  return body;
}

function print(body) {
  console.table(body.timing ?? {});
  console.log(
    "required_columns:",
    body.required_columns?.length,
    "logical chars:",
    body.optimized_logical?.length,
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
