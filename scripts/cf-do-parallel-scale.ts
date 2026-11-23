/**
 * Parallel cold-start scale: fan-out (chunks of 6) vs single-invocation (no chunk limit).
 *
 *   npm run bench:cf-do-parallel
 */

if (!process.env.DO_START_DRIVER_URL) throw new Error("set DO_START_DRIVER_URL (for example https://do-start-driver.<your-subdomain>.workers.dev)");
const driver = process.env.DO_START_DRIVER_URL.replace(
  /\/$/,
  "",
);

const totals = [10, 100, 1000] as const;
const budgetMs = 400;
const allArms: unknown[] = [];

async function runArm(total: number, strategy: "fan-out-chunks" | "single-invocation"): Promise<void> {
  const body =
    strategy === "fan-out-chunks"
      ? { totals: [total], budgetMs, locationHint: "enam", includeSingleInvocation: false, includeFanOut: true }
      : { totals: [total], budgetMs, locationHint: "enam", includeSingleInvocation: true, includeFanOut: false };
  const timeoutMs = strategy === "single-invocation" ? 600_000 : 180_000;
  try {
    const response = await fetch(`${driver}/run/parallel-scale`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await response.text();
    if (!response.ok) {
      allArms.push({ total, strategy, error: text.slice(0, 500) });
      return;
    }
    const payload = JSON.parse(text) as { arms?: unknown[] };
    for (const arm of payload.arms ?? []) {
      allArms.push(arm);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    allArms.push({ total, strategy, error: message });
  }
}

for (const total of totals) {
  await runArm(total, "fan-out-chunks");
  await runArm(total, "single-invocation");
}

console.log(
  JSON.stringify(
    {
      target: driver,
      budgetMs,
      inflightLimitPerInvocation: 6,
      note: "single-invocation = one Worker, Promise.all(N) with no chunk fan-out",
      arms: allArms,
    },
    null,
    2,
  ),
);
