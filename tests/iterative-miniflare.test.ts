import { strict as assert } from "node:assert";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { before, describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { build } from "esbuild";
import { bundlePlugin, distDir } from "../scripts/build-workers.ts";
import { buildWorkers } from "../scripts/build-workers.ts";
import { createSpillMiniflare } from "../scripts/cf-spill-shared.ts";
import type { R2OutputBucket } from "../cloudflare/src/r2.ts";

const faultNodePath = path.join(distDir, "iterative-fault-node.js");

describe(
  "resident iterative execution on Miniflare",
  { concurrency: 1 },
  () => {
    before(async () => {
      await buildWorkers("prod");
      await build({
        entryPoints: [path.resolve("tests/iterative-fault-node.ts")],
        outfile: faultNodePath,
        bundle: true,
        format: "esm",
        platform: "browser",
        target: "es2022",
        plugins: [bundlePlugin()],
      });
    });

    it(
      "recovers from node loss at a committed barrier and matches uninterrupted training",
      { timeout: 180_000 },
      async () => {
        const tempDir = fs.mkdtempSync(
          path.join(os.tmpdir(), "wcol-iterative-mf-"),
        );
        const parquetPath = path.join(tempDir, "features.parquet");
        const duck = await (await DuckDBInstance.create(":memory:")).connect();
        await duck.run(
          `COPY (
        SELECT
          (CASE WHEN i % 7 = 0 THEN -1.0 WHEN i % 2 = 0 THEN 1.0 + (i % 11) * 0.01 ELSE 9.0 + (i % 11) * 0.01 END)::DOUBLE AS income,
          (CASE WHEN i % 9 = 0 THEN NULL WHEN i % 2 = 0 THEN 1.0 + (i % 13) * 0.01 ELSE 9.0 + (i % 13) * 0.01 END)::DOUBLE AS spending
        FROM range(4096) t(i)
      ) TO '${parquetPath}' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)`,
        );

        const [expected] = (
          await duck.runAndReadAll(
            `SELECT AVG(income), AVG(spending), STDDEV_POP(income), STDDEV_POP(spending) FROM '${parquetPath}' WHERE income > 0`,
          )
        ).getRows();

        const mf = createSpillMiniflare(undefined, faultNodePath);
        try {
          const output = (await mf.getR2Bucket("OUTPUT")) as unknown as Pick<
            R2OutputBucket,
            "put" | "get"
          >;
          await output.put("features.parquet", fs.readFileSync(parquetPath));

          const submitted = await mf.dispatchFetch("http://wcol.local/jobs", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              jobId: "miniflare-kmeans-smoke",
              maxNodes: 4,
              sources: {
                features: { bucket: "output", key: "features.parquet" },
              },
              sql: "CREATE MODEL smoke_segments OPTIONS (model_type = 'kmeans', num_clusters = 2, max_iterations = 10, seed = 42) AS SELECT income, spending FROM features WHERE income > 0",
            }),
          });
          const submitText = await submitted.text();
          assert.equal(submitted.status, 202, submitText);

          const deadline = Date.now() + 120_000;
          let status: Record<string, unknown> = {};
          while (Date.now() < deadline) {
            const response = await mf.dispatchFetch(
              "http://wcol.local/jobs/miniflare-kmeans-smoke",
            );
            status = (await response.json()) as Record<string, unknown>;
            if (status.state === "completed" || status.state === "failed")
              break;
            await new Promise((resolve) => setTimeout(resolve, 50));
          }

          assert.equal(status.state, "completed", JSON.stringify(status));
          assert.ok(
            Number(status.nodeCount) >= 2,
            `expected multiple resident nodes, got ${status.nodeCount}`,
          );
          assert.equal(status.modelName, "smoke_segments");
          assert.equal(status.modelKey, "_models/smoke_segments/model.json");
          assert.deepEqual(
            status.modelSummary &&
              Object.keys(status.modelSummary as object).sort(),
            ["converged", "inertia", "iterations"],
          );
          const summary = status.modelSummary as {
            iterations: number;
            converged: boolean;
            inertia: number;
          };
          assert.ok(summary.iterations >= 1 && summary.iterations <= 10);
          assert.ok(Number.isFinite(summary.inertia));

          const artifact = await output.get(
            "_models/smoke_segments/model.json",
          );
          assert.ok(artifact, "expected the trained model artifact in R2");
          const model = JSON.parse(await artifact.text()) as {
            formatVersion: number;
            features: string[];
            centroids: number[];
            means: number[];
            scales: number[];
          };
          assert.equal(model.formatVersion, 1);
          assert.deepEqual(model.features, ["income", "spending"]);
          assert.equal(model.centroids.length, 4);
          assert.ok(model.centroids.every(Number.isFinite));
          const normalization = [...model.means, ...model.scales];
          for (let index = 0; index < normalization.length; index++) {
            assert.ok(
              Math.abs(normalization[index]! - Number(expected![index])) < 1e-9,
              `normalization mismatch at feature ${index}`,
            );
          }
          assert.equal(summary.converged, true);

          const recoveredSubmission = await mf.dispatchFetch(
            "http://wcol.local/jobs",
            {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                jobId: "miniflare-kmeans-recovery",
                maxNodes: 4,
                sources: {
                  features: { bucket: "output", key: "features.parquet" },
                },
                sql: "CREATE MODEL recovered_segments OPTIONS (model_type = 'kmeans', num_clusters = 2, max_iterations = 10, seed = 42) AS SELECT income, spending FROM features WHERE income > 0",
              }),
            },
          );
          assert.equal(
            recoveredSubmission.status,
            202,
            await recoveredSubmission.text(),
          );
          const recovered = await waitForJob(mf, "miniflare-kmeans-recovery");
          assert.equal(recovered.state, "completed", JSON.stringify(recovered));
          assert.equal(recovered.executionAttempt, 2);
          assert.deepEqual(recovered.modelSummary, summary);
          const recoveredArtifact = await output.get(
            "_models/recovered_segments/model.json",
          );
          assert.ok(recoveredArtifact);
          const recoveredModel = JSON.parse(
            await recoveredArtifact.text(),
          ) as typeof model;
          assert.deepEqual(recoveredModel.centroids, model.centroids);
          assert.deepEqual(recoveredModel.means, model.means);
          assert.deepEqual(recoveredModel.scales, model.scales);

          const bucket = (await mf.getR2Bucket(
            "OUTPUT",
          )) as unknown as Required<Pick<R2OutputBucket, "get" | "list">>;
          const events = await bucket.list({
            prefix: "_faults/miniflare-kmeans-recovery/",
          });
          const records: Array<{
            key: string;
            startIteration?: number;
            moments?: number[];
            iteration?: number;
            state?: number[];
          }> = [];
          for (const event of events.objects) {
            const object = await bucket.get(event.key);
            assert.ok(object);
            records.push({
              key: event.key,
              ...JSON.parse(await object.text()),
            });
          }
          const loads = records.filter((event) =>
            event.key.endsWith("-load.json"),
          );
          const firstLoads = loads.filter((event) =>
            event.key.includes("/a1-"),
          );
          const reloaded = loads.filter((event) => event.key.includes("/a2-"));
          assert.ok(firstLoads.length >= 2);
          assert.equal(reloaded.length, firstLoads.length);
          for (const loaded of reloaded) {
            assert.equal(
              loaded.startIteration,
              2,
              "reload must resume after the committed barrier",
            );
            const original = firstLoads.find(
              (event) => event.key.split("-s")[1] === loaded.key.split("-s")[1],
            );
            assert.ok(original);
            assert.deepEqual(
              loaded.moments,
              original.moments,
              "R2 reload must reconstruct the same partition",
            );
          }
          const failedRound = records.find(
            (event) => event.key.includes("/a1-") && event.iteration === 2,
          );
          const resumedRound = records.find(
            (event) => event.key.includes("/a2-") && event.iteration === 2,
          );
          assert.ok(failedRound && resumedRound);
          assert.deepEqual(
            resumedRound.state,
            failedRound.state,
            "retry must use the committed global state",
          );
          assert.equal(
            records.some(
              (event) => event.key.includes("/a2-") && event.iteration === 1,
            ),
            false,
          );
        } finally {
          await mf.dispose();
          fs.rmSync(tempDir, { recursive: true, force: true });
        }
      },
    );

    it(
      "fences resident sessions and releases them on cancellation",
      { timeout: 120_000 },
      async () => {
        const { loadCoordinatorWasm } = await import(
          "../src/runtime/host/wasm.ts"
        );
        const { planJob, readParquetFooter } = await import(
          "../src/runtime/coordinator/plan-dispatch.ts"
        );
        const { prepareResidentKMeans } = await import(
          "../src/runtime/host/iterative-wasm.ts"
        );
        const bytes = new Uint8Array(
          fs.readFileSync(
            new URL("./fixtures/duckdb_nulls.parquet", import.meta.url),
          ),
        );
        const wasm = await loadCoordinatorWasm();
        const footer = await readParquetFooter(
          async (offset, length) => bytes.slice(offset, offset + length),
          bytes.length,
        );
        const planned = planJob(wasm, {
          body: JSON.stringify({
            sources: { t: { key: "http://unused/t.parquet" } },
            sql: "CREATE MODEL lifecycle OPTIONS (model_type='kmeans', num_clusters=2) AS SELECT d, p FROM t WHERE d > 0",
          }),
          footers: [{ fileSize: bytes.length, footer }],
          maxNodes: 1,
        });
        assert.ok(planned.kind === "model" && planned.graph.kind === "graph");
        const task = planned.graph.tasks[0];
        assert.ok(task);
        const mf = createSpillMiniflare(undefined, faultNodePath);
        try {
          const output = (await mf.getR2Bucket("OUTPUT")) as unknown as Pick<
            R2OutputBucket,
            "put"
          >;
          await output.put("lifecycle.parquet", bytes);
          const namespace = (await mf.getDurableObjectNamespace(
            "WCOL_NODE",
            "wcol-node",
          )) as unknown as {
            idFromName(name: string): unknown;
            get(id: unknown): unknown;
          };
          // Miniflare's namespace type omits application RPC methods.
          const node = namespace.get(
            namespace.idFromName("resident-lifecycle"),
          ) as unknown as import("../cloudflare/src/node/types.ts").GraphTaskNode & {
            roundError(
              params: import("../cloudflare/src/node/types.ts").ResidentKMeansRoundParams,
            ): Promise<string | null>;
            loadError(
              params: import("../cloudflare/src/node/types.ts").ResidentKMeansLoadParams,
            ): Promise<string | null>;
          };
          const common = {
            jobId: "resident-lifecycle",
            executionAttempt: 1,
            sessionId: "first",
          };
          const load = {
            ...common,
            deployId: "",
            taskId: task.id,
            slot: 0,
            placement: { kind: "job" } as const,
            program: task.program,
            sources: [{ bucket: "output", key: "lifecycle.parquet" }],
            startIteration: 1,
          };
          const { moments } = await node.loadResidentKMeans(load);
          const preparation = prepareResidentKMeans(wasm, moments, {
            dimensions: 2,
            clusters: 2,
            maxIterations: 100,
            tolerance: 1e-4,
            seed: 42,
          });
          await node.normalizeResidentKMeans({
            ...common,
            normalization: preparation.normalization,
          });
          const round = {
            ...common,
            iteration: 1,
            globalState: preparation.state,
          };
          assert.match(
            (await node.roundError({ ...round, sessionId: "wrong" })) ??
              "expected rejection",
            /missing or fenced/,
          );
          assert.match(
            (await node.roundError({ ...round, iteration: 2 })) ??
              "expected rejection",
            /out of order/,
          );
          const expected = await node.runResidentKMeansRound(round);
          assert.match(
            (await node.roundError(round)) ?? "expected rejection",
            /out of order/,
          );

          const newer = { ...common, executionAttempt: 2, sessionId: "second" };
          await node.loadResidentKMeans({
            ...load,
            ...newer,
            startIteration: 2,
          });
          await node.normalizeResidentKMeans({
            ...newer,
            normalization: preparation.normalization,
          });
          assert.match(
            (await node.loadError(load)) ?? "expected rejection",
            /superseded/,
          );
          assert.match(
            (await node.roundError(round)) ?? "expected rejection",
            /superseded/,
          );
          // A late release or abort of attempt one must leave attempt two usable.
          await node.releaseResidentKMeans(common);
          await node.abortHashAttempt({
            ...common,
            error: "late old cancellation",
          });
          assert.deepEqual(
            await node.runResidentKMeansRound({
              ...newer,
              iteration: 2,
              globalState: preparation.state,
            }),
            expected,
          );
          await node.abortHashAttempt({
            ...newer,
            error: "cancel resident attempt",
          });
          assert.match(
            (await node.roundError({
              ...newer,
              iteration: 3,
              globalState: preparation.state,
            })) ?? "expected rejection",
            /aborted or superseded/,
          );
          assert.match(
            (await node.loadError({ ...load, ...newer })) ??
              "expected rejection",
            /aborted or superseded/,
          );

          const released = {
            ...common,
            executionAttempt: 3,
            sessionId: "third",
          };
          await node.loadResidentKMeans({ ...load, ...released });
          await node.normalizeResidentKMeans({
            ...released,
            normalization: preparation.normalization,
          });
          await node.releaseResidentKMeans(released);
          await node.releaseResidentKMeans(released);
          assert.match(
            (await node.roundError({
              ...released,
              iteration: 1,
              globalState: preparation.state,
            })) ?? "expected rejection",
            /missing or fenced/,
          );
        } finally {
          await mf.dispose();
        }
      },
    );

    it(
      "fails an oversized partition once without publishing a model",
      { timeout: 120_000 },
      async () => {
        const mf = createSpillMiniflare(
          { WCOL_RESIDENT_MEMORY_BYTES: "1024" },
          faultNodePath,
        );
        try {
          const output = (await mf.getR2Bucket("OUTPUT")) as unknown as Pick<
            R2OutputBucket,
            "put" | "get"
          >;
          await output.put(
            "capped.parquet",
            new Uint8Array(
              fs.readFileSync(
                new URL("./fixtures/duckdb_nulls.parquet", import.meta.url),
              ),
            ),
          );
          const response = await mf.dispatchFetch("http://wcol.local/jobs", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              jobId: "memory-cap",
              maxNodes: 1,
              sources: { t: { bucket: "output", key: "capped.parquet" } },
              sql: "CREATE MODEL capped OPTIONS (model_type='kmeans', num_clusters=2) AS SELECT d, p FROM t WHERE d > 0",
            }),
          });
          assert.equal(response.status, 202, await response.text());
          const status = await waitForJob(mf, "memory-cap");
          assert.equal(status.state, "failed", JSON.stringify(status));
          assert.equal(
            status.executionAttempt,
            1,
            "a fixed memory limit must not be retried",
          );
          assert.match(
            String(status.error),
            /resident feature memory limit exceeded/,
          );
          assert.equal(await output.get("_models/capped/model.json"), null);
        } finally {
          await mf.dispose();
        }
      },
    );

    it(
      "measures loading, normalization and round memory on local subsets",
      { timeout: 180_000 },
      async () => {
        const directory = fs.mkdtempSync(
          path.join(os.tmpdir(), "wcol-resident-memory-"),
        );
        const mf = createSpillMiniflare(undefined, faultNodePath);
        try {
          const output = (await mf.getR2Bucket(
            "OUTPUT",
          )) as unknown as Required<
            Pick<R2OutputBucket, "put" | "get" | "list">
          >;
          const duck = await (
            await DuckDBInstance.create(":memory:")
          ).connect();
          for (const rows of [16_384, 65_536, 262_144, 524_288]) {
            const parquet = path.join(directory, `features-${rows}.parquet`);
            const features = Array.from(
              { length: 8 },
              (_, col) =>
                `((i % 4) * 10 + (i % ${31 + col * 2}) * 0.01)::DOUBLE AS f${col}`,
            ).join(", ");
            await duck.run(
              `COPY (SELECT ${features} FROM range(${rows}) t(i)) TO '${parquet}' (FORMAT PARQUET, ROW_GROUP_SIZE 8192)`,
            );
            const key = `memory-${rows}.parquet`;
            await output.put(key, new Uint8Array(fs.readFileSync(parquet)));
            const jobId = `memory-${rows}`;
            const submitted = await mf.dispatchFetch("http://wcol.local/jobs", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                jobId,
                maxNodes: 2,
                sources: { t: { bucket: "output", key } },
                sql: `CREATE MODEL memory_${rows} OPTIONS (model_type='kmeans', num_clusters=4, max_iterations=5) AS SELECT ${Array.from({ length: 8 }, (_, col) => `f${col}`).join(", ")} FROM t`,
              }),
            });
            assert.equal(submitted.status, 202, await submitted.text());
            const status = await waitForJob(mf, jobId);
            assert.equal(status.state, "completed", JSON.stringify(status));
            const objects = await output.list({ prefix: `_jobs/${jobId}/a1/` });
            const measurements = [];
            for (const object of objects.objects.filter((object) =>
              object.key.endsWith("-resident-memory.json"),
            )) {
              const value = await output.get(object.key);
              assert.ok(value);
              const measurement = JSON.parse(await value.text()) as {
                load: import("../src/runtime/host/resident-graph.ts").ResidentMemoryStats & {
                  wallMs: number;
                };
                normalization: import("../src/runtime/host/resident-graph.ts").ResidentMemoryStats & {
                  wallMs: number;
                };
                rounds: import("../src/runtime/host/resident-graph.ts").ResidentMemoryStats & {
                  wallMs: number;
                  count: number;
                };
              };
              assert.ok(measurement.load.loadPeakBytes > 0);
              assert.ok(
                measurement.normalization.normalizePeakBytes >
                  measurement.load.featureBytes,
              );
              assert.equal(
                measurement.rounds.featureBytes,
                measurement.normalization.featureBytes,
              );
              assert.ok(
                measurement.rounds.wasmMemoryBytes >=
                  measurement.load.wasmMemoryBytes,
              );
              assert.ok(
                Math.max(
                  measurement.load.loadPeakBytes,
                  measurement.normalization.normalizePeakBytes,
                ) <= measurement.load.limitBytes,
              );
              measurements.push(measurement);
            }
            assert.equal(measurements.length, Number(status.nodeCount));
            assert.equal(measurements.length, 2);
            assert.equal(
              measurements.reduce(
                (sum, item) => sum + item.rounds.featureBytes,
                0,
              ),
              rows * 8 * 4,
            );
            console.log(
              JSON.stringify({
                event: "resident_memory_benchmark",
                rows,
                dimensions: 8,
                workers: measurements.length,
                measurements,
              }),
            );
          }
        } finally {
          await mf.dispose();
          fs.rmSync(directory, { recursive: true, force: true });
        }
      },
    );
  },
);

async function waitForJob(
  mf: ReturnType<typeof createSpillMiniflare>,
  jobId: string,
) {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    const response = await mf.dispatchFetch(`http://wcol.local/jobs/${jobId}`);
    const status = (await response.json()) as Record<string, unknown>;
    if (status.state === "completed" || status.state === "failed")
      return status;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`job ${jobId} timed out`);
}
