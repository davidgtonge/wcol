/**
 * Compare prod vs bench node Worker JS bundles (esbuild metafile).
 *
 *   npx tsx scripts/report-node-worker-bundle.ts
 */

import { reportNodeWorkerBundle } from "./build-workers.ts";

const prod = await reportNodeWorkerBundle("prod");
const bench = await reportNodeWorkerBundle("bench");

console.log(
  JSON.stringify(
    {
      prod,
      bench,
      jsSavingsBytes: bench.jsBytes - prod.jsBytes,
      jsSavingsGzipBytes: bench.jsGzipBytes - prod.jsGzipBytes,
    },
    null,
    2,
  ),
);
