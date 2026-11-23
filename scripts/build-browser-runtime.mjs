#!/usr/bin/env node
/** Browser columnar bundle is not built from the live tree (see gitignored legacy tree if present). */
console.warn("build:browser skipped — no live browser entrypoint.");
process.exit(0);
