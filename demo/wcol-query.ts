/**
 * Re-exported by scripts/build-demo.mjs as dist/browser/wcol-query.js
 * (`export * from './main.js'`). Worker bundle imports this shim; Wasm stays off the main thread.
 */
export type { QueryPlan, QueryResult, ByteSource } from "./main.js";
export {
  WcolFile,
  buildPlan,
  HttpRangeSource,
  LocalFileSource,
} from "./main.js";
