/** What the node Wasm must and must not export, for its build and its size analysis. */

/** Planner code belongs to the coordinator Wasm; a node Wasm exporting it pulled the planner in. */
export const PLANNER_EXPORTS = ["wcol_plan_query", "wcol_plan_output_ptr", "wcol_columns_for_sql"];

/** The stream-pool entry every node program opens through. */
export const NODE_ENTRY_EXPORT = "wcs_sp_open_graph";

export function wasmExportNames(bytes) {
  return WebAssembly.Module.exports(new WebAssembly.Module(bytes)).map((entry) => entry.name);
}
