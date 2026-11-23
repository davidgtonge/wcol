/**
 * WCS FRAME_PARTIAL body format.
 *
 * Prefer [`partial-oracle.ts`](./partial-oracle.ts) to read the columns.
 */

export type PartialWireFormat = "pwcl" | "unknown";

export function partialWireFormat(partial: Uint8Array): PartialWireFormat {
  if (partial.byteLength < 4) return "unknown";
  const magic = String.fromCharCode(partial[0]!, partial[1]!, partial[2]!, partial[3]!);
  if (magic === "PWCL") return "pwcl";
  return "unknown";
}
