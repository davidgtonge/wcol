/** Wire constants shared with Rust `wcol-engine/src/spill/host.rs`. */

export const SPILL_OP_BEGIN = 1;
export const SPILL_OP_APPEND = 2;
export const SPILL_OP_SCAN = 3;
export const SPILL_OP_END = 4;

export function tableName(session: bigint): string {
  return `spill_${session.toString(16).padStart(16, "0")}`;
}
