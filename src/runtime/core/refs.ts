import type { ColumnRef } from "./types.ts";

export function columnRef(spec: { column?: ColumnRef; col?: ColumnRef }): ColumnRef | undefined {
  return spec.column ?? spec.col;
}

export function columnName(ref: ColumnRef | undefined): string | undefined {
  if (ref === undefined) return undefined;
  return typeof ref === "string" ? ref : String(ref);
}
