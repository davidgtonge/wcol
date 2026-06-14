import type { WcolFile } from "../wcol-query.ts";

export type ColumnInfo = {
  physicalType: number;
  flags: number;
};

export type WcolFileWithRuntimeDict = WcolFile & {
  getRuntimeDictValue(colId: number, poolId: number): Promise<string>;
};

export function isDictEncoded(info: ColumnInfo): boolean {
  return (info.flags & 2) !== 0;
}

function isRuntimeStringColumn(info: ColumnInfo): boolean {
  return info.physicalType === 10 && !isDictEncoded(info);
}

function formatScalar(value: number | bigint, physicalType?: number): string {
  if (physicalType === 7) return value ? "true" : "false";
  return typeof value === "bigint" ? value.toLocaleString() : String(value);
}

/** Resolve a group-by key to a display label. */
export async function resolveGroupKey(
  file: WcolFile,
  keyInfo: { colId: number; physicalType: number; flags: number } | undefined,
  key: number | bigint
): Promise<string> {
  if (!keyInfo) return formatScalar(key);
  if (isDictEncoded(keyInfo)) {
    const label = await file.getColumnDictValue(keyInfo.colId, Number(key));
    return label ?? `#${key}`;
  }
  if (isRuntimeStringColumn(keyInfo)) {
    try {
      const label = await (file as WcolFileWithRuntimeDict).getRuntimeDictValue(
        keyInfo.colId,
        Number(key)
      );
      if (label) return label;
    } catch {
      // Group-by keys for TYPE_STRING are often opaque hashes, not projection pool ids.
    }
  }
  return formatScalar(key, keyInfo.physicalType);
}

/** Resolve one projected cell for display. */
export async function resolveProjectionCell(
  file: WcolFile,
  meta: { name: string; colId: number; kind: number },
  col: { kind: number; values: ArrayLike<number | bigint | boolean>; nulls: Uint8Array },
  row: number
): Promise<string | number | boolean | null> {
  if (col.nulls[row] === 0) return null;

  if (col.kind === 0) {
    return col.values[row] as number;
  }
  if (col.kind === 2) {
    return Boolean(col.values[row]);
  }

  const info = await file.getColumnInfo(meta.colId);
  const raw = Number(col.values[row]);
  if (isDictEncoded(info)) {
    const label = await file.getColumnDictValue(meta.colId, raw);
    return label ?? `#${raw}`;
  }

  if (isRuntimeStringColumn(info) && col.kind === 1) {
    const label = await (file as WcolFileWithRuntimeDict).getRuntimeDictValue(meta.colId, raw);
    return label;
  }

  return raw;
}
