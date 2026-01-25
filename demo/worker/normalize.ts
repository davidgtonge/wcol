/** JSON-safe clone; bigint fields become numbers for stable CBOR across the worker wire. */
export function normalizeWireValue<T>(value: T): T {
  return JSON.parse(
    JSON.stringify(value, (_key, v) => (typeof v === "bigint" ? Number(v) : v)),
  ) as T;
}
