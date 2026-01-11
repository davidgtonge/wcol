import { decode, encode } from "cbor-x";

export function encodeCbor(value: unknown): Uint8Array {
  return encode(value);
}

export function decodeCbor<T>(bytes: ArrayBuffer | Uint8Array): T {
  const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  return decode(view) as T;
}
