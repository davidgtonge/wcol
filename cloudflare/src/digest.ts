/** Hex digests and secret comparison: the Workers' one implementation of each. */

const encoder = new TextEncoder();

export function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function sha256(bytes: Uint8Array): Promise<Uint8Array> {
  // Every caller hashes bytes it owns (a program, a request body), backed by an ArrayBuffer;
  // TypeScript types a bare Uint8Array as possibly shared, which `digest` does not accept.
  return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes as Uint8Array<ArrayBuffer>));
}

export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  return hex(await sha256(bytes));
}

/**
 * Whether `sent` is `secret`, in constant time. Both are hashed first, so the comparison
 * does not reveal the secret's length either.
 */
export async function sameSecret(sent: string, secret: string): Promise<boolean> {
  const [a, b] = await Promise.all([sha256(encoder.encode(sent)), sha256(encoder.encode(secret))]);
  return crypto.subtle.timingSafeEqual(a, b);
}
