/** A call's outcome within `timeoutMs`. The call itself is not cancelled: an RPC cannot be. */
export type Timed<T> = { kind: "ok"; value: T } | { kind: "timeout" } | { kind: "error"; error: string };

export async function raceTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<Timed<T>> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise.then((value): Timed<T> => ({ kind: "ok", value })),
      new Promise<Timed<T>>((resolve) => {
        timer = setTimeout(() => resolve({ kind: "timeout" }), timeoutMs);
      }),
    ]);
  } catch (error) {
    return { kind: "error", error: error instanceof Error ? error.message : String(error) };
  } finally {
    clearTimeout(timer);
  }
}
