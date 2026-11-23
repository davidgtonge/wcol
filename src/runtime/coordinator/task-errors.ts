/** Prefix Rust puts on input errors that will fail again with the same bytes. */
export const FATAL_TASK_MARK = "wcol-fatal:";

/**
 * A task error the coordinator must not retry.
 * `dictionary_entry_too_large` and `single_row_too_large` are fatal even if a
 * host dropped the prefix, because another attempt reads the same values.
 */
export function isFatalTaskError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return (
    message.includes(FATAL_TASK_MARK) ||
    message.includes("dictionary_entry_too_large") ||
    message.includes("single_row_too_large")
  );
}

/**
 * Partial-state bytes the coordinator's finalizer accepts from all reducers together. A JSON
 * answer is capped at 1 MiB, but the finalizer merges everything it receives before it knows
 * the answer's size: `GROUP BY Title` with no LIMIT sent hundreds of MB of groups and ran the
 * coordinator's isolate out of memory, failing every attempt and the jobs after it. Merging
 * expands each group several times over (every key and stat its own allocation), so a 48 MiB
 * budget still ran out of memory; any answer that fits 1 MiB arrives well inside this one.
 */
export const FINALIZER_INPUT_BYTES = 8 * 1024 * 1024;

/**
 * Counts bytes across every stream it wraps. Once the total passes `maxBytes`, each stream
 * errors with a fatal error (not retried: another attempt sends the same groups). A failure
 * of the wrapped body itself is prefixed with `source`: the runtime's "ReadableStream
 * received over RPC disconnected prematurely" does not say which reducer went away.
 */
export function finalizerInputGuard(maxBytes: number): {
  wrap: (body: ReadableStream<Uint8Array>, source: string) => ReadableStream<Uint8Array>;
  /** Bytes received so far, from every stream. */
  bytes: () => number;
} {
  let total = 0;
  const wrap = (body: ReadableStream<Uint8Array>, source: string) => {
    const reader = body.getReader();
    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        let next: ReadableStreamReadResult<Uint8Array>;
        try {
          next = await reader.read();
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          throw new Error(`${source}: ${message}`);
        }
        if (next.done) {
          controller.close();
          return;
        }
        total += next.value.byteLength;
        if (total > maxBytes) {
          const tooLarge = new Error(
            `${FATAL_TASK_MARK} the result is too large for a JSON response ` +
              `(reducers sent more than ${maxBytes} bytes of groups): add a LIMIT, or ask for Parquet output`,
          );
          await reader.cancel(tooLarge).catch(() => undefined);
          throw tooLarge;
        }
        controller.enqueue(next.value);
      },
      cancel: (reason) => reader.cancel(reason),
    });
  };
  return { wrap, bytes: () => total };
}
