import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { createReducerIngressSession } from "../src/runtime/host/scan-edge-session.ts";
import { readableFrom } from "./helpers/streams.ts";

async function readAll(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const parts: Uint8Array[] = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    parts.push(value);
  }
  const out = new Uint8Array(parts.reduce((n, p) => n + p.byteLength, 0));
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.byteLength;
  }
  return out;
}

describe("reducer ingress session", () => {
  it("does not treat prepare/accept as stream completion", async () => {
    const session = createReducerIngressSession({
      jobId: "j",
      executionAttempt: 1,
      expectedScans: 1,
    });
    const waiting = session.waitForStream(0);
    await session.waitConsumeReady(0);
    let pulled = false;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (pulled) {
          controller.close();
          return;
        }
        pulled = true;
        controller.enqueue(new Uint8Array([7]));
      },
    });
    const accepted = session.acceptScanEdge(0, stream);
    assert.equal(accepted.accepted, true);
    const inner = await waiting;
    assert.deepEqual([...(await readAll(inner))], [7]);
    assert.equal(pulled, true);
  });

  it("rejects duplicate, stale, and post-seal accepts", () => {
    const session = createReducerIngressSession({
      jobId: "j",
      executionAttempt: 2,
      expectedScans: 2,
    });
    session.acceptScanEdge(0, readableFrom([new Uint8Array([1])]));
    assert.throws(() => session.acceptScanEdge(0, readableFrom([])), /duplicate/);
    assert.throws(() => session.assertAttempt("j", 1), /stale hash attempt/);
    session.seal();
    assert.throws(() => session.acceptScanEdge(1, readableFrom([])), /after seal/);
  });

  it("completeScan EOFs unregistered slots and leaves accepted streams", async () => {
    const session = createReducerIngressSession({
      jobId: "j",
      executionAttempt: 1,
      expectedScans: 2,
    });
    session.acceptScanEdge(0, readableFrom([new Uint8Array([3])]));
    session.completeScan(1);
    const empty = await session.waitForStream(1);
    assert.equal((await readAll(empty)).byteLength, 0);
    session.completeScan(0);
    assert.deepEqual([...(await readAll(await session.waitForStream(0)))], [3]);
  });

  it("a failed attempt does not seal unfinished slots empty", async () => {
    const session = createReducerIngressSession({
      jobId: "j",
      executionAttempt: 1,
      expectedScans: 1,
    });
    const pending = session.waitForStream(0);
    session.abort(new Error("scan failed"));
    await assert.rejects(pending, /scan failed/);
    assert.equal(session.sealed, false);
    assert.throws(
      () => session.acceptScanEdge(0, readableFrom([new Uint8Array([1])])),
      /scan failed/,
    );
  });
});
