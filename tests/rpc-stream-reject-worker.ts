/**
 * Miniflare Durable Objects for rpc-stream-reject.test.ts.
 * A caller streams bytes into a callee RPC that refuses them without reading.
 * Reports whether the caller's call settles.
 */

import { DurableObject } from "cloudflare:workers";

type Outcome = { settled: boolean; value?: unknown; error?: string };

type RejectEnv = {
  CALLEE: {
    idFromName(name: string): unknown;
    get(id: unknown): {
      rejectByReturning(stream: ReadableStream<Uint8Array>): Promise<unknown>;
      acceptAndRead(stream: ReadableStream<Uint8Array>): Promise<unknown>;
    };
  };
  CALLER: {
    idFromName(name: string): unknown;
    get(id: unknown): { call(mode: string): Promise<Outcome> };
  };
};

export class Callee extends DurableObject<RejectEnv> {
  async rejectByReturning(stream: ReadableStream<Uint8Array>): Promise<unknown> {
    void stream.cancel(new Error("rejected")).catch(() => undefined);
    return { accepted: false, error: "not the active node session" };
  }

  async acceptAndRead(stream: ReadableStream<Uint8Array>): Promise<unknown> {
    let bytes = 0;
    for await (const chunk of stream) bytes += chunk.byteLength;
    return { accepted: true, bytes };
  }
}

export class Caller extends DurableObject<RejectEnv> {
  async call(mode: string): Promise<Outcome> {
    const callee = this.env.CALLEE.get(this.env.CALLEE.idFromName("callee"));
    const pipe = new TransformStream<Uint8Array, Uint8Array>();
    // Like a scan: keep producing into the edge while the RPC is open.
    void (async () => {
      const writer = pipe.writable.getWriter();
      try {
        for (let i = 0; i < 64; i++) await writer.write(new Uint8Array(64 * 1024));
        await writer.close();
      } catch {
        // The callee cancelled the stream.
      }
    })();
    // Stub methods are RPC proxies: call them directly, not via Function.prototype.call.
    const sent = mode === "return" ? callee.rejectByReturning(pipe.readable) : callee.acceptAndRead(pipe.readable);
    const call = sent.then(
      (value) => ({ settled: true, value }),
      (error: unknown) => ({ settled: true, error: String(error) }),
    );
    const timeout = new Promise<Outcome>((resolve) => setTimeout(() => resolve({ settled: false }), 3000));
    return Promise.race([call, timeout]);
  }
}

export default {
  async fetch(request: Request, env: RejectEnv): Promise<Response> {
    const mode = new URL(request.url).pathname.slice(1);
    const caller = env.CALLER.get(env.CALLER.idFromName(`caller-${mode}`));
    return Response.json(await caller.call(mode));
  },
};
