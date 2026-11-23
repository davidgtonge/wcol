import { SignalClient } from "./signal-client.ts";
import { RouterCore } from "./signal-router-core.ts";

type Listener = (event: { data?: unknown; code?: number }) => void;

/** In-memory WebSocket pair for tests and in-process coordinator clients. */
export class MemoryWebSocket {
  readyState = 1;
  binaryType = "arraybuffer";
  bufferedAmount = 0;
  peer: MemoryWebSocket | null = null;
  private readonly listeners = new Map<string, Listener[]>();

  addEventListener(type: string, fn: Listener): void {
    const handlers = this.listeners.get(type) ?? [];
    handlers.push(fn);
    this.listeners.set(type, handlers);
  }

  emit(type: string, event: { data?: unknown; code?: number } = {}): void {
    for (const handler of this.listeners.get(type) ?? []) handler(event);
  }

  send(data: string | ArrayBuffer | Uint8Array): void {
    if (this.readyState !== 1) throw new Error("Connection closed");
    const message = data instanceof Uint8Array ? data.slice() : data;
    const length = typeof message === "string" ? message.length : message.byteLength;
    this.bufferedAmount += length;
    queueMicrotask(() => {
      this.bufferedAmount -= length;
      if (this.readyState === 1 && this.peer?.readyState === 1) {
        this.peer.emit("message", { data: message });
      }
    });
  }

  close(code = 1000): void {
    if (this.readyState !== 1) return;
    this.readyState = 3;
    if (this.peer && this.peer.readyState === 1) this.peer.readyState = 3;
    queueMicrotask(() => {
      this.emit("close", { code });
      this.peer?.emit("close", { code });
    });
  }
}

export function memorySocketPair(): { client: MemoryWebSocket; server: MemoryWebSocket } {
  const client = new MemoryWebSocket();
  const server = new MemoryWebSocket();
  client.peer = server;
  server.peer = client;
  return { client, server };
}

/** Attach a SignalClient to a RouterCore without a network hop. */
export async function connectLocalSignals(
  router: RouterCore,
  taskId: string,
  epoch: number,
  timeouts: { connectTimeoutMs?: number; readyTimeoutMs?: number; finishTimeoutMs?: number } = {},
): Promise<SignalClient> {
  return SignalClient.connect({
    url: "ws://local.test/q/local/ws",
    ticket: "local",
    connectTimeoutMs: timeouts.connectTimeoutMs ?? 2000,
    readyTimeoutMs: timeouts.readyTimeoutMs ?? 2000,
    finishTimeoutMs: timeouts.finishTimeoutMs ?? 2000,
    createSocket: () => {
      const { client, server } = memorySocketPair();
      server.addEventListener("message", (event) => {
        router.message(server, event.data as string | ArrayBuffer | Uint8Array);
      });
      server.addEventListener("close", () => router.disconnect(server));
      queueMicrotask(() => router.attach(server, taskId, epoch));
      return client as unknown as WebSocket;
    },
  });
}
