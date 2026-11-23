import { decodeFrame, parseControl, type ClientControl, type ServerControl } from "./signal-wire.ts";

/** Narrow adapter, so the message router can be unit-tested without Cloudflare. */
export interface RouterSocket {
  readonly readyState: number;
  readonly bufferedAmount: number;
  send(data: string | ArrayBuffer | Uint8Array): void;
  close(code?: number, reason?: string): void;
}

export interface Session {
  taskId: string;
  epoch: number;
  reads: string[];
  writes: Array<{ edge: string; finished: boolean }>;
}

const MAX_EDGE_CHANNELS_PER_WORKER = 128;
const MAX_BUFFERED_BYTES = 2 * 1024 * 1024;
const OPEN = 1;

/**
 * Ephemeral signal router: one publisher + one subscriber per edge. No chunk parsing.
 * All writes are fenced by task/epoch at the connection boundary.
 * Calls to save() persist socket metadata across DO hibernation, not signal bytes.
 */
export class RouterCore {
  private readonly sessions = new Map<RouterSocket, Session>();
  private readonly tasks = new Map<string, RouterSocket>();
  private readonly readers = new Map<string, RouterSocket>();
  private readonly writers = new Map<string, { socket: RouterSocket; finished: boolean }>();

  constructor(private readonly save: (socket: RouterSocket, session: Session) => void) {}

  /** Called when the DO wakes up with already-accepted WebSockets. */
  restore(socket: RouterSocket, state: Session): void {
    if (socket.readyState !== OPEN) return;
    this.sessions.set(socket, state);
    this.tasks.set(state.taskId, socket);
    for (const edge of state.reads) this.readers.set(edge, socket);
    for (const { edge, finished } of state.writes) this.writers.set(edge, { socket, finished });
  }

  activeEpoch(taskId: string): number | undefined {
    const socket = this.tasks.get(taskId);
    return socket ? this.sessions.get(socket)?.epoch : undefined;
  }

  /** Attaches an authorized socket; only a strictly newer generation replaces a live one. */
  attach(socket: RouterSocket, taskId: string, epoch: number): void {
    const existing = this.tasks.get(taskId);
    if (existing) {
      if (this.sessions.get(existing)!.epoch >= epoch) {
        throw new Error("Duplicate or stale task connection");
      }
      this.disconnect(existing, "Task connection replaced");
      existing.close(4001, "Replaced by new connection");
    }
    const state: Session = { taskId, epoch, reads: [], writes: [] };
    this.sessions.set(socket, state);
    this.tasks.set(taskId, socket);
    this.save(socket, state);
    this.send(socket, { type: "registered" });
  }

  message(socket: RouterSocket, raw: string | ArrayBuffer | Uint8Array): void {
    if (!this.sessions.has(socket)) return;
    try {
      if (typeof raw === "string") {
        const msg = parseControl(raw);
        if (!(msg.type === "subscribe" || msg.type === "unsubscribe" ||
              msg.type === "publish" || msg.type === "finish" || msg.type === "abort")) {
          throw new Error("Unexpected server-only control type");
        }
        this.control(socket, msg);
      } else {
        this.data(socket, raw);
      }
    } catch (cause) {
      this.disconnect(socket, cause instanceof Error ? cause.message : "Invalid message");
      socket.close(1008, "Protocol error");
    }
  }

  disconnect(socket: RouterSocket, reason = "Peer disconnected"): void {
    const session = this.sessions.get(socket);
    if (!session) return;
    this.sessions.delete(socket);
    if (this.tasks.get(session.taskId) === socket) this.tasks.delete(session.taskId);

    for (const edge of session.reads) {
      if (this.readers.get(edge) !== socket) continue;
      this.readers.delete(edge);
      const writer = this.writers.get(edge);
      if (writer && !writer.finished) this.send(writer.socket, { type: "error", edge, reason });
    }
    for (const { edge, finished } of session.writes) {
      if (this.writers.get(edge)?.socket !== socket) continue;
      this.writers.delete(edge);
      if (!finished) {
        const reader = this.readers.get(edge);
        if (reader) this.send(reader, { type: "error", edge, reason });
      }
    }
  }

  private send(socket: RouterSocket, value: ServerControl): void {
    if (socket.readyState !== OPEN) return;
    if (socket.bufferedAmount > MAX_BUFFERED_BYTES) {
      this.disconnect(socket, "Slow signal consumer");
      socket.close(1013, "Slow signal consumer");
      return;
    }
    try {
      socket.send(JSON.stringify(value));
    } catch {
      this.disconnect(socket, "Signal delivery failed");
      socket.close(1011, "Signal delivery failed");
    }
  }

  private persist(socket: RouterSocket): void {
    const state = this.sessions.get(socket)!;
    if (new TextEncoder().encode(JSON.stringify(state)).byteLength > 12_000) {
      throw new Error("Socket attachment exceeds the safe size limit");
    }
    this.save(socket, state);
  }

  private edgeError(socket: RouterSocket, edge: string, reason: string): void {
    this.send(socket, { type: "error", edge, reason });
  }

  private control(socket: RouterSocket, msg: ClientControl): void {
    const state = this.sessions.get(socket)!;
    const { edge } = msg;
    switch (msg.type) {
      case "subscribe": {
        const existing = this.readers.get(edge);
        if (existing && existing !== socket) return this.edgeError(socket, edge, "Edge already has a reader");
        if (!state.reads.includes(edge)) {
          if (state.reads.length + state.writes.length >= MAX_EDGE_CHANNELS_PER_WORKER) {
            return this.edgeError(socket, edge, "Too many edge channels");
          }
          state.reads.push(edge);
          this.readers.set(edge, socket);
          this.persist(socket);
        }
        this.send(socket, { type: "subscribed", edge });
        const publisher = this.writers.get(edge);
        if (publisher && !publisher.finished) this.send(publisher.socket, { type: "ready", edge });
        return;
      }
      case "unsubscribe": {
        const index = state.reads.indexOf(edge);
        if (index < 0) return;
        state.reads.splice(index, 1);
        if (this.readers.get(edge) === socket) this.readers.delete(edge);
        this.persist(socket);
        const publisher = this.writers.get(edge);
        if (publisher && !publisher.finished) {
          this.edgeError(publisher.socket, edge, "Reader cancelled");
        }
        return;
      }
      case "publish": {
        const existing = this.writers.get(edge);
        if (existing && existing.socket !== socket) return this.edgeError(socket, edge, "Edge already has a writer");
        if (!existing) {
          if (state.reads.length + state.writes.length >= MAX_EDGE_CHANNELS_PER_WORKER) {
            return this.edgeError(socket, edge, "Too many edge channels");
          }
          state.writes.push({ edge, finished: false });
          this.writers.set(edge, { socket, finished: false });
          this.persist(socket);
        }
        if (this.readers.has(edge)) this.send(socket, { type: "ready", edge });
        return;
      }
      case "finish": {
        const writer = this.writers.get(edge);
        if (!writer || writer.socket !== socket || writer.finished) {
          return this.edgeError(socket, edge, "Not an open publisher");
        }
        const reader = this.readers.get(edge);
        if (!reader) return this.edgeError(socket, edge, "No reader available");
        writer.finished = true;
        state.writes.find((w) => w.edge === edge)!.finished = true;
        this.persist(socket);
        this.send(reader, { type: "end", edge });
        this.send(socket, { type: "finished", edge });
        return;
      }
      case "abort": {
        const writer = this.writers.get(edge);
        if (!writer || writer.socket !== socket || writer.finished) return;
        writer.finished = true;
        state.writes.find((w) => w.edge === edge)!.finished = true;
        this.persist(socket);
        const reader = this.readers.get(edge);
        if (reader) this.edgeError(reader, edge, `Writer aborted: ${msg.reason}`);
        return;
      }
    }
  }

  private data(socket: RouterSocket, frame: ArrayBuffer | Uint8Array): void {
    const { edge } = decodeFrame(frame);
    const writer = this.writers.get(edge);
    if (!writer || writer.socket !== socket || writer.finished) {
      throw new Error("Unregistered publisher sent bytes");
    }
    const reader = this.readers.get(edge);
    if (!reader || reader.readyState !== OPEN) {
      this.edgeError(socket, edge, "No reader available");
      return;
    }
    if (reader.bufferedAmount > MAX_BUFFERED_BYTES) {
      this.disconnect(reader, "Slow signal consumer");
      reader.close(1013, "Slow signal consumer");
      return;
    }
    try {
      reader.send(frame);
    } catch {
      this.disconnect(reader, "Signal delivery failed");
      reader.close(1011, "Signal delivery failed");
    }
  }
}
