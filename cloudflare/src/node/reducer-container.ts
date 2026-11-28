/**
 * A hash reducer in a Cloudflare Container: the native reducer (`rust/wcol-reducer`) with
 * dedicated memory and local-disk spill, behind the same RPC methods a node reducer answers.
 *
 * The container app uses the `durable_object` scheduling policy, so nothing is provisioned
 * until this object starts its container. One container per reducer per attempt: it starts on
 * `prepareReducer` and is destroyed as soon as its output has been read or the attempt aborts.
 * The inactivity timeout is a backstop for a coordinator that disappears mid-job, and the
 * deadline alarm one for a container kept busy (and billed) by an attempt nobody ends.
 */
import { DurableObject } from "cloudflare:workers";
import { DEFAULT_CONTAINER_INSTANCE, type ContainerInstance } from "./containers.ts";
import { coloOnce } from "../colo.ts";
import type { ReducerPlacementFacts } from "./types.ts";
import type { AcceptScanEdgeParams, GraphTaskEnv, HashAttemptParams, PrepareReducerHandle, PrepareReducerParams } from "./types.ts";

const REDUCER_PORT = 8080;
/** The `images` entry in `wrangler.node.toml`. */
const REDUCER_IMAGE = "reducer";
const READY_TIMEOUT_MS = 30_000;
const READY_POLL_MS = 100;
const BACKSTOP_IDLE_MS = 30_000;
/** No reducer attempt runs this long; one still running is stopped. */
const DEADLINE_MS = 30 * 60 * 1000;

/** Settles once this attempt's reducer has started; scans may reach it before that. */
function gate(): { promise: Promise<void>; open: () => void; fail: (error: unknown) => void } {
  let open!: () => void;
  let fail!: (error: unknown) => void;
  const promise = new Promise<void>((resolve, reject) => ((open = resolve), (fail = reject)));
  // A failed prepare is reported by prepare itself; waiting scans see it when they await.
  promise.catch(() => undefined);
  return { promise, open, fail };
}

export class WcolReducer extends DurableObject<GraphTaskEnv> {
  /** Scan edges this reducer was given; a finished scan that sent none is marked absent. */
  private readonly opened = new Set<number>();
  private expectedScans = 0;
  private started = gate();
  /** Set when this object stops its own container, whose SIGKILL (exit 137) is then expected. */
  private stopping = false;
  private slot = -1;
  private instance: ContainerInstance = DEFAULT_CONTAINER_INSTANCE;
  /** How long the container took to answer, from `start`. */
  private readyMs = 0;

  async prepareReducer(params: PrepareReducerParams): Promise<PrepareReducerHandle> {
    this.slot = params.slot;
    try {
      await this.ctx.storage.setAlarm(Date.now() + DEADLINE_MS);
      await this.startContainer(params.containerInstance ?? DEFAULT_CONTAINER_INSTANCE);
      await this.reducerCall(`/start?inputs=${params.expectedScans}`, {
        method: "POST",
        // The program arrived over RPC, so it owns its buffer.
        body: params.program as Uint8Array<ArrayBuffer>,
      });
    } catch (error) {
      this.started.fail(error);
      await this.shutDown();
      throw error;
    }
    this.expectedScans = params.expectedScans;
    this.opened.clear();
    // The output request is opened before scans are let in, so it is waiting when they send.
    const body = this.output();
    this.started.open();
    return { ready: true, invokedSlots: [params.slot], body };
  }

  async placement(): Promise<ReducerPlacementFacts> {
    return { colo: await coloOnce(), runtime: "container", instance: this.instance, readyMs: this.readyMs };
  }

  async waitConsumeReady(_params: HashAttemptParams): Promise<void> {
    await this.started.promise;
  }

  async acceptScanEdge(params: AcceptScanEdgeParams): Promise<{ accepted: true }> {
    await this.started.promise;
    this.opened.add(params.scanIndex);
    await this.reducerCall(`/input/${params.scanIndex}`, { method: "PUT", body: params.stream });
    return { accepted: true };
  }

  async completeScan(params: HashAttemptParams): Promise<void> {
    if (params.scanIndex == null) throw new Error("completeScan needs scanIndex");
    await this.started.promise;
    await this.markAbsent(params.scanIndex);
  }

  async sealReducer(_params: HashAttemptParams): Promise<void> {
    await this.started.promise;
    for (let scan = 0; scan < this.expectedScans; scan++) await this.markAbsent(scan);
  }

  async abortHashAttempt(params: HashAttemptParams & { error?: string }): Promise<void> {
    this.started.fail(new Error(`attempt aborted: ${params.error ?? "no reason"}`));
    await this.shutDown();
  }

  /** The deadline: whatever this reducer is doing, its container stops now. */
  async alarm(): Promise<void> {
    if (this.ctx.container?.running) {
      console.warn(JSON.stringify({ event: "container_reducer_deadline", slot: this.slot, ms: DEADLINE_MS }));
    }
    this.started.fail(new Error(`container reducer passed its ${DEADLINE_MS} ms deadline`));
    await this.shutDown();
  }

  private container(): Container {
    const container = this.ctx.container;
    if (!container) throw new Error("WcolReducer has no container: check the containers entry in wrangler.node.toml");
    return container;
  }

  private reducerFetch(path: string, init?: RequestInit): Promise<Response> {
    return this.container().getTcpPort(REDUCER_PORT).fetch(`http://reducer${path}`, init);
  }

  /**
   * A request whose answer is short. The body is always read: an unread one keeps its
   * connection open, and a Durable Object has only six, which the edges need.
   */
  private async reducerCall(path: string, init?: RequestInit): Promise<string> {
    const response = await this.reducerFetch(path, init);
    const text = await response.text();
    if (!response.ok) throw new Error(`container reducer ${path}: ${response.status} ${text}`);
    return text;
  }

  /** Start a fresh container and wait until the reducer answers on its port. */
  private async startContainer(instance: ContainerInstance): Promise<void> {
    const container = this.container();
    // A container still running here belongs to an earlier, abandoned prepare.
    if (container.running) await container.destroy();
    const image = container.images[REDUCER_IMAGE];
    if (!image) throw new Error(`no container image named ${REDUCER_IMAGE}`);
    const startedAt = Date.now();
    this.stopping = false;
    container.start({ image, instance, enableInternet: false });
    this.ctx.waitUntil(
      container.monitor().catch((error: unknown) => {
        // Only an exit this object did not ask for is news: a crash or an out-of-memory kill.
        if (!this.stopping) console.log(JSON.stringify({ event: "container_reducer_exit", error: String(error) }));
      }),
    );
    await container.setInactivityTimeout(BACKSTOP_IDLE_MS);
    while (!(await this.reducerCall("/health").then(() => true, () => false))) {
      if (Date.now() - startedAt > READY_TIMEOUT_MS) {
        await this.shutDown();
        throw new Error(`container reducer not ready after ${READY_TIMEOUT_MS} ms`);
      }
      await new Promise((resolve) => setTimeout(resolve, READY_POLL_MS));
    }
    this.instance = instance;
    this.readyMs = Date.now() - startedAt;
    console.log(JSON.stringify({ event: "container_reducer_ready", slot: this.slot, instance, ms: this.readyMs }));
  }

  private async markAbsent(scanIndex: number): Promise<void> {
    if (this.opened.has(scanIndex)) return;
    this.opened.add(scanIndex);
    await this.reducerCall(`/absent/${scanIndex}`, { method: "POST" });
  }

  /**
   * The reducer's output, streamed as the finalizer reads it: one chunk per pull, so the
   * container waits for a slow reader instead of this object buffering its output. Once it
   * ends the reducer's status decides success; then, or on cancel, the container stops.
   */
  private output(): ReadableStream<Uint8Array> {
    // Requested now, so the output request is open before scans are let in.
    const response = this.reducerFetch("/output");
    response.catch(() => undefined);
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    return new ReadableStream<Uint8Array>(
      {
        pull: async (controller) => {
          try {
            reader ??= (await response).body?.getReader();
            if (!reader) throw new Error("container reducer sent no output");
            const { value, done } = await reader.read();
            if (!done) {
              controller.enqueue(value);
              return;
            }
            const status = await this.reducerCall("/status");
            if (!status.startsWith("ok")) throw new Error(`container reducer: ${status}`);
            console.log(JSON.stringify({ event: "container_reducer_done", slot: this.slot, status }));
            controller.close();
            await this.shutDown();
          } catch (error) {
            controller.error(error);
            await this.shutDown();
          }
        },
        cancel: async (reason) => {
          await reader?.cancel(reason).catch(() => undefined);
          await this.shutDown();
        },
      },
      { highWaterMark: 0 },
    );
  }

  /** Stop the container now: a running one is billed until it does. */
  private async shutDown(): Promise<void> {
    const container = this.container();
    this.stopping = true;
    if (container.running) await container.destroy().catch(() => undefined);
    await this.ctx.storage.deleteAlarm().catch(() => undefined);
  }
}
