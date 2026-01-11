import type { WireMessage } from "./types";

export type WasmWorkerOptions<M> = {
  loadWasm: () => Promise<unknown>;
  createEngine: (wasm: unknown) => M;
  handleInput: (engine: M, payload: Uint8Array) => Uint8Array;
};

/** Install the thin worker shell: CBOR bytes in, CBOR bytes out — no JS domain parsing. */
export function installWasmWorker<M>(options: WasmWorkerOptions<M>): void {
  const engineReady = options.loadWasm().then((wasm) => options.createEngine(wasm));

  self.onmessage = async (event: MessageEvent<WireMessage>) => {
    const engine = await engineReady;
    const inbound = new Uint8Array(event.data.bytes);
    const response = options.handleInput(engine, inbound);
    const outBytes = new Uint8Array(response).buffer;
    const out: WireMessage = { bytes: outBytes };
    self.postMessage(out, { transfer: [outBytes] });
  };
}
