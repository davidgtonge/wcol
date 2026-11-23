export type PhaseMark = {
  phase: number;
  port: number;
  rows: number;
  frames: number;
  groupBytes: number;
};

let phaseSink: ((mark: PhaseMark) => void) | null = null;

/** The node sets this for the one reducer being traced. Other isolates leave it empty. */
export function setPhaseSink(sink: ((mark: PhaseMark) => void) | null): void {
  phaseSink = sink;
}

/** Memory is filled in after instantiate. The trace import reads it synchronously. */
export function streamWasmImports(bind: { memory?: WebAssembly.Memory }): WebAssembly.Imports {
  return {
    wcol: {
      wcol_trace(ptr: number, len: number) {
        const memory = bind.memory;
        if (!memory || len <= 0) return;
        console.error(new TextDecoder().decode(new Uint8Array(memory.buffer, ptr, len)));
      },
      wcol_phase(phase: number, port: number, rows: number, frames: number, groupBytes: number) {
        phaseSink?.({ phase, port, rows, frames, groupBytes });
      },
    },
  };
}
