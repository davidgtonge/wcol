/** Memory is filled in after instantiate. The trace import reads it synchronously. */
export function streamWasmImports(bind: { memory?: WebAssembly.Memory }): WebAssembly.Imports {
  return {
    wcol: {
      wcol_trace(ptr: number, len: number) {
        const memory = bind.memory;
        if (!memory || len <= 0) return;
        console.error(new TextDecoder().decode(new Uint8Array(memory.buffer, ptr, len)));
      },
    },
  };
}
