/** Node test stand-in. Worker bundles keep `cloudflare:workers` external. */
export class DurableObject<Env = unknown> {
  protected readonly ctx: DurableObjectState;
  protected readonly env: Env;
  constructor(state: DurableObjectState, env: Env) {
    this.ctx = state;
    this.env = env;
  }
}
