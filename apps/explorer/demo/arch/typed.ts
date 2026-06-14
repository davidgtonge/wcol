/** Discriminated-union helpers for typed handler dicts. */

export type HandlerMap<T extends { type: string }, R> = {
  [K in T["type"]]: (event: Extract<T, { type: K }>) => R;
};

export type StateHandlerMap<T extends { type: string }, S, R> = {
  [K in T["type"]]: (state: S, event: Extract<T, { type: K }>) => R;
};

export type KindHandlerMap<T extends { kind: string }, R> = {
  [K in T["kind"]]: (value: Extract<T, { kind: K }>) => R;
};

export function applyHandler<T extends { type: string }, R>(
  map: HandlerMap<T, R>,
  event: T
): R {
  type K = T["type"];
  return map[event.type as K](event as Extract<T, { type: K }>);
}

export function reduceWith<T extends { type: string }, S, R>(
  map: StateHandlerMap<T, S, R>,
  state: S,
  event: T
): R {
  type K = T["type"];
  return map[event.type as K](state, event as Extract<T, { type: K }>);
}

export function applyKind<T extends { kind: string }, R>(map: KindHandlerMap<T, R>, value: T): R {
  type K = T["kind"];
  return map[value.kind as K](value as Extract<T, { kind: K }>);
}

export const clamp = (n: number, min: number, max: number) => Math.max(min, Math.min(max, n));

export const errMsg = (err: unknown) => (err instanceof Error ? err.message : String(err));
