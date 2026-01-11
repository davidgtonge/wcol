export type EffectRegistry<TEffect, TEvent> = {
  run: (effect: TEffect, dispatch: (event: TEvent) => void) => void;
  runAll: (effects: TEffect[], dispatch: (event: TEvent) => void) => void;
  dispose: () => void;
};

export type BuiltinTimerEffect = {
  type: "timerStart";
  id: string;
  intervalMs: number;
};

export type BuiltinTimerStopEffect = {
  type: "timerStop";
  id: string;
};

export type BuiltinRandomIntEffect = {
  type: "randomInt";
  id: string;
  min: number;
  max: number;
};

export type BuiltinRandomIntResult = {
  type: "randomInt";
  value: number;
};

export type BuiltinEffectHandlers<TEffect, TEvent> = {
  onTimerTick: (id: string) => TEvent;
  onRandomInt: (id: string, result: BuiltinRandomIntResult) => TEvent;
  match: (effect: TEffect) => BuiltinTimerEffect | BuiltinTimerStopEffect | BuiltinRandomIntEffect | null;
};

/** Built-in timer + randomInt handlers shared by Tetris-style engines. */
export function createBuiltinEffectRegistry<TEffect, TEvent>(
  handlers: BuiltinEffectHandlers<TEffect, TEvent>,
): EffectRegistry<TEffect, TEvent> {
  const timers = new Map<string, ReturnType<typeof setInterval>>();

  function stopTimer(id: string): void {
    const handle = timers.get(id);
    if (handle) {
      clearInterval(handle);
      timers.delete(id);
    }
  }

  function run(effect: TEffect, dispatch: (event: TEvent) => void): void {
    const builtin = handlers.match(effect);
    if (!builtin) return;

    switch (builtin.type) {
      case "timerStart": {
        stopTimer(builtin.id);
        const handle = setInterval(() => {
          dispatch(handlers.onTimerTick(builtin.id));
        }, builtin.intervalMs);
        timers.set(builtin.id, handle);
        break;
      }
      case "timerStop": {
        stopTimer(builtin.id);
        break;
      }
      case "randomInt": {
        const span = builtin.max - builtin.min + 1;
        const value = builtin.min + Math.floor(Math.random() * span);
        dispatch(
          handlers.onRandomInt(builtin.id, {
            type: "randomInt",
            value,
          }),
        );
        break;
      }
    }
  }

  return {
    run,
    runAll: (effects, dispatch) => effects.forEach((e) => run(e, dispatch)),
    dispose: () => {
      timers.forEach((handle) => clearInterval(handle));
      timers.clear();
    },
  };
}
