import { useCallback, useEffect, useRef, useState } from "preact/hooks";
import type { EffectRegistry } from "./effect-registry";
import type { EngineUpdate, ViewModelPatch } from "./types";
import type { ViewModelStore } from "./view-model-store";
import type { WorkerClient } from "./worker-client";

export type UseEngineRuntimeOptions<
  TViewModel,
  TEvent,
  TPatch extends ViewModelPatch,
  TEffect,
  TInput,
  TOutput,
> = {
  store: ViewModelStore<TViewModel>;
  client: WorkerClient<TInput, TOutput, TViewModel, TPatch, TEffect>;
  effects: EffectRegistry<TEffect, TEvent>;
  toEventInput: (event: TEvent) => TInput;
};

/** Preact hook: worker client + view-model store + effect registry. */
export function useEngineRuntime<
  TViewModel,
  TEvent,
  TPatch extends ViewModelPatch,
  TEffect,
  TInput,
  TOutput,
>(options: UseEngineRuntimeOptions<TViewModel, TEvent, TPatch, TEffect, TInput, TOutput>) {
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const applyUpdate = useCallback(
    async (
      dispatchFn: () => Promise<EngineUpdate<TViewModel, TPatch, TEffect>>,
      onEffectEvent: (event: TEvent) => void,
    ) => {
      const update = await dispatchFn();
      if (update.viewModel) {
        options.store.replace(update.viewModel);
      } else {
        options.store.applyPatchBatch(update.patches);
      }
      options.effects.runAll(update.effects, onEffectEvent);
    },
    [options.store, options.effects],
  );

  const dispatchRef = useRef<(event: TEvent) => void>(() => {});

  const dispatch = useCallback(
    async (event: TEvent) => {
      try {
        await applyUpdate(
          () => options.client.dispatch(options.toEventInput(event)),
          (effectEvent: TEvent) => dispatchRef.current(effectEvent),
        );
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [applyUpdate, options.client, options.toEventInput],
  );

  dispatchRef.current = (event: TEvent) => {
    void dispatch(event);
  };

  const init = useCallback(
    async (input: TInput) => {
      try {
        await applyUpdate(
          () => options.client.init(input),
          (effectEvent: TEvent) => dispatchRef.current(effectEvent),
        );
        setReady(true);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [applyUpdate, options.client],
  );

  useEffect(() => {
    return () => {
      options.effects.dispose();
      options.client.dispose();
    };
  }, [options.effects, options.client]);

  return { ready, error, dispatch, init };
}

export type UsePatchesOnlyRuntimeOptions<
  TViewModel,
  TEvent,
  TPatch extends ViewModelPatch,
  TInput,
  TOutput,
> = {
  store: ViewModelStore<TViewModel>;
  client: WorkerClient<TInput, TOutput, TViewModel, TPatch, never>;
  toEventInput: (event: TEvent) => TInput;
};

/**
 * Preact hook for workers that drain effects internally (e.g. wcol I/O runtime).
 * Applies patches / full view-model replacements only — no main-thread effect loop.
 */
export function usePatchesOnlyRuntime<
  TViewModel,
  TEvent,
  TPatch extends ViewModelPatch,
  TInput,
  TOutput,
>(options: UsePatchesOnlyRuntimeOptions<TViewModel, TEvent, TPatch, TInput, TOutput>) {
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const applyUpdate = useCallback(
    async (dispatchFn: () => Promise<EngineUpdate<TViewModel, TPatch, never>>) => {
      const update = await dispatchFn();
      if (update.viewModel) {
        options.store.replace(update.viewModel);
      } else {
        options.store.applyPatchBatch(update.patches);
      }
    },
    [options.store],
  );

  const dispatch = useCallback(
    async (event: TEvent) => {
      try {
        await applyUpdate(() => options.client.dispatch(options.toEventInput(event)));
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [applyUpdate, options.client, options.toEventInput],
  );

  const init = useCallback(
    async (input: TInput) => {
      try {
        await applyUpdate(() => options.client.init(input));
        setReady(true);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [applyUpdate, options.client],
  );

  useEffect(() => {
    return () => {
      options.client.dispose();
    };
  }, [options.client]);

  return { ready, error, dispatch, init, applyUpdate };
}
