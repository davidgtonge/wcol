import {
  createViewModelStore,
  usePatchesOnlyRuntime,
} from "@dtonge/engine-shell";
import { useCallback, useEffect, useMemo, useRef } from "preact/hooks";
import { applyShareableUrl, onHashChange, readWorkspaceHash } from "../workspace/url-sync.ts";
import { AppProvider } from "../arch/app-context.tsx";
import { emptyViewModel } from "../arch/empty-view-model.ts";
import type { Event } from "../arch/events.ts";
import { errMsg } from "../arch/typed.ts";
import { DemoView } from "../components/DemoView.tsx";
import type { AppEvent, ViewModel, ViewModelPatch, WorkerInput, WorkerOutput } from "../protocol/types.ts";
import {
  createWcolWorkerClient,
  wcolEventInput,
  wcolInitInput,
} from "../worker/wcol-client.ts";
import { render } from "preact";

export function useWcolDemo() {
  const store = useMemo(() => createViewModelStore(emptyViewModel()), []);
  const client = useMemo(() => createWcolWorkerClient(), []);

  const { ready, error, dispatch: baseDispatch, init, applyUpdate } = usePatchesOnlyRuntime<
    ViewModel,
    AppEvent,
    ViewModelPatch,
    WorkerInput,
    WorkerOutput
  >({
    store,
    client,
    toEventInput: wcolEventInput,
  });

  const dispatch = useCallback(
    async (event: Event) => {
      try {
        if (event.type === "LOAD_FILE") {
          await applyUpdate(() => client.openFile(event.file));
          return;
        }
        await baseDispatch(event);
      } catch (err) {
        await baseDispatch({
          type: "QUERY_FAILED",
          message: errMsg(err),
        });
      }
    },
    [applyUpdate, baseDispatch, client],
  );

  const hydrated = useRef(false);
  const initialHash = useRef(readWorkspaceHash());

  useEffect(() => {
    void (async () => {
      await init(wcolInitInput());
      const hash = initialHash.current;
      if (hash.length > 1 && !hydrated.current) {
        hydrated.current = true;
        await baseDispatch({ type: "WORKSPACE_HYDRATE", hash });
      }
    })();
  }, [init, baseDispatch]);

  useEffect(() => {
    return store.subscribe(() => {
      applyShareableUrl(store.getSnapshot().explore.shareableUrl);
    });
  }, [store]);

  useEffect(() => {
    return onHashChange((hash) => {
      if (hash.length > 1) {
        void baseDispatch({ type: "WORKSPACE_HYDRATE", hash });
      }
    });
  }, [baseDispatch]);

  useEffect(() => {
    const onError = (e: ErrorEvent) => {
      void dispatch({ type: "FILE_OPEN_FAILED", message: e.message || "Script error" });
    };
    const onRejection = (e: PromiseRejectionEvent) => {
      void dispatch({ type: "QUERY_FAILED", message: errMsg(e.reason) });
    };
    window.addEventListener("error", onError);
    window.addEventListener("unhandledrejection", onRejection);
    return () => {
      window.removeEventListener("error", onError);
      window.removeEventListener("unhandledrejection", onRejection);
    };
  }, [dispatch]);

  return { store, ready, error, dispatch };
}

export function mountWcolDemo(root: HTMLElement) {
  function App() {
    const { store, ready, error, dispatch } = useWcolDemo();

    if (error) {
      return (
        <div class="p-8 text-center text-red-600">
          <p>{error}</p>
        </div>
      );
    }

    if (!ready) {
      return (
        <div class="p-8 text-center text-slate-500">
          <p>Starting worker…</p>
        </div>
      );
    }

    return (
      <AppProvider store={store} onEvent={(e) => void dispatch(e)}>
        <DemoView />
      </AppProvider>
    );
  }

  render(<App />, root);
}
