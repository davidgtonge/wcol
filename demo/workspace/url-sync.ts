/** Sync worker-owned shareable URL hash to the browser location bar. */
export function readWorkspaceHash(): string {
  return window.location.hash;
}

export function applyShareableUrl(hash: string): void {
  if (!hash || hash === window.location.hash) return;
  const url = `${window.location.pathname}${window.location.search}${hash}`;
  window.history.replaceState(window.history.state, "", url);
}

export function onHashChange(handler: (hash: string) => void): () => void {
  const listener = () => handler(window.location.hash);
  window.addEventListener("hashchange", listener);
  return () => window.removeEventListener("hashchange", listener);
}
