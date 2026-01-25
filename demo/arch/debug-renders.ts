/** Opt-in render tracing: add `?debugRenders=1` to the URL. */
export function debugRendersEnabled(): boolean {
  try {
    return new URLSearchParams(globalThis.location?.search ?? "").has("debugRenders");
  } catch {
    return false;
  }
}

const renderCounts: Record<string, number> = {};

export function getRenderCounts(): Readonly<Record<string, number>> {
  return renderCounts;
}

export function traceRender(component: string): void {
  if (!debugRendersEnabled()) return;
  renderCounts[component] = (renderCounts[component] ?? 0) + 1;
  console.count(`[render] ${component}`);
  (globalThis as { __renderCounts?: typeof renderCounts }).__renderCounts = renderCounts;
}
