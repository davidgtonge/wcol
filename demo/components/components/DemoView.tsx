import { footer, footerLink, page, shell } from "../ui/classes.ts";
import { WiredWorkspaceHeader } from "../wiring/WiredWorkspaceHeader.tsx";
import { WiredMainContent } from "../wiring/WiredMainContent.tsx";
import { WiredDataDrawer } from "../wiring/WiredDataDrawer.tsx";

/** Layout shell — wiring components own store subscriptions. */
export function DemoView() {
  return (
    <div class={page}>
      <div class={shell}>
        <WiredWorkspaceHeader />
        <WiredMainContent />

        <footer class={footer}>
          <p>
            <code class="font-mono text-xs">npm run demo</code>
            <span class="mx-2 text-slate-300 dark:text-slate-600">·</span>
            <code class="font-mono text-xs">npm run demo:serve</code>
            <span class="mx-2 text-slate-300 dark:text-slate-600">·</span>
            <a class={footerLink} href="https://crates.io/data-access" target="_blank" rel="noopener">
              crates.io dump
            </a>
          </p>
        </footer>
      </div>

      <WiredDataDrawer />
    </div>
  );
}
