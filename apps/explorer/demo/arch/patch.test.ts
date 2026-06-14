import assert from "node:assert/strict";
import { test } from "node:test";
import { applyPatches } from "@dtonge/engine-shell";

const demoVm = () => ({
  urlInput: "",
  dataDrawerOpen: false,
  queryDraft: {
    searchText: "",
    filters: [] as { id: string; column: string; op: string; value: string }[],
  },
});

test("rust-shaped replace patch applies on urlInput", () => {
  const prev = demoVm();
  const patches = [{ op: "replace" as const, path: ["urlInput"], value: "hello" }];
  const applied = applyPatches(prev, patches);
  assert.equal(applied.urlInput, "hello");
});
