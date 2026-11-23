import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { preferHashFailure } from "../src/runtime/coordinator/run-hash-job.ts";
import { isFatalTaskError } from "../src/runtime/coordinator/task-errors.ts";

describe("fatal task errors", () => {
  it("treats an oversized dictionary entry as fatal and keeps it over a later cancel", () => {
    const fatal = new Error(
      "wcol-fatal: wcs: dictionary_entry_too_large (scope=1 column=2 index=0 encoded=90000 max=65523)",
    );
    const cancel = new Error("transport EOF before FRAME_END");
    assert.equal(isFatalTaskError(fatal), true);
    assert.equal(isFatalTaskError("single_row_too_large"), true);
    assert.equal(isFatalTaskError(new Error("reducer spilled")), false);
    assert.equal(preferHashFailure(cancel, fatal), fatal);
    assert.equal(preferHashFailure(fatal, cancel), fatal);
  });
});
