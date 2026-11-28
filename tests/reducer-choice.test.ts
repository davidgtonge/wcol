/** Where a job's reducers run, and the budget container reducers plan with. */
import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import {
  chooseReducers,
  CONTAINER_REDUCERS,
  containerPlanKnobs,
  NODE_REDUCER_MEMORY_BYTES,
  parseReducerRequest,
} from "../cloudflare/src/coordinator/reducer-choice.ts";
import { containerGroupBytes } from "../cloudflare/src/node/containers.ts";

const MIB = 1024 * 1024;
const GIB = 1024 * MIB;
const auto = parseReducerRequest({});

describe("reducer choice", () => {
  it("defaults to auto on basic containers", () => {
    assert.deepEqual(auto, { runtime: "auto", instance: "basic" });
    assert.deepEqual(parseReducerRequest({ reducers: "container", reducerInstance: "lite" }), { runtime: "container", instance: "lite" });
    assert.equal(parseReducerRequest({ reducerInstance: "huge" }).instance, "basic");
  });

  it("keeps groups that fit node reducers on Durable Objects", () => {
    assert.equal(chooseReducers(auto, 0).runtime, "node");
    assert.equal(chooseReducers(auto, 100 * MIB).runtime, "node");
    assert.equal(chooseReducers(auto, NODE_REDUCER_MEMORY_BYTES).runtime, "node");
  });

  it("moves groups node reducers would spill into containers", () => {
    // Q32 on full hits: about 100M groups.
    const choice = chooseReducers(auto, 100_000_000 * 128);
    assert.equal(choice.runtime, "container");
    assert.match(choice.reason, /exceeds the 2\.0 GiB node reducers hold/);
  });

  it("honours an explicit runtime whatever the estimate", () => {
    assert.equal(chooseReducers({ runtime: "node", instance: "basic" }, 40 * GIB).runtime, "node");
    assert.equal(chooseReducers({ runtime: "container", instance: "basic" }, 0).runtime, "container");
  });

  it("plans container reducers with the instance's budget and the measured count", () => {
    const planned = containerPlanKnobs({}, "basic");
    assert.equal(planned.targetGroupBytes, 896 * MIB);
    assert.equal(planned.hashReducers, CONTAINER_REDUCERS);
    const own = containerPlanKnobs({ targetGroupBytes: 1024, hashReducers: 8 }, "basic");
    assert.equal(own.targetGroupBytes, 1024);
    assert.equal(own.hashReducers, 8);
  });

  it("sizes a container's budget as its memory less 128 MiB", () => {
    assert.equal(containerGroupBytes("basic"), 896 * MIB);
    assert.equal(containerGroupBytes("lite"), 128 * MIB);
    assert.equal(containerGroupBytes("standard-1"), 3968 * MIB);
  });
});
