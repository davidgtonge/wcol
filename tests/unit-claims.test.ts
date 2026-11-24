import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  attemptKey,
  claimingReadAt,
  CLAIM_SOURCE,
  forgetWork,
  nextUnit,
  NO_UNIT,
  registerWork,
  type WorkQueues,
} from "../src/runtime/coordinator/unit-claims.ts";

const tasks = [
  { id: 0, work: { group: 0, ownUnits: [0, 1, 2, 3] } },
  { id: 1, work: { group: 0, ownUnits: [4] } },
  { id: 2, work: { group: 1, ownUnits: [9] } },
  { id: 3 },
];

describe("scan work queues", () => {
  it("hands out the front of the fullest queue, the asker's own on a tie", () => {
    const queues: WorkQueues = new Map();
    const a1 = attemptKey("job", 1);
    registerWork(queues, a1, [
      { id: 0, work: { group: 0, ownUnits: [0, 2] } },
      { id: 1, work: { group: 0, ownUnits: [1, 3] } },
    ]);
    // Tied: task 0 takes its own front, the heaviest work left.
    assert.equal(nextUnit(queues, a1, 0), 0);
    // Task 1's queue is now fuller, so task 0 takes its front rather than its own light unit.
    assert.equal(nextUnit(queues, a1, 0), 1);
    assert.equal(nextUnit(queues, a1, 1), 3);
    assert.equal(nextUnit(queues, a1, 1), 2);
    assert.equal(nextUnit(queues, a1, 0), null);
  });

  it("takes from another queue when it is fuller, within the same group only", () => {
    const queues: WorkQueues = new Map();
    const a1 = attemptKey("job", 1);
    registerWork(queues, a1, tasks);
    assert.equal(nextUnit(queues, a1, 1), 0);
    assert.equal(nextUnit(queues, a1, 1), 1);
    assert.equal(nextUnit(queues, a1, 1), 2);
    assert.equal(nextUnit(queues, a1, 0), 3);
    assert.equal(nextUnit(queues, a1, 0), 4);
    assert.equal(nextUnit(queues, a1, 0), null);
    assert.equal(nextUnit(queues, a1, 1), null);
    // Group 1 kept its unit.
    assert.equal(nextUnit(queues, a1, 2), 9);
  });

  it("gives every unit out exactly once", () => {
    const queues: WorkQueues = new Map();
    const a1 = attemptKey("job", 1);
    registerWork(queues, a1, tasks);
    const given: number[] = [];
    for (let turn = 0; ; turn++) {
      const unit = nextUnit(queues, a1, turn % 2);
      if (unit === null) {
        if (nextUnit(queues, a1, (turn + 1) % 2) === null) break;
        continue;
      }
      given.push(unit);
    }
    assert.deepEqual(given.sort(), [0, 1, 2, 3, 4]);
  });

  it("keeps attempts apart and refuses a task without a queue", () => {
    const queues: WorkQueues = new Map();
    registerWork(queues, attemptKey("job", 1), tasks);
    registerWork(queues, attemptKey("job", 2), tasks);
    assert.equal(nextUnit(queues, attemptKey("job", 1), 1), 0);
    assert.equal(nextUnit(queues, attemptKey("job", 2), 1), 0);
    forgetWork(queues, attemptKey("job", 1));
    assert.throws(() => nextUnit(queues, attemptKey("job", 1), 1));
    assert.throws(() => nextUnit(queues, attemptKey("job", 2), 3));
  });

  it("answers the scan in four little-endian bytes", async () => {
    const read = claimingReadAt(async () => Uint8Array.of(7), () => null);
    assert.deepEqual([...(await read(CLAIM_SOURCE, 0, 4))], [0xff, 0xff, 0xff, 0xff]);
    assert.equal(NO_UNIT, 0xffff_ffff);
    const five = claimingReadAt(async () => Uint8Array.of(7), () => 5);
    assert.deepEqual([...(await five(CLAIM_SOURCE, 0, 4))], [5, 0, 0, 0]);
    assert.deepEqual([...(await five(0, 0, 1))], [7]);
  });
});
