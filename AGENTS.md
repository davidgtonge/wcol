# WCOL Implementation Guidelines

## Engineering philosophy

Write clean, minimal, functional code. Prioritize simplicity, correctness, readability and performance.

**Prefer elegant composition of small, well-defined functions over large classes, stateful abstractions or complex orchestration frameworks.**

The goal is not merely to make the tests pass. The implementation should make the system easier to understand, maintain and extend.

### 1. Functional core, imperative shell

Separate pure decisions from side effects.

**Pure logic:**
- Graph traversal and task selection
- Placement and transport decisions
- Recovery and replay planning
- Validation and state transition decisions
- Deterministic key and identifier generation
- Result and manifest construction

**Side effects:**
- Durable Object RPC
- R2 reads and writes
- SQLite transactions
- WASM execution
- Stream consumption
- Alarm scheduling

Pure functions should receive their dependencies explicitly, return values without mutation of external state, and be straightforward to unit test.

The imperative layer should execute decisions made by the pure core.

For example:

```typescript
const tasks = selectTasksToRun(graph, ledger);

const recovery = planRecovery(graph, ledger);

const transitions = applyTaskResult(
  ledger,
  taskResult
);
```

These functions should not perform I/O.

### 2. DRY: one implementation of each concept

This is particularly important for the graph runner.

There must be:

- One executable graph representation
- One graph scheduler
- One task execution contract
- One mechanism for opening graph edges
- One recovery path using the same runner
- One definition of task states and transitions

Do not duplicate orchestration for JSON, Parquet, concat, local execution or remote execution.

Differences belong in explicit plan data, sink implementations and narrow I/O adapters.

**Do not create a second implementation because it seems easier for one particular use case.**

Equally, avoid premature abstractions. Two similar functions are preferable to a complicated generic abstraction that obscures their differences.

### 3. Small functions, explicit data flow

Functions should have one clear responsibility.

Prefer:

```typescript
const graph = await loadGraph(job);
const recovery = planRecovery(graph, ledger);
const results = await runGraph(graph, recovery, runtime);
await commitResults(job, results);
```

Avoid functions that mix loading, validation, planning, execution, retries, persistence and response formatting.

Keep dependency direction clear:

```text
Pure planning and graph logic
              ↑
Execution and recovery orchestration
              ↑
Cloudflare adapters / I/O
```

Infrastructure should depend on domain logic, not the reverse.

### 4. Use explicit types

Prefer discriminated unions and exhaustive matching over optional fields, string comparisons and implicit state.

- Make illegal states difficult to represent.
- Distinguish ephemeral stream completion from durable task completion.
- Use explicit attempt identities.
- Keep graph definitions immutable after planning.
- Keep runtime execution state separate from the graph.
- Validate external input once at the boundary.

In Rust, prefer enums, pattern matching and `Result`.

In TypeScript, prefer `readonly` data structures, discriminated unions and exhaustive switches where appropriate.

Avoid `any`, non-null assertions, boolean mode flags and broad casts unless unavoidable and documented.

### 5. Minimize mutable state

Mutable state is appropriate where necessary for performance, streaming and external resources.

However:

- Keep mutation local and ownership obvious.
- Avoid hidden shared state.
- Avoid mutable global registries.
- Avoid storing state that can cheaply be derived.
- Do not maintain two sources of truth for the same information.

The physical graph is authoritative for computation, placement and transport.

The task ledger is authoritative for execution state.

R2 stores immutable programs and durable outputs.

### 6. Keep async execution understandable

Concurrency should be bounded and explicit.

- Avoid unbounded `Promise.all`.
- Use one small, reusable concurrency limiter.
- Separate RPC-opening limits from active-stream memory limits.
- Release permits in `finally` or on the appropriate lifecycle transition.
- Never hold a permit while waiting for nested work requiring that same permit.
- Respect streaming backpressure: graph fan-in is per-edge pull with a bounded ready-port scheduler (at most one pending read and one ready chunk per active port). Do not concatenate producer streams into one Wasm ingress.
- Propagate cancellation and errors deliberately.

Do not introduce custom concurrency machinery when existing primitives suffice.

Avoid deeply nested callbacks or recursive orchestration that hides resource ownership.

### 7. Recovery should be a planning operation

Do not build a separate recovery engine.

Recovery should:

1. Read the persisted graph and ledger.
2. Use a pure function to determine which tasks require replay.
3. Claim a new execution attempt transactionally.
4. Execute the selected work through the existing graph runner.
5. Commit the results with attempt fencing.

Keep leases, idempotency, replay and publication as distinct concepts.

Do not assume R2 and SQLite operations can be committed atomically together. Model external publication as a recoverable state transition.

### 8. Avoid unnecessary abstractions

Do not introduce:

- A generic workflow engine
- An event bus
- A new task scheduler
- An actor framework
- A general-purpose dependency injection container
- Additional graph representations
- Redundant wrapper classes
- Configuration options without a concrete use case

Use plain functions, data structures and narrow interfaces.

Reuse existing WCOL primitives, particularly `GraphHost`, `LinearColocatedGraph`, WCS and the executable physical graph.

Before adding an abstraction, check whether an existing concept already handles the responsibility.

### 9. Make error handling precise

- Return explicit, typed errors where practical.
- Fail early on invalid plans.
- Preserve the original cause of execution failures.
- Distinguish retryable infrastructure errors from permanent planning errors.
- Never silently fall back to another execution path.
- Do not swallow errors to keep a job appearing successful.

Recovery must be deliberate, not an accidental consequence of catching every exception.

### 10. Test behavior, not implementation details

Test pure functions directly with small, readable inputs.

Use integration tests for Cloudflare behavior and genuine multi-DO execution.

Important invariants:

- Identical graph and ledger inputs produce identical scheduling decisions.
- Every task executes on its planned slot.
- Every edge uses its declared transport.
- An attempt cannot commit after being superseded.
- Ephemeral streams are replayed when needed.
- Durable outputs are reused only when verified.
- No source data is silently lost or duplicated.
- Memory and concurrency remain bounded.

Avoid excessive mocking. Prefer real data structures and narrow fake I/O boundaries.

### 11. Respect existing architecture

Before changing code:

1. Read the relevant existing implementation.
2. Identify the smallest set of changes needed.
3. Reuse existing functions and types wherever possible.
4. Remove superseded paths rather than leaving duplicate implementations.
5. Update affected tests and documentation.

Do not perform broad refactoring unrelated to the task.

Do not retain obsolete branches, compatibility layers or configuration flags unless explicitly required.

### 12. Keep the codebase lean

After implementation:

- Remove dead code and unused imports.
- Eliminate duplicated execution logic.
- Prefer meaningful names over explanatory comments.
- Comment on non-obvious invariants and *why* decisions are made, not what straightforward code does.
- Keep modules cohesive and their public interfaces small.
- Avoid unnecessary allocation and copying, especially on WASM and streaming paths.
- Run formatting, linting and relevant tests.
- Report any remaining architectural duplication or technical debt.

## Acceptance standard

Before considering the work complete, ask:

1. Could this be expressed with fewer concepts?
2. Is each responsibility implemented exactly once?
3. Can the important decisions be tested without R2, SQLite or Durable Objects?
4. Is mutation isolated and clearly owned?
5. Are concurrency and failure semantics explicit?
6. Does the implementation follow the physical graph rather than reconstructing orchestration?
7. Have obsolete code paths been removed?

**Prefer the smallest coherent implementation that preserves the architecture and satisfies the correctness requirements.**

Do not optimize for the fewest lines of code. Optimize for the fewest concepts, clear data flow, and the lowest long-term maintenance cost.