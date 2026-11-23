# Host I/O contract (ABI v1)

Rust owns execution, ports, buffering, backpressure, and memory credits.
TypeScript is an asynchronous system-call layer: it copies bytes, reports
completions, and never parses WCS, partition IDs, or plans to route data.

GraphHost now polls this table (`wcs_io_poll` / `wcs_io_complete`). The host
loop in `run-wcol-engine.ts` copies bytes and reports completions. It does not
choose ports, retain `leftover` via `subarray`, or demux WCS.

Hash fan-out writes use the partition id as the output port. Empty partitions
never emit Write or Close; the coordinator seals those edges.

Reducer ingress is the same table. Rust issues `Read` on specific input ports
with at most one outstanding sequential read per port and at most
`max_active_ingress` live streams (default 8, `wcs_io_set_max_active_ingress`).
TypeScript attaches that port's scan stream lazily and copies bytes. It does
not call `nextReady`, parse WCS, or choose which producer may progress.
Parquet ingest does not use sequential stream reads (`stream_input_ports = 0`).

## Records

Little-endian, version `HOST_IO_ABI_VERSION = 1`.

**Action (32 bytes):** `ticket u32`, `kind u16`, `flags u16`, `port u32`,
`slot u32`, `length u32`, `aux u32`, `extra u64`.

**Completion (16 bytes):** `ticket u32`, `kind u16`, `status u16`,
`length u32`, `error_code u32`.

Kinds: Open=1, Read=2, Write=3, Close=4, Abort=5, Sql=6, Yield=7.
Status: Ok=0, Eof=1, Failed=2. Yield uses ticket 0 and needs no completion.
Read flags: `FLAG_READ_AT=1` (`extra` = offset, `aux` = source id).

One poll may return many actions. Completions are queued and delivered on a
single engine thread. Host callbacks must not invoke the engine reentrantly.

## Buffer ownership

| Region | Owner | Lifetime |
| --- | --- | --- |
| Input slot | Rust | Assigned on Read; host copies into it before Read completion; freed after the engine consumes (passthrough: after the matching Write completes) |
| Output slot | Rust | Valid until Write completion; host copies out before the async write |
| Host remainder | TypeScript | Tight `slice()` copy when a transport chunk exceeds `max_bytes`; charged until the next Read on that port drains it |
| Host write copy | TypeScript | Charged until the physical write settles |

Do not retain a large `ArrayBuffer` via `subarray`. WASM linear memory is not
borrowed across `memory.grow` or an await. Slot capacities are reused; they
must plateau.

Per-port FIFO: at most one outstanding Read or Write per port. The outstanding
operation budget is not a cap on how many ports exist.

## SQLite is a byte stream

Rust does not issue SQL. `HostIoKind::Sql` is unused. SQLite is another Open
resource:

| `aux` | Resource | Host |
| --- | --- | --- |
| 4 | `SqliteWrite { session }` | `sqlite.writable(session)` |
| 5 | `SqliteRead { session, order }` | `sqlite.readable(session, { kind })` |

`flags` on a SQLite read Open: 0 = sorted key order, 1 = insertion order.

Framing (Rust encodes, host stores blobs as-is):

```
u32 key_length
u32 value_length
key bytes
value bytes
```

The host batches up to 50 inserts (100 SQLite bindings) and paginates reads
with `(key_blob, seq)` keysets. Close on a writer flushes the last batch.
Open a reader only after that writer has closed.

Spill still arrives as `HostIoKind::Sql` until GraphHost opens SQLite as byte
ports. The host adapter runs the existing spill SQL and writes the scan page
into the slot.

## Baseline

- Native hash WCS exchange: `rust/wcol/tests/runtime/hash_partition_wcs_exchange.rs`
- Hosted 16×16: `npm run test:cf-hash-fanout` (`HASH_FANOUT_JOBS=1`)
- Q15 discard: `npm run test:cf-q15-discard`
- Production driver: `run-wcol-engine.ts` uses ABI v1. `demuxPartitionStream`
  remains for coordinator-side `pushHashFramesOnNode` until that helper is
  deleted after parity tests.
