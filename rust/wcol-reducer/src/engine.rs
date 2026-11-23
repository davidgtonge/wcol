//! One hash reducer's stream pool, driven natively: the loop the Wasm host runs
//! (`src/runtime/host/stream-pool.ts`), with scan edges arriving on channels and spill runs
//! on local disk.

use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::time::Duration;

use wcol_engine::exec::stream_pool::{
    StreamPoolSession, STEP_FAILED, STEP_FINISHED, STEP_PROGRESS, STEP_SPILL_EXEC,
};

use crate::spill::LocalSpill;

/// Same per-step work budget as the Wasm host.
const STEP_WORK: u32 = 100;
const SCRATCH_BYTES: usize = 1024 * 1024;
const INPUT_SLOT_BYTES: u32 = 256 * 1024;
const OUTPUT_SLOT_BYTES: u32 = 256 * 1024;
/// How long an idle loop waits for an edge before stepping again.
const IDLE_WAIT: Duration = Duration::from_millis(20);

/// What a scan edge delivers to its input port.
pub enum Edge {
    Bytes(Vec<u8>),
    /// The edge's stream ended.
    End,
    /// The scan finished without opening this partition.
    Absent,
}

/// One input port: its edge channel and what it took from it but could not hand over yet.
struct Port {
    edge: Receiver<Edge>,
    held: Option<Held>,
    done: bool,
}

/// An edge item waiting for the port's slot to empty.
enum Held {
    Bytes(Vec<u8>, usize),
    End,
    Absent,
}

pub struct Report {
    pub spill_written: u64,
    pub spill_read: u64,
    /// The engine's own spill stats: runs written, groups in them, and the largest live
    /// table it measured (its estimate, which the finish decision compares with the budget).
    pub spill_runs: u64,
    pub spill_groups: u64,
    pub peak_live_bytes: u64,
}

/// Run `program` until its output has all been sent. `wake` is signalled whenever an edge
/// delivers, so an idle loop sleeps instead of spinning.
pub fn run(
    program: &[u8],
    edges: Vec<Receiver<Edge>>,
    wake: &Receiver<()>,
    output: &SyncSender<Vec<u8>>,
    mut spill: LocalSpill,
) -> Result<Report, String> {
    let inputs = u32::try_from(edges.len()).map_err(|_| "too many inputs")?;
    let mut session = StreamPoolSession::open_graph(
        program,
        SCRATCH_BYTES,
        inputs,
        1,
        INPUT_SLOT_BYTES,
        OUTPUT_SLOT_BYTES,
        1,
    )
    .map_err(|e| e.to_string())?;
    let mut ports: Vec<Port> = edges
        .into_iter()
        .map(|edge| Port {
            edge,
            held: None,
            done: false,
        })
        .collect();
    loop {
        let fed = ports
            .iter_mut()
            .zip(0u32..)
            .try_fold(false, |fed, (port, index)| {
                Ok::<_, String>(feed(&mut session, index, port)? || fed)
            })?;
        let status = session.step(STEP_WORK);
        if status & STEP_FAILED != 0 {
            return Err(session.error().unwrap_or("reducer failed").to_string());
        }
        if status & STEP_SPILL_EXEC != 0 {
            spill.serve(&mut session)?;
            continue;
        }
        let sent = drain(&mut session, output)?;
        if status & STEP_FINISHED != 0 && session.output_len(0) == 0 {
            use wcol_engine::ops::query::batch as stats;
            return Ok(Report {
                spill_written: spill.bytes_written,
                spill_read: spill.bytes_read,
                spill_runs: stats::last_spill_runs(),
                spill_groups: stats::last_spill_groups_flushed(),
                peak_live_bytes: stats::last_spill_peak_live_bytes(),
            });
        }
        if !fed && !sent && status & STEP_PROGRESS == 0 {
            match wake.recv_timeout(IDLE_WAIT) {
                Ok(()) | Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Err("reducer job dropped".into()),
            }
        }
    }
}

/// Hand the port's next edge item to its input slot while the slot is empty; true if
/// anything moved. The slot takes one commit of up to `INPUT_SLOT_BYTES` at a time, and an
/// end or absence waits, like bytes, until the engine has emptied it.
fn feed(session: &mut StreamPoolSession, index: u32, port: &mut Port) -> Result<bool, String> {
    let mut moved = false;
    while !port.done && session.input_free(index) == 1 {
        let held = match port.held.take() {
            Some(held) => held,
            None => match port.edge.try_recv() {
                Ok(Edge::Bytes(bytes)) => Held::Bytes(bytes, 0),
                Ok(Edge::End) => Held::End,
                Ok(Edge::Absent) => Held::Absent,
                Err(TryRecvError::Empty) => return Ok(moved),
                Err(TryRecvError::Disconnected) => {
                    return Err(format!("scan edge {index} closed before it ended"))
                }
            },
        };
        match held {
            Held::Bytes(bytes, at) => {
                let take = (INPUT_SLOT_BYTES as usize).min(bytes.len() - at);
                // SAFETY: an empty slot has `INPUT_SLOT_BYTES` writable bytes until
                // `input_commit`, and only this thread touches the session.
                let dst = unsafe { std::slice::from_raw_parts_mut(session.input_ptr(index), take) };
                dst.copy_from_slice(&bytes[at..at + take]);
                session
                    .input_commit(index, take as u32)
                    .map_err(|e| e.to_string())?;
                if at + take < bytes.len() {
                    port.held = Some(Held::Bytes(bytes, at + take));
                }
            }
            Held::End => {
                session.input_end(index).map_err(|e| e.to_string())?;
                port.done = true;
            }
            Held::Absent => {
                session.input_absent(index).map_err(|e| e.to_string())?;
                port.done = true;
            }
        }
        moved = true;
    }
    Ok(moved)
}

/// Send the output slot's bytes, if any, then release the slot.
fn drain(session: &mut StreamPoolSession, output: &SyncSender<Vec<u8>>) -> Result<bool, String> {
    let len = session.output_len(0) as usize;
    if len == 0 {
        return Ok(false);
    }
    // SAFETY: the slot holds `len` readable bytes until `output_release`.
    let bytes = unsafe { std::slice::from_raw_parts(session.output_ptr(0), len) }.to_vec();
    output
        .send(bytes)
        .map_err(|_| "reducer output consumer went away")?;
    session.output_release(0).map_err(|e| e.to_string())?;
    Ok(true)
}
