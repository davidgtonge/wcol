//! R2 run upload and commit, one fixed-size part at a time.

use super::super::host::{
    await_host, HostSpillRequest, SpillProgress, SPILL_OP_RUN_COMMIT, SPILL_OP_RUN_PUT,
};
use super::super::SpillScanResume;
use super::codec::{encode_run_put_open, RunPutPlan, RUN_PART_BYTES};
use super::{CommittedRunMeta, HostR2SortedRunStore};

/// The part buffer: one part plus room for the page that overflows it. Pages are bounded by
/// the spill's batch bytes (256 KiB), well under the sixteenth of slack.
pub(super) const PART_BUFFER_BYTES: usize = RUN_PART_BYTES + RUN_PART_BYTES / 16;

/// A run whose last part is uploading or uploaded, not yet registered for the merge.
pub(super) struct StagedRun {
    object_key: String,
    byte_length: u64,
    record_count: u64,
}

pub(super) enum PutPhase {
    Idle,
    /// A full part of the open run is uploading; the run keeps filling once it lands.
    PartInFlight,
    LastPartInFlight {
        run: StagedRun,
    },
    CommitPending {
        run: StagedRun,
    },
}

impl HostR2SortedRunStore {
    pub(super) fn object_key(&self, seq: u32) -> String {
        let session = self.bridge.borrow().session_id();
        format!("wcol-spill/{session:016x}/run-{seq:06}.bin")
    }

    pub(super) fn commit_staged_run(&mut self) -> Result<(), String> {
        let PutPhase::CommitPending { run } = std::mem::replace(&mut self.put, PutPhase::Idle)
        else {
            return Err("run commit without a staged run".into());
        };
        let seq_base = self.global_seq.saturating_add(1);
        self.global_seq = self
            .global_seq
            .saturating_add(i64::try_from(run.record_count).unwrap_or(i64::MAX));
        self.runs.push(CommittedRunMeta {
            object_key: run.object_key,
            byte_length: run.byte_length,
            record_count: run.record_count,
            seq_base,
        });
        Ok(())
    }

    /// Upload a full part once the run body holds more than one. Strictly more: the run's
    /// last part then always has bytes, whichever page ends it. A run started while the
    /// previous one commits waits for that commit before its first part goes.
    pub(super) fn send_full_part(&mut self) -> Result<SpillProgress<()>, String> {
        if matches!(self.put, PutPhase::Idle) && self.run_body.len() > RUN_PART_BYTES {
            return self.send_part(false);
        }
        Ok(SpillProgress::Ready(()))
    }

    /// The host landed a full part and gave its buffer back: continue the run in it.
    pub(super) fn resume_after_part(&mut self) {
        if !matches!(self.put, PutPhase::PartInFlight) {
            return;
        }
        self.put = PutPhase::Idle;
        self.adopt_spare_body();
        self.run_body
            .reserve_exact(PART_BUFFER_BYTES.saturating_sub(self.run_body.len()));
        self.run_body.extend_from_slice(&self.run_carry);
        self.run_carry.clear();
    }

    pub(super) fn finish_put(&mut self) -> Result<SpillProgress<()>, String> {
        // Wait only on a queued request. `host_inflight` alone means the host
        // already took the op; blocking on it here prevents COMMIT after PUT.
        let queued = self.bridge.borrow().pending_request().is_some();
        match self.put {
            PutPhase::PartInFlight => Ok(SpillProgress::AwaitHost),
            PutPhase::CommitPending { .. } if queued => Ok(SpillProgress::AwaitHost),
            PutPhase::CommitPending { .. } => {
                self.commit_staged_run()?;
                Ok(SpillProgress::Ready(()))
            }
            PutPhase::LastPartInFlight { .. } if queued => Ok(SpillProgress::AwaitHost),
            PutPhase::LastPartInFlight { .. } => {
                let PutPhase::LastPartInFlight { run } =
                    std::mem::replace(&mut self.put, PutPhase::Idle)
                else {
                    return Err("run put phase changed".into());
                };
                self.put = PutPhase::CommitPending { run };
                self.request_host(SPILL_OP_RUN_COMMIT, Vec::new())
            }
            PutPhase::Idle if self.run_records == 0 => Ok(SpillProgress::Ready(())),
            PutPhase::Idle => {
                if self.run_records != self.run_expected {
                    return Err(format!(
                        "sorted run holds {} records, its header says {}",
                        self.run_records, self.run_expected
                    ));
                }
                match self.send_full_part()? {
                    SpillProgress::Ready(()) => self.send_part(true),
                    pending => Ok(pending),
                }
            }
        }
    }

    /// Hand the next part to the host: the whole body if `last`, else exactly one part
    /// with the rest carried into the next.
    fn send_part(&mut self, last: bool) -> Result<SpillProgress<()>, String> {
        let mut part = std::mem::take(&mut self.run_body);
        let len = if last { part.len() } else { RUN_PART_BYTES };
        self.run_carry.clear();
        self.run_carry.extend_from_slice(&part[len..]);
        part.truncate(len);
        self.run_parts = self.run_parts.saturating_add(1);
        self.run_bytes = self.run_bytes.saturating_add(len as u64);
        let object_key = self.object_key(self.run_seq);
        let open = encode_run_put_open(&RunPutPlan {
            object_key: object_key.clone(),
            part: self.run_parts,
            last,
            byte_length: len as u64,
            record_count: self.run_expected,
        })?;
        {
            let mut bridge = self.bridge.borrow_mut();
            bridge.run_upload = part;
            bridge.run_upload_off = 0;
        }
        self.put = if last {
            let run = StagedRun {
                object_key,
                byte_length: self.run_bytes,
                record_count: self.run_records,
            };
            self.close_run();
            PutPhase::LastPartInFlight { run }
        } else {
            PutPhase::PartInFlight
        };
        self.request_host(SPILL_OP_RUN_PUT, open)
    }

    fn close_run(&mut self) {
        self.run_records = 0;
        self.run_expected = 0;
        self.run_parts = 0;
        self.run_bytes = 0;
        self.run_last_key.clear();
    }

    fn request_host(&mut self, op: u8, append_bytes: Vec<u8>) -> Result<SpillProgress<()>, String> {
        let session = self.bridge.borrow().session_id();
        await_host(
            &mut self.bridge.borrow_mut(),
            HostSpillRequest {
                op,
                session,
                resume: SpillScanResume::default(),
                max_rows: 0,
                max_bytes: 0,
                append_bytes,
            },
        )
    }
}
