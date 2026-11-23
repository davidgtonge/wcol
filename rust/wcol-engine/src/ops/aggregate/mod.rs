//! Aggregate: row WCOL → typed partial-state WCOL.

use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::ir::{AggExpr, CatalogColumn};
use crate::ops::query::batch::finish_stream::BatchQueryFinish;
use crate::ops::query::batch::BatchQuery;
use crate::ops::query::SelectionMask;
use crate::spill::SpillProgress;
use crate::stream::partials::PartialStateWcol;
use crate::stream::OwnedWcolBatch;

pub struct AggregateOp {
    query: BatchQuery,
    finished: bool,
    finish: Option<BatchQueryFinish>,
    /// Soft cap on resident group-table bytes. `None` does not check.
    group_budget: Option<u64>,
    /// Set on a scan-side combiner; `None` holds every group until input ends.
    combiner: Option<Combiner>,
}

/// A scan-side combiner: past `flush_bytes` resident bytes, every group goes to its
/// reducer as encoded partial frames (`outbox`) and the table starts again.
struct Combiner {
    flush_bytes: u64,
    route: crate::ops::exchange::CombinerRoute,
    /// `(partition, FRAME_PARTIAL payload)`, in the order the host sends them.
    outbox: std::collections::VecDeque<(u32, Vec<u8>)>,
}

/// Resident table bytes after which a scan-side combiner sends its groups on. Linear memory
/// never shrinks, so a flush's peak (the table, one partition's partial, the encoded frames)
/// is the scan's: at 16 MiB, building the whole partial at once ran scans combining URLs out
/// of memory beside their pipes.
pub const COMBINER_FLUSH_BYTES: u64 = 4 * 1024 * 1024;

/// Bytes the planner charges for one group, including hash-table overhead.
pub const ESTIMATED_GROUP_BYTES: u64 = crate::spill::ESTIMATED_GROUP_BYTES;

/// Smallest live table a spill-enabled reducer always finishes from memory (the default
/// flush target).
const EAGER_FINISH_MIN_BYTES: u64 = 16 * 1024 * 1024;

/// Largest live table a reducer with `budget` bytes finishes from memory: finishing builds
/// the whole result beside the table, so the table may take half the budget.
fn eager_finish_max_bytes(budget: u64) -> u64 {
    (budget / 2).max(EAGER_FINISH_MIN_BYTES)
}

impl AggregateOp {
    pub fn from_batch(query: BatchQuery) -> Self {
        Self {
            query,
            finished: false,
            finish: None,
            group_budget: None,
            combiner: None,
        }
    }

    /// Fail the reducer when its group table exceeds this many bytes. Groups are not dropped.
    pub fn set_group_budget(&mut self, bytes: u64) {
        self.group_budget = Some(bytes);
    }

    pub fn resident_groups(&self) -> usize {
        self.query.resident_groups()
    }

    pub(crate) fn publish_growth_gauges(&self) {
        self.query.publish_growth_gauges();
    }

    pub fn resident_state_bytes(&self) -> u64 {
        self.query.resident_state_bytes()
    }

    pub fn from_aggregate(
        group_by: &[u32],
        aggregates: &[AggExpr],
        limit: Option<u32>,
        offset: u32,
        order_groups_by_count: bool,
        catalog: &[CatalogColumn],
    ) -> Result<Self, String> {
        Ok(Self::from_batch(BatchQuery::from_ir(
            &[],
            group_by,
            aggregates,
            limit,
            offset,
            order_groups_by_count,
            catalog,
        )?))
    }

    pub fn spill_flush_incomplete(&self) -> bool {
        self.query.spill_flush_incomplete()
    }

    pub fn flush_live_run(&mut self) -> Result<SpillProgress<()>, String> {
        self.query.flush_live_run()
    }

    pub fn live_group_len(&self) -> usize {
        self.query.live_group_len()
    }

    pub fn set_final_rank(&mut self, index: Option<u32>) {
        self.query.set_final_rank(index);
    }

    pub fn enable_group_spill(
        &mut self,
        config: crate::spill::SpillConfig,
        store: Box<dyn crate::spill::SpillStore>,
    ) -> Result<(), String> {
        self.query.enable_spill(config, store)
    }

    /// Make this a scan-side combiner: past `flush_bytes` (see [`COMBINER_FLUSH_BYTES`]) and
    /// at the end of input, every group goes to the reducer `route` names, as encoded partial
    /// frames taken with [`AggregateOp::pop_routed_payload`]. It then emits no stream items.
    /// Only for a table without spill: a spill keeps its groups.
    pub fn set_combiner(
        &mut self,
        flush_bytes: u64,
        route: crate::ops::exchange::CombinerRoute,
    ) -> Result<(), String> {
        if self.query.has_group_spill() {
            return Err("a spilling aggregate cannot also flush partials".to_string());
        }
        self.combiner = Some(Combiner {
            flush_bytes,
            route,
            outbox: std::collections::VecDeque::new(),
        });
        Ok(())
    }

    /// The next encoded partial frame a combiner has for a reducer: `(partition, payload)`.
    pub fn pop_routed_payload(&mut self) -> Option<(u32, Vec<u8>)> {
        self.combiner.as_mut()?.outbox.pop_front()
    }

    pub fn has_routed_payload(&self) -> bool {
        self.combiner
            .as_ref()
            .is_some_and(|combiner| !combiner.outbox.is_empty())
    }

    /// Route every group to its reducer's outbox and empty the table. The emptied table keeps
    /// half the budget: kept above it, it would flush after every batch.
    fn flush_combiner(&mut self) -> Result<(), String> {
        let Some(combiner) = self.combiner.as_mut() else {
            return Ok(());
        };
        let outbox = &mut combiner.outbox;
        self.query.flush_routed(
            &combiner.route,
            combiner.flush_bytes / 2,
            |partition, state| {
                for payload in crate::stream::partials::partial_state_payloads(
                    &state,
                    crate::stream::wcs::MAX_WCS_PAYLOAD,
                )? {
                    outbox.push_back((partition, payload));
                }
                Ok(())
            },
        )
    }

    pub fn set_final_aggregate(&mut self, final_aggregate: bool) {
        self.query.set_final_aggregate(final_aggregate);
    }

    pub fn metadata_only(&self) -> bool {
        self.query.metadata_only()
    }

    /// [`Self::apply_metadata`] for a query that reads no columns (only `COUNT(*)`).
    pub fn apply_metadata_rows(&mut self, rows: u64) -> Result<(), String> {
        self.query.add_metadata_rows(rows)
    }

    /// Answer from footer metadata: `rows` for `COUNT(*)`, `bounds` (one per aggregate) for
    /// `MIN`/`MAX`. See [`BatchQuery::add_metadata`].
    pub fn apply_metadata(&mut self, rows: u64, bounds: &[Option<(i64, i64)>]) -> Result<(), String> {
        self.query.add_metadata(rows, bounds)
    }

    /// Rows that passed the filter so far.
    pub fn passed_rows(&self) -> u64 {
        self.query.passed_rows()
    }

    pub fn consume_row_batch(
        &mut self,
        batch: &OwnedWcolBatch,
        mask: &SelectionMask,
    ) -> Result<SpillProgress<()>, String> {
        let mut col_view = Vec::new();
        let view = batch.view_with(&mut col_view);
        if matches!(self.query.consume(&view, mask)?, SpillProgress::AwaitHost) {
            return Ok(SpillProgress::AwaitHost);
        }
        if let Some(budget) = self.group_budget {
            let bytes = self.query.resident_state_bytes();
            if bytes > budget {
                return Err(format!(
                    "resource limit: reducer group state is {bytes} bytes and the budget is {budget}"
                ));
            }
        }
        Ok(SpillProgress::Ready(()))
    }

    /// Merge another node's partial state, as [`AggregateOp::consume_row_batch`] does rows.
    pub fn consume_partial(
        &mut self,
        state: &PartialStateWcol,
    ) -> Result<SpillProgress<()>, String> {
        if matches!(self.query.consume_partial(state)?, SpillProgress::AwaitHost) {
            return Ok(SpillProgress::AwaitHost);
        }
        if let Some(budget) = self.group_budget {
            let bytes = self.query.resident_state_bytes();
            if bytes > budget {
                return Err(format!(
                    "resource limit: reducer group state is {bytes} bytes and the budget is {budget}"
                ));
            }
        }
        Ok(SpillProgress::Ready(()))
    }

    pub fn finish_partial_state(&mut self) -> Result<SpillProgress<PartialStateWcol>, String> {
        // A reducer that never spilled finishes from memory instead of writing a run just to
        // read it back, unless its table is over half its budget: building the whole result
        // beside it could exceed that budget, while a flush frees the table first.
        let flush_first = self.query.spill_finalization_needed()
            || self.query.spill_budget_bytes().is_some_and(|budget| {
                self.query.resident_state_bytes() > eager_finish_max_bytes(budget)
            });
        if flush_first && matches!(self.query.flush_live_run()?, SpillProgress::AwaitHost) {
            return Ok(SpillProgress::AwaitHost);
        }
        let out = self.query.finish_partial_state();
        self.finish = None;
        if matches!(self.query.close_spill()?, SpillProgress::AwaitHost) {
            return Ok(SpillProgress::AwaitHost);
        }
        Ok(SpillProgress::Ready(out))
    }

    pub fn spill_runs_flushed(&self) -> u64 {
        self.query.spill_flush_stats().map(|s| s.runs).unwrap_or(0)
    }

    pub fn spill_groups_flushed(&self) -> u64 {
        self.query
            .spill_flush_stats()
            .map(|s| s.groups)
            .unwrap_or(0)
    }

    pub fn peak_live_groups(&self) -> u64 {
        self.query
            .spill_flush_stats()
            .map(|s| s.peak_live_groups)
            .unwrap_or(0)
    }

    pub fn peak_live_bytes(&self) -> u64 {
        self.query
            .spill_flush_stats()
            .map(|s| s.peak_live_bytes)
            .unwrap_or(0)
    }

    pub fn peak_interner_bytes(&self) -> u64 {
        self.query
            .spill_flush_stats()
            .map(|s| s.peak_interner_bytes)
            .unwrap_or(0)
    }

    pub fn live_interner_bytes(&self) -> usize {
        self.query.live_interner_bytes()
    }

    pub fn live_table_bytes(&self) -> usize {
        self.query.live_table_bytes()
    }

    /// Start bounded spill finalization (one chunk per [`Self::finish_next_chunk`]).
    pub fn begin_finish_chunks(&mut self) -> Result<SpillProgress<()>, String> {
        for _ in 0..64 {
            if self.query.live_group_len() == 0 && !self.query.spill_needs_finish_run() {
                break;
            }
            if matches!(self.query.flush_live_run()?, SpillProgress::AwaitHost) {
                return Ok(SpillProgress::AwaitHost);
            }
            if self.query.spill_host_op_pending() {
                return Ok(SpillProgress::AwaitHost);
            }
        }
        if self.finish.is_none() {
            self.query.release_table_for_merge();
            self.finish = Some(BatchQueryFinish::begin(&mut self.query)?);
        }
        Ok(SpillProgress::Ready(()))
    }

    pub fn finish_next_chunk(&mut self) -> Result<SpillProgress<Option<PartialStateWcol>>, String> {
        let finish = self
            .finish
            .as_mut()
            .ok_or("finish chunks not started; call begin_finish_chunks")?;
        finish.next_chunk(&mut self.query)
    }

    pub fn finish_finalize_resident_rows(&self) -> usize {
        self.finish
            .as_ref()
            .map(|f| f.finalize_pending_rows())
            .unwrap_or(0)
    }

    pub fn finish_chunks_done(&mut self) -> Result<SpillProgress<()>, String> {
        self.finish = None;
        self.query.close_spill()
    }

    pub fn has_group_spill(&self) -> bool {
        self.query.has_group_spill()
    }

    pub(crate) fn spill_host_op_pending(&self) -> bool {
        self.query.spill_host_op_pending()
    }

    pub(crate) fn ingest_spill_host_read(&mut self) -> Result<(), String> {
        self.query.ingest_spill_host_read()
    }

    pub(crate) fn on_spill_run_put_handoff(&mut self) -> Result<(), String> {
        self.query.on_spill_run_put_handoff()
    }

    pub(crate) fn on_spill_run_commit_host_complete(&mut self) -> Result<(), String> {
        self.query.on_spill_run_commit_host_complete()
    }

    pub fn needs_spill_chunk_finish(&self) -> bool {
        self.query.spill_finalization_needed()
    }

    pub fn finish_stream_active(&self) -> bool {
        self.finish.is_some()
    }

    pub fn close_spill(&mut self) -> Result<SpillProgress<()>, String> {
        self.query.close_spill()
    }
}

impl Operator for AggregateOp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn poll(&mut self, io: &mut OperatorIo<'_, '_>) -> Result<StreamProgress, String> {
        if self.finished {
            return Ok(StreamProgress::Finished);
        }
        if !io.output_slot_free() {
            return Ok(StreamProgress::Blocked);
        }
        if let Some(input) = io.input_item() {
            if self.query.spill_flush_incomplete() {
                match self.query.flush_live_run()? {
                    SpillProgress::Ready(()) => {}
                    SpillProgress::AwaitHost => return Ok(StreamProgress::Blocked),
                }
            }
            let Some(batch) = input.as_row_batch() else {
                return Err("aggregate expects row batches".to_string());
            };
            let consumed = match batch.selection_mask() {
                Some(mask) => self.consume_row_batch(batch, mask)?,
                None => self.consume_row_batch(batch, &SelectionMask::all(batch.row_count()))?,
            };
            if matches!(consumed, SpillProgress::AwaitHost) {
                return Ok(StreamProgress::Blocked);
            }
            if self.query.spill_flush_incomplete() {
                return Ok(StreamProgress::Blocked);
            }
            if self
                .combiner
                .as_ref()
                .is_some_and(|c| self.query.resident_state_bytes() > c.flush_bytes)
            {
                self.flush_combiner()?;
            }
            return Ok(StreamProgress::NeedInput);
        }
        if io.input_eof && self.combiner.is_some() {
            self.flush_combiner()?;
            self.finished = true;
            return Ok(StreamProgress::Finished);
        }
        if io.input_eof {
            if self.query.has_group_spill() && self.needs_spill_chunk_finish() {
                match self.begin_finish_chunks()? {
                    SpillProgress::Ready(()) => {}
                    SpillProgress::AwaitHost => return Ok(StreamProgress::Blocked),
                }
                match self.finish_next_chunk()? {
                    SpillProgress::Ready(Some(chunk)) => {
                        *io.output = Some(WcolStreamItem::from_partial_state(chunk));
                        return Ok(StreamProgress::HaveOutput);
                    }
                    SpillProgress::Ready(None) => {
                        if matches!(self.finish_chunks_done()?, SpillProgress::AwaitHost) {
                            return Ok(StreamProgress::Blocked);
                        }
                        self.finished = true;
                        return Ok(StreamProgress::Finished);
                    }
                    SpillProgress::AwaitHost => return Ok(StreamProgress::Blocked),
                }
            }
            let partial = match self.finish_partial_state()? {
                SpillProgress::Ready(partial) => partial,
                SpillProgress::AwaitHost => return Ok(StreamProgress::Blocked),
            };
            *io.output = Some(WcolStreamItem::from_partial_state(partial));
            self.finished = true;
            return Ok(StreamProgress::HaveOutput);
        }
        Ok(StreamProgress::NeedInput)
    }
}

impl Drop for AggregateOp {
    fn drop(&mut self) {
        let _ = self.query.close_spill();
    }
}

#[cfg(test)]
mod tests;
