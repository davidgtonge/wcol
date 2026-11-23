//! Generic linear colocated graph executor: optional source [`Operator`] + stream/tail nodes.
//!
//! All query stages implement [`Operator`](crate::exec::operator::Operator) + [`OperatorIo`].

use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::exec::runtime::wcol_stream::{drive_operator, partial_state_to_wcs_bytes};
use crate::ops::{AggregateOp, FilterOp, FinalizeOp, ParquetSourceOp};
use crate::stream::partials::PartialStateWcol;
use crate::stream::OwnedWcolBatch;

/// Host action while driving a colocated graph (sources may request ReadAt).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphPump {
    ReadAt {
        ticket: u32,
        source: u16,
        offset: u64,
        length: u32,
    },
    /// Ingest blocked until the host completes one or more outstanding ReadAt tickets.
    AwaitHost,
    Progress,
    /// A row batch is waiting in [`LinearColocatedGraph::pop_row_batch`].
    RowsReady,
    Done,
}

/// Progress while driving an attached Parquet (or other byte) sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkPoll {
    Blocked,
    Progress,
    Finished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollSinkOutcome {
    Done(SinkPoll, usize),
    ReadAt {
        ticket: u32,
        source: u16,
        offset: u64,
        length: u32,
    },
}

/// One step of a row-returning graph whose sink writes bytes (Parquet file).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteSinkDrive {
    ReadAt {
        ticket: u32,
        source: u16,
        offset: u64,
        length: u32,
    },
    AwaitHost,
    Wrote(usize),
    Finished,
    /// The sink is idle and the source has not finished. The caller feeds the next batch.
    NeedInput,
}

/// Linear colocated graph: optional source → stream operators (per batch) → tail (on EOF).
pub struct LinearColocatedGraph {
    source: Option<Box<dyn Operator>>,
    stream_ops: Vec<Box<dyn Operator>>,
    tail_ops: Vec<Box<dyn Operator>>,
    sink: Option<Box<dyn Operator>>,
    sink_batch: Option<WcolStreamItem>,
    sink_queue: std::collections::VecDeque<OwnedWcolBatch>,
    /// A row batch reached the sink, so an empty fallback batch is unnecessary.
    sink_fed: bool,
    sink_eof: bool,
    ingest_done: bool,
    finished: bool,
    result: Option<OwnedWcolBatch>,
    /// Row fragments queue projected batches here instead of finishing a partial.
    emit_rows: bool,
    pending_rows: std::collections::VecDeque<OwnedWcolBatch>,
    rows_flushed: bool,
}

impl LinearColocatedGraph {
    pub(crate) fn from_plan_parts(
        source: Option<Box<dyn Operator>>,
        stream_ops: Vec<Box<dyn Operator>>,
        tail_ops: Vec<Box<dyn Operator>>,
        ingest_done: bool,
    ) -> Self {
        Self {
            source,
            stream_ops,
            tail_ops,
            sink: None,
            sink_batch: None,
            sink_queue: std::collections::VecDeque::new(),
            sink_fed: false,
            sink_eof: false,
            ingest_done,
            finished: false,
            result: None,
            emit_rows: false,
            pending_rows: std::collections::VecDeque::new(),
            rows_flushed: false,
        }
    }

    pub fn set_emit_rows(&mut self) {
        self.emit_rows = true;
    }

    pub fn emits_rows(&self) -> bool {
        self.emit_rows
    }

    /// The next encoded partial frame a scan-side combiner has for a reducer.
    pub fn pop_routed_payload(&mut self) -> Option<(u32, Vec<u8>)> {
        self.stream_op_mut::<AggregateOp>(1)?.pop_routed_payload()
    }

    fn has_routed_payload(&self) -> bool {
        self.stream_op_ref::<AggregateOp>(1)
            .is_some_and(AggregateOp::has_routed_payload)
    }

    /// End a scan-side combiner's input: its last groups go to the outbox. It emits no
    /// stream items, unlike [`Self::finish_partial_state`].
    pub fn finish_routed(&mut self) -> Result<(), String> {
        if !self.ingest_done {
            return Err("finish called before ingest completed".to_string());
        }
        let mut items: Vec<WcolStreamItem> = Vec::new();
        let stream_len = self.stream_ops.len();
        for idx in 0..stream_len {
            items = drive_operator(self.stream_ops[idx].as_mut(), items, idx + 1 == stream_len)?;
        }
        if !items.is_empty() {
            return Err("combiner emitted stream items".to_string());
        }
        Ok(())
    }

    pub fn pop_row_batch(&mut self) -> Option<OwnedWcolBatch> {
        self.pending_rows.pop_front()
    }

    /// EOF-flush sort and limit after the source is exhausted. Idempotent.
    pub fn flush_row_ops(&mut self) -> Result<(), String> {
        if self.rows_flushed {
            return Ok(());
        }
        if !self.emit_rows && self.sink.is_none() {
            return Err("row flush on an aggregate graph".to_string());
        }
        if !self.ingest_done {
            return Err("row flush before ingest completed".to_string());
        }
        self.rows_flushed = true;
        let mut items = Vec::new();
        let stream_len = self.stream_ops.len();
        for idx in 0..stream_len {
            items = drive_operator(self.stream_ops[idx].as_mut(), items, true)?;
        }
        for item in items {
            if self.sink.is_some() {
                self.enqueue_sink_batch(item.into_batch())?;
            } else {
                self.queue_row_item(item)?;
            }
        }
        Ok(())
    }

    pub fn metadata_only(&self) -> bool {
        self.stream_op_ref::<AggregateOp>(1)
            .map(|a| a.metadata_only())
            .unwrap_or(false)
    }

    pub fn filter_is_empty(&self) -> bool {
        self.stream_op_ref::<FilterOp>(0)
            .map(|f| f.is_empty())
            .unwrap_or(true)
    }

    pub fn inject_row_batch(&mut self, batch: OwnedWcolBatch) -> Result<(), String> {
        self.route_owned_batch(batch)
    }

    pub fn tail_op_count(&self) -> usize {
        self.tail_ops.len()
    }

    /// Host prefetch hints for a node-fragment Parquet source (empty if there is none).
    pub fn parquet_read_hints(&self, max: usize) -> Vec<(u64, u32)> {
        let Some(source) = self.source.as_ref() else {
            return Vec::new();
        };
        source
            .as_any()
            .downcast_ref::<ParquetSourceOp>()
            .map(|op| op.read_hints(max))
            .unwrap_or_default()
    }

    pub fn apply_metadata_rows(&mut self, rows: u64) -> Result<(), String> {
        self.stream_op_mut::<AggregateOp>(1)
            .ok_or("missing aggregate operator")?
            .apply_metadata_rows(rows)
    }

    /// Typed partial encoded for WCS `FRAME_PARTIAL` (`PWCL` v1).
    pub fn finish_wire_partial_bytes(&mut self) -> Result<Vec<u8>, String> {
        partial_state_to_wcs_bytes(&self.finish_partial_state_with_eof()?)
    }

    /// Like [`Self::finish_partial_state`] but marks ingest complete when the graph has no source.
    pub fn finish_partial_state_with_eof(&mut self) -> Result<PartialStateWcol, String> {
        if !self.ingest_complete() {
            self.mark_ingest_done();
        }
        self.finish_partial_state()
    }

    pub fn ingest_complete(&self) -> bool {
        self.ingest_done
    }

    pub fn open_custom(
        source: Option<Box<dyn Operator>>,
        stream_ops: Vec<Box<dyn Operator>>,
        tail_ops: Vec<Box<dyn Operator>>,
    ) -> Self {
        Self {
            source,
            stream_ops,
            tail_ops,
            sink: None,
            sink_batch: None,
            sink_queue: std::collections::VecDeque::new(),
            sink_fed: false,
            sink_eof: false,
            ingest_done: false,
            finished: false,
            result: None,
            emit_rows: false,
            pending_rows: std::collections::VecDeque::new(),
            rows_flushed: false,
        }
    }

    pub fn has_byte_sink(&self) -> bool {
        self.sink.is_some()
    }

    /// Decode at most until the sink produces bytes, needs a host read, or finishes.
    /// A full output buffer stops upstream `ParquetDecode` on the next call.
    pub fn drive_byte_sink(&mut self, output: &mut [u8]) -> Result<ByteSinkDrive, String> {
        self.service_byte_sink(output, true)
    }

    /// Drain a sink whose batches arrive by [`Self::inject_row_batch`], not a Parquet source.
    pub fn drive_ingress_byte_sink(&mut self, output: &mut [u8]) -> Result<ByteSinkDrive, String> {
        self.service_byte_sink(output, false)
    }

    fn service_byte_sink(
        &mut self,
        output: &mut [u8],
        read_source: bool,
    ) -> Result<ByteSinkDrive, String> {
        for _ in 0..1_000_000 {
            match self.poll_sink(output)? {
                PollSinkOutcome::Done(poll, n) => {
                    if n > 0 {
                        return Ok(ByteSinkDrive::Wrote(n));
                    }
                    if matches!(poll, SinkPoll::Finished) {
                        return Ok(ByteSinkDrive::Finished);
                    }
                }
                PollSinkOutcome::ReadAt {
                    ticket,
                    source,
                    offset,
                    length,
                } => {
                    return Ok(ByteSinkDrive::ReadAt {
                        ticket,
                        source,
                        offset,
                        length,
                    });
                }
            }
            // A page-copy sink waiting on its own host reads hands back to the host, which
            // completes them; polling it again before then would read as a stall.
            if self.sink_awaits_reads() {
                return Ok(ByteSinkDrive::AwaitHost);
            }
            if self.sink_batch.is_some() {
                return Err("row sink did not accept the pending batch".to_string());
            }
            if let Some(batch) = self.sink_queue.pop_front() {
                self.sink_batch = Some(WcolStreamItem::from_row_batch(batch));
                continue;
            }
            if self.ingest_done {
                if !self.rows_flushed {
                    self.flush_row_ops()?;
                    continue;
                }
                if !self.sink_eof {
                    if !self.sink_fed {
                        self.sink_batch =
                            Some(WcolStreamItem::from_row_batch(OwnedWcolBatch::empty()));
                        self.sink_fed = true;
                        continue;
                    }
                    self.sink_eof = true;
                    continue;
                }
                return Err("row sink stalled after ingest".to_string());
            }
            if !read_source {
                return Ok(ByteSinkDrive::NeedInput);
            }
            match self.pump()? {
                GraphPump::ReadAt {
                    ticket,
                    source,
                    offset,
                    length,
                } => {
                    return Ok(ByteSinkDrive::ReadAt {
                        ticket,
                        source,
                        offset,
                        length,
                    });
                }
                GraphPump::AwaitHost => return Ok(ByteSinkDrive::AwaitHost),
                GraphPump::Progress => continue,
                GraphPump::RowsReady => {
                    return Err("byte sink received a row batch".to_string());
                }
                GraphPump::Done => continue,
            }
        }
        Err("row sink pump exceeded step budget".to_string())
    }

    pub fn attach_sink(&mut self, sink: Box<dyn Operator>) {
        self.sink = Some(sink);
        self.sink_batch = None;
        self.sink_queue.clear();
        self.sink_fed = false;
        self.sink_eof = false;
    }

    pub fn feed_sink_row_batch(&mut self, batch: OwnedWcolBatch) -> Result<(), String> {
        if self.sink.is_none() {
            return Err("no sink attached".to_string());
        }
        self.enqueue_sink_batch(batch)
    }

    fn enqueue_sink_batch(&mut self, batch: OwnedWcolBatch) -> Result<(), String> {
        if !batch.is_row_batch() {
            return Err("sink expects row batches".to_string());
        }
        if batch.row_count() == 0 {
            return Ok(());
        }
        self.sink_fed = true;
        if self.sink_batch.is_none() {
            self.sink_batch = Some(WcolStreamItem::from_row_batch(batch));
        } else {
            self.sink_queue.push_back(batch);
        }
        Ok(())
    }

    pub fn mark_sink_eof(&mut self) {
        self.sink_eof = true;
    }

    /// Pump the attached sink into `output`.
    pub fn poll_sink(&mut self, output: &mut [u8]) -> Result<PollSinkOutcome, String> {
        let Some(sink) = self.sink.as_mut() else {
            return Err("no sink attached".to_string());
        };
        let mut bytes_written = 0usize;
        let mut out_item = None;
        let mut io = OperatorIo {
            input: None,
            input_slot: Some(&mut self.sink_batch),
            input_eof: self.sink_eof,
            output: &mut out_item,
            host_bytes: None,
            output_port: Some(output),
            bytes_written: &mut bytes_written,
        };
        match sink.poll(&mut io)? {
            StreamProgress::Blocked => Ok(PollSinkOutcome::Done(SinkPoll::Blocked, bytes_written)),
            StreamProgress::HaveOutput => {
                Ok(PollSinkOutcome::Done(SinkPoll::Progress, bytes_written))
            }
            StreamProgress::Finished => {
                Ok(PollSinkOutcome::Done(SinkPoll::Finished, bytes_written))
            }
            StreamProgress::NeedInput => {
                if self.sink_batch.is_none() && !self.sink_eof {
                    Ok(PollSinkOutcome::Done(SinkPoll::Blocked, bytes_written))
                } else {
                    Ok(PollSinkOutcome::Done(SinkPoll::Progress, bytes_written))
                }
            }
            StreamProgress::NeedHostRead {
                ticket,
                source,
                offset,
                length,
            } => Ok(PollSinkOutcome::ReadAt {
                ticket,
                source,
                offset,
                length,
            }),
        }
    }

    pub fn complete_ingest_read(&mut self, ticket: u32, bytes: Vec<u8>) -> Result<(), String> {
        if let Some(source) = self.source.as_mut() {
            let Some(parquet) = source.as_any_mut().downcast_mut::<ParquetSourceOp>() else {
                return Err("complete_read on graph without parquet source".to_string());
            };
            return parquet.complete_host_read(ticket, bytes);
        }
        self.complete_sink_read(ticket, &bytes)
    }

    fn sink_awaits_reads(&self) -> bool {
        self.sink
            .as_ref()
            .and_then(|sink| {
                sink.as_any()
                    .downcast_ref::<crate::formats::parquet::page_compact::PageCompact>()
            })
            .is_some_and(|compact| compact.reads_in_flight())
    }

    pub fn complete_sink_read(&mut self, ticket: u32, bytes: &[u8]) -> Result<(), String> {
        let Some(sink) = self.sink.as_mut() else {
            return Err("complete_read on graph without sink".to_string());
        };
        let Some(compact) = sink
            .as_any_mut()
            .downcast_mut::<crate::formats::parquet::page_compact::PageCompact>()
        else {
            return Err("complete_read on graph without page compact sink".to_string());
        };
        compact.complete_host_read(ticket, bytes)
    }

    pub fn cancel_ingest_read(&mut self, ticket: u32) -> Result<(), String> {
        if let Some(source) = self.source.as_mut() {
            let Some(parquet) = source.as_any_mut().downcast_mut::<ParquetSourceOp>() else {
                return Err("cancel_read on graph without parquet source".to_string());
            };
            return parquet.cancel_host_read(ticket);
        }
        self.cancel_sink_read(ticket)
    }

    pub fn cancel_sink_read(&mut self, ticket: u32) -> Result<(), String> {
        let Some(sink) = self.sink.as_mut() else {
            return Err("cancel_read on graph without sink".to_string());
        };
        let Some(compact) = sink
            .as_any_mut()
            .downcast_mut::<crate::formats::parquet::page_compact::PageCompact>()
        else {
            return Err("cancel_read on graph without page compact sink".to_string());
        };
        compact.cancel_host_read(ticket)
    }

    pub fn push_ingest_partial(&mut self, input: &mut &[u8]) -> Result<(), String> {
        if input.is_empty() {
            return Ok(());
        }
        let Some(source) = self.source.as_mut() else {
            *input = &[];
            return Ok(());
        };
        let Some(parquet) = source.as_any_mut().downcast_mut::<ParquetSourceOp>() else {
            *input = &[];
            return Ok(());
        };
        parquet.ingest_host_bytes(input)
    }

    pub fn push_ingest(&mut self, input: &[u8]) -> Result<(), String> {
        let mut slice = input;
        self.push_ingest_partial(&mut slice)?;
        if !slice.is_empty() {
            return Err("decode did not consume full ingest slice".to_string());
        }
        Ok(())
    }

    pub fn drive_ingest(&mut self, input: &mut &[u8]) -> Result<GraphPump, String> {
        while !input.is_empty() {
            self.push_ingest_partial(input)?;
        }
        loop {
            match self.pump()? {
                GraphPump::Progress => {
                    // Output goes out before the next batch, so it never piles up.
                    if (self.emit_rows && !self.pending_rows.is_empty())
                        || self.has_routed_payload()
                    {
                        return Ok(GraphPump::RowsReady);
                    }
                }
                other => return Ok(other),
            }
        }
    }

    pub fn pump(&mut self) -> Result<GraphPump, String> {
        if self.finished {
            return Ok(GraphPump::Done);
        }
        if self.ingest_done {
            return Ok(GraphPump::Done);
        }

        let Some(source) = self.source.as_mut() else {
            return Err("graph pump without parquet source".to_string());
        };
        let mut out_item = None;
        let mut io = OperatorIo {
            input: None,
            input_slot: None,
            input_eof: false,
            output: &mut out_item,
            host_bytes: None,
            output_port: None,
            bytes_written: &mut 0,
        };
        match source.poll(&mut io)? {
            StreamProgress::HaveOutput => {
                let batch = out_item
                    .take()
                    .ok_or("source emitted no batch")?
                    .into_batch();
                if batch.is_partial_batch() {
                    return Err("source emitted partial batch".to_string());
                }
                self.route_owned_batch(batch)?;
                Ok(GraphPump::Progress)
            }
            StreamProgress::NeedHostRead {
                ticket,
                source,
                offset,
                length,
            } => Ok(GraphPump::ReadAt {
                ticket,
                source,
                offset,
                length,
            }),
            StreamProgress::Finished => {
                self.ingest_done = true;
                Ok(GraphPump::Done)
            }
            StreamProgress::NeedInput | StreamProgress::Blocked => {
                let pending = source
                    .as_any()
                    .downcast_ref::<ParquetSourceOp>()
                    .map(|p| p.host_reads_pending())
                    .unwrap_or(false);
                if pending {
                    Ok(GraphPump::AwaitHost)
                } else {
                    Err("parquet source blocked".to_string())
                }
            }
        }
    }

    fn route_owned_batch(&mut self, batch: OwnedWcolBatch) -> Result<(), String> {
        let mut items = vec![WcolStreamItem::from_row_batch(batch)];
        for idx in 0..self.stream_ops.len() {
            items = drive_operator(self.stream_ops[idx].as_mut(), items, false)?;
        }
        if self.sink.is_some() {
            for item in items {
                self.enqueue_sink_batch(item.into_batch())?;
            }
            return Ok(());
        }
        if self.emit_rows {
            for item in items {
                self.queue_row_item(item)?;
            }
            return Ok(());
        }
        if !items.is_empty() {
            return Err("aggregate fragment emitted output mid-scan".to_string());
        }
        Ok(())
    }

    fn queue_row_item(&mut self, item: WcolStreamItem) -> Result<(), String> {
        let batch = item.into_batch();
        if !batch.is_row_batch() {
            return Err("row fragment emitted a partial".to_string());
        }
        if batch.row_count() == 0 {
            return Ok(());
        }
        let mask = batch.effective_selection_mask();
        if mask.all_selected() {
            self.pending_rows.push_back(batch);
            return Ok(());
        }
        let rows: Vec<usize> = (0..batch.row_count())
            .filter(|&row| mask.is_selected(row))
            .collect();
        if rows.is_empty() {
            return Ok(());
        }
        let mut kept = OwnedWcolBatch::empty();
        kept.append_rows_from(&batch, &rows)?;
        self.pending_rows.push_back(kept);
        Ok(())
    }

    pub fn finish_partial_state(&mut self) -> Result<PartialStateWcol, String> {
        if !self.ingest_done {
            return Err("finish called before ingest completed".to_string());
        }
        let mut items: Vec<WcolStreamItem> = Vec::new();
        let stream_len = self.stream_ops.len();
        for idx in 0..stream_len {
            let eof = idx + 1 == stream_len;
            items = drive_operator(self.stream_ops[idx].as_mut(), items, eof)?;
        }
        if items.len() != 1 {
            return Err(format!(
                "stream chain emitted {} items after EOF, expected 1 partial",
                items.len()
            ));
        }
        let tail_len = self.tail_ops.len();
        for idx in 0..tail_len {
            let input = std::mem::take(&mut items);
            items = drive_operator(self.tail_ops[idx].as_mut(), input, true)?;
        }
        match items.pop() {
            Some(item) if items.is_empty() && item.is_partial_batch() => item.into_partial_state(),
            _ => Err("tail chain did not emit partial state".to_string()),
        }
    }

    pub fn finish(&mut self) -> Result<&OwnedWcolBatch, String> {
        if self.result.is_some() {
            return Ok(self.result.as_ref().unwrap());
        }
        if !self.ingest_done {
            return Err("finish called before ingest completed".to_string());
        }

        let partial = self.finish_partial_state()?;
        let mut input_item = Some(WcolStreamItem::from_partial_state(partial));
        let mut finalize = FinalizeOp::new();
        let mut out = None;
        let mut io = OperatorIo {
            input: None,
            input_slot: Some(&mut input_item),
            input_eof: false,
            output: &mut out,
            host_bytes: None,
            output_port: None,
            bytes_written: &mut 0,
        };
        match finalize.poll(&mut io)? {
            StreamProgress::NeedInput => {}
            other => return Err(format!("finalize unexpected progress: {other:?}")),
        }
        let mut io = OperatorIo {
            input: None,
            input_slot: None,
            input_eof: true,
            output: &mut out,
            host_bytes: None,
            output_port: None,
            bytes_written: &mut 0,
        };
        match finalize.poll(&mut io)? {
            StreamProgress::HaveOutput => {}
            other => return Err(format!("finalize did not emit result: {other:?}")),
        }
        let batch = out.ok_or("finalize missing output")?.into_batch();
        if batch.is_partial_batch() {
            return Err("finalize output was not a row batch".to_string());
        }
        self.result = Some(batch);
        self.finished = true;
        Ok(self.result.as_ref().unwrap())
    }

    pub fn mark_ingest_done(&mut self) {
        self.ingest_done = true;
    }

    pub fn stream_op_mut<T: 'static>(&mut self, index: usize) -> Option<&mut T> {
        self.stream_ops
            .get_mut(index)?
            .as_any_mut()
            .downcast_mut::<T>()
    }

    pub fn stream_op_ref<T: 'static>(&self, index: usize) -> Option<&T> {
        self.stream_ops.get(index)?.as_any().downcast_ref::<T>()
    }

    /// Rows that passed the filter so far, when the graph has an aggregate stage.
    pub fn aggregate_passed_rows(&self) -> Option<u64> {
        self.stream_op_ref::<AggregateOp>(1)
            .map(|a| a.passed_rows())
    }

    pub fn run_to_finish<F>(&mut self, mut read_at: F) -> Result<&OwnedWcolBatch, String>
    where
        F: FnMut(u32, u16, u64, u32) -> Result<Vec<u8>, String>,
    {
        loop {
            match self.pump()? {
                GraphPump::ReadAt {
                    ticket,
                    source,
                    offset,
                    length,
                } => {
                    let bytes = read_at(ticket, source, offset, length)?;
                    self.complete_ingest_read(ticket, bytes)?;
                }
                GraphPump::Progress => {}
                GraphPump::AwaitHost => {
                    return Err(
                        "run_to_finish: ingest waiting on host ReadAt completion".to_string()
                    );
                }
                GraphPump::RowsReady => {
                    return Err("run_to_finish does not drain row batches".to_string());
                }
                GraphPump::Done => break,
            }
        }
        self.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{AggExpr, AggKind, ColumnKind};
    use crate::ops::query::CatalogColumn;
    use crate::ops::{AggregateOp, FilterOp, ReduceOp};
    use crate::program::IngestMemoryProfile;

    fn pass_all_filter(catalog: &[CatalogColumn]) -> FilterOp {
        FilterOp::from_predicates(&[], catalog).expect("empty filter")
    }

    fn sum_column(catalog: &[CatalogColumn], column: u32) -> AggregateOp {
        AggregateOp::from_aggregate(
            &[],
            &[AggExpr {
                column: Some(column),
                kind: AggKind::Sum,
                offset: 0,
            }],
            None,
            0,
            false,
            catalog,
        )
        .expect("sum")
    }

    #[test]
    fn custom_graph_routes_through_stream_ops() {
        let catalog = vec![CatalogColumn {
            name: "a".to_string(),
            kind: ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        }];
        struct OneBatchSource {
            sent: bool,
        }
        impl Operator for OneBatchSource {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }
            fn poll(&mut self, io: &mut OperatorIo<'_, '_>) -> Result<StreamProgress, String> {
                if !io.output_slot_free() {
                    return Ok(StreamProgress::Blocked);
                }
                if self.sent {
                    return Ok(StreamProgress::Finished);
                }
                self.sent = true;
                let mut batch = OwnedWcolBatch::empty();
                batch.push_i64_column(0, vec![10, 10], vec![1, 1]);
                batch.set_row_count(2).map_err(|e| e.to_string())?;
                *io.output = Some(WcolStreamItem::from_row_batch(batch));
                Ok(StreamProgress::HaveOutput)
            }
        }

        let mut graph = LinearColocatedGraph::open_custom(
            Some(Box::new(OneBatchSource { sent: false })),
            vec![
                Box::new(pass_all_filter(&catalog)),
                Box::new(sum_column(&catalog, 0)),
            ],
            vec![Box::new(ReduceOp::new())],
        );
        assert!(matches!(graph.pump().unwrap(), GraphPump::Progress));
        assert!(matches!(graph.pump().unwrap(), GraphPump::Done));
        let partial = graph.finish_partial_state().unwrap();
        assert_eq!(partial.row_count, 1);
    }

    /// WCS projection payload → typed graph (remote edge simulation without byte chain ops).
    #[test]
    fn wcs_projection_injected_graph_matches_direct_ingest() {
        use std::io::{Read, Seek, SeekFrom};
        use std::path::PathBuf;

        crate::formats::parquet::ingest_replay_reset();

        use crate::exec::operators::{
            owned_batch_from_projection_body, projection_body_from_owned_batch,
        };
        use crate::formats::parquet::{open_plan, ParquetDecode};
        use crate::ingest_groups_from_plan;
        use crate::stream::partials::partial_state_first_sum_i;

        fn wrap_data_batch_payload(dict_scope: u32, chunk_id: u32, proj: &[u8]) -> Vec<u8> {
            let mut payload = vec![0u8; 13 + proj.len()];
            payload[0] = 2;
            payload[1..5].copy_from_slice(&dict_scope.to_le_bytes());
            payload[5..9].copy_from_slice(&chunk_id.to_le_bytes());
            payload[9..13].copy_from_slice(&(proj.len() as u32).to_le_bytes());
            payload[13..].copy_from_slice(proj);
            payload
        }

        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet");
        if !path.exists() {
            return;
        }
        let (mut file, plan) = open_plan(&path).unwrap();
        let catalog: Vec<CatalogColumn> = plan
            .leaves
            .iter()
            .map(|leaf| CatalogColumn {
                name: leaf.name.clone(),
                kind: if leaf.physical == 6 || leaf.physical == 7 {
                    ColumnKind::Text
                } else {
                    ColumnKind::Number
                },
                physical: leaf.physical,
                type_length: leaf.type_length,
                max_def: leaf.max_def,
                max_rep: leaf.max_rep,
            })
            .collect();
        let column = catalog.iter().position(|c| c.name == "a").expect("a") as u32;
        let columns = vec![column];
        let groups = ingest_groups_from_plan(&plan, &columns);

        let mut direct = LinearColocatedGraph::from_plan_parts(
            Some(Box::new(crate::ops::ParquetSourceOp::new(
                4,
                catalog.clone(),
                groups.clone(),
                IngestMemoryProfile::default(),
            ))),
            vec![
                Box::new(pass_all_filter(&catalog)),
                Box::new(sum_column(&catalog, column)),
            ],
            vec![Box::new(ReduceOp::new())],
            false,
        );
        loop {
            match direct.pump().unwrap() {
                GraphPump::ReadAt {
                    ticket,
                    source: _,
                    offset,
                    length,
                } => {
                    let mut bytes = vec![0u8; length as usize];
                    file.seek(SeekFrom::Start(offset)).unwrap();
                    file.read_exact(&mut bytes).unwrap();
                    direct.complete_ingest_read(ticket, bytes).unwrap();
                }
                GraphPump::Progress => {}
                GraphPump::AwaitHost => {
                    panic!("test harness must complete reads before AwaitHost");
                }
                GraphPump::RowsReady => panic!("direct ingest test is not a row query"),
                GraphPump::Done => break,
            }
        }
        let direct_sum =
            partial_state_first_sum_i(&direct.finish_partial_state().unwrap(), 0).unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();
        use crate::decode_groups_from_ingest;
        use crate::dict_columns_from_ingest_groups;
        let mut decode = ParquetDecode::new(
            4,
            catalog.clone(),
            decode_groups_from_ingest(&groups),
            dict_columns_from_ingest_groups(&groups),
        );
        let mut batch = OwnedWcolBatch::empty();
        let mut graph = LinearColocatedGraph::from_plan_parts(
            None,
            vec![
                Box::new(pass_all_filter(&catalog)),
                Box::new(sum_column(&catalog, column)),
            ],
            vec![Box::new(ReduceOp::new())],
            false,
        );

        loop {
            while decode.fill_batch(&mut batch).unwrap() {
                let proj = projection_body_from_owned_batch(&batch)
                    .map_err(|e| e.to_string())
                    .unwrap();
                let mut col_view = Vec::new();
                let scope = batch.view_with(&mut col_view).dict_scope;
                let payload = wrap_data_batch_payload(scope, scope, &proj);
                let injected = owned_batch_from_projection_body(&payload).unwrap();
                graph.inject_row_batch(injected).unwrap();
            }
            if decode.ingest_complete() {
                break;
            }
            if let Some(read) = decode.host_read_request().unwrap() {
                let mut bytes = vec![0u8; read.length as usize];
                file.seek(SeekFrom::Start(read.offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                decode.complete_read(read.ticket, &bytes).unwrap();
            } else {
                panic!("decode stalled");
            }
        }
        graph.mark_ingest_done();
        let edge_sum =
            partial_state_first_sum_i(&graph.finish_partial_state().unwrap(), 0).unwrap();

        assert_eq!(direct_sum, edge_sum);
        assert_eq!(edge_sum, 20);
    }
}
