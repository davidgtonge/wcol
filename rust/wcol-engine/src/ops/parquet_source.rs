//! Parquet planned-chunk source as a canonical [`Operator`](crate::exec::operator::Operator).

use crate::exec::fetch_stage::{
    FetchStage, FetchStageMetrics, HostFetchRead, SliceError, FETCH_BUFFER_BYTES,
};
use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::formats::parquet::ParquetDecode;
use crate::ops::query::CatalogColumn;
use crate::program::{ClaimUnit, FetchRequest, IngestGroup, IngestMemoryProfile, LogicalRead};
use crate::stream::OwnedWcolBatch;

/// The host read source that hands out units of scan work: each read returns the next
/// unit's id (`u32`, little endian) for this scan, or [`NO_UNIT`] when none is left.
pub const CLAIM_SOURCE: u16 = u16::MAX;
pub const NO_UNIT: u32 = u32::MAX;
/// Claim tickets sit between decode slice tickets and fetch tickets.
const CLAIM_TICKET_BASE: u32 = 0x2000_0000;
/// A claiming scan asks for another unit while its granted fetches number fewer than the
/// fetch depth, so prefetch stays full: a unit of one small fetch (RegionID) keeps eight in
/// flight, a unit of nine (Q31) about one unit ahead. It never holds more than this many
/// row groups ahead, so a slow scan cannot sit on work a faster one could take.
const CLAIM_MAX_GROUPS_AHEAD: usize = 8;
/// It also stops asking while this many claimed row groups are fully fetched but not yet
/// decoded (the one decoding included). Reads that keep up are not the limit then: the scan
/// is short of CPU or held back by its output, and whatever it claimed now would wait in
/// it while idle scans could take it.
const CLAIM_MAX_FETCHED_AHEAD: usize = 2;

/// Units this scan may be given, by id, and the row groups they hold.
struct Claims {
    units: std::collections::BTreeMap<u32, ClaimUnit>,
    groups: std::collections::BTreeMap<u32, IngestGroup>,
    in_flight: Option<u32>,
    /// The coordinator said no unit is left.
    done: bool,
    next_ticket: u32,
}

pub struct ParquetSourceOp {
    decode: ParquetDecode,
    fetch: Option<FetchStage>,
    batch: OwnedWcolBatch,
    /// ReadAt source of the object this scan reads (a scan reads one object).
    source: u16,
    /// Run-time unit claims; `None` decodes every planned group.
    claims: Option<Claims>,
}

impl ParquetSourceOp {
    pub fn new(
        batch_rows: usize,
        catalog: Vec<CatalogColumn>,
        groups: Vec<IngestGroup>,
        memory_profile: IngestMemoryProfile,
    ) -> Self {
        Self {
            decode: ParquetDecode::from_ingest_groups_with_profile(
                batch_rows,
                catalog,
                &groups,
                memory_profile,
            ),
            fetch: None,
            batch: OwnedWcolBatch::empty(),
            source: 0,
            claims: None,
        }
    }

    pub fn with_fetch_plan(
        batch_rows: usize,
        catalog: Vec<CatalogColumn>,
        groups: Vec<IngestGroup>,
        memory_profile: IngestMemoryProfile,
        fetches: Vec<FetchRequest>,
        logical_reads: Vec<LogicalRead>,
        fetch_depth: usize,
    ) -> Result<Self, String> {
        if fetch_depth == 0 {
            return Err("fetch depth is zero".to_string());
        }
        Ok(Self::with_fetches(
            batch_rows,
            catalog,
            groups,
            memory_profile,
            fetches,
            logical_reads,
            fetch_depth,
        ))
    }

    /// Decode reads are served from the plan's `fetches`; at most `fetch_depth` are in flight.
    pub fn with_fetches(
        batch_rows: usize,
        catalog: Vec<CatalogColumn>,
        groups: Vec<IngestGroup>,
        memory_profile: IngestMemoryProfile,
        fetches: Vec<FetchRequest>,
        logical_reads: Vec<LogicalRead>,
        fetch_depth: usize,
    ) -> Self {
        Self {
            decode: ParquetDecode::from_ingest_groups_with_profile(
                batch_rows,
                catalog,
                &groups,
                memory_profile,
            ),
            fetch: Some(FetchStage::new(
                fetches,
                logical_reads,
                fetch_depth,
                FETCH_BUFFER_BYTES,
            )),
            batch: OwnedWcolBatch::empty(),
            source: 0,
            claims: None,
        }
    }

    /// Decode `groups` only as the coordinator gives this scan the `units` holding them.
    /// Built with no planned groups: each given group is appended to the decoder.
    pub fn with_claims(
        mut self,
        units: Vec<ClaimUnit>,
        groups: Vec<IngestGroup>,
    ) -> Result<Self, String> {
        if self.decode.group_count() != 0 {
            return Err("a claiming scan starts with no planned groups".to_string());
        }
        let fetch = self
            .fetch
            .as_mut()
            .ok_or("a claiming scan needs a fetch plan")?;
        fetch.gate_by_row_group();
        let groups: std::collections::BTreeMap<u32, IngestGroup> = groups
            .into_iter()
            .map(|group| (group.row_group, group))
            .collect();
        for unit in &units {
            if let Some(missing) = unit
                .row_groups
                .iter()
                .find(|group| !groups.contains_key(group))
            {
                return Err(format!(
                    "claim unit {} names row group {missing} this scan has no plan for",
                    unit.id
                ));
            }
        }
        self.claims = Some(Claims {
            units: units.into_iter().map(|unit| (unit.id, unit)).collect(),
            groups,
            in_flight: None,
            done: false,
            next_ticket: CLAIM_TICKET_BASE,
        });
        Ok(self)
    }

    fn is_claim_ticket(ticket: u32) -> bool {
        (CLAIM_TICKET_BASE..crate::exec::fetch_stage::FETCH_TICKET_BASE).contains(&ticket)
    }

    /// Ask for the next unit, when none is asked for and prefetch is running short.
    fn next_claim(&mut self) -> Option<StreamProgress> {
        let queued = self.decode.queued_groups();
        let fetch = self.fetch.as_ref()?;
        let short = queued == 0 || fetch.granted_backlog() < fetch.fetch_depth();
        let open = self
            .claims
            .as_ref()
            .is_some_and(|claims| !claims.done && claims.in_flight.is_none());
        if !open || !short || queued >= CLAIM_MAX_GROUPS_AHEAD {
            return None;
        }
        let loading = fetch.loading_row_groups();
        let fetched = self
            .decode
            .queued_row_groups()
            .filter(|group| !loading.contains(group))
            .count();
        if fetched >= CLAIM_MAX_FETCHED_AHEAD {
            return None;
        }
        let claims = self.claims.as_mut()?;
        let ticket = claims.next_ticket;
        claims.next_ticket += 1;
        claims.in_flight = Some(ticket);
        Some(StreamProgress::NeedHostRead {
            ticket,
            source: CLAIM_SOURCE,
            offset: 0,
            length: 4,
        })
    }

    fn claims_unresolved(&self) -> bool {
        self.claims.as_ref().is_some_and(|claims| !claims.done)
    }

    fn complete_claim(&mut self, ticket: u32, bytes: &[u8]) -> Result<(), String> {
        let claims = self.claims.as_mut().ok_or("claim answer without claims")?;
        if claims.in_flight != Some(ticket) {
            return Err(format!("unknown claim ticket {ticket}"));
        }
        claims.in_flight = None;
        let id = u32::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| format!("a unit answer is 4 bytes, got {}", bytes.len()))?,
        );
        let fetch = self.fetch.as_mut().ok_or("claims without a fetch plan")?;
        if id == NO_UNIT {
            // Nothing left for this scan: forget the fetches of units it was never given.
            claims.done = true;
            let rest: Vec<u32> = claims.groups.keys().copied().collect();
            claims.groups.clear();
            fetch.drop_row_groups(&rest);
            return Ok(());
        }
        let unit = claims
            .units
            .remove(&id)
            .ok_or_else(|| format!("unit {id} is not in this scan's plan, or was given twice"))?;
        for row_group in &unit.row_groups {
            let group = claims
                .groups
                .remove(row_group)
                .ok_or("a given unit's row group is missing")?;
            self.decode.push_ingest_group(&group);
        }
        fetch.grant(&unit.row_groups);
        Ok(())
    }

    /// Read from `source` (index into the job's sources) instead of source 0.
    pub fn with_source(mut self, source: u16) -> Self {
        self.source = source;
        self
    }

    /// Fill `column` with each row's id.
    pub fn with_row_id(mut self, column: u32) -> Self {
        self.decode.set_row_id(column, self.source);
        self
    }

    pub fn fetch_metrics(&self) -> Option<&FetchStageMetrics> {
        self.fetch.as_ref().map(FetchStage::metrics)
    }

    pub fn complete_host_read(&mut self, ticket: u32, bytes: Vec<u8>) -> Result<(), String> {
        if Self::is_claim_ticket(ticket) {
            return self.complete_claim(ticket, &bytes);
        }
        if FetchStage::is_fetch_ticket(ticket) {
            return self
                .fetch
                .as_mut()
                .ok_or_else(|| "fetch ticket without fetch stage".to_string())?
                .complete_host_fetch(ticket, bytes);
        }
        self.decode
            .complete_read(ticket, &bytes)
            .map_err(|e| e.to_string())
    }

    pub fn cancel_host_read(&mut self, ticket: u32) -> Result<(), String> {
        if Self::is_claim_ticket(ticket) {
            // Ask again on the next poll.
            let claims = self.claims.as_mut().ok_or("claim cancel without claims")?;
            if claims.in_flight != Some(ticket) {
                return Err(format!("unknown claim ticket {ticket}"));
            }
            claims.in_flight = None;
            return Ok(());
        }
        if FetchStage::is_fetch_ticket(ticket) {
            return self
                .fetch
                .as_mut()
                .ok_or_else(|| "fetch ticket without fetch stage".to_string())?
                .cancel_host_fetch(ticket);
        }
        self.decode.cancel_read(ticket).map_err(|e| e.to_string())
    }

    pub fn ingest_host_bytes(&mut self, input: &mut &[u8]) -> Result<(), String> {
        if input.is_empty() {
            return Ok(());
        }
        let mut consumed = 0usize;
        self.decode
            .ingest_input(input, &mut consumed)
            .map_err(|e| e.to_string())?;
        *input = &input[consumed..];
        Ok(())
    }

    pub fn ingest_complete(&self) -> bool {
        self.decode.ingest_complete() && !self.claims_unresolved()
    }

    /// Host reads still owed to this source: decode slices or whole plan fetches.
    /// Fetches stay in flight while the host keeps stepping, so a blocked source with
    /// fetches outstanding is waiting on I/O, not stuck.
    pub fn host_reads_pending(&self) -> bool {
        self.decode.outstanding_host_reads() > 0
            || self
                .fetch
                .as_ref()
                .is_some_and(|fetch| fetch.has_in_flight())
            || self
                .claims
                .as_ref()
                .is_some_and(|claims| claims.in_flight.is_some())
    }

    pub fn read_hints(&self, _max: usize) -> Vec<(u64, u32)> {
        if self.fetch.is_some() {
            return Vec::new();
        }
        self.decode.host_read_hints(_max)
    }

    fn try_prefetch_host_fetch(&mut self) -> Option<HostFetchRead> {
        let fetch = self.fetch.as_mut()?;
        if !fetch.has_pending_loads() {
            return None;
        }
        fetch.poll_host_fetch(false)
    }
}

impl Operator for ParquetSourceOp {
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
        if io.input.is_some() || io.input_eof {
            return Err("parquet source does not accept stream input".to_string());
        }

        loop {
            // A claiming scan with nothing granted must not touch the decoder: claim, wait
            // for the claim in flight, or finish once every unit is resolved.
            if self.claims.is_some() && self.decode.queued_groups() == 0 {
                if let Some(claim) = self.next_claim() {
                    return Ok(claim);
                }
                if self.claims_unresolved() {
                    return Ok(StreamProgress::Blocked);
                }
                if self.decode.ingest_complete() {
                    return Ok(StreamProgress::Finished);
                }
            }
            if self
                .decode
                .fill_batch(&mut self.batch)
                .map_err(|e| e.to_string())?
            {
                let out_batch = std::mem::replace(&mut self.batch, OwnedWcolBatch::empty());
                *io.output = Some(WcolStreamItem::from_row_batch(out_batch));
                return Ok(StreamProgress::HaveOutput);
            }
            if let Some(claim) = self.next_claim() {
                return Ok(claim);
            }
            if self.decode.ingest_complete() {
                if self.claims_unresolved() {
                    // Nothing granted to decode yet; the claim in flight decides what is next.
                    return Ok(StreamProgress::Blocked);
                }
                return Ok(StreamProgress::Finished);
            }

            if let Some(host_fetch) = self.try_prefetch_host_fetch() {
                return Ok(StreamProgress::NeedHostRead {
                    ticket: host_fetch.ticket,
                    source: self.source,
                    offset: host_fetch.offset,
                    length: host_fetch.length,
                });
            }

            match self.decode.host_read_request().map_err(|e| e.to_string())? {
                Some(read) if self.fetch.is_some() => {
                    let fetch = self.fetch.as_mut().expect("fetch stage");
                    let missing = match fetch.slice_for_range(read.offset, read.length) {
                        Ok(bytes) => {
                            self.decode
                                .complete_read(read.ticket, &bytes)
                                .map_err(|e| e.to_string())?;
                            continue;
                        }
                        Err(SliceError::NotLoaded { fetch_id }) => fetch_id,
                        Err(err @ SliceError::Plan(_)) => return Err(err.to_string()),
                    };
                    self.decode
                        .cancel_read(read.ticket)
                        .map_err(|e| e.to_string())?;
                    fetch.prioritize_fetch(missing);
                    if let Some(host_fetch) = fetch.poll_host_fetch(true) {
                        return Ok(StreamProgress::NeedHostRead {
                            ticket: host_fetch.ticket,
                            source: self.source,
                            offset: host_fetch.offset,
                            length: host_fetch.length,
                        });
                    }
                    if self.decode.outstanding_host_reads() > 0 || fetch.has_in_flight() {
                        return Ok(StreamProgress::Blocked);
                    }
                    return Err(format!(
                        "fetch plan has no bytes for decode read at {} ({} bytes)",
                        read.offset, read.length
                    ));
                }
                Some(read) => {
                    return Ok(StreamProgress::NeedHostRead {
                        ticket: read.ticket,
                        source: self.source,
                        offset: read.offset,
                        length: read.length,
                    });
                }
                None if self.decode.outstanding_host_reads() > 0 => {
                    return Ok(StreamProgress::Blocked);
                }
                None => {
                    return Err(format!(
                        "parquet decode stalled ({})",
                        self.decode.stall_report()
                    ));
                }
            }
        }
    }
}
