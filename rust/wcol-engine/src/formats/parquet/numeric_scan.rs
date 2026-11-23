//! Bounded compressed ReadAt feed → PageEvent decode → i64 batches.

use compact_parquet_thrift::pages::{page_layout, ColumnSpec, PageEvent, Physical, Value};
use compact_parquet_thrift::parquet::{page_header, PageHeader};

use super::decode::{DecodeProgress, ScanLimits};
use super::dictionary::{DictionaryStore, ValueRef};
use super::page_cursor::{
    inflate_page_body, owned_cursor_supports_data_page, ActivePlainPage, Flow, PageStep,
};
use super::page_scratch::PageScratch;
use super::schema::Leaf;
use super::Error;

pub struct NumericBatch<'a> {
    pub values: &'a [i64],
    pub nulls: &'a [u8],
}

pub struct DictIdBatch<'a> {
    pub ids: &'a [u32],
    pub nulls: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanValueMode {
    /// Expand dictionary (and plain) values into i64 batches (aggregate / legacy path).
    MaterializedI64,
    /// FLOAT and DOUBLE values as `f64` (FLOAT widens exactly). The i64 lane is not used.
    MaterializedF64,
    /// Emit Parquet dictionary ids + validity without resolving values.
    DictionaryIds,
    /// PLAIN byte-array values as offsets + bytes (no per-row allocation).
    PlainUtf8,
}

pub struct NumericScan {
    leaf: Leaf,
    codec: i32,
    value_mode: ScanValueMode,
    limits: ScanLimits,
    expected_chunk_len: u32,
    /// Bytes the host committed (monotonic; not reduced by compaction).
    total_fed: usize,
    fed_len: usize,
    compressed: Vec<u8>,
    read_at: usize,
    dict: DictionaryStore,
    pending: Vec<i64>,
    pending_f64: Vec<f64>,
    pending_ids: Vec<u32>,
    utf8_offsets: Vec<u32>,
    utf8_bytes: Vec<u8>,
    nulls: Vec<u8>,
    pending_at: usize,
    scratch: PageScratch,
    /// Owned page decode.
    active_plain: Option<ActivePlainPage>,
    replay: super::replay_metrics::ScanReplayMetrics,
    /// Logical rows this column chunk must produce (flat columns: row-group row count).
    expected_rows: Option<u64>,
    /// Rows handed off via [`Self::advance_batch`] (includes null positions).
    rows_emitted: u64,
    /// Bytes reserved for outstanding host ReadAt tickets (not yet delivered).
    read_reserved: usize,
}

impl NumericScan {
    pub fn open(bytes: Vec<u8>, leaf: Leaf, codec: i32) -> Result<Self, Error> {
        Self::open_with_limits(bytes, leaf, codec, ScanLimits::default())
    }

    pub fn open_with_limits(
        bytes: Vec<u8>,
        leaf: Leaf,
        codec: i32,
        limits: ScanLimits,
    ) -> Result<Self, Error> {
        let len = u32::try_from(bytes.len()).map_err(|_| Error::Message("chunk too large"))?;
        let mut scan = Self::begin_with_limits(leaf, codec, limits, len, None)?;
        scan.push_compressed(&bytes)?;
        Ok(scan)
    }

    pub fn begin_with_limits(
        leaf: Leaf,
        codec: i32,
        limits: ScanLimits,
        chunk_len: u32,
        expected_rows: Option<u64>,
    ) -> Result<Self, Error> {
        Self::begin_with_mode(
            leaf,
            codec,
            limits,
            chunk_len,
            expected_rows,
            ScanValueMode::MaterializedI64,
            false,
        )
    }

    pub fn begin_dict_ids_with_limits(
        leaf: Leaf,
        codec: i32,
        limits: ScanLimits,
        chunk_len: u32,
        expected_rows: Option<u64>,
    ) -> Result<Self, Error> {
        Self::begin_with_mode(
            leaf,
            codec,
            limits,
            chunk_len,
            expected_rows,
            ScanValueMode::DictionaryIds,
            false,
        )
    }

    /// Like [`Self::begin_with_limits`] but without a per-scan codec workspace (group-owned scratch).
    pub fn begin_with_limits_shared_scratch(
        leaf: Leaf,
        codec: i32,
        limits: ScanLimits,
        chunk_len: u32,
        expected_rows: Option<u64>,
    ) -> Result<Self, Error> {
        Self::begin_with_mode(
            leaf,
            codec,
            limits,
            chunk_len,
            expected_rows,
            ScanValueMode::MaterializedI64,
            true,
        )
    }

    pub fn begin_dict_ids_with_limits_shared_scratch(
        leaf: Leaf,
        codec: i32,
        limits: ScanLimits,
        chunk_len: u32,
        expected_rows: Option<u64>,
    ) -> Result<Self, Error> {
        Self::begin_with_mode(
            leaf,
            codec,
            limits,
            chunk_len,
            expected_rows,
            ScanValueMode::DictionaryIds,
            true,
        )
    }

    pub fn begin_plain_utf8_with_limits(
        leaf: Leaf,
        codec: i32,
        limits: ScanLimits,
        chunk_len: u32,
        expected_rows: Option<u64>,
    ) -> Result<Self, Error> {
        Self::begin_with_mode(
            leaf,
            codec,
            limits,
            chunk_len,
            expected_rows,
            ScanValueMode::PlainUtf8,
            false,
        )
    }

    pub fn begin_plain_utf8_with_limits_shared_scratch(
        leaf: Leaf,
        codec: i32,
        limits: ScanLimits,
        chunk_len: u32,
        expected_rows: Option<u64>,
    ) -> Result<Self, Error> {
        Self::begin_with_mode(
            leaf,
            codec,
            limits,
            chunk_len,
            expected_rows,
            ScanValueMode::PlainUtf8,
            true,
        )
    }

    fn begin_with_mode(
        leaf: Leaf,
        codec: i32,
        limits: ScanLimits,
        chunk_len: u32,
        expected_rows: Option<u64>,
        value_mode: ScanValueMode,
        shared_group_scratch: bool,
    ) -> Result<Self, Error> {
        let value_mode =
            if matches!(leaf.physical, 4 | 5) && value_mode == ScanValueMode::MaterializedI64 {
                ScanValueMode::MaterializedF64
            } else {
                value_mode
            };
        validate_leaf(&leaf, codec, &limits, value_mode)?;
        if limits.budget.compressed_bytes == 0 {
            return Err(Error::Message("compressed window budget is zero"));
        }
        let scratch = if shared_group_scratch {
            PageScratch::minimal()
        } else {
            PageScratch::for_limits(&limits)
        };
        Ok(Self {
            leaf,
            codec,
            value_mode,
            limits,
            expected_chunk_len: chunk_len,
            total_fed: 0,
            fed_len: 0,
            compressed: Vec::new(),
            read_at: 0,
            dict: DictionaryStore::with_limits(limits),
            pending: Vec::with_capacity(limits.max_pending_rows.min(4096)),
            pending_f64: Vec::with_capacity(limits.max_pending_rows.min(4096)),
            pending_ids: Vec::with_capacity(limits.max_pending_rows.min(4096)),
            utf8_offsets: vec![0],
            utf8_bytes: Vec::new(),
            nulls: Vec::with_capacity(limits.max_pending_rows.min(4096)),
            pending_at: 0,
            scratch,
            active_plain: None,
            replay: super::replay_metrics::ScanReplayMetrics::default(),
            expected_rows,
            rows_emitted: 0,
            read_reserved: 0,
        })
    }

    pub fn rows_emitted(&self) -> u64 {
        self.rows_emitted
    }

    pub fn expected_rows(&self) -> Option<u64> {
        self.expected_rows
    }

    pub fn replay_metrics(&self) -> super::replay_metrics::ScanReplayMetrics {
        self.replay
    }

    pub fn limits(&self) -> ScanLimits {
        self.limits
    }

    pub fn value_mode(&self) -> ScanValueMode {
        self.value_mode
    }

    pub fn dictionary(&self) -> &DictionaryStore {
        &self.dict
    }

    pub fn chunk_wants(&self) -> usize {
        if self.total_fed >= self.expected_chunk_len as usize {
            return 0;
        }
        self.expected_chunk_len as usize - self.total_fed
    }

    pub fn total_fed_bytes(&self) -> usize {
        self.total_fed
    }

    pub fn chunk_complete(&self) -> bool {
        self.total_fed >= self.expected_chunk_len as usize
    }

    /// Live compressed bytes in the sliding window (not [`Vec::capacity`]).
    pub fn resident_compressed_bytes(&self) -> usize {
        self.fed_len
    }

    /// Decoded rows waiting in the pending buffer (not yet advanced).
    pub fn available_rows(&self) -> usize {
        self.pending_available()
    }

    /// Bytes the host may append without exceeding the per-column compressed window.
    pub fn read_credit(&self) -> usize {
        let window = self.limits.budget.compressed_bytes;
        self.chunk_wants().min(
            window
                .saturating_sub(self.fed_len)
                .saturating_sub(self.read_reserved),
        )
    }

    pub fn reserve_read(&mut self, bytes: usize) {
        self.read_reserved = self.read_reserved.saturating_add(bytes);
    }

    pub fn release_read(&mut self, bytes: usize) {
        self.read_reserved = self.read_reserved.saturating_sub(bytes);
    }

    /// Copy bytes already charged by [`Self::reserve_read`] into the compressed window.
    ///
    /// [`Self::read_credit`] subtracts outstanding reservations, so a response that
    /// reserved the whole window would see zero credit if it went through
    /// [`Self::push_compressed`]. Acceptance converts the reservation into resident
    /// bytes instead of asking for a second credit check.
    pub fn accept_reserved(&mut self, data: &[u8]) -> Result<usize, Error> {
        let want = self.chunk_wants();
        if want == 0 || data.is_empty() {
            return Ok(0);
        }
        let take = data.len().min(want).min(self.read_reserved);
        if take == 0 {
            return Ok(0);
        }
        let new_fed = self.fed_len + take;
        if new_fed > self.limits.budget.compressed_bytes {
            return Err(Error::Message("reserved input exceeds compressed window"));
        }
        if self.total_fed + take > self.expected_chunk_len as usize {
            return Err(Error::Message("compressed input exceeds chunk length"));
        }
        if new_fed > self.compressed.len() {
            self.compressed.resize(new_fed, 0);
        }
        self.compressed[self.fed_len..new_fed].copy_from_slice(&data[..take]);
        self.fed_len += take;
        self.total_fed += take;
        self.read_reserved = self.read_reserved.saturating_sub(take);
        self.try_compact();
        Ok(take)
    }

    pub fn push_compressed(&mut self, data: &[u8]) -> Result<usize, Error> {
        let want = self.chunk_wants();
        if want == 0 {
            return Ok(0);
        }
        let credit = self.read_credit();
        if credit == 0 {
            return Ok(0);
        }
        let take = data.len().min(want).min(credit);
        let new_fed = self.fed_len + take;
        if self.total_fed + take > self.expected_chunk_len as usize {
            return Err(Error::Message("compressed input exceeds chunk length"));
        }
        if new_fed > self.compressed.len() {
            self.compressed.resize(new_fed, 0);
        }
        self.compressed[self.fed_len..new_fed].copy_from_slice(&data[..take]);
        self.fed_len += take;
        self.total_fed += take;
        self.try_compact();
        Ok(take)
    }

    pub fn copy_materialized_rows(
        &self,
        out_values: &mut Vec<i64>,
        out_nulls: &mut Vec<u8>,
        rows: usize,
    ) -> Result<(), Error> {
        if self.value_mode != ScanValueMode::MaterializedI64 {
            return Err(Error::Message("scan is not in materialized i64 mode"));
        }
        if rows > self.pending_available() {
            return Err(Error::Message("copy exceeds pending rows"));
        }
        let start = self.pending_at;
        out_values.extend_from_slice(&self.pending[start..start + rows]);
        out_nulls.extend_from_slice(&self.nulls[start..start + rows]);
        Ok(())
    }

    pub fn copy_f64_rows(
        &self,
        out_values: &mut Vec<f64>,
        out_nulls: &mut Vec<u8>,
        rows: usize,
    ) -> Result<(), Error> {
        if self.value_mode != ScanValueMode::MaterializedF64 {
            return Err(Error::Message("scan is not in materialized f64 mode"));
        }
        if rows > self.pending_available() {
            return Err(Error::Message("copy exceeds pending rows"));
        }
        let start = self.pending_at;
        out_values.extend_from_slice(&self.pending_f64[start..start + rows]);
        out_nulls.extend_from_slice(&self.nulls[start..start + rows]);
        Ok(())
    }

    pub fn copy_dict_id_rows(
        &self,
        out_ids: &mut Vec<u32>,
        out_nulls: &mut Vec<u8>,
        rows: usize,
    ) -> Result<(), Error> {
        if self.value_mode != ScanValueMode::DictionaryIds {
            return Err(Error::Message("scan is not in dictionary-id mode"));
        }
        if rows > self.pending_available() {
            return Err(Error::Message("copy exceeds pending rows"));
        }
        let start = self.pending_at;
        out_ids.extend_from_slice(&self.pending_ids[start..start + rows]);
        out_nulls.extend_from_slice(&self.nulls[start..start + rows]);
        Ok(())
    }

    pub fn copy_plain_utf8_rows(
        &self,
        out_offsets: &mut Vec<u32>,
        out_bytes: &mut Vec<u8>,
        out_nulls: &mut Vec<u8>,
        rows: usize,
    ) -> Result<(), Error> {
        if self.value_mode != ScanValueMode::PlainUtf8 {
            return Err(Error::Message("scan is not in plain utf8 mode"));
        }
        if rows > self.pending_available() {
            return Err(Error::Message("copy exceeds pending rows"));
        }
        let start = self.pending_at;
        let base_off = *self
            .utf8_offsets
            .get(start)
            .ok_or(Error::Message("utf8 offsets corrupt"))?;
        out_nulls.extend_from_slice(&self.nulls[start..start + rows]);
        let end_off = *self
            .utf8_offsets
            .get(start + rows)
            .ok_or(Error::Message("utf8 offsets corrupt"))?;
        let slice = self
            .utf8_bytes
            .get(base_off as usize..end_off as usize)
            .ok_or(Error::Message("utf8 bytes corrupt"))?;
        let byte_base = u32::try_from(out_bytes.len())
            .map_err(|_| Error::Message("utf8 batch byte overflow"))?;
        out_bytes.extend_from_slice(slice);
        for i in 0..=rows {
            let rel = self.utf8_offsets[start + i].saturating_sub(base_off);
            out_offsets.push(byte_base.saturating_add(rel));
        }
        Ok(())
    }

    pub fn progress(&self) -> DecodeProgress {
        if self.pending_available() > 0 {
            return DecodeProgress::BatchReady {
                rows: self.pending_available(),
            };
        }
        if self.decode_exhausted() {
            return DecodeProgress::Finished;
        }
        if self.read_at < self.fed_len {
            return DecodeProgress::Advanced;
        }
        DecodeProgress::NeedInput
    }

    /// Drive decode for up to `max_emit` rows (`0` = advance parser only, no row output).
    pub fn poll(&mut self, max_emit: usize) -> Result<DecodeProgress, Error> {
        let mut scratch = std::mem::replace(&mut self.scratch, PageScratch::minimal());
        let progress = self.poll_with_scratch(max_emit, &mut scratch);
        self.scratch = scratch;
        progress
    }

    pub fn poll_with_scratch(
        &mut self,
        max_emit: usize,
        scratch: &mut PageScratch,
    ) -> Result<DecodeProgress, Error> {
        if max_emit > self.limits.max_batch_rows {
            return Err(Error::Message("batch row limit exceeds scan budget"));
        }
        for _ in 0..4096 {
            if self.pending_available() > 0 && max_emit > 0 {
                return Ok(DecodeProgress::BatchReady {
                    rows: self.pending_available().min(max_emit),
                });
            }
            if self.decode_exhausted() {
                self.check_row_budget()?;
                return Ok(DecodeProgress::Finished);
            }
            if self.read_at >= self.fed_len && !self.chunk_complete() {
                // A read still in flight holds the window: wait for it rather than call it too small.
                if self.read_credit() > 0 || self.read_reserved > 0 {
                    return Ok(DecodeProgress::NeedInput);
                }
                let progressed = self.drive_decode_with_scratch(0, scratch)?;
                if progressed {
                    continue;
                }
                return Err(Error::Message("compressed page exceeds memory budget"));
            }

            let emit_budget = if max_emit == 0 {
                0
            } else {
                self.cap_chunk_emit(max_emit.saturating_sub(self.pending_available()))
            };
            let row_budget_met = matches!(
                self.expected_rows,
                Some(expected) if self.rows_emitted >= expected
            );
            let mut progressed = false;
            if max_emit == 0 || emit_budget > 0 {
                progressed = self.drive_decode_with_scratch(emit_budget, scratch)?;
                if self.decode_exhausted() {
                    self.check_row_budget()?;
                    return Ok(DecodeProgress::Finished);
                }
                if self.pending_available() > 0 && max_emit > 0 {
                    return Ok(DecodeProgress::BatchReady {
                        rows: self.pending_available().min(max_emit),
                    });
                }
            } else if row_budget_met {
                progressed = self.drive_decode_with_scratch(0, scratch)?;
                if self.parser_idle() {
                    self.check_row_budget()?;
                    return Ok(DecodeProgress::Finished);
                }
            }
            if self.chunk_complete() && !self.parser_idle() {
                if row_budget_met {
                    progressed |= self.drive_decode_with_scratch(0, scratch)?;
                    if self.parser_idle() {
                        self.check_row_budget()?;
                        return Ok(DecodeProgress::Finished);
                    }
                } else if max_emit == 0 {
                    self.skip_chunk_padding_with_scratch(scratch)?;
                    progressed = true;
                }
                if self.decode_exhausted() {
                    self.check_row_budget()?;
                    return Ok(DecodeProgress::Finished);
                }
                if progressed {
                    continue;
                }
            }
            if progressed {
                if max_emit == 0 {
                    return Ok(DecodeProgress::Advanced);
                }
                continue;
            }
            if self.parser_idle() {
                self.check_row_budget()?;
            }
            return Ok(DecodeProgress::NeedInput);
        }
        Err(Error::Message("column decode stalled with no progress"))
    }

    fn cap_chunk_emit(&self, want: usize) -> usize {
        if want == 0 {
            return 0;
        }
        let Some(expected) = self.expected_rows else {
            return want;
        };
        let produced = self
            .rows_emitted
            .saturating_add(self.pending_available() as u64);
        if produced >= expected {
            return 0;
        }
        want.min((expected - produced) as usize)
    }

    /// Internal counters for stall diagnostics.
    pub fn debug_state(&self) -> String {
        format!(
            "emitted={} expected={:?} pending={} read_at={} fed_len={} plain={}",
            self.rows_emitted,
            self.expected_rows,
            self.pending_available(),
            self.read_at,
            self.fed_len,
            self.active_plain
                .as_ref()
                .map(|a| format!("{} (plain len {})", a.cursor.debug_state(), a.plain.len()))
                .unwrap_or_else(|| "none".to_string()),
        )
    }

    fn parser_idle(&self) -> bool {
        self.chunk_complete() && self.read_at >= self.fed_len && self.active_plain.is_none()
    }

    fn check_row_budget(&self) -> Result<(), Error> {
        if !self.parser_idle() {
            return Ok(());
        }
        let Some(expected) = self.expected_rows else {
            return Ok(());
        };
        if self.rows_emitted < expected {
            return Err(Error::Message(
                "column chunk ended before the expected row count was decoded",
            ));
        }
        if self.rows_emitted > expected {
            return Err(Error::Message(
                "column chunk decoded more rows than expected",
            ));
        }
        Ok(())
    }

    fn skip_chunk_padding_with_scratch(&mut self, scratch: &mut PageScratch) -> Result<(), Error> {
        if !self.chunk_complete() {
            return Ok(());
        }
        while self.read_at < self.fed_len {
            if self.start_page_if_ready(0, scratch)? {
                continue;
            }
            self.read_at = self.fed_len;
            self.try_compact();
            break;
        }
        Ok(())
    }

    pub fn decode_exhausted(&self) -> bool {
        if self.expected_rows == Some(0) && self.total_fed == 0 {
            return true;
        }
        if !self.parser_idle() {
            return false;
        }
        match self.expected_rows {
            None => true,
            Some(expected) => self.rows_emitted == expected,
        }
    }

    pub fn next_batch(&mut self, max_rows: usize) -> Result<Option<NumericBatch<'_>>, Error> {
        if max_rows == 0 || max_rows > self.limits.max_batch_rows {
            return Err(Error::Message("batch row limit exceeds scan budget"));
        }
        let rows = match self.poll(max_rows)? {
            DecodeProgress::BatchReady { rows } => rows,
            DecodeProgress::Finished => return Ok(None),
            DecodeProgress::Advanced | DecodeProgress::NeedInput => {
                return Err(Error::Message("column decode blocked with no pending rows"));
            }
        };
        let take = rows.min(max_rows);
        let start = self.pending_at;
        Ok(Some(NumericBatch {
            values: &self.pending[start..start + take],
            nulls: &self.nulls[start..start + take],
        }))
    }

    pub fn advance_batch(&mut self, rows: usize) -> Result<(), Error> {
        let available = self.pending_available();
        if rows > available {
            return Err(Error::Message("advance exceeds pending rows"));
        }
        self.pending_at += rows;
        self.rows_emitted = self
            .rows_emitted
            .saturating_add(u64::try_from(rows).map_err(|_| Error::Message("row count overflow"))?);
        match self.value_mode {
            ScanValueMode::MaterializedI64 if self.pending_at == self.pending.len() => {
                self.pending.clear();
                self.nulls.clear();
                self.pending_at = 0;
            }
            ScanValueMode::MaterializedF64 if self.pending_at == self.pending_f64.len() => {
                self.pending_f64.clear();
                self.nulls.clear();
                self.pending_at = 0;
            }
            ScanValueMode::DictionaryIds if self.pending_at == self.pending_ids.len() => {
                self.pending_ids.clear();
                self.nulls.clear();
                self.pending_at = 0;
            }
            ScanValueMode::PlainUtf8 if self.pending_at == self.nulls.len() => {
                self.utf8_offsets.clear();
                self.utf8_offsets.push(0);
                self.utf8_bytes.clear();
                self.nulls.clear();
                self.pending_at = 0;
            }
            _ => {}
        }
        Ok(())
    }

    pub fn next_dict_batch(&mut self, max_rows: usize) -> Result<Option<DictIdBatch<'_>>, Error> {
        if self.value_mode != ScanValueMode::DictionaryIds {
            return Err(Error::Message("scan is not in dictionary-id mode"));
        }
        if max_rows == 0 || max_rows > self.limits.max_batch_rows {
            return Err(Error::Message("batch row limit exceeds scan budget"));
        }
        let rows = match self.poll(max_rows)? {
            DecodeProgress::BatchReady { rows } => rows,
            DecodeProgress::Finished => return Ok(None),
            DecodeProgress::Advanced | DecodeProgress::NeedInput => {
                return Err(Error::Message("column decode blocked with no pending rows"));
            }
        };
        let take = rows.min(max_rows);
        let start = self.pending_at;
        Ok(Some(DictIdBatch {
            ids: &self.pending_ids[start..start + take],
            nulls: &self.nulls[start..start + take],
        }))
    }

    pub fn next_dict_owned(
        &mut self,
        max_rows: usize,
    ) -> Result<Option<(Vec<u32>, Vec<u8>)>, Error> {
        let Some(batch) = self.next_dict_batch(max_rows)? else {
            return Ok(None);
        };
        let rows = batch.ids.len();
        let ids = batch.ids.to_vec();
        let nulls = batch.nulls.to_vec();
        self.advance_batch(rows)?;
        Ok(Some((ids, nulls)))
    }

    pub fn next_owned(&mut self, max_rows: usize) -> Result<Option<(Vec<i64>, Vec<u8>)>, Error> {
        let Some(batch) = self.next_batch(max_rows)? else {
            return Ok(None);
        };
        let rows = batch.values.len();
        let values = batch.values.to_vec();
        let nulls = batch.nulls.to_vec();
        self.advance_batch(rows)?;
        Ok(Some((values, nulls)))
    }

    pub fn next_batch_into(
        &mut self,
        values: &mut Vec<i64>,
        nulls: &mut Vec<u8>,
        max_rows: usize,
    ) -> Result<Option<usize>, Error> {
        if self.value_mode != ScanValueMode::MaterializedI64 {
            return Err(Error::Message("scan is not in materialized i64 mode"));
        }
        if max_rows > self.limits.max_batch_rows {
            return Err(Error::Message("batch row limit exceeds scan budget"));
        }
        if values.capacity() < max_rows || nulls.capacity() < max_rows {
            return Err(Error::Message("batch buffer capacity below row limit"));
        }
        values.clear();
        nulls.clear();
        loop {
            match self.poll(max_rows)? {
                DecodeProgress::BatchReady { rows } => {
                    let take = rows.min(max_rows);
                    let start = self.pending_at;
                    values.extend_from_slice(&self.pending[start..start + take]);
                    nulls.extend_from_slice(&self.nulls[start..start + take]);
                    self.advance_batch(take)?;
                    return Ok(Some(take));
                }
                DecodeProgress::Finished => {
                    self.check_row_budget()?;
                    return Ok(None);
                }
                DecodeProgress::Advanced => continue,
                DecodeProgress::NeedInput => {
                    if !self.chunk_complete() {
                        return Ok(None);
                    }
                    if self.parser_idle() {
                        self.check_row_budget()?;
                        return Ok(None);
                    }
                    continue;
                }
            }
        }
    }

    fn pending_available(&self) -> usize {
        match self.value_mode {
            ScanValueMode::MaterializedI64 => self.pending.len().saturating_sub(self.pending_at),
            ScanValueMode::MaterializedF64 => {
                self.pending_f64.len().saturating_sub(self.pending_at)
            }
            ScanValueMode::DictionaryIds => self.pending_ids.len().saturating_sub(self.pending_at),
            ScanValueMode::PlainUtf8 => self.nulls.len().saturating_sub(self.pending_at),
        }
    }

    fn drive_decode_with_scratch(
        &mut self,
        emit_budget: usize,
        scratch: &mut PageScratch,
    ) -> Result<bool, Error> {
        let before_pending = self.pending_available();
        let before_read = self.read_at;
        let before_fed = self.fed_len;
        let before_active_events = self
            .active_plain
            .as_ref()
            .map(|a| a.cursor.events_emitted());
        if emit_budget == 0 {
            for _ in 0..4096 {
                if self.decode_exhausted() {
                    break;
                }
                if self.read_at >= self.fed_len {
                    break;
                }
                if !self.decode_step(0, scratch)? {
                    break;
                }
            }
        } else {
            for _ in 0..4096 {
                if self.pending_available() >= emit_budget {
                    break;
                }
                if !self.decode_step(emit_budget, scratch)? {
                    break;
                }
            }
        }
        Ok(self.pending_available() != before_pending
            || self.read_at != before_read
            || self.fed_len != before_fed
            || self
                .active_plain
                .as_ref()
                .map(|a| a.cursor.events_emitted())
                != before_active_events)
    }

    fn decode_step(
        &mut self,
        emit_budget: usize,
        scratch: &mut PageScratch,
    ) -> Result<bool, Error> {
        let before_pending = self.pending_available();
        let before_read = self.read_at;
        let before_fed = self.fed_len;
        let before_active = self.active_plain.is_some();
        let before_active_events = self
            .active_plain
            .as_ref()
            .map(|a| a.cursor.events_emitted());
        if let Some(mut active) = self.active_plain.take() {
            let plain_before = active.cursor.plain_offset;
            self.feed_active_plain_taken(&mut active, emit_budget, scratch)?;
            if !active.cursor.page_complete() {
                self.active_plain = Some(active);
            }
            let plain_after = self
                .active_plain
                .as_ref()
                .map(|a| a.cursor.plain_offset)
                .unwrap_or(plain_before);
            return Ok(self.pending_available() != before_pending
                || self.read_at != before_read
                || self.fed_len != before_fed
                || plain_after != plain_before
                || self
                    .active_plain
                    .as_ref()
                    .map(|a| a.cursor.events_emitted())
                    != before_active_events);
        } else {
            let _ = self.start_page_if_ready(emit_budget, scratch)?;
        }
        Ok(self.pending_available() != before_pending
            || self.read_at != before_read
            || self.fed_len != before_fed
            || self.active_plain.is_some() != before_active
            || self
                .active_plain
                .as_ref()
                .map(|a| a.cursor.events_emitted())
                != before_active_events)
    }

    fn feed_active_plain_taken(
        &mut self,
        active: &mut ActivePlainPage,
        emit_budget: usize,
        _scratch: &mut PageScratch,
    ) -> Result<(), Error> {
        let max_new = if emit_budget == 0 {
            0
        } else {
            self.limits
                .max_pending_rows
                .saturating_sub(self.pending_available())
        };
        let max_out = if emit_budget == 0 {
            usize::MAX
        } else {
            max_new.min(emit_budget)
        };
        if self.value_mode == ScanValueMode::PlainUtf8 {
            // Stop the page before the pending string bytes outgrow the batch budget.
            let pending_rows = self.nulls.len() > self.pending_at;
            let room = self
                .limits
                .budget
                .batch_bytes
                .saturating_sub(self.utf8_bytes.len());
            active.cursor.set_byte_budget(room, !pending_rows);
        }
        let is_dict = active.is_dict;
        // Dictionary-id and PLAIN integer data pages: decode runs of values straight into the
        // pending buffers rather than one page event per row.
        if !is_dict && emit_budget > 0 {
            let before = self.nulls.len();
            let bulk = match self.value_mode {
                ScanValueMode::DictionaryIds => active.cursor.step_dictionary_ids(
                    &active.plain,
                    max_out.min(emit_budget),
                    &mut self.pending_ids,
                    &mut self.nulls,
                )?,
                ScanValueMode::MaterializedI64 => active.cursor.step_plain_i64(
                    &active.plain,
                    max_out.min(emit_budget),
                    &mut self.pending,
                    &mut self.nulls,
                )?,
                _ => None,
            };
            if let Some(step) = bulk {
                let rows = (self.nulls.len() - before) as u64;
                if self.pending_ids.len().max(self.pending.len()) > self.limits.max_pending_rows {
                    return Err(Error::Message("pending row budget exhausted"));
                }
                self.replay.page_events_visited =
                    self.replay.page_events_visited.saturating_add(rows);
                self.replay.page_events_committed =
                    self.replay.page_events_committed.saturating_add(rows);
                if active.cursor.page_complete() {
                    self.try_compact();
                }
                return match step {
                    PageStep::Emitted(_) | PageStep::Blocked | PageStep::PageComplete => Ok(()),
                };
            }
        }
        let (step, visited, committed, mode) = {
            let mut handler = PageEventHandler {
                is_dict,
                value_mode: self.value_mode,
                dict: &mut self.dict,
                pending: &mut self.pending,
                pending_f64: &mut self.pending_f64,
                pending_ids: &mut self.pending_ids,
                utf8_offsets: &mut self.utf8_offsets,
                utf8_bytes: &mut self.utf8_bytes,
                nulls: &mut self.nulls,
                pending_at: self.pending_at,
                limits: self.limits,
                emit_budget,
                next_input_index: 0,
                events_visited_this_pass: 0,
                rows_emitted_this_pass: 0,
                utf8_build: None,
            };
            let step = active.cursor.step(&active.plain, max_out, |event| {
                handler.push_owned_event(event)
            })?;
            let mode = handler.value_mode;
            (
                step,
                handler.events_visited_this_pass,
                handler.next_input_index,
                mode,
            )
        };
        self.value_mode = mode;
        self.replay.page_events_visited = self.replay.page_events_visited.saturating_add(visited);
        self.replay.page_events_committed = self
            .replay
            .page_events_committed
            .saturating_add(committed as u64);
        if active.cursor.page_complete() {
            if is_dict {
                self.dict.commit_dict_page()?;
            }
            self.try_compact();
            return Ok(());
        }
        match step {
            PageStep::Emitted(_) | PageStep::Blocked => Ok(()),
            PageStep::PageComplete => Ok(()),
        }
    }

    fn begin_owned_page(
        &mut self,
        header: PageHeader,
        spec: ColumnSpec,
        body_start: usize,
        body_end: usize,
        is_dict: bool,
        emit_budget: usize,
        scratch: &mut PageScratch,
    ) -> Result<bool, Error> {
        use super::page_cursor::PageCursor;
        let cursor = PageCursor::new(header, spec, &self.limits)?;
        let body = &self.compressed[body_start..body_end];
        let plain = inflate_page_body(header.clone(), spec, self.codec, body, scratch)?;
        self.read_at = body_end;
        self.try_compact();
        self.replay.snappy_page_inflates = self.replay.snappy_page_inflates.saturating_add(1);
        if is_dict {
            self.dict.begin_dict_page();
        }
        self.active_plain = Some(ActivePlainPage {
            is_dict,
            plain,
            cursor,
        });
        if let Some(mut active) = self.active_plain.take() {
            self.feed_active_plain_taken(&mut active, emit_budget, scratch)?;
            if !active.cursor.page_complete() {
                self.active_plain = Some(active);
            }
        }
        Ok(self.active_plain.is_some())
    }

    fn start_page_if_ready(
        &mut self,
        emit_budget: usize,
        scratch: &mut PageScratch,
    ) -> Result<bool, Error> {
        self.try_compact();
        if self.read_at >= self.fed_len {
            if self.read_credit() == 0
                && self.read_reserved == 0
                && !self.chunk_complete()
                && self.fed_len > 0
            {
                return Err(Error::Message("compressed page exceeds memory budget"));
            }
            return Ok(false);
        }
        let window = self.limits.budget.compressed_bytes;
        let tail = &self.compressed[self.read_at..self.fed_len];
        let (header, header_len) = match page_header(tail) {
            Ok(v) => v,
            Err(compact_parquet_thrift::Error::EndOfInput) => {
                if self.read_credit() == 0 && self.read_reserved == 0 && !self.chunk_complete() {
                    return Err(Error::Message("compressed page exceeds memory budget"));
                }
                return Ok(false);
            }
            Err(e) => return Err(Error::Thrift(e)),
        };
        let page_values = page_value_count(&header)?;
        if page_values > self.limits.max_page_values {
            return Err(Error::Message(
                "parquet page exceeds configured value budget",
            ));
        }
        let compressed = usize::try_from(
            header
                .compressed_size
                .ok_or(Error::Message("page missing size"))?,
        )
        .map_err(|_| Error::Message("negative page size"))?;
        if compressed > self.limits.budget.codec_scratch_bytes {
            return Err(Error::Message(
                "compressed page exceeds codec scratch budget",
            ));
        }
        let body_start = self.read_at + header_len;
        let body_end = body_start
            .checked_add(compressed)
            .ok_or(Error::Message("page size overflow"))?;
        let page_on_disk = body_end.saturating_sub(self.read_at);
        if page_on_disk > window {
            return Err(Error::Message("compressed page exceeds memory budget"));
        }
        if body_end > self.fed_len {
            if self.read_credit() == 0 && self.read_reserved == 0 && !self.chunk_complete() {
                return Err(Error::Message("compressed page exceeds memory budget"));
            }
            return Ok(false);
        }
        if let Some(uncompressed) = header.uncompressed_size {
            let uncompressed = usize::try_from(uncompressed).unwrap_or(usize::MAX);
            if uncompressed > self.limits.budget.codec_scratch_bytes {
                return Err(Error::Message(
                    "uncompressed page exceeds codec scratch budget",
                ));
            }
        }
        let physical = physical_of(&self.leaf)?;
        let mut spec = ColumnSpec::flat(physical, self.codec);
        if self.leaf.max_def == 1 {
            spec = spec.optional();
        }
        spec.max_page_bytes = self.limits.budget.codec_scratch_bytes;
        spec.max_page_values = self.limits.max_page_values;
        spec.max_value_bytes = 8 * 1024 * 1024;
        if header.page_type != Some(2) && !self.dict.is_empty() {
            let len = u32::try_from(self.dict.len())
                .map_err(|_| Error::Message("dictionary longer than u32"))?;
            spec = spec.with_dictionary(len);
        }
        let is_dict = header.page_type == Some(2);
        let layout = page_layout(header.clone(), spec).map_err(Error::Page)?;
        let dictionary_data_page = !is_dict && !self.dict.is_empty();
        let encoding_supported = page_values == 0
            || owned_cursor_supports_data_page(physical, layout.encoding, dictionary_data_page);
        if self.leaf.max_def <= 1
            && encoding_supported
            && matches!(
                self.value_mode,
                ScanValueMode::MaterializedI64
                    | ScanValueMode::MaterializedF64
                    | ScanValueMode::DictionaryIds
                    | ScanValueMode::PlainUtf8
            )
        {
            match self.begin_owned_page(
                header,
                spec,
                body_start,
                body_end,
                is_dict,
                emit_budget,
                scratch,
            ) {
                Ok(paused) => {
                    return Ok(paused
                        || self.active_plain.is_some()
                        || self.pending_available() > 0);
                }
                Err(Error::Message("page encoding not supported by owned cursor yet")) => {
                    return Err(Error::Detail(format!(
                        "page encoding {} is not supported",
                        layout.encoding
                    )));
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::Detail(format!(
            "page encoding {} is not supported",
            layout.encoding
        )))
    }

    fn try_compact(&mut self) {
        if self.read_at > 0 && self.active_plain.is_none() {
            self.compressed.copy_within(self.read_at..self.fed_len, 0);
            self.fed_len -= self.read_at;
            self.read_at = 0;
            self.compressed.truncate(self.fed_len);
        }
    }
}

struct PageEventHandler<'a> {
    is_dict: bool,
    value_mode: ScanValueMode,
    dict: &'a mut DictionaryStore,
    pending: &'a mut Vec<i64>,
    pending_f64: &'a mut Vec<f64>,
    pending_ids: &'a mut Vec<u32>,
    utf8_offsets: &'a mut Vec<u32>,
    utf8_bytes: &'a mut Vec<u8>,
    nulls: &'a mut Vec<u8>,
    pending_at: usize,
    limits: ScanLimits,
    /// Max new rows to emit this page pass; `0` advances logical input without output.
    emit_budget: usize,
    next_input_index: usize,
    events_visited_this_pass: u64,
    rows_emitted_this_pass: usize,
    utf8_build: Option<Utf8Build>,
}

struct Utf8Build {
    length: u32,
    seen: usize,
}

impl PageEventHandler<'_> {
    /// The cursor stops before the byte budget when it can peek a value's length. When it
    /// cannot (dictionary ids, a split length prefix, an unprimed delta page), the value
    /// is already consumed: keep it and end the step, so a batch overshoots by at most one
    /// value instead of failing the query (Q37 `GROUP BY Title` on full hits).
    fn utf8_batch_flow(&self) -> Flow {
        if self.utf8_bytes.len() > self.limits.budget.batch_bytes {
            Flow::StopAfterValue
        } else {
            Flow::Continue
        }
    }

    fn push_utf8_null_row(&mut self) -> Result<(), Error> {
        if self.nulls.len() >= self.limits.max_pending_rows {
            return Err(Error::Message("pending row budget exhausted"));
        }
        self.nulls.push(0);
        let last = *self
            .utf8_offsets
            .last()
            .ok_or(Error::Message("utf8 offsets missing"))?;
        self.utf8_offsets.push(last);
        Ok(())
    }

    fn absorb_utf8_data_event(&mut self, event: PageEvent<'_>) -> Result<Flow, Error> {
        match event {
            PageEvent::Null(_) => self.push_utf8_null_row()?,
            PageEvent::Value(_, value) => match value {
                Value::BytesStart(length) => {
                    if self.utf8_build.is_some() {
                        return Err(Error::Message("nested byte array"));
                    }
                    self.utf8_build = Some(Utf8Build { length, seen: 0 });
                }
                Value::BytesChunk(chunk) => {
                    let build = self
                        .utf8_build
                        .as_mut()
                        .ok_or(Error::Message("chunk without start"))?;
                    build.seen += chunk.len();
                    self.utf8_bytes.extend_from_slice(chunk);
                }
                Value::BytesEnd => {
                    let build = self
                        .utf8_build
                        .take()
                        .ok_or(Error::Message("end without start"))?;
                    if build.seen != build.length as usize {
                        return Err(Error::Message("byte array length"));
                    }
                    if self.nulls.len() >= self.limits.max_pending_rows {
                        return Err(Error::Message("pending row budget exhausted"));
                    }
                    self.nulls.push(1);
                    self.utf8_offsets.push(
                        u32::try_from(self.utf8_bytes.len())
                            .map_err(|_| Error::Message("utf8 byte arena overflow"))?,
                    );
                    self.rows_emitted_this_pass += 1;
                }
                Value::DictionaryId(id) => {
                    // Borrow the entry: `dict` and the output buffers are separate fields.
                    let ValueRef::Bytes(bytes) = self.dict.resolve(id)? else {
                        return Err(Error::Message("dictionary id is not bytes"));
                    };
                    if self.nulls.len() >= self.limits.max_pending_rows {
                        return Err(Error::Message("pending row budget exhausted"));
                    }
                    self.nulls.push(1);
                    self.utf8_bytes.extend_from_slice(bytes);
                    self.utf8_offsets.push(
                        u32::try_from(self.utf8_bytes.len())
                            .map_err(|_| Error::Message("utf8 byte arena overflow"))?,
                    );
                    self.rows_emitted_this_pass += 1;
                }
                _ => {
                    return Err(Error::Message(
                        "unsupported page value for plain utf8 batch",
                    ))
                }
            },
        }
        Ok(self.utf8_batch_flow())
    }

    /// Writers fall back from dictionary to PLAIN pages part-way through a column chunk. The
    /// first PLAIN value in a dictionary-id scan switches the rest of the chunk to materialised
    /// values (strings or integers); rows still pending are converted first.
    fn switch_on_plain(&mut self, event: &PageEvent<'_>) -> Result<(), Error> {
        if self.value_mode != ScanValueMode::DictionaryIds {
            return Ok(());
        }
        match event {
            PageEvent::Value(_, Value::BytesStart(_) | Value::BytesChunk(_) | Value::BytesEnd) => {
                self.leave_dictionary_ids(ScanValueMode::PlainUtf8)
            }
            PageEvent::Value(_, Value::Boolean(_) | Value::I32(_) | Value::I64(_)) => {
                self.leave_dictionary_ids(ScanValueMode::MaterializedI64)
            }
            PageEvent::Value(_, Value::F32(_) | Value::F64(_)) => {
                self.leave_dictionary_ids(ScanValueMode::MaterializedF64)
            }
            _ => Ok(()),
        }
    }

    fn leave_dictionary_ids(&mut self, to: ScanValueMode) -> Result<(), Error> {
        match to {
            ScanValueMode::PlainUtf8 => {
                self.utf8_offsets.clear();
                self.utf8_bytes.clear();
                self.utf8_offsets.push(0);
                for (row, id) in self.pending_ids.iter().enumerate() {
                    // Rows before `pending_at` were already emitted; they only keep indices aligned.
                    if row >= self.pending_at && self.nulls[row] != 0 {
                        let ValueRef::Bytes(bytes) = self.dict.resolve(*id)? else {
                            return Err(Error::Message(
                                "PLAIN byte values in a non-byte dictionary column",
                            ));
                        };
                        self.utf8_bytes.extend_from_slice(bytes);
                    }
                    self.utf8_offsets.push(
                        u32::try_from(self.utf8_bytes.len())
                            .map_err(|_| Error::Message("utf8 byte arena overflow"))?,
                    );
                }
            }
            ScanValueMode::MaterializedI64 => {
                self.pending.clear();
                for (row, id) in self.pending_ids.iter().enumerate() {
                    let value = if row >= self.pending_at && self.nulls[row] != 0 {
                        match self.dict.resolve(*id)? {
                            ValueRef::Int(v) => v,
                            ValueRef::Bool(v) => i64::from(v),
                            _ => {
                                return Err(Error::Message(
                                    "PLAIN integer values in a non-integer dictionary column",
                                ))
                            }
                        }
                    } else {
                        0
                    };
                    self.pending.push(value);
                }
            }
            ScanValueMode::MaterializedF64 => {
                self.pending_f64.clear();
                for (row, id) in self.pending_ids.iter().enumerate() {
                    let value = if row >= self.pending_at && self.nulls[row] != 0 {
                        match self.dict.resolve(*id)? {
                            ValueRef::Float(v) => v,
                            _ => {
                                return Err(Error::Message(
                                    "PLAIN float values in a non-float dictionary column",
                                ))
                            }
                        }
                    } else {
                        0.0
                    };
                    self.pending_f64.push(value);
                }
            }
            ScanValueMode::DictionaryIds => return Ok(()),
        }
        self.pending_ids.clear();
        self.value_mode = to;
        Ok(())
    }

    /// Owned cursor path: every callback is a consumed, accepted page event.
    fn push_owned_event(&mut self, event: PageEvent<'_>) -> Result<Flow, Error> {
        self.events_visited_this_pass += 1;
        let mut flow = Flow::Continue;
        if self.is_dict {
            self.dict.on_dict_event(event)?;
        } else if self.emit_budget > 0 {
            self.switch_on_plain(&event)?;
            match self.value_mode {
                ScanValueMode::MaterializedI64 => {
                    if let Some((value, present)) = self.dict.data_row_for_numeric(event, true)? {
                        self.pending.push(value);
                        self.nulls.push(present);
                        self.rows_emitted_this_pass += 1;
                        if self.pending.len() > self.limits.max_pending_rows {
                            return Err(Error::Message("pending row budget exhausted"));
                        }
                    }
                }
                ScanValueMode::MaterializedF64 => {
                    if let Some((value, present)) = self.dict.data_row_for_float(event)? {
                        self.pending_f64.push(value);
                        self.nulls.push(present);
                        self.rows_emitted_this_pass += 1;
                        if self.pending_f64.len() > self.limits.max_pending_rows {
                            return Err(Error::Message("pending row budget exhausted"));
                        }
                    }
                }
                ScanValueMode::DictionaryIds => {
                    if let Some((id, present)) = self.dict.data_dict_id_row(event)? {
                        self.pending_ids.push(id);
                        self.nulls.push(present);
                        self.rows_emitted_this_pass += 1;
                        if self.pending_ids.len() > self.limits.max_pending_rows {
                            return Err(Error::Message("pending row budget exhausted"));
                        }
                    }
                }
                ScanValueMode::PlainUtf8 => flow = self.absorb_utf8_data_event(event)?,
            }
        }
        self.next_input_index += 1;
        Ok(flow)
    }
}

fn validate_leaf(
    leaf: &Leaf,
    codec: i32,
    limits: &ScanLimits,
    mode: ScanValueMode,
) -> Result<(), Error> {
    if leaf.max_rep != 0 || leaf.max_def > 1 {
        return Err(Error::Message("nested parquet columns are not supported"));
    }
    if codec != 0 && codec != 1 {
        return Err(Error::Message(
            "only uncompressed and snappy pages are supported",
        ));
    }
    match leaf.physical {
        0 | 1 | 2 | 4 | 5 => {}
        6 | 7 if mode == ScanValueMode::MaterializedI64 => {
            return Err(Error::Message(
                "string columns require plain utf8 or dictionary-id mode",
            ));
        }
        6 | 7 => {}
        _ => return Err(Error::Message("unsupported parquet physical type")),
    }
    let _ = physical_of(leaf)?;
    if limits.max_batch_rows == 0 || limits.max_pending_rows == 0 {
        return Err(Error::Message("scan limits must be non-zero"));
    }
    Ok(())
}

fn physical_of(leaf: &Leaf) -> Result<Physical, Error> {
    match leaf.physical {
        0 => Ok(Physical::Boolean),
        1 => Ok(Physical::Int32),
        2 => Ok(Physical::Int64),
        4 => Ok(Physical::Float),
        5 => Ok(Physical::Double),
        6 => Ok(Physical::ByteArray),
        7 if leaf.type_length > 0 => Ok(Physical::FixedByteArray(leaf.type_length as u32)),
        _ => Err(Error::Message("unsupported parquet physical type")),
    }
}

fn page_value_count(header: &PageHeader) -> Result<usize, Error> {
    let count = match header.page_type {
        Some(0) => header.data_v1_num_values,
        Some(2) => header.dictionary_num_values,
        Some(3) => header.data_v2.and_then(|page| page.num_values),
        _ => return Err(Error::Message("unsupported parquet page type")),
    };
    usize::try_from(count.ok_or(Error::Message("page is missing its value count"))?)
        .map_err(|_| Error::Message("negative page count"))
}

#[cfg(test)]
mod tests {
    use super::super::ingest_memory::ScanMemoryBudget;
    use super::super::schema::Leaf;
    use super::{NumericScan, ScanLimits};

    #[test]
    fn reserved_input_fills_the_compressed_window() {
        let window = 64 * 1024;
        let limits = ScanLimits::for_batch_rows_and_budget(
            1024,
            ScanMemoryBudget {
                compressed_bytes: window,
                codec_scratch_bytes: 64 * 1024,
                dictionary_bytes: 4096,
                batch_bytes: 4096,
                output_bytes: 4096,
            },
        );
        let leaf = Leaf {
            name: "a".into(),
            physical: 2,
            type_length: 0,
            max_def: 0,
            max_rep: 0,
        };
        let mut scan =
            NumericScan::begin_with_limits(leaf, 0, limits, (window * 2) as u32, None).unwrap();
        let credit = scan.read_credit();
        assert_eq!(credit, window);
        scan.reserve_read(credit);
        assert_eq!(scan.read_credit(), 0);
        let bytes = vec![1u8; credit];
        assert_eq!(scan.push_compressed(&bytes).unwrap(), 0);
        assert_eq!(scan.accept_reserved(&bytes).unwrap(), credit);
        assert_eq!(scan.resident_compressed_bytes(), credit);
        assert_eq!(scan.total_fed_bytes(), credit);
        assert_eq!(scan.read_credit(), 0);
    }
}
