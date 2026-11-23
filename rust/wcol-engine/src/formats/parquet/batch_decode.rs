//! Parquet column chunks → WCOL batches (decode only; no SQL).

use std::collections::HashSet;
use std::sync::Arc;

use crate::error::{WcolError, WcolResult};
use crate::formats::parquet::decode::{DecodeProgress, ScanLimits};
use crate::formats::parquet::ingest_memory::ScanMemoryBudget;
use crate::formats::parquet::page_scratch::PageScratch;
use crate::formats::parquet::schema::Leaf;
use crate::formats::parquet::Error;
use crate::formats::parquet::{NumericScan, ScanValueMode};
use crate::ops::query::CatalogColumn;
use crate::program::row_id::{row_id, RowPosition};
use crate::program::IngestMemoryProfile;
use crate::stream::{DictEntries, DictionaryValues, OwnedWcolBatch};

/// Max bytes requested per host ReadAt (JS retains the full response until commit).
pub const HOST_READ_LIMIT: usize = 256 * 1024;
const MAX_OUTSTANDING_READS: usize = 2;

/// Host range read issued by [`ParquetDecode::host_read_request`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostReadAt {
    pub ticket: u32,
    pub offset: u64,
    pub length: u32,
}

struct PlannedChunk {
    column: u32,
    offset: u64,
    codec: i32,
    length: u32,
    dict_encoded: bool,
}

struct PlannedGroup {
    row_group: u32,
    rows: u64,
    chunks: Vec<PlannedChunk>,
}

struct ColumnScan {
    column: u32,
    scan: NumericScan,
    /// Built once per column chunk; reused for every dict-id batch (I-09).
    shared_dict: Option<DictionaryValues>,
}

struct IssuedRead {
    ticket: u32,
    scan_index: usize,
    offset: u64,
    length: usize,
    filled: usize,
    reserved: usize,
}

fn decode_err(err: Error) -> WcolError {
    match err {
        Error::Message(msg) => WcolError::Static(msg),
        Error::BadFooter => WcolError::Static("bad footer"),
        other => WcolError::msg(format!("decode: {other}")),
    }
}

fn group_scan_limits(batch_rows: usize, profile: IngestMemoryProfile) -> ScanLimits {
    match profile {
        IngestMemoryProfile::Q3Restrictive => ScanLimits::for_q3_restrictive_acceptance(batch_rows),
        IngestMemoryProfile::ExecutionGroup => ScanLimits::for_execution_group(batch_rows),
    }
}

/// One column's share of the row group's budgets. The compressed window is split in proportion
/// to each column's chunk size so simultaneous retention stays near the group budget. When a
/// column's chunk still fits in that cap, its window is raised to at least `chunk_len` so
/// single-page VARCHAR columns (common in full-schema row queries) can decode. Dictionary
/// columns use the full group dictionary cap when fair split would be too small.
fn per_column_limits(
    group_limits: ScanLimits,
    column_count: usize,
    chunk_len: u64,
    group_len: u64,
    dict_encoded: bool,
) -> ScanLimits {
    let n = column_count.max(1);
    let total_compressed = group_limits.budget.compressed_bytes;
    let chunk_len_usize = chunk_len as usize;
    let mut share = if group_len == 0 {
        total_compressed / n
    } else if group_len as u128 <= total_compressed as u128 {
        // The whole row group fits: every column can buffer its entire chunk.
        chunk_len_usize
    } else {
        (total_compressed as u128 * u128::from(chunk_len) / u128::from(group_len)) as usize
    }
    .max(1);
    if chunk_len_usize <= total_compressed {
        share = share.max(chunk_len_usize);
    }
    let total_dictionary = group_limits.budget.dictionary_bytes;
    let mut dict_share = (total_dictionary / n).max(1);
    if dict_encoded {
        // Fair split can be far below a column's dictionary page; allow up to the group cap.
        dict_share = dict_share.max(total_dictionary);
    }
    ScanLimits {
        max_batch_rows: group_limits.max_batch_rows,
        max_pending_rows: group_limits.max_pending_rows,
        max_page_values: group_limits.max_page_values,
        max_dictionary_entries: group_limits.max_dictionary_entries,
        budget: ScanMemoryBudget {
            compressed_bytes: share,
            codec_scratch_bytes: group_limits.budget.codec_scratch_bytes,
            dictionary_bytes: dict_share,
            batch_bytes: group_limits.budget.batch_bytes,
            output_bytes: group_limits.budget.output_bytes,
        },
    }
}

/// Reads planned Parquet chunks and emits [`OwnedWcolBatch`] values.
pub struct ParquetDecode {
    batch_rows: usize,
    catalog: Vec<CatalogColumn>,
    groups: Vec<PlannedGroup>,
    group_at: usize,
    scans: Vec<ColumnScan>,
    page_scratch: Option<PageScratch>,
    outstanding_reads: Vec<IssuedRead>,
    next_ticket: u32,
    memory_profile: IngestMemoryProfile,
    /// Fill this column with each row's id, as rows of `source`.
    row_id: Option<RowIdColumn>,
    /// Rows of the current group already emitted.
    group_row: u64,
}

#[derive(Clone, Copy)]
struct RowIdColumn {
    column: u32,
    source: u16,
}

fn planned_group(group: &crate::program::IngestGroup) -> PlannedGroup {
    PlannedGroup {
        row_group: group.row_group,
        rows: group.rows,
        chunks: group
            .chunks
            .iter()
            .map(|chunk| PlannedChunk {
                column: chunk.column,
                offset: chunk.offset,
                codec: chunk.codec,
                length: chunk.length,
                dict_encoded: chunk.dict_encoded,
            })
            .collect(),
    }
}

/// `(column, offset, codec, length)` of one column chunk to decode.
pub type ChunkSpec = (u32, u64, i32, u32);
/// `(row_group, rows, chunks)` of one row group to decode.
pub type RowGroupSpec = (u32, u64, Vec<ChunkSpec>);

impl ParquetDecode {
    pub fn new(
        batch_rows: usize,
        catalog: Vec<CatalogColumn>,
        groups: Vec<RowGroupSpec>,
        dict_columns: HashSet<u32>,
    ) -> Self {
        let groups = groups
            .into_iter()
            .map(|(row_group, rows, chunks)| PlannedGroup {
                row_group,
                rows,
                chunks: chunks
                    .into_iter()
                    .map(|(column, offset, codec, length)| PlannedChunk {
                        column,
                        offset,
                        codec,
                        length,
                        dict_encoded: dict_columns.contains(&column),
                    })
                    .collect(),
            })
            .collect();
        Self {
            batch_rows,
            catalog,
            groups,
            group_at: 0,
            scans: Vec::new(),
            page_scratch: None,
            outstanding_reads: Vec::new(),
            next_ticket: 1,
            memory_profile: IngestMemoryProfile::default(),
            row_id: None,
            group_row: 0,
        }
    }

    pub fn from_ingest_groups(
        batch_rows: usize,
        catalog: Vec<CatalogColumn>,
        groups: &[crate::program::IngestGroup],
    ) -> Self {
        Self::from_ingest_groups_with_profile(
            batch_rows,
            catalog,
            groups,
            IngestMemoryProfile::default(),
        )
    }

    pub fn from_ingest_groups_with_profile(
        batch_rows: usize,
        catalog: Vec<CatalogColumn>,
        groups: &[crate::program::IngestGroup],
        memory_profile: IngestMemoryProfile,
    ) -> Self {
        let groups = groups.iter().map(planned_group).collect();
        Self {
            batch_rows,
            catalog,
            groups,
            group_at: 0,
            scans: Vec::new(),
            page_scratch: None,
            outstanding_reads: Vec::new(),
            next_ticket: 1,
            memory_profile,
            row_id: None,
            group_row: 0,
        }
    }

    /// Emit `column` with every batch [`fill_batch`](Self::fill_batch) fills: each row's id
    /// as a row of `source`.
    pub fn set_row_id(&mut self, column: u32, source: u16) {
        self.row_id = Some(RowIdColumn { column, source });
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Append a row group to decode after the planned ones (a unit this scan just claimed).
    pub fn push_ingest_group(&mut self, group: &crate::program::IngestGroup) {
        self.groups.push(planned_group(group));
    }

    /// Row groups appended and not yet retired, including the one being decoded.
    pub fn queued_groups(&self) -> usize {
        self.groups.len().saturating_sub(self.group_at)
    }

    /// The [`Self::queued_groups`] by row group id.
    pub fn queued_row_groups(&self) -> impl Iterator<Item = u32> + '_ {
        self.groups
            .iter()
            .skip(self.group_at)
            .map(|group| group.row_group)
    }

    pub fn batch_limit(&self) -> usize {
        self.batch_rows
    }

    pub fn ensure_scans(&mut self) -> WcolResult<bool> {
        if self.scans.is_empty() {
            if self.group_at >= self.groups.len() {
                return Ok(false);
            }
            self.begin_row_group()?;
        }
        Ok(!self.scans.is_empty())
    }

    pub fn rows_at(&self, index: usize) -> u64 {
        self.groups.get(index).map(|g| g.rows).unwrap_or(0)
    }

    pub fn ingest_complete(&self) -> bool {
        self.group_at >= self.groups.len() && self.scans.is_empty()
    }

    pub fn outstanding_host_reads(&self) -> usize {
        self.outstanding_reads.len()
    }

    /// Per-column scan state, for diagnosing a decode that can neither emit rows nor read.
    pub fn stall_report(&self) -> String {
        let scans: Vec<String> = self
            .scans
            .iter()
            .map(|s| {
                format!(
                    "col {}: avail={} exhausted={} credit={} wants={} fed={} [{}]",
                    s.column,
                    s.scan.available_rows(),
                    s.scan.decode_exhausted(),
                    s.scan.read_credit(),
                    s.scan.chunk_wants(),
                    s.scan.total_fed_bytes(),
                    s.scan.debug_state(),
                )
            })
            .collect();
        format!(
            "row group {}/{}, outstanding reads {}; {}",
            self.group_at,
            self.groups.len(),
            self.outstanding_reads.len(),
            scans.join("; ")
        )
    }

    pub fn resident_compressed_bytes(&self) -> usize {
        self.scans
            .iter()
            .map(|s| s.scan.resident_compressed_bytes())
            .sum()
    }

    /// Issue another host ReadAt when fewer than [`MAX_OUTSTANDING_READS`] are in flight.
    pub fn host_read_request(&mut self) -> WcolResult<Option<HostReadAt>> {
        if self.ingest_complete() {
            return Ok(None);
        }
        if self.outstanding_reads.len() >= MAX_OUTSTANDING_READS {
            return Ok(None);
        }
        if self.scans.is_empty() {
            self.begin_row_group()?;
        }
        let ready = self.min_available_rows();
        if ready > 0 {
            return Ok(None);
        }
        let Some(scan_index) = self.select_read_scan() else {
            return Ok(None);
        };
        let scan = &mut self.scans[scan_index].scan;
        let credit = scan.read_credit();
        if credit == 0 {
            return Ok(None);
        }
        let length = credit.min(HOST_READ_LIMIT);
        let chunk = &self.groups[self.group_at].chunks[scan_index];
        let offset = chunk.offset.saturating_add(scan.total_fed_bytes() as u64);
        scan.reserve_read(length);
        let ticket = self.next_ticket;
        self.next_ticket = self.next_ticket.wrapping_add(1);
        self.outstanding_reads.push(IssuedRead {
            ticket,
            scan_index,
            offset,
            length,
            filled: 0,
            reserved: length,
        });
        Ok(Some(HostReadAt {
            ticket,
            offset,
            length: u32::try_from(length).unwrap_or(u32::MAX),
        }))
    }

    /// Deliver bytes for an outstanding ticket (any completion order).
    pub fn complete_read(&mut self, ticket: u32, input: &[u8]) -> WcolResult<()> {
        if self.group_at >= self.groups.len() && self.scans.is_empty() {
            return Ok(());
        }
        self.begin_row_group()?;
        let pos = self
            .outstanding_reads
            .iter()
            .position(|r| r.ticket == ticket)
            .ok_or_else(|| WcolError::from("unknown read ticket"))?;
        let want = self.outstanding_reads[pos]
            .length
            .saturating_sub(self.outstanding_reads[pos].filled);
        if want == 0 {
            return Err(WcolError::from("read ticket already complete"));
        }
        if input.len() != want {
            return Err(WcolError::from("read completion length mismatch"));
        }
        let scan_index = self.outstanding_reads[pos].scan_index;
        let reserved = self.outstanding_reads[pos].reserved;
        let scan = &mut self.scans[scan_index].scan;
        let pushed = scan.accept_reserved(input).map_err(decode_err)?;
        if pushed != input.len() {
            return Err(WcolError::from(
                "column ingest did not accept read response",
            ));
        }
        if reserved > pushed {
            scan.release_read(reserved - pushed);
        }
        self.outstanding_reads.remove(pos);
        Ok(())
    }

    /// Drop reservation when the host fetch fails (avoids stalling on zero credit).
    pub fn cancel_read(&mut self, ticket: u32) -> WcolResult<()> {
        let pos = self
            .outstanding_reads
            .iter()
            .position(|r| r.ticket == ticket)
            .ok_or_else(|| WcolError::from("unknown read ticket"))?;
        let read = self.outstanding_reads.remove(pos);
        self.scans[read.scan_index]
            .scan
            .release_read(read.reserved.saturating_sub(read.filled));
        Ok(())
    }

    /// Upcoming host reads for prefetch (read-only; does not advance decode).
    pub fn host_read_hints(&self, max: usize) -> Vec<(u64, u32)> {
        if max == 0 || self.ingest_complete() {
            return Vec::new();
        }
        let mut out = Vec::new();
        for read in &self.outstanding_reads {
            if out.len() >= max {
                break;
            }
            let rem = read.length.saturating_sub(read.filled);
            if rem == 0 {
                continue;
            }
            let len = rem.min(HOST_READ_LIMIT);
            let len_u32 = u32::try_from(len).unwrap_or(u32::MAX);
            out.push((read.offset.saturating_add(read.filled as u64), len_u32));
        }
        if self.outstanding_reads.len() >= MAX_OUTSTANDING_READS {
            return out;
        }
        if self.scans.is_empty() {
            if let Some(group) = self.groups.get(self.group_at) {
                for chunk in &group.chunks {
                    if out.len() >= max {
                        break;
                    }
                    let len = (chunk.length as usize).min(HOST_READ_LIMIT);
                    let len_u32 = u32::try_from(len).unwrap_or(u32::MAX);
                    out.push((chunk.offset, len_u32));
                }
            }
            return out;
        }
        let busy: HashSet<usize> = self
            .outstanding_reads
            .iter()
            .map(|r| r.scan_index)
            .collect();
        for (scan_index, col_scan) in self.scans.iter().enumerate() {
            if out.len() >= max {
                break;
            }
            if busy.contains(&scan_index) {
                continue;
            }
            let scan = &col_scan.scan;
            if scan.decode_exhausted() || scan.available_rows() > 0 {
                continue;
            }
            if scan.read_credit() == 0 || scan.chunk_wants() == 0 {
                continue;
            }
            let credit = scan.read_credit().min(HOST_READ_LIMIT);
            let chunk = &self.groups[self.group_at].chunks[scan_index];
            let offset = chunk.offset.saturating_add(scan.total_fed_bytes() as u64);
            let len_u32 = u32::try_from(credit).unwrap_or(u32::MAX);
            out.push((offset, len_u32));
        }
        out
    }

    pub fn push_input(&mut self, input: &[u8], consumed: &mut usize) -> WcolResult<()> {
        *consumed = input.len();
        if input.is_empty() {
            return Ok(());
        }
        if self.outstanding_reads.is_empty() {
            return Err(WcolError::from("ingest input without outstanding read"));
        }
        let ticket = self.outstanding_reads[0].ticket;
        self.complete_read(ticket, input)
    }

    /// Streaming ingest (same semantics as legacy execution-group decode).
    pub fn ingest_input(&mut self, input: &[u8], consumed: &mut usize) -> WcolResult<()> {
        self.push_input(input, consumed)
    }

    pub fn scans_active(&self) -> bool {
        !self.scans.is_empty()
    }

    pub fn any_column_chunk_incomplete(&self) -> bool {
        self.scans.iter().any(|s| !s.scan.chunk_complete())
    }

    pub fn fill_batch(&mut self, out: &mut OwnedWcolBatch) -> WcolResult<bool> {
        out.clear();
        if self.scans.is_empty() {
            self.begin_row_group()?;
            if self.scans.is_empty() {
                return Ok(false);
            }
        }
        self.drive_scans(self.batch_rows)?;
        if self.check_row_mismatch()? {
            return Err(WcolError::from("misaligned parquet columns"));
        }
        let ready = self.min_available_rows();
        if ready == 0 {
            if self.scans.iter().all(|s| s.scan.decode_exhausted()) {
                for col_scan in &self.scans {
                    crate::formats::parquet::ingest_replay_merge(col_scan.scan.replay_metrics());
                }
                self.scans.clear();
                self.page_scratch = None;
                self.outstanding_reads.clear();
                self.group_at += 1;
            }
            return Ok(false);
        }
        let take = ready.min(self.batch_rows);
        let scope = self
            .groups
            .get(self.group_at)
            .map(|g| g.row_group)
            .unwrap_or(0);
        out.set_scope(scope);

        struct ColumnPayload {
            column: u32,
            mode: ScanValueMode,
            values: Vec<i64>,
            floats: Vec<f64>,
            ids: Vec<u32>,
            utf8_offsets: Vec<u32>,
            utf8_bytes: Vec<u8>,
            nulls: Vec<u8>,
        }

        let mut payloads = Vec::with_capacity(self.scans.len());
        for col_scan in &self.scans {
            let mode = col_scan.scan.value_mode();
            let mut values = Vec::new();
            let mut floats = Vec::new();
            let mut ids = Vec::new();
            let mut utf8_offsets = Vec::new();
            let mut utf8_bytes = Vec::new();
            let mut nulls = Vec::new();
            match mode {
                ScanValueMode::MaterializedI64 => {
                    col_scan
                        .scan
                        .copy_materialized_rows(&mut values, &mut nulls, take)
                        .map_err(decode_err)?;
                }
                ScanValueMode::MaterializedF64 => {
                    col_scan
                        .scan
                        .copy_f64_rows(&mut floats, &mut nulls, take)
                        .map_err(decode_err)?;
                }
                ScanValueMode::DictionaryIds => {
                    col_scan
                        .scan
                        .copy_dict_id_rows(&mut ids, &mut nulls, take)
                        .map_err(decode_err)?;
                }
                ScanValueMode::PlainUtf8 => {
                    col_scan
                        .scan
                        .copy_plain_utf8_rows(&mut utf8_offsets, &mut utf8_bytes, &mut nulls, take)
                        .map_err(decode_err)?;
                }
            }
            payloads.push(ColumnPayload {
                column: col_scan.column,
                mode,
                values,
                floats,
                ids,
                utf8_offsets,
                utf8_bytes,
                nulls,
            });
        }

        for (col_scan, payload) in self.scans.iter_mut().zip(payloads.into_iter()) {
            match payload.mode {
                ScanValueMode::MaterializedI64 => {
                    out.push_i64_column(payload.column, payload.values, payload.nulls);
                }
                ScanValueMode::MaterializedF64 => {
                    out.push_f64_column(payload.column, payload.floats, payload.nulls);
                }
                ScanValueMode::DictionaryIds => {
                    if col_scan.shared_dict.is_none() {
                        let dict = col_scan.scan.dictionary();
                        let entries = if let Ok(floats) = dict.copy_float_entries() {
                            DictEntries::Float(Arc::from(floats))
                        } else {
                            match dict.copy_int_entries() {
                                Ok(ints) => DictEntries::Int(Arc::from(ints)),
                                Err(_) => {
                                    let (offsets, bytes) =
                                        dict.copy_byte_entries().map_err(decode_err)?;
                                    DictEntries::Bytes {
                                        offsets: Arc::from(offsets),
                                        bytes: Arc::from(bytes),
                                    }
                                }
                            }
                        };
                        col_scan.shared_dict = Some(DictionaryValues {
                            scope,
                            column: payload.column,
                            entries,
                        });
                    }
                    out.push_dict_id_column(
                        payload.column,
                        payload.ids,
                        payload.nulls,
                        col_scan.shared_dict.clone().unwrap(),
                    );
                }
                ScanValueMode::PlainUtf8 => {
                    out.push_utf8_column(
                        payload.column,
                        payload.utf8_offsets,
                        payload.utf8_bytes,
                        payload.nulls,
                    );
                }
            }
        }
        if let Some(row_id) = self.row_id {
            let ids = self.row_ids(row_id.source, scope, take)?;
            out.push_i64_column(row_id.column, ids, vec![1; take]);
        }
        out.set_row_count(take)?;
        self.group_row += take as u64;

        for col_scan in &mut self.scans {
            col_scan.scan.advance_batch(take).map_err(decode_err)?;
        }
        Ok(true)
    }

    fn row_ids(&self, source: u16, row_group: u32, take: usize) -> WcolResult<Vec<i64>> {
        (self.group_row..self.group_row + take as u64)
            .map(|row| {
                row_id(RowPosition {
                    source,
                    row_group,
                    row,
                })
                .ok_or_else(|| WcolError::msg(format!("row {row} of group {row_group} has no id")))
            })
            .collect()
    }

    fn min_available_rows(&self) -> usize {
        self.scans
            .iter()
            .map(|s| s.scan.available_rows())
            .min()
            .unwrap_or(0)
    }

    fn check_row_mismatch(&self) -> WcolResult<bool> {
        let any_exhausted_empty = self
            .scans
            .iter()
            .any(|s| s.scan.decode_exhausted() && s.scan.available_rows() == 0);
        let any_pending = self.scans.iter().any(|s| s.scan.available_rows() > 0);
        if any_exhausted_empty && any_pending {
            return Ok(true);
        }
        Ok(false)
    }

    fn drive_scans(&mut self, max_emit: usize) -> WcolResult<()> {
        let scratch = self
            .page_scratch
            .as_mut()
            .ok_or_else(|| WcolError::from("decode scratch missing"))?;
        for col_scan in &mut self.scans {
            if col_scan.scan.decode_exhausted() {
                continue;
            }
            loop {
                match col_scan
                    .scan
                    .poll_with_scratch(max_emit, scratch)
                    .map_err(decode_err)?
                {
                    DecodeProgress::BatchReady { .. } | DecodeProgress::Finished => break,
                    DecodeProgress::Advanced => continue,
                    DecodeProgress::NeedInput => break,
                }
            }
        }
        Ok(())
    }

    fn select_read_scan(&self) -> Option<usize> {
        let busy: HashSet<usize> = self
            .outstanding_reads
            .iter()
            .map(|r| r.scan_index)
            .collect();
        for (index, col_scan) in self.scans.iter().enumerate() {
            let scan = &col_scan.scan;
            if scan.decode_exhausted() || scan.available_rows() > 0 {
                continue;
            }
            if scan.read_credit() == 0 {
                continue;
            }
            if scan.chunk_wants() == 0 {
                continue;
            }
            if !busy.contains(&index) {
                return Some(index);
            }
        }
        for (index, col_scan) in self.scans.iter().enumerate() {
            if !busy.contains(&index) {
                continue;
            }
            let partial = self
                .outstanding_reads
                .iter()
                .any(|r| r.scan_index == index && r.filled > 0 && r.filled < r.length);
            if !partial {
                continue;
            }
            let scan = &col_scan.scan;
            if scan.decode_exhausted() || scan.available_rows() > 0 {
                continue;
            }
            if scan.read_credit() == 0 || scan.chunk_wants() == 0 {
                continue;
            }
            return Some(index);
        }
        None
    }

    fn begin_row_group(&mut self) -> WcolResult<()> {
        if !self.scans.is_empty() {
            return Ok(());
        }
        let group = self
            .groups
            .get(self.group_at)
            .ok_or_else(|| WcolError::from("no row group"))?;
        let group_limits = group_scan_limits(self.batch_rows, self.memory_profile);
        self.page_scratch = Some(PageScratch::for_limits(&group_limits));
        let column_count = group.chunks.len().max(1);
        let group_len: u64 = group.chunks.iter().map(|c| u64::from(c.length)).sum();
        let mut new_scans = Vec::with_capacity(group.chunks.len());
        for chunk in &group.chunks {
            let column = self
                .catalog
                .get(chunk.column as usize)
                .ok_or_else(|| WcolError::from("column missing"))?;
            let leaf = Leaf {
                name: column.name.clone(),
                physical: column.physical,
                type_length: column.type_length,
                max_def: column.max_def,
                max_rep: column.max_rep,
            };
            let limits = per_column_limits(
                group_limits,
                column_count,
                u64::from(chunk.length),
                group_len,
                chunk.dict_encoded,
            );
            let string_col = matches!(column.physical, 6 | 7);
            let scan = if string_col {
                if chunk.dict_encoded {
                    NumericScan::begin_dict_ids_with_limits_shared_scratch(
                        leaf,
                        chunk.codec,
                        limits,
                        chunk.length,
                        Some(group.rows),
                    )
                } else {
                    NumericScan::begin_plain_utf8_with_limits_shared_scratch(
                        leaf,
                        chunk.codec,
                        limits,
                        chunk.length,
                        Some(group.rows),
                    )
                }
            } else if chunk.dict_encoded {
                NumericScan::begin_dict_ids_with_limits_shared_scratch(
                    leaf,
                    chunk.codec,
                    limits,
                    chunk.length,
                    Some(group.rows),
                )
            } else {
                NumericScan::begin_with_limits_shared_scratch(
                    leaf,
                    chunk.codec,
                    limits,
                    chunk.length,
                    Some(group.rows),
                )
            }
            .map_err(|e| WcolError::msg(format!("scan open failed: {e}")))?;
            new_scans.push(ColumnScan {
                column: chunk.column,
                scan,
                shared_dict: None,
            });
        }
        self.scans = new_scans;
        self.group_row = 0;
        self.outstanding_reads.clear();
        if group.chunks.is_empty() {
            self.page_scratch = None;
            self.group_at += 1;
            return self.begin_row_group();
        }
        Ok(())
    }
}

#[cfg(test)]
mod per_column_limits_tests {
    use super::per_column_limits;
    use crate::formats::parquet::decode::ScanLimits;

    #[test]
    fn wide_chunk_gets_window_at_least_chunk_len_when_chunk_fits_group_cap() {
        let group_limits = ScanLimits::for_execution_group(8192);
        let group_len = 14_922_554u64;
        let wide_chunk = 5_566_841u64;
        let column_count = 105usize;
        let limits = per_column_limits(group_limits, column_count, wide_chunk, group_len, false);
        let proportional = (group_limits.budget.compressed_bytes as u128 * u128::from(wide_chunk)
            / u128::from(group_len)) as usize;
        assert!(
            proportional < wide_chunk as usize,
            "fixture should use a chunk larger than the fair split"
        );
        assert_eq!(limits.budget.compressed_bytes, wide_chunk as usize);
    }
}
