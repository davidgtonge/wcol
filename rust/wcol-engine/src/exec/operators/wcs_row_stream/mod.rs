//! WCS1 `DATA_BATCH` egress / ingress for hash-partitioned row exchange (Milestone B).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{WcolError, WcolResult};

use super::wcs_frame::{CompleteFrame, WcsIngress};
use super::wcs_projection::owned_batch_from_projection_body_with_dicts;
use crate::formats::parquet::stream_wcs::{
    apply_dictionary_payload, encode_one_dictionary_frame, rebase_dictionary_frame,
};
use crate::stream::projection::{
    encode_projection_columns, max_rows_for_body_budget, projection_columns_for_range,
};
use crate::stream::wcs::MAX_WCS_PAYLOAD;
use crate::stream::wcs::{self, FRAME_DATA_BATCH, FRAME_DICTIONARY, FRAME_END, FRAME_PARTIAL};
use crate::stream::{DictionaryValues, OwnedWcolBatch, WcolColumn};

static EGRESS_PENDING_BYTES: AtomicU64 = AtomicU64::new(0);
static EGRESS_PEAK_PENDING_BYTES: AtomicU64 = AtomicU64::new(0);

fn note_referenced_ids(view: &crate::stream::WcolBatch<'_>, partition: u32) {
    for col in view.columns {
        let WcolColumn::DictId {
            column,
            ids,
            nulls,
            dictionary,
        } = col
        else {
            continue;
        };
        for row in 0..ids.len() {
            if nulls.get(row).copied().unwrap_or(0) == 0 {
                continue;
            }
            crate::exec::dict_traffic::note_referenced(
                partition,
                dictionary,
                *column,
                ids[row] as usize,
            );
        }
    }
}

/// Frame bytes still waiting to be copied into the host output buffer.
pub fn egress_pending_bytes() -> u64 {
    EGRESS_PENDING_BYTES.load(Ordering::Relaxed)
}

/// Largest `egress_pending_bytes` seen in this process.
pub fn egress_peak_pending_bytes() -> u64 {
    EGRESS_PEAK_PENDING_BYTES.load(Ordering::Relaxed)
}

pub fn note_egress_pending(bytes: u64) {
    EGRESS_PENDING_BYTES.store(bytes, Ordering::Relaxed);
    EGRESS_PEAK_PENDING_BYTES.fetch_max(bytes, Ordering::Relaxed);
}

/// Work that has not been encoded yet. At most one frame is materialized.
enum Staged {
    Dictionary {
        dictionary: DictionaryValues,
        next_entry: usize,
        /// Index of `dictionary`'s first entry on the wire: non-zero for a sparse chunk.
        base: usize,
        partition: u32,
    },
    Rows {
        batch: OwnedWcolBatch,
        next_row: usize,
        partition: u32,
    },
    /// One encoded `FRAME_PARTIAL` payload (`PWCL`).
    Partial {
        payload: Vec<u8>,
        partition: u32,
    },
    End {
        partition: u32,
    },
}

/// Encode row batches as a standalone WCS1 stream (sequence starts at 0 per stream).
/// Completed frames are dropped once their bytes have been copied out.
/// Dictionary and row frames are encoded one at a time, after the previous frame drains.
pub struct RowBatchWcsEgress {
    current: Option<Vec<u8>>,
    byte_off: usize,
    staged: VecDeque<Staged>,
    next_sequence: u32,
    stream_ended: bool,
    /// `(dict_scope, column)` dictionaries already fully queued on this stream.
    sent_dict_entries: HashMap<(u32, u32), usize>,
    /// Rows of each `(dict_scope, column)` sent as plain values instead of its dictionary.
    plain_rows: HashMap<(u32, u32), usize>,
    /// Partition streams only: scan dictionary id -> partition-local id, per column of
    /// `sparse_scope`. Cleared when the scan moves to its next row group.
    sparse_ids: HashMap<u32, HashMap<u32, u32>>,
    sparse_scope: Option<u32>,
}

impl Default for RowBatchWcsEgress {
    fn default() -> Self {
        Self::new()
    }
}

impl RowBatchWcsEgress {
    pub fn new() -> Self {
        Self {
            current: None,
            byte_off: 0,
            staged: VecDeque::new(),
            next_sequence: 0,
            stream_ended: false,
            sent_dict_entries: HashMap::new(),
            plain_rows: HashMap::new(),
            sparse_ids: HashMap::new(),
            sparse_scope: None,
        }
    }

    pub fn stream_ended(&self) -> bool {
        self.stream_ended
    }

    pub fn has_pending_output(&self) -> bool {
        self.current.is_some() || !self.staged.is_empty()
    }

    /// Bytes of the encoded frame not yet copied out.
    /// Later frames stay as cursors until this one drains.
    pub fn pending_bytes(&self) -> usize {
        self.current
            .as_ref()
            .map(|frame| frame.len().saturating_sub(self.byte_off))
            .unwrap_or(0)
    }

    /// A frame was only partly copied. The next write must finish it before another partition.
    pub fn mid_frame(&self) -> bool {
        self.byte_off != 0
    }

    /// One batch of a single (unpartitioned) stream. Each dictionary is sent whole, once,
    /// but only after the stream has sent as many of its rows as it has entries: until then
    /// those rows go as plain values. A filtered or limited stream (a late read-back keeps
    /// one row of a row group) never ships a dictionary it barely uses, and a full scan
    /// sends at most twice the bytes of the cheaper choice.
    pub fn push_batch(&mut self, batch: &OwnedWcolBatch) -> WcolResult<()> {
        let partition = 0;
        if self.stream_ended {
            return Err(WcolError::from("WCS stream already ended"));
        }
        let batch = self.plain_until_dictionary_pays(batch)?;
        let mut col_view = Vec::new();
        let view = batch.view_with(&mut col_view);
        for col in view.columns {
            if let WcolColumn::DictId {
                column, dictionary, ..
            } = col
            {
                self.enqueue_dictionary(view.dict_scope, dictionary, *column, partition)?;
            }
        }
        note_referenced_ids(&view, partition);
        self.staged.push_back(Staged::Rows {
            batch,
            next_row: 0,
            partition,
        });
        self.fill_current()?;
        Ok(())
    }

    /// `batch` with each dictionary column that is not yet worth sending as plain values.
    fn plain_until_dictionary_pays(
        &mut self,
        batch: &OwnedWcolBatch,
    ) -> WcolResult<OwnedWcolBatch> {
        let rows = batch.row_count();
        let scope = batch.dict_scope();
        let mut plain = Vec::new();
        let mut col_view = Vec::new();
        for col in batch.view_with(&mut col_view).columns {
            let WcolColumn::DictId {
                column, dictionary, ..
            } = col
            else {
                continue;
            };
            let key = (scope, *column);
            if self.sent_dict_entries.contains_key(&key) {
                continue;
            }
            let sent = self.plain_rows.entry(key).or_default();
            if *sent + rows < dictionary.entry_count() {
                *sent += rows;
                plain.push(*column);
            }
        }
        let mut out = batch.clone();
        out.expand_dicts(&plain).map_err(WcolError::msg)?;
        Ok(out)
    }

    /// One hash partition's rows, stamped with `partition`.
    ///
    /// A partition holds about 1/N of the scan's rows, so it sends only the dictionary
    /// entries those rows reference, numbered locally and appended as chunks. Sending each
    /// scan dictionary whole to every reducer moved ~216 MB of URL dictionaries into each
    /// of 64 reducers on full hits (`COUNT(DISTINCT URL)`) and held one per active scan.
    pub fn push_batch_partition(
        &mut self,
        mut batch: OwnedWcolBatch,
        partition: u32,
    ) -> WcolResult<()> {
        if self.stream_ended {
            return Err(WcolError::from("WCS stream already ended"));
        }
        let scope = batch.dict_scope();
        if self.sparse_scope != Some(scope) {
            // Row groups arrive in order, once each: no later batch references this scope.
            self.sparse_ids.clear();
            self.sparse_scope = Some(scope);
        }
        // Frames carry only ids, so the staged batch keeps the scan's dictionary handle.
        let mut chunks = Vec::new();
        batch.remap_dict_ids(|column, ids, nulls, dict| -> WcolResult<()> {
            if dict.scope != scope || dict.column != column {
                return Err(WcolError::from(
                    "dictionary scope does not match the batch column",
                ));
            }
            let local = self.sparse_ids.entry(column).or_default();
            let base = local.len();
            let mut added = Vec::new();
            for (row, id) in ids.iter_mut().enumerate() {
                // A null row's id is never read; local 0 keeps it in range.
                if nulls.get(row).copied().unwrap_or(0) == 0 {
                    continue;
                }
                *id = *local.entry(*id).or_insert_with(|| {
                    added.push(*id);
                    (base + added.len() - 1) as u32
                });
            }
            // The receiver needs this column's dictionary installed even if every row is null.
            if local.is_empty() {
                if let Some(first) = ids.first().copied() {
                    local.insert(first, 0);
                    added.push(first);
                }
            }
            for (row, id) in ids.iter_mut().enumerate() {
                if nulls.get(row).copied().unwrap_or(0) == 0 {
                    *id = 0;
                }
            }
            if !added.is_empty() {
                let chunk = dict.subset(&added).map_err(WcolError::msg)?;
                crate::exec::dict_traffic::note_sent(
                    added.len() as u64,
                    crate::exec::dict_traffic::payload_bytes(&chunk),
                );
                chunks.push((chunk, base));
            }
            Ok(())
        })?;
        for (dictionary, base) in chunks {
            // Fail before queueing if the next entry cannot fit in one frame.
            encode_one_dictionary_frame(&dictionary, 0, MAX_WCS_PAYLOAD)?;
            self.staged.push_back(Staged::Dictionary {
                dictionary,
                next_entry: 0,
                base,
                partition,
            });
        }
        self.staged.push_back(Staged::Rows {
            batch,
            next_row: 0,
            partition,
        });
        self.fill_current()
    }

    pub fn push_end_partition(&mut self, partition: u32) -> WcolResult<()> {
        self.enqueue_end(partition)
    }

    /// Queue a partial state for `partition`, split into frames that each fit one WCS payload.
    pub fn push_partial_partition(
        &mut self,
        state: &crate::stream::partials::PartialStateWcol,
        partition: u32,
    ) -> WcolResult<()> {
        if self.stream_ended {
            return Err(WcolError::from("WCS stream already ended"));
        }
        for payload in crate::stream::partials::partial_state_payloads(state, MAX_WCS_PAYLOAD)
            .map_err(WcolError::msg)?
        {
            self.push_partial_payload(payload, partition)?;
        }
        Ok(())
    }

    /// Queue one encoded `FRAME_PARTIAL` payload (`PWCL`) for `partition`.
    pub fn push_partial_payload(&mut self, payload: Vec<u8>, partition: u32) -> WcolResult<()> {
        if self.stream_ended {
            return Err(WcolError::from("WCS stream already ended"));
        }
        if payload.len() > MAX_WCS_PAYLOAD {
            return Err(WcolError::from("partial payload exceeds one WCS frame"));
        }
        self.staged
            .push_back(Staged::Partial { payload, partition });
        self.fill_current()
    }

    fn enqueue_dictionary(
        &mut self,
        view_scope: u32,
        dictionary: &DictionaryValues,
        column: u32,
        partition: u32,
    ) -> WcolResult<()> {
        if dictionary.scope != view_scope || dictionary.column != column {
            return Err(WcolError::from(
                "dictionary scope does not match the batch column",
            ));
        }
        let key = (dictionary.scope, column);
        let sent = self.sent_dict_entries.get(&key).copied().unwrap_or(0);
        if sent >= dictionary.entry_count() {
            return Ok(());
        }
        crate::exec::dict_traffic::note_sent(
            (dictionary.entry_count() - sent) as u64,
            crate::exec::dict_traffic::payload_bytes_from(dictionary, sent),
        );
        // Fail before queueing if the next entry cannot fit in one frame.
        encode_one_dictionary_frame(dictionary, sent, MAX_WCS_PAYLOAD)?;
        self.staged.push_back(Staged::Dictionary {
            dictionary: dictionary.clone(),
            next_entry: sent,
            base: 0,
            partition,
        });
        self.sent_dict_entries.insert(key, dictionary.entry_count());
        Ok(())
    }

    pub fn push_end(&mut self) -> WcolResult<()> {
        self.enqueue_end(0)
    }

    fn enqueue_end(&mut self, partition: u32) -> WcolResult<()> {
        if self.stream_ended {
            return Err(WcolError::from("WCS stream already ended"));
        }
        self.staged.push_back(Staged::End { partition });
        self.stream_ended = true;
        self.fill_current()?;
        Ok(())
    }

    fn fill_current(&mut self) -> WcolResult<()> {
        if self.current.is_some() {
            return Ok(());
        }
        loop {
            let Some(stage) = self.staged.pop_front() else {
                return Ok(());
            };
            match stage {
                Staged::End { partition } => {
                    self.current = Some(self.encode_end_frame(partition)?);
                    return Ok(());
                }
                Staged::Partial { payload, partition } => {
                    self.current =
                        Some(self.encode_payload_frame(FRAME_PARTIAL, partition, &payload)?);
                    return Ok(());
                }
                Staged::Dictionary {
                    dictionary,
                    next_entry,
                    base,
                    partition,
                } => {
                    let encoded =
                        match encode_one_dictionary_frame(&dictionary, next_entry, MAX_WCS_PAYLOAD)
                        {
                            Ok(encoded) => encoded,
                            Err(err) => {
                                self.staged.push_front(Staged::Dictionary {
                                    dictionary,
                                    next_entry,
                                    base,
                                    partition,
                                });
                                return Err(err);
                            }
                        };
                    let Some((mut payload, next)) = encoded else {
                        continue;
                    };
                    if base > 0 {
                        rebase_dictionary_frame(&mut payload, base)?;
                    }
                    if payload.len() > MAX_WCS_PAYLOAD {
                        return Err(WcolError::msg(format!(
                            "wcs: dictionary_chunk_too_large (encoded={} max={})",
                            payload.len(),
                            MAX_WCS_PAYLOAD
                        )));
                    }
                    if next < dictionary.entry_count() {
                        self.staged.push_front(Staged::Dictionary {
                            dictionary,
                            next_entry: next,
                            base,
                            partition,
                        });
                    }
                    self.current =
                        Some(self.encode_payload_frame(FRAME_DICTIONARY, partition, &payload)?);
                    return Ok(());
                }
                Staged::Rows {
                    batch,
                    next_row,
                    partition,
                } => {
                    if batch.row_count() == 0 && next_row == 0 {
                        let frame = encode_empty_row_frame(&batch, self.next_sequence, partition)?;
                        self.next_sequence = self.next_sequence.wrapping_add(1);
                        self.current = Some(frame);
                        return Ok(());
                    }
                    if next_row >= batch.row_count() {
                        continue;
                    }
                    let (frame, next) =
                        match encode_row_frame(&batch, next_row, self.next_sequence, partition) {
                            Ok(encoded) => encoded,
                            Err(err) => {
                                self.staged.push_front(Staged::Rows {
                                    batch,
                                    next_row,
                                    partition,
                                });
                                return Err(err);
                            }
                        };
                    self.next_sequence = self.next_sequence.wrapping_add(1);
                    if next < batch.row_count() {
                        self.staged.push_front(Staged::Rows {
                            batch,
                            next_row: next,
                            partition,
                        });
                    }
                    self.current = Some(frame);
                    return Ok(());
                }
            }
        }
    }

    /// A whole-payload frame (dictionary or partial) stamped with its partition.
    fn encode_payload_frame(
        &mut self,
        frame_type: u8,
        partition: u32,
        payload: &[u8],
    ) -> WcolResult<Vec<u8>> {
        let mut scratch = vec![0u8; crate::stream::core::WCS_HEADER_BYTES + payload.len()];
        let n = wcs::encode_frame(frame_type, self.next_sequence, payload, &mut scratch)
            .ok_or("WCS frame too large")?;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        scratch.truncate(n);
        scratch[12..16].copy_from_slice(&partition.to_le_bytes());
        Ok(scratch)
    }

    fn encode_end_frame(&mut self, partition: u32) -> WcolResult<Vec<u8>> {
        let mut scratch = vec![0u8; crate::stream::core::WCS_HEADER_BYTES];
        let n = wcs::encode_frame(FRAME_END, self.next_sequence, &[], &mut scratch)
            .ok_or("end frame encode failed")?;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        scratch.truncate(n);
        scratch[12..16].copy_from_slice(&partition.to_le_bytes());
        Ok(scratch)
    }

    pub fn poll_write(&mut self, output: &mut [u8]) -> WcolResult<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        self.fill_current()?;
        let (n, frame_len) = {
            let Some(frame) = self.current.as_ref() else {
                return Ok(0);
            };
            let frame_len = frame.len();
            let n = frame_len.saturating_sub(self.byte_off).min(output.len());
            output[..n].copy_from_slice(&frame[self.byte_off..self.byte_off + n]);
            (n, frame_len)
        };
        self.byte_off += n;
        if self.byte_off == frame_len {
            self.current = None;
            self.byte_off = 0;
        }
        Ok(n)
    }

    pub fn drain_to_vec(&mut self) -> WcolResult<Vec<u8>> {
        let mut out = Vec::new();
        let mut chunk = [0u8; 4096];
        while self.has_pending_output() {
            let n = self.poll_write(&mut chunk)?;
            if n == 0 {
                break;
            }
            out.extend_from_slice(&chunk[..n]);
        }
        Ok(out)
    }
}

fn encode_empty_row_frame(
    batch: &OwnedWcolBatch,
    sequence: u32,
    partition: u32,
) -> WcolResult<Vec<u8>> {
    let mut col_view = Vec::new();
    let view = batch.view_with(&mut col_view);
    let columns = projection_columns_for_range(view, 0, 0)?;
    let body = encode_projection_columns(&columns)?;
    let payload_len = 13 + body.len();
    if payload_len > MAX_WCS_PAYLOAD {
        return Err(WcolError::msg(format!(
            "wcs: row_frame_exceeds_payload (rows=0 encoded={payload_len} max={})",
            MAX_WCS_PAYLOAD
        )));
    }
    let mut scratch = vec![0u8; crate::stream::core::WCS_HEADER_BYTES + payload_len];
    let n = wcs::encode_data_batch_projection(sequence, view.dict_scope, 0, &body, &mut scratch)
        .ok_or("data batch frame too large")?;
    scratch.truncate(n);
    scratch[12..16].copy_from_slice(&partition.to_le_bytes());
    Ok(scratch)
}

fn encode_row_frame(
    batch: &OwnedWcolBatch,
    start: usize,
    sequence: u32,
    partition: u32,
) -> WcolResult<(Vec<u8>, usize)> {
    let mut col_view = Vec::new();
    let view = batch.view_with(&mut col_view);
    let rows = row_span(view, start)?;
    let end = start + rows;
    let columns = projection_columns_for_range(view, start, end)?;
    let body = encode_projection_columns(&columns)?;
    let payload_len = 13 + body.len();
    if payload_len > MAX_WCS_PAYLOAD {
        if rows == 1 {
            return Err(WcolError::fatal(format!(
                "wcs: single_row_too_large (dict_scope={} rows=1 encoded={payload_len} max={})",
                view.dict_scope, MAX_WCS_PAYLOAD
            )));
        }
        return Err(WcolError::msg(format!(
            "wcs: row_frame_exceeds_payload (rows={rows} encoded={payload_len} max={})",
            MAX_WCS_PAYLOAD
        )));
    }
    let mut scratch = vec![0u8; crate::stream::core::WCS_HEADER_BYTES + payload_len];
    let n = wcs::encode_data_batch_projection(sequence, view.dict_scope, 0, &body, &mut scratch)
        .ok_or("data batch frame too large")?;
    scratch.truncate(n);
    scratch[12..16].copy_from_slice(&partition.to_le_bytes());
    Ok((scratch, end))
}

fn row_span(view: crate::stream::WcolBatch<'_>, start: usize) -> WcolResult<usize> {
    let budget = MAX_WCS_PAYLOAD - 13;
    match max_rows_for_body_budget(view, start, budget) {
        Ok(rows) if rows > 0 => Ok(rows),
        Ok(_) => Err(WcolError::from("projection row range produced no rows")),
        Err(err) if err.as_str().contains("single projection row") => {
            Err(WcolError::fatal(format!(
                "wcs: single_row_too_large (dict_scope={} rows=1 max={})",
                view.dict_scope, MAX_WCS_PAYLOAD
            )))
        }
        Err(err) => Err(err),
    }
}

/// Split a mixed producer byte stream into one WCS stream per partition.
/// Header bytes 12..16 are the partition. Each output keeps that producer's frame order.
pub fn demux_partition_stream(bytes: &[u8], partitions: u32) -> Result<Vec<Vec<u8>>, String> {
    use crate::stream::core::{WCS_HEADER_BYTES, WCS_MAGIC};
    let mut out = vec![Vec::new(); partitions as usize];
    let mut at = 0;
    while at < bytes.len() {
        if bytes.len() - at < WCS_HEADER_BYTES {
            return Err("truncated WCS header in a hash exchange".into());
        }
        let header = &bytes[at..at + WCS_HEADER_BYTES];
        if u32::from_le_bytes(header[..4].try_into().unwrap()) != WCS_MAGIC {
            return Err("hash exchange frame has a bad WCS magic".into());
        }
        let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let partition = u32::from_le_bytes(header[12..16].try_into().unwrap());
        let end = at + WCS_HEADER_BYTES + len;
        if end > bytes.len() {
            return Err("truncated WCS payload in a hash exchange".into());
        }
        if partition >= partitions {
            return Err(format!(
                "hash exchange partition {partition} is outside 0..{partitions}"
            ));
        }
        out[partition as usize].extend_from_slice(&bytes[at..end]);
        at = end;
    }
    Ok(out)
}

/// Decode one WCS1 row-batch stream.
pub struct RowBatchWcsIngress {
    ingress: WcsIngress,
    stream_complete: bool,
    pub rows_received: u64,
    pub batches_received: u64,
    batches: Vec<OwnedWcolBatch>,
    dicts: HashMap<(u32, u32), DictionaryValues>,
}

impl Default for RowBatchWcsIngress {
    fn default() -> Self {
        Self::new()
    }
}

impl RowBatchWcsIngress {
    pub fn new() -> Self {
        Self {
            ingress: WcsIngress::new(),
            stream_complete: false,
            rows_received: 0,
            batches_received: 0,
            batches: Vec::new(),
            dicts: HashMap::new(),
        }
    }

    pub fn stream_complete(&self) -> bool {
        self.stream_complete
    }

    pub fn take_batches(&mut self) -> Vec<OwnedWcolBatch> {
        std::mem::take(&mut self.batches)
    }

    pub fn feed(&mut self, input: &[u8]) -> WcolResult<usize> {
        if self.stream_complete {
            return Err(WcolError::from("WCS stream already complete"));
        }
        let (consumed, frames) = self.ingress.feed(input)?;
        for frame in frames {
            self.handle_frame(frame)?;
        }
        Ok(consumed)
    }

    pub fn finish_feed(&mut self) -> WcolResult<()> {
        if !self.stream_complete {
            return Err(WcolError::from("WCS stream ended without END frame"));
        }
        Ok(())
    }

    fn handle_frame(&mut self, frame: CompleteFrame) -> WcolResult<()> {
        match frame.frame_type {
            FRAME_DICTIONARY => {
                apply_dictionary_payload(&mut self.dicts, &frame.payload)?;
            }
            FRAME_DATA_BATCH => {
                let batch =
                    owned_batch_from_projection_body_with_dicts(&frame.payload, &self.dicts)?;
                self.rows_received += batch.row_count() as u64;
                self.batches_received += 1;
                self.batches.push(batch);
            }
            FRAME_END => {
                self.stream_complete = true;
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
