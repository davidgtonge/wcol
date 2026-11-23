//! Streaming page-copy Parquet compaction (opcode 11 sink).

use compact_parquet_thrift::encode::{
    write_footer, write_magic, ChunkPlace, ChunkStats, Column, ColumnValues, EncodeError,
    RowGroupFooter,
};

use crate::exec::operator::{Operator, OperatorIo, StreamProgress};
use crate::program::stream_program::PlanCursor;
use crate::program::{CopySegment, MergedColumnPlan, MergedRowGroupPlan};

const DEFAULT_FOOTER_BUDGET: usize = 8 * 1024 * 1024;
const FOOTER_BUDGET: usize = DEFAULT_FOOTER_BUDGET;
const MAX_OUTSTANDING_READS: usize = 2;

#[derive(Clone)]
struct SchemaCol {
    name: String,
    codec: i32,
    max_def: u8,
}

struct CopyJob {
    source: u16,
    offset: u64,
    length: u32,
    /// Bytes asked of the host; reads after the first may still be in flight.
    requested: u32,
}

enum Phase {
    NeedMagic,
    CopyColumn { rg: usize, col: usize, seg: usize },
    Footer,
    Done,
}

/// One host read, in request order. Reads can complete out of order; their bytes wait here
/// until every earlier read has been written, so the output stays in copy order.
struct OutstandingRead {
    ticket: u32,
    length: u32,
    bytes: Option<Vec<u8>>,
}

/// Where the output footer comes from.
enum FooterSource {
    /// Built from the copied row groups (WCOL's own uncompressed, dictionary-free shards).
    Build,
    /// Planner-supplied `FileMetaData` thrift, eg. a source footer rewritten for a row-group
    /// split, which keeps every codec, encoding and statistic of the copied chunks.
    Verbatim(Vec<u8>),
}

/// Wire tag after the row groups: `0` builds the footer, `1` is followed by `u32 len, bytes`.
pub const FOOTER_BUILD: u8 = 0;
pub const FOOTER_VERBATIM: u8 = 1;

struct StoredRowGroupMeta {
    num_rows: i64,
    places: Vec<ChunkPlace>,
}

pub struct PageCompact {
    schema: Vec<SchemaCol>,
    row_groups: Vec<MergedRowGroupPlan>,
    jobs: Vec<CopyJob>,
    footer: FooterSource,
    footer_groups: Vec<StoredRowGroupMeta>,
    phase: Phase,
    file_offset: u64,
    pending: Vec<u8>,
    footer_buf: Vec<u8>,
    outstanding: Vec<OutstandingRead>,
    next_ticket: u32,
    bytes_copied: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageCompactStats {
    pub bytes_copied: u64,
    pub output_row_groups: u32,
    pub output_rows: u64,
}

impl PageCompact {
    pub fn open(cur: &mut PlanCursor<'_>) -> Result<Self, String> {
        let input_count = cur.u32()? as usize;
        if input_count == 0 || input_count > 256 {
            return Err("invalid compaction input count".to_string());
        }
        let catalog = cur.catalog()?;
        let schema = catalog
            .iter()
            .map(|col| SchemaCol {
                name: col.name.clone(),
                codec: 0,
                max_def: col.max_def,
            })
            .collect::<Vec<_>>();
        let rg_count = cur.u32()? as usize;
        let mut row_groups = Vec::with_capacity(rg_count);
        let mut jobs = Vec::new();
        for _ in 0..rg_count {
            let num_rows = cur.i64()?;
            let mut columns = Vec::with_capacity(schema.len());
            for leaf in 0..schema.len() {
                let num_values = cur.i64()?;
                let uncompressed_size = cur.u64()?;
                let compressed_size = cur.u64()?;
                let seg_count = cur.u32()? as usize;
                let mut segments = Vec::with_capacity(seg_count);
                for _ in 0..seg_count {
                    let input_index = cur.u16()?;
                    if input_index as usize >= input_count {
                        return Err("segment input index out of range".to_string());
                    }
                    let offset = cur.u64()?;
                    let length = cur.u32()?;
                    segments.push(CopySegment {
                        input_index,
                        offset,
                        length,
                    });
                }
                columns.push(MergedColumnPlan {
                    leaf: leaf as u32,
                    num_values,
                    uncompressed_size,
                    compressed_size,
                    segments,
                });
            }
            row_groups.push(MergedRowGroupPlan { num_rows, columns });
        }
        for (rg_index, group) in row_groups.iter().enumerate() {
            for (col_index, column) in group.columns.iter().enumerate() {
                let mut sum = 0u64;
                for segment in &column.segments {
                    sum = sum
                        .checked_add(segment.length as u64)
                        .ok_or_else(|| "segment size overflow".to_string())?;
                }
                if sum != column.compressed_size {
                    return Err(format!(
                        "row group {rg_index} column {col_index} segment sizes disagree with metadata"
                    ));
                }
            }
        }
        build_copy_jobs(&row_groups, &mut jobs);
        let footer = match cur.u8()? {
            FOOTER_BUILD => FooterSource::Build,
            FOOTER_VERBATIM => {
                let len = cur.u32()? as usize;
                FooterSource::Verbatim(cur.bytes(len)?.to_vec())
            }
            tag => return Err(format!("unknown page-copy footer tag {tag}")),
        };
        let footer_buf = match footer {
            FooterSource::Build => vec![0u8; FOOTER_BUDGET],
            FooterSource::Verbatim(_) => Vec::new(),
        };
        Ok(Self {
            schema,
            row_groups,
            jobs,
            footer,
            footer_groups: Vec::new(),
            phase: Phase::NeedMagic,
            file_offset: 0,
            pending: Vec::new(),
            footer_buf,
            outstanding: Vec::new(),
            next_ticket: 1,
            bytes_copied: 0,
        })
    }

    pub fn stats(&self) -> PageCompactStats {
        let output_rows = self
            .row_groups
            .iter()
            .fold(0u64, |acc, rg| acc.saturating_add(rg.num_rows as u64));
        PageCompactStats {
            bytes_copied: self.bytes_copied,
            output_row_groups: self.row_groups.len() as u32,
            output_rows,
        }
    }

    fn encode_err(err: EncodeError) -> String {
        format!("parquet compact encode: {err:?}")
    }

    fn queue_bytes(&mut self, chunk: &[u8]) -> Result<(), String> {
        self.pending.extend_from_slice(chunk);
        self.file_offset = self
            .file_offset
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| "file size overflow".to_string())?;
        Ok(())
    }

    fn flush_pending_port(
        &mut self,
        io: &mut OperatorIo<'_, '_>,
    ) -> Result<StreamProgress, String> {
        let Some(port) = io.output_port.as_mut() else {
            return Err("page compact requires output port".to_string());
        };
        if self.pending.is_empty() {
            return Ok(StreamProgress::NeedInput);
        }
        let n = self.pending.len().min(port.len());
        port[..n].copy_from_slice(&self.pending[..n]);
        self.pending.drain(..n);
        *io.bytes_written += n;
        Ok(StreamProgress::HaveOutput)
    }

    fn launch_read(&mut self, job_index: usize) -> StreamProgress {
        let job = &mut self.jobs[job_index];
        let remaining = job.length.saturating_sub(job.requested);
        if remaining == 0 {
            return StreamProgress::NeedInput;
        }
        let len = remaining.min(256 * 1024);
        let offset = job.offset.saturating_add(job.requested as u64);
        job.requested += len;
        let source = job.source;
        let ticket = self.next_ticket;
        self.next_ticket = self.next_ticket.wrapping_add(1);
        self.outstanding.push(OutstandingRead {
            ticket,
            length: len,
            bytes: None,
        });
        StreamProgress::NeedHostRead {
            ticket,
            source,
            offset,
            length: len,
        }
    }

    /// Reads asked of the host and not yet written out: the sink is waiting, not stalled.
    pub fn reads_in_flight(&self) -> bool {
        !self.outstanding.is_empty()
    }

    pub fn complete_host_read(&mut self, ticket: u32, bytes: &[u8]) -> Result<(), String> {
        let read = self
            .outstanding
            .iter_mut()
            .find(|read| read.ticket == ticket)
            .ok_or_else(|| "unknown read ticket".to_string())?;
        if bytes.len() != read.length as usize {
            return Err(format!(
                "read returned {} bytes, asked for {}",
                bytes.len(),
                read.length
            ));
        }
        read.bytes = Some(bytes.to_vec());
        while self
            .outstanding
            .first()
            .is_some_and(|read| read.bytes.is_some())
        {
            let done = self.outstanding.remove(0).bytes.unwrap_or_default();
            self.bytes_copied = self.bytes_copied.saturating_add(done.len() as u64);
            self.queue_bytes(&done)?;
        }
        Ok(())
    }

    pub fn cancel_host_read(&mut self, ticket: u32) -> Result<(), String> {
        if let Some(index) = self
            .outstanding
            .iter()
            .position(|read| read.ticket == ticket)
        {
            self.outstanding.remove(index);
            Ok(())
        } else {
            Err("unknown read ticket".to_string())
        }
    }

    fn prepare_output_layout(&mut self) -> Result<(), String> {
        let mut buf = [0u8; 4];
        let n = write_magic(&mut buf).map_err(Self::encode_err)?;
        self.queue_bytes(&buf[..n])?;
        let mut at = self.file_offset;
        self.footer_groups.clear();
        for group in &self.row_groups {
            let mut places = Vec::with_capacity(group.columns.len());
            for column in &group.columns {
                let data_page_offset = at;
                at = at
                    .checked_add(column.compressed_size)
                    .ok_or_else(|| "chunk offset overflow".to_string())?;
                places.push(ChunkPlace {
                    offset: data_page_offset,
                    data_page_offset,
                    dictionary_page_offset: None,
                    compressed_size: column.compressed_size,
                    uncompressed_size: column.uncompressed_size,
                    // A merged chunk spans several sources and the plan carries no
                    // statistics, so the built footer claims none.
                    stats: ChunkStats::default(),
                });
            }
            self.footer_groups.push(StoredRowGroupMeta {
                num_rows: group.num_rows,
                places,
            });
        }
        Ok(())
    }

    fn build_footer_metadata(&mut self) -> Result<(), String> {
        self.prepare_output_layout()
    }

    fn write_footer_once(&mut self) -> Result<(), String> {
        if let FooterSource::Verbatim(thrift) = &self.footer {
            let len = u32::try_from(thrift.len()).map_err(|_| "footer too large".to_string())?;
            let mut trailer = std::mem::take(&mut self.footer_buf);
            trailer.clear();
            trailer.extend_from_slice(thrift);
            trailer.extend_from_slice(&len.to_le_bytes());
            trailer.extend_from_slice(b"PAR1");
            self.queue_bytes(&trailer)?;
            self.phase = Phase::Done;
            return Ok(());
        }
        let mut per_rg_cols: Vec<Vec<Column<'_>>> = Vec::with_capacity(self.footer_groups.len());
        for group in &self.footer_groups {
            let rows =
                usize::try_from(group.num_rows).map_err(|_| "row count overflow".to_string())?;
            per_rg_cols.push(
                self.schema
                    .iter()
                    .map(|col| Column {
                        name: col.name.as_str(),
                        rows,
                        codec: col.codec,
                        present: if col.max_def > 0 {
                            Some(&[] as &[u8])
                        } else {
                            None
                        },
                        values: ColumnValues::I64(&[]),
                    })
                    .collect(),
            );
        }
        let footers: Vec<RowGroupFooter<'_>> = self
            .footer_groups
            .iter()
            .zip(per_rg_cols.iter())
            .map(|(group, cols)| RowGroupFooter {
                num_rows: group.num_rows,
                columns: cols.as_slice(),
                places: &group.places,
            })
            .collect();
        let schema_template = per_rg_cols.first().map(|v| v.as_slice()).unwrap_or(&[]);
        let footer_len = loop {
            match write_footer(&mut self.footer_buf, schema_template, &footers) {
                Ok(n) => break n,
                Err(EncodeError::OutputFull) if self.footer_buf.len() < FOOTER_BUDGET => {
                    self.footer_buf
                        .resize(self.footer_buf.len().saturating_mul(2).max(4096), 0);
                }
                Err(err) => return Err(Self::encode_err(err)),
            }
        };
        let chunk = self.footer_buf[..footer_len].to_vec();
        self.queue_bytes(&chunk)?;
        self.phase = Phase::Done;
        Ok(())
    }
}

fn build_copy_jobs(groups: &[MergedRowGroupPlan], jobs: &mut Vec<CopyJob>) {
    for group in groups {
        for column in &group.columns {
            for segment in &column.segments {
                jobs.push(CopyJob {
                    source: segment.input_index,
                    offset: segment.offset,
                    length: segment.length,
                    requested: 0,
                });
            }
        }
    }
}

impl Operator for PageCompact {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn poll(&mut self, io: &mut OperatorIo<'_, '_>) -> Result<StreamProgress, String> {
        if matches!(self.phase, Phase::Done) {
            if !self.pending.is_empty() {
                return self.flush_pending_port(io);
            }
            return Ok(StreamProgress::Finished);
        }
        if !self.pending.is_empty() {
            let progress = self.flush_pending_port(io)?;
            if progress != StreamProgress::NeedInput {
                return Ok(progress);
            }
        }
        if self.outstanding.len() >= MAX_OUTSTANDING_READS {
            return Ok(StreamProgress::Blocked);
        }
        loop {
            match self.phase {
                Phase::NeedMagic => {
                    self.build_footer_metadata()?;
                    self.phase = Phase::CopyColumn {
                        rg: 0,
                        col: 0,
                        seg: 0,
                    };
                    if !self.pending.is_empty() {
                        return self.flush_pending_port(io);
                    }
                }
                Phase::CopyColumn { rg, col, seg } => {
                    let job_index = copy_job_index(&self.row_groups, rg, col, seg);
                    if job_index >= self.jobs.len() {
                        self.phase = Phase::Footer;
                        continue;
                    }
                    let job = &self.jobs[job_index];
                    if job.requested < job.length {
                        return Ok(self.launch_read(job_index));
                    }
                    let (next_rg, next_col, next_seg) =
                        advance_copy(rg, col, seg, &self.row_groups);
                    self.phase = Phase::CopyColumn {
                        rg: next_rg,
                        col: next_col,
                        seg: next_seg,
                    };
                }
                Phase::Footer => {
                    // The last chunks may still be in flight; the footer follows their bytes.
                    if self.reads_in_flight() {
                        return Ok(StreamProgress::Blocked);
                    }
                    self.write_footer_once()?;
                    if !self.pending.is_empty() {
                        return self.flush_pending_port(io);
                    }
                    return Ok(StreamProgress::Finished);
                }
                Phase::Done => return Ok(StreamProgress::Finished),
            }
        }
    }
}

fn copy_job_index(groups: &[MergedRowGroupPlan], rg: usize, col: usize, seg: usize) -> usize {
    let mut index = 0usize;
    for (rg_i, group) in groups.iter().enumerate() {
        for (col_i, column) in group.columns.iter().enumerate() {
            for (seg_i, _) in column.segments.iter().enumerate() {
                if rg_i == rg && col_i == col && seg_i == seg {
                    return index;
                }
                index += 1;
            }
        }
    }
    index
}

fn advance_copy(
    rg: usize,
    col: usize,
    seg: usize,
    groups: &[MergedRowGroupPlan],
) -> (usize, usize, usize) {
    let group = &groups[rg];
    if seg + 1 < group.columns[col].segments.len() {
        return (rg, col, seg + 1);
    }
    if col + 1 < group.columns.len() {
        return (rg, col + 1, 0);
    }
    if rg + 1 < groups.len() {
        return (rg + 1, 0, 0);
    }
    (groups.len(), 0, 0)
}

pub fn open_page_compact_graph(
    cur: &mut PlanCursor<'_>,
) -> Result<crate::exec::graph_scheduler::LinearColocatedGraph, String> {
    let compact = PageCompact::open(cur)?;
    let mut graph = crate::exec::graph_scheduler::LinearColocatedGraph::from_plan_parts(
        None,
        vec![],
        vec![],
        true,
    );
    graph.attach_sink(Box::new(compact));
    graph.mark_sink_eof();
    Ok(graph)
}
