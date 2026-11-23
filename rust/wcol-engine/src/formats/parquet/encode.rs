//! WCOL row-batch stream → incremental Parquet file bytes (format sink only).

use compact_parquet_thrift::encode::{
    scratch_len, write_footer, write_magic, write_row_group, ChunkPlace, Column, ColumnValues,
    EncodeError, RowGroupFooter, UNCOMPRESSED,
};

use crate::exec::operator::{Operator, OperatorIo, StreamProgress};
use crate::stream::OwnedWcolBatch;
use crate::stream::WcolColumn;

const FOOTER_BUDGET: usize = 64 * 1024;

#[derive(Clone)]
struct SchemaCol {
    name: String,
    optional: bool,
    physical: i32,
    type_length: i32,
    date: bool,
}

/// One output column of a typed Parquet sink. `physical` is the Parquet physical type
/// (0 BOOLEAN, 1 INT32, 2 INT64, 4 FLOAT, 5 DOUBLE, 6 BYTE_ARRAY, 7 FIXED_LEN_BYTE_ARRAY).
/// `date` writes converted type DATE on an INT32 column.
#[derive(Clone, Debug)]
pub struct ParquetSinkColumn {
    pub name: String,
    pub optional: bool,
    pub physical: i32,
    pub type_length: i32,
    pub date: bool,
}

struct ColAccum {
    optional: bool,
    values: Vec<i64>,
    /// WCOL null bytes: 1 = present, 0 = null (only when optional).
    nulls: Vec<u8>,
}

/// Value bytes a typed row group may hold before it is written, whatever its row count.
/// Writing one holds its values, page scratch and output at once, about 4-5x the value
/// bytes: 65,536 Title strings took a reducer's shard export from 46 MiB to 105 MiB of Wasm
/// memory, an 8 MiB cap to 82 MiB, and this cap to 54 MiB (its ingest peak).
const MAX_ROW_GROUP_VALUE_BYTES: usize = 2 << 20;

struct RowGroupAccum {
    rows: usize,
    /// Bytes of the byte-array values held, with their per-value overhead.
    value_bytes: usize,
    cols: Vec<ColAccum>,
}

struct CompletedRowGroup {
    num_rows: i64,
    places: Vec<ChunkPlace>,
    /// Per column: the row group was written with a dictionary page.
    dictionary: Vec<bool>,
}

enum CopyState {
    None,
    Active { cursor: usize },
}

enum Phase {
    NeedMagic,
    Accepting,
    Done,
}

/// Streaming Parquet sink: consumes a sequence of row [`OwnedWcolBatch`] values.
pub struct ParquetEncode {
    schema: Vec<SchemaCol>,
    max_row_group_rows: usize,
    max_row_groups: usize,
    codec: i32,
    phase: Phase,
    group: RowGroupAccum,
    completed: Vec<CompletedRowGroup>,
    file_offset: u64,
    page_scratch: Vec<u8>,
    snappy_table: Vec<u32>,
    footer_buf: Vec<u8>,
    pending: Vec<u8>,
    copy: CopyState,
    input_eof: bool,
    schema_checked: bool,
    /// `open` widens every numeric column to INT64. `open_typed` keeps source types.
    widen_i64: bool,
    typed: Vec<TypedAccum>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetEncodeConfig {
    pub max_row_group_rows: usize,
    pub max_row_groups: usize,
}

impl Default for ParquetEncodeConfig {
    fn default() -> Self {
        Self {
            max_row_group_rows: 8192,
            max_row_groups: 256,
        }
    }
}

impl ParquetEncodeConfig {
    /// Full-file row sink: larger groups, footer grows up to 1 MiB.
    /// Numeric values are written as physical INT64 (explicit widening).
    pub const FULL_FILE: Self = Self {
        max_row_group_rows: 65_536,
        max_row_groups: 8_192,
    };
}

enum TypedAccum {
    Unset,
    Bool {
        values: Vec<bool>,
        nulls: Vec<u8>,
    },
    I32 {
        values: Vec<i32>,
        nulls: Vec<u8>,
    },
    I64 {
        values: Vec<i64>,
        nulls: Vec<u8>,
    },
    F32 {
        values: Vec<f32>,
        nulls: Vec<u8>,
    },
    F64 {
        values: Vec<f64>,
        nulls: Vec<u8>,
    },
    Bytes {
        values: Vec<Vec<u8>>,
        nulls: Vec<u8>,
    },
    Fixed {
        values: Vec<Vec<u8>>,
        nulls: Vec<u8>,
    },
    DictI32 {
        dict: Vec<i32>,
        ids: Vec<u32>,
        nulls: Vec<u8>,
    },
    DictI64 {
        dict: Vec<i64>,
        ids: Vec<u32>,
        nulls: Vec<u8>,
    },
    DictF32 {
        dict: Vec<f32>,
        ids: Vec<u32>,
        nulls: Vec<u8>,
    },
    DictF64 {
        dict: Vec<f64>,
        ids: Vec<u32>,
        nulls: Vec<u8>,
    },
    DictBytes {
        dict: Vec<Vec<u8>>,
        ids: Vec<u32>,
        nulls: Vec<u8>,
    },
    DictFixed {
        dict: Vec<Vec<u8>>,
        ids: Vec<u32>,
        nulls: Vec<u8>,
    },
}

impl ParquetEncode {
    pub fn open(
        column_names: Vec<String>,
        optional: Vec<bool>,
        config: ParquetEncodeConfig,
    ) -> Result<Self, String> {
        if column_names.is_empty() {
            return Err("parquet encode requires at least one column".to_string());
        }
        if column_names.len() != optional.len() {
            return Err("column_names and optional length mismatch".to_string());
        }
        if config.max_row_group_rows == 0 {
            return Err("max_row_group_rows must be positive".to_string());
        }
        if config.max_row_groups == 0 {
            return Err("max_row_groups must be positive".to_string());
        }
        let schema: Vec<SchemaCol> = column_names
            .into_iter()
            .zip(optional)
            .map(|(name, optional)| SchemaCol {
                name,
                optional,
                physical: 2,
                type_length: 0,
                date: false,
            })
            .collect();
        let cols = schema
            .iter()
            .map(|s| ColAccum {
                optional: s.optional,
                values: Vec::new(),
                nulls: Vec::new(),
            })
            .collect();
        Ok(Self {
            schema,
            max_row_group_rows: config.max_row_group_rows,
            max_row_groups: config.max_row_groups,
            codec: UNCOMPRESSED,
            phase: Phase::NeedMagic,
            group: RowGroupAccum {
                rows: 0,
                value_bytes: 0,
                cols,
            },
            completed: Vec::new(),
            file_offset: 0,
            // Sized by the first row group that needs it: a typed sink of short rows never
            // needs the full-group worst case, and a shard export opens before its first row.
            page_scratch: Vec::new(),
            snappy_table: Vec::new(),
            footer_buf: vec![0; FOOTER_BUDGET],
            pending: Vec::new(),
            copy: CopyState::None,
            input_eof: false,
            schema_checked: false,
            widen_i64: true,
            typed: Vec::new(),
        })
    }

    pub fn open_typed(
        columns: Vec<ParquetSinkColumn>,
        config: ParquetEncodeConfig,
    ) -> Result<Self, String> {
        if columns.is_empty() {
            return Err("parquet encode requires at least one column".to_string());
        }
        if config.max_row_group_rows == 0 || config.max_row_groups == 0 {
            return Err("row group limits must be positive".to_string());
        }
        for column in &columns {
            if !matches!(column.physical, 0 | 1 | 2 | 4 | 5 | 6 | 7) {
                return Err(format!(
                    "unsupported parquet physical type {}",
                    column.physical
                ));
            }
            if column.physical == 7 && column.type_length <= 0 {
                return Err("FIXED_LEN_BYTE_ARRAY needs a positive type_length".to_string());
            }
            if column.date && column.physical != 1 {
                return Err("DATE columns are INT32".to_string());
            }
        }
        let mut enc = Self::open(
            columns.iter().map(|col| col.name.clone()).collect(),
            columns.iter().map(|col| col.optional).collect(),
            config,
        )?;
        for (schema, column) in enc.schema.iter_mut().zip(columns) {
            schema.physical = column.physical;
            schema.type_length = column.type_length;
            schema.date = column.date;
        }
        enc.widen_i64 = false;
        enc.typed = (0..enc.schema.len()).map(|_| TypedAccum::Unset).collect();
        Ok(enc)
    }

    pub fn max_row_group_rows(&self) -> usize {
        self.max_row_group_rows
    }

    pub fn max_row_groups(&self) -> usize {
        self.max_row_groups
    }

    fn encode_err(err: EncodeError) -> String {
        format!("parquet encode: {err:?}")
    }

    fn flush_pending_port(
        &mut self,
        io: &mut OperatorIo<'_, '_>,
    ) -> Result<StreamProgress, String> {
        *io.bytes_written = 0;
        if self.pending.is_empty() {
            return Ok(StreamProgress::NeedInput);
        }
        let Some(port) = io.output_port.as_mut() else {
            return Ok(StreamProgress::Blocked);
        };
        if port.is_empty() {
            return Ok(StreamProgress::Blocked);
        }
        let cursor = match self.copy {
            CopyState::Active { cursor } => cursor,
            CopyState::None => 0,
        };
        let rest = &self.pending[cursor..];
        let n = rest.len().min(port.len());
        port[..n].copy_from_slice(&rest[..n]);
        *io.bytes_written = n;
        let next = cursor + n;
        if next >= self.pending.len() {
            self.pending.clear();
            self.copy = CopyState::None;
        } else {
            self.copy = CopyState::Active { cursor: next };
        }
        Ok(StreamProgress::HaveOutput)
    }

    fn queue_bytes(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
        self.mark_queued();
    }

    /// Start sending queued bytes, keeping the cursor of any not yet sent.
    fn mark_queued(&mut self) {
        if matches!(self.copy, CopyState::None) {
            self.copy = CopyState::Active { cursor: 0 };
        }
    }

    fn write_magic_once(&mut self) -> Result<(), String> {
        let mut buf = [0u8; 4];
        let n = write_magic(&mut buf).map_err(Self::encode_err)?;
        debug_assert_eq!(n, 4);
        self.queue_bytes(&buf[..n]);
        self.file_offset = n as u64;
        self.phase = Phase::Accepting;
        Ok(())
    }

    fn validate_batch(&self, batch: &OwnedWcolBatch) -> Result<(), String> {
        if !batch.is_row_batch() {
            return Err("parquet encode expects row batches".to_string());
        }
        if batch.row_count() == 0 {
            return Ok(());
        }
        let mut col_view = Vec::new();
        let view = batch.view_with(&mut col_view);
        if view.columns.len() != self.schema.len() {
            return Err(format!(
                "batch column count {} != schema {}",
                view.columns.len(),
                self.schema.len()
            ));
        }
        if self.widen_i64 {
            for col in view.columns.iter() {
                match col {
                    WcolColumn::I64 { .. } | WcolColumn::DictId { .. } => {}
                    WcolColumn::F64 { .. } => {
                        return Err(
                            "float columns cannot be encoded to the int64 parquet sink".to_string()
                        );
                    }
                    WcolColumn::Utf8 { .. } => {
                        return Err(
                            "utf8 columns cannot be encoded to parquet int64 sink".to_string()
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// One selected row as INT64. Dictionary ids are resolved here; the file stores INT64.
    fn row_i64(col: &WcolColumn<'_>, row: usize) -> Result<(u8, i64), String> {
        match col {
            WcolColumn::I64 { values, nulls, .. } => {
                let present = nulls.get(row).copied().unwrap_or(1);
                let value = values.get(row).copied().unwrap_or(0);
                Ok((present, value))
            }
            WcolColumn::DictId {
                ids,
                nulls,
                dictionary,
                ..
            } => {
                let present = nulls.get(row).copied().unwrap_or(1);
                if present == 0 {
                    return Ok((0, 0));
                }
                let id = *ids.get(row).ok_or("dictionary id row missing")?;
                let value = dictionary
                    .int_entries()
                    .ok_or("dictionary is not numeric")?
                    .get(id as usize)
                    .copied()
                    .ok_or("dictionary index out of range")?;
                Ok((present, value))
            }
            WcolColumn::F64 { .. } => Err("float column cannot be encoded as i64".to_string()),
            WcolColumn::Utf8 { .. } => Err("utf8 column cannot be encoded as i64".to_string()),
        }
    }

    fn append_batch(&mut self, batch: &OwnedWcolBatch) -> Result<(), String> {
        if !self.widen_i64 {
            return self.append_typed(batch);
        }
        if batch.row_count() == 0 {
            return Ok(());
        }
        let mask = batch.effective_selection_mask();
        let mut col_view = Vec::new();
        let view = batch.view_with(&mut col_view);
        let rows = view.row_count;
        for row in 0..rows {
            if !mask.is_selected(row) {
                continue;
            }
            for (idx, col) in view.columns.iter().enumerate() {
                let (present, value) = Self::row_i64(col, row)?;
                let acc = &mut self.group.cols[idx];
                if acc.optional {
                    acc.nulls.push(present);
                    acc.values.push(value);
                } else if present == 0 {
                    return Err("null in required INT64 column".to_string());
                } else {
                    acc.values.push(value);
                }
            }
            self.group.rows += 1;
            if self.group.rows >= self.max_row_group_rows {
                self.flush_row_group()?;
            }
        }
        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<(), String> {
        if !self.widen_i64 {
            return self.flush_typed();
        }
        if self.group.rows == 0 {
            return Ok(());
        }
        if self.completed.len() >= self.max_row_groups {
            return Err("max_row_groups exceeded".to_string());
        }
        let rows = self.group.rows;
        let num_rows = i64::try_from(rows).map_err(|_| "row count overflow")?;
        struct EncCol {
            present: Option<Vec<u8>>,
            values: Vec<i64>,
        }
        let owned_cols: Vec<EncCol> = self
            .group
            .cols
            .iter()
            .map(|col| {
                if !col.optional {
                    return EncCol {
                        present: None,
                        values: col.values.clone(),
                    };
                }
                let mut present = vec![0u8; rows.div_ceil(8)];
                let mut values = Vec::new();
                for row in 0..rows {
                    if col.nulls[row] != 0 {
                        present[row >> 3] |= 1 << (row & 7);
                        values.push(col.values[row]);
                    }
                }
                EncCol {
                    present: Some(present),
                    values,
                }
            })
            .collect();
        let names: Vec<String> = self.schema.iter().map(|s| s.name.clone()).collect();
        let codec = self.codec;
        let encode_cols: Vec<Column<'_>> = names
            .iter()
            .zip(&owned_cols)
            .map(|(name, enc)| Column {
                name: name.as_str(),
                rows,
                codec,
                present: enc.present.as_deref(),
                values: ColumnValues::I64(enc.values.as_slice()),
            })
            .collect();
        self.write_encoded_group(num_rows, &encode_cols, vec![false; names.len()])?;
        for col in &mut self.group.cols {
            col.values.clear();
            col.nulls.clear();
        }
        self.group.rows = 0;
        self.group.value_bytes = 0;
        Ok(())
    }

    fn write_footer_once(&mut self) -> Result<(), String> {
        self.flush_row_group()?;
        if !self.widen_i64 {
            return self.write_typed_footer();
        }
        let empty: &[i64] = &[];
        let schema_cols: Vec<Column<'_>> = self
            .schema
            .iter()
            .map(|schema| Column {
                name: schema.name.as_str(),
                rows: 0,
                codec: self.codec,
                present: if schema.optional { Some(&[]) } else { None },
                values: ColumnValues::I64(empty),
            })
            .collect();
        let mut group_col_storage = Vec::with_capacity(self.completed.len());
        for done in &self.completed {
            let cols: Vec<Column<'_>> = self
                .schema
                .iter()
                .map(|schema| Column {
                    name: schema.name.as_str(),
                    rows: done.num_rows as usize,
                    codec: self.codec,
                    present: if schema.optional { Some(&[]) } else { None },
                    values: ColumnValues::I64(empty),
                })
                .collect();
            group_col_storage.push(cols);
        }
        let footers: Vec<RowGroupFooter<'_>> = self
            .completed
            .iter()
            .zip(&group_col_storage)
            .map(|(done, cols)| RowGroupFooter {
                num_rows: done.num_rows,
                columns: cols,
                places: &done.places,
            })
            .collect();
        let footer_len = loop {
            match write_footer(&mut self.footer_buf, &schema_cols, &footers) {
                Ok(n) => break n,
                Err(EncodeError::OutputFull) if self.footer_buf.len() < 1024 * 1024 => {
                    let next = self
                        .footer_buf
                        .len()
                        .saturating_mul(2)
                        .max(self.footer_buf.len() + 4096);
                    self.footer_buf.resize(next.min(1024 * 1024), 0);
                }
                Err(err) => return Err(Self::encode_err(err)),
            }
        };
        let chunk = self.footer_buf[..footer_len].to_vec();
        self.queue_bytes(&chunk);
        self.file_offset = self
            .file_offset
            .checked_add(footer_len as u64)
            .ok_or("file size overflow")?;
        self.phase = Phase::Done;
        Ok(())
    }
}

impl ParquetEncode {
    fn append_typed(&mut self, batch: &OwnedWcolBatch) -> Result<(), String> {
        if batch.row_count() == 0 {
            return Ok(());
        }
        self.prepare_typed_batch(batch)?;
        let mask = batch.effective_selection_mask();
        let mut col_view = Vec::new();
        let view = batch.view_with(&mut col_view);
        if view.columns.len() != self.schema.len() {
            return Err("batch column count does not match the parquet schema".to_string());
        }
        for row in 0..view.row_count {
            if !mask.is_selected(row) {
                continue;
            }
            for (idx, col) in view.columns.iter().enumerate() {
                self.group.value_bytes +=
                    push_typed_cell(&mut self.typed[idx], &self.schema[idx], col, row)?;
            }
            self.group.rows += 1;
            if self.group.rows >= self.max_row_group_rows
                || self.group.value_bytes >= MAX_ROW_GROUP_VALUE_BYTES
            {
                self.flush_typed()?;
            }
        }
        Ok(())
    }

    fn prepare_typed_batch(&mut self, batch: &OwnedWcolBatch) -> Result<(), String> {
        let mut col_view = Vec::new();
        let view = batch.view_with(&mut col_view);
        if view.columns.len() != self.typed.len() {
            return Err("batch column count does not match the parquet schema".to_string());
        }
        let mut flush = false;
        for (idx, col) in view.columns.iter().enumerate() {
            if !accum_compatible(&self.typed[idx], &self.schema[idx], col)? {
                flush = true;
            }
        }
        if flush && self.group.rows > 0 {
            self.flush_typed()?;
        }
        Ok(())
    }

    fn flush_typed(&mut self) -> Result<(), String> {
        if self.group.rows == 0 {
            return Ok(());
        }
        if self.completed.len() >= self.max_row_groups {
            return Err("max_row_groups exceeded".to_string());
        }
        let rows = self.group.rows;
        let num_rows = i64::try_from(rows).map_err(|_| "row count overflow")?;
        let typed = std::mem::replace(
            &mut self.typed,
            (0..self.schema.len()).map(|_| TypedAccum::Unset).collect(),
        );
        let dictionary: Vec<bool> = typed.iter().map(accum_is_dict).collect();
        let owned = typed
            .iter()
            .zip(&self.schema)
            .map(|(accum, schema)| prepare_column(accum, schema, rows))
            .collect::<Result<Vec<_>, _>>()?;
        let mut byte_refs: Vec<Vec<&[u8]>> = vec![Vec::new(); owned.len()];
        for (idx, prep) in owned.iter().enumerate() {
            if let Some(values) = prep.byte_values() {
                byte_refs[idx] = values.iter().map(|value| value.as_slice()).collect();
            }
        }
        let mut present_bufs: Vec<Option<Vec<u8>>> = Vec::with_capacity(owned.len());
        for prep in &owned {
            present_bufs.push(prep.present.clone());
        }
        let names: Vec<String> = self
            .schema
            .iter()
            .map(|schema| schema.name.clone())
            .collect();
        let codec = self.codec;
        let encode_cols: Vec<Column<'_>> = names
            .iter()
            .zip(owned.iter())
            .zip(byte_refs.iter())
            .zip(present_bufs.iter())
            .map(|(((name, prep), refs), present)| Column {
                name: name.as_str(),
                rows,
                codec,
                present: present.as_deref(),
                values: prep.values(refs),
            })
            .collect();
        self.write_encoded_group(num_rows, &encode_cols, dictionary)?;
        self.group.rows = 0;
        self.group.value_bytes = 0;
        Ok(())
    }

    fn write_encoded_group(
        &mut self,
        num_rows: i64,
        columns: &[Column<'_>],
        dictionary: Vec<bool>,
    ) -> Result<(), String> {
        let mut places = vec![ChunkPlace::default(); columns.len()];
        let rows = num_rows as usize;
        let estimates: Vec<usize> = columns
            .iter()
            .map(|column| estimate_value_bytes(&column.values).max(64))
            .collect();
        // Doubled on each retry. The page scratch holds the widest column; the group holds
        // every column, so it is the sum of their needs, not the widest one times the count.
        let mut slack = 1usize;
        for _ in 0..6 {
            let need = |bytes: usize| scratch_len(rows, bytes.saturating_mul(slack));
            let scratch_need = estimates
                .iter()
                .map(|&bytes| need(bytes))
                .try_fold(0usize, |widest, column| column.map(|c| widest.max(c)))
                .ok_or("scratch overflow")?;
            if self.page_scratch.len() < scratch_need {
                self.page_scratch.resize(scratch_need, 0);
            }
            let group_need = estimates
                .iter()
                .map(|&bytes| need(bytes))
                .try_fold(4096usize, |sum, column| {
                    column.map(|c| sum.saturating_add(c))
                })
                .ok_or("scratch overflow")?;
            // Encode straight into the output queue, after any bytes not yet sent. A
            // separate group buffer and a copy out of it held each row group three times.
            let queued = self.pending.len();
            self.pending.resize(queued.saturating_add(group_need), 0);
            match write_row_group(
                &mut self.pending[queued..],
                num_rows,
                columns,
                &mut places,
                &mut self.page_scratch,
                &mut self.snappy_table,
            ) {
                Ok(rg_len) => {
                    let base = self.file_offset;
                    for place in &mut places {
                        place.offset = place.offset.checked_add(base).ok_or("offset overflow")?;
                        place.data_page_offset = place
                            .data_page_offset
                            .checked_add(base)
                            .ok_or("offset overflow")?;
                        if let Some(dict) = place.dictionary_page_offset {
                            place.dictionary_page_offset =
                                Some(dict.checked_add(base).ok_or("offset overflow")?);
                        }
                    }
                    self.pending.truncate(queued + rg_len);
                    self.mark_queued();
                    self.file_offset = self
                        .file_offset
                        .checked_add(rg_len as u64)
                        .ok_or("file size overflow")?;
                    self.completed.push(CompletedRowGroup {
                        num_rows,
                        places,
                        dictionary,
                    });
                    return Ok(());
                }
                Err(EncodeError::OutputFull) => {
                    self.pending.truncate(queued);
                    slack = slack.saturating_mul(2);
                }
                Err(err) => {
                    self.pending.truncate(queued);
                    return Err(Self::encode_err(err));
                }
            }
        }
        Err("parquet row group did not fit the encode buffer".to_string())
    }

    fn write_typed_footer(&mut self) -> Result<(), String> {
        let empty_i32: &[i32] = &[];
        let empty_i64: &[i64] = &[];
        let empty_f32: &[f32] = &[];
        let empty_f64: &[f64] = &[];
        let empty_bool: &[bool] = &[];
        let empty_bytes: &[&[u8]] = &[];
        let empty_ids: &[u32] = &[];
        let schema_cols: Vec<Column<'_>> = self
            .schema
            .iter()
            .map(|schema| Column {
                name: schema.name.as_str(),
                rows: 0,
                codec: self.codec,
                present: if schema.optional { Some(&[]) } else { None },
                values: schema_values(
                    schema,
                    empty_i32,
                    empty_i64,
                    empty_f32,
                    empty_f64,
                    empty_bool,
                    empty_bytes,
                ),
            })
            .collect();
        let mut group_storage = Vec::with_capacity(self.completed.len());
        for done in &self.completed {
            let cols: Vec<Column<'_>> = self
                .schema
                .iter()
                .zip(&done.dictionary)
                .map(|(schema, dict)| Column {
                    name: schema.name.as_str(),
                    rows: done.num_rows as usize,
                    codec: self.codec,
                    present: if schema.optional { Some(&[]) } else { None },
                    values: if *dict {
                        dict_values(
                            schema,
                            empty_i32,
                            empty_i64,
                            empty_f32,
                            empty_f64,
                            empty_bytes,
                            empty_ids,
                        )
                    } else {
                        schema_values(
                            schema,
                            empty_i32,
                            empty_i64,
                            empty_f32,
                            empty_f64,
                            empty_bool,
                            empty_bytes,
                        )
                    },
                })
                .collect();
            group_storage.push(cols);
        }
        let footers: Vec<RowGroupFooter<'_>> = self
            .completed
            .iter()
            .zip(&group_storage)
            .map(|(done, cols)| RowGroupFooter {
                num_rows: done.num_rows,
                columns: cols,
                places: &done.places,
            })
            .collect();
        let footer_len = loop {
            match write_footer(&mut self.footer_buf, &schema_cols, &footers) {
                Ok(n) => break n,
                Err(EncodeError::OutputFull) if self.footer_buf.len() < 1024 * 1024 => {
                    let next = self
                        .footer_buf
                        .len()
                        .saturating_mul(2)
                        .max(self.footer_buf.len() + 4096);
                    self.footer_buf.resize(next.min(1024 * 1024), 0);
                }
                Err(err) => return Err(Self::encode_err(err)),
            }
        };
        let chunk = self.footer_buf[..footer_len].to_vec();
        self.queue_bytes(&chunk);
        self.file_offset = self
            .file_offset
            .checked_add(footer_len as u64)
            .ok_or("file size overflow")?;
        self.phase = Phase::Done;
        Ok(())
    }
}

struct Prepared {
    present: Option<Vec<u8>>,
    kind: PreparedKind,
}

enum PreparedKind {
    Bool(Vec<bool>),
    I32(Vec<i32>),
    Date(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bytes(Vec<Vec<u8>>),
    Fixed {
        width: u32,
        values: Vec<Vec<u8>>,
    },
    DictI32 {
        dict: Vec<i32>,
        ids: Vec<u32>,
    },
    DictI64 {
        dict: Vec<i64>,
        ids: Vec<u32>,
    },
    DictF32 {
        dict: Vec<f32>,
        ids: Vec<u32>,
    },
    DictF64 {
        dict: Vec<f64>,
        ids: Vec<u32>,
    },
    DictBytes {
        dict: Vec<Vec<u8>>,
        ids: Vec<u32>,
    },
    DictFixed {
        width: u32,
        dict: Vec<Vec<u8>>,
        ids: Vec<u32>,
    },
}

impl Prepared {
    fn byte_values(&self) -> Option<&[Vec<u8>]> {
        match &self.kind {
            PreparedKind::Bytes(values) | PreparedKind::Fixed { values, .. } => Some(values),
            PreparedKind::DictBytes { dict, .. } | PreparedKind::DictFixed { dict, .. } => {
                Some(dict)
            }
            _ => None,
        }
    }

    fn values<'a>(&'a self, refs: &'a [&'a [u8]]) -> ColumnValues<'a> {
        match &self.kind {
            PreparedKind::Bool(values) => ColumnValues::Bool(values),
            PreparedKind::I32(values) => ColumnValues::I32(values),
            PreparedKind::Date(values) => ColumnValues::Date(values),
            PreparedKind::I64(values) => ColumnValues::I64(values),
            PreparedKind::F32(values) => ColumnValues::F32(values),
            PreparedKind::F64(values) => ColumnValues::F64(values),
            PreparedKind::Bytes(_) => ColumnValues::Bytes(refs),
            PreparedKind::Fixed { width, .. } => ColumnValues::Fixed {
                width: *width,
                values: refs,
            },
            PreparedKind::DictI32 { dict, ids } => ColumnValues::DictI32 {
                dictionary: dict,
                ids,
            },
            PreparedKind::DictI64 { dict, ids } => ColumnValues::DictI64 {
                dictionary: dict,
                ids,
            },
            PreparedKind::DictF32 { dict, ids } => ColumnValues::DictF32 {
                dictionary: dict,
                ids,
            },
            PreparedKind::DictF64 { dict, ids } => ColumnValues::DictF64 {
                dictionary: dict,
                ids,
            },
            PreparedKind::DictBytes { ids, .. } => ColumnValues::DictBytes {
                dictionary: refs,
                ids,
            },
            PreparedKind::DictFixed { width, ids, .. } => ColumnValues::DictFixed {
                width: *width,
                dictionary: refs,
                ids,
            },
        }
    }
}

fn schema_values<'a>(
    schema: &'a SchemaCol,
    i32s: &'a [i32],
    i64s: &'a [i64],
    f32s: &'a [f32],
    f64s: &'a [f64],
    bools: &'a [bool],
    bytes: &'a [&'a [u8]],
) -> ColumnValues<'a> {
    match schema.physical {
        0 => ColumnValues::Bool(bools),
        1 if schema.date => ColumnValues::Date(i32s),
        1 => ColumnValues::I32(i32s),
        2 => ColumnValues::I64(i64s),
        4 => ColumnValues::F32(f32s),
        5 => ColumnValues::F64(f64s),
        6 => ColumnValues::Bytes(bytes),
        7 => ColumnValues::Fixed {
            width: schema.type_length as u32,
            values: bytes,
        },
        _ => ColumnValues::I64(i64s),
    }
}

fn dict_values<'a>(
    schema: &'a SchemaCol,
    i32s: &'a [i32],
    i64s: &'a [i64],
    f32s: &'a [f32],
    f64s: &'a [f64],
    bytes: &'a [&'a [u8]],
    ids: &'a [u32],
) -> ColumnValues<'a> {
    match schema.physical {
        1 => ColumnValues::DictI32 {
            dictionary: i32s,
            ids,
        },
        2 => ColumnValues::DictI64 {
            dictionary: i64s,
            ids,
        },
        4 => ColumnValues::DictF32 {
            dictionary: f32s,
            ids,
        },
        5 => ColumnValues::DictF64 {
            dictionary: f64s,
            ids,
        },
        6 => ColumnValues::DictBytes {
            dictionary: bytes,
            ids,
        },
        7 => ColumnValues::DictFixed {
            width: schema.type_length as u32,
            dictionary: bytes,
            ids,
        },
        _ => ColumnValues::I64(i64s),
    }
}

fn estimate_value_bytes(values: &ColumnValues<'_>) -> usize {
    match values {
        ColumnValues::Bool(v) => v.len().div_ceil(8).saturating_add(16),
        ColumnValues::I32(v) | ColumnValues::Date(v) | ColumnValues::DeltaI32(v) => {
            v.len().saturating_mul(4)
        }
        ColumnValues::I64(v) | ColumnValues::DeltaI64(v) => v.len().saturating_mul(8),
        ColumnValues::F32(v) => v.len().saturating_mul(4),
        ColumnValues::F64(v) => v.len().saturating_mul(8),
        ColumnValues::Bytes(v)
        | ColumnValues::Fixed { values: v, .. }
        | ColumnValues::DeltaLengthBytes(v)
        | ColumnValues::DeltaBytes(v) => v
            .iter()
            .fold(16usize, |acc, value| acc.saturating_add(value.len() + 4)),
        ColumnValues::DictI32 { dictionary, ids } => {
            dictionary.len().saturating_mul(4) + ids.len().saturating_mul(4)
        }
        ColumnValues::DictI64 { dictionary, ids } => {
            dictionary.len().saturating_mul(8) + ids.len().saturating_mul(4)
        }
        ColumnValues::DictF32 { dictionary, ids } => {
            dictionary.len().saturating_mul(4) + ids.len().saturating_mul(4)
        }
        ColumnValues::DictF64 { dictionary, ids } => {
            dictionary.len().saturating_mul(8) + ids.len().saturating_mul(4)
        }
        ColumnValues::DictBytes { dictionary, ids }
        | ColumnValues::DictFixed {
            dictionary, ids, ..
        } => dictionary
            .iter()
            .fold(ids.len().saturating_mul(4), |acc, value| {
                acc.saturating_add(value.len() + 4)
            }),
    }
}

fn accum_is_dict(accum: &TypedAccum) -> bool {
    matches!(
        accum,
        TypedAccum::DictI32 { .. }
            | TypedAccum::DictI64 { .. }
            | TypedAccum::DictF32 { .. }
            | TypedAccum::DictF64 { .. }
            | TypedAccum::DictBytes { .. }
            | TypedAccum::DictFixed { .. }
    )
}

fn prepare_column(accum: &TypedAccum, schema: &SchemaCol, rows: usize) -> Result<Prepared, String> {
    let present_of = |nulls: &[u8]| -> Result<Option<Vec<u8>>, String> {
        if schema.optional {
            if nulls.len() != rows {
                return Err("null bitmap does not match the row count".to_string());
            }
            Ok(Some(present_bits(nulls)))
        } else {
            Ok(None)
        }
    };
    let (present, kind) = match accum {
        TypedAccum::Unset => return Err("parquet column received no values".to_string()),
        TypedAccum::Bool { values, nulls } => {
            (present_of(nulls)?, PreparedKind::Bool(values.clone()))
        }
        TypedAccum::I32 { values, nulls } => {
            let kind = if schema.date {
                PreparedKind::Date(values.clone())
            } else {
                PreparedKind::I32(values.clone())
            };
            (present_of(nulls)?, kind)
        }
        TypedAccum::I64 { values, nulls } => {
            (present_of(nulls)?, PreparedKind::I64(values.clone()))
        }
        TypedAccum::F32 { values, nulls } => {
            (present_of(nulls)?, PreparedKind::F32(values.clone()))
        }
        TypedAccum::F64 { values, nulls } => {
            (present_of(nulls)?, PreparedKind::F64(values.clone()))
        }
        TypedAccum::Bytes { values, nulls } => {
            (present_of(nulls)?, PreparedKind::Bytes(values.clone()))
        }
        TypedAccum::Fixed { values, nulls } => (
            present_of(nulls)?,
            PreparedKind::Fixed {
                width: schema.type_length as u32,
                values: values.clone(),
            },
        ),
        TypedAccum::DictI32 { dict, ids, nulls } => (
            present_of(nulls)?,
            PreparedKind::DictI32 {
                dict: dict.clone(),
                ids: ids.clone(),
            },
        ),
        TypedAccum::DictI64 { dict, ids, nulls } => (
            present_of(nulls)?,
            PreparedKind::DictI64 {
                dict: dict.clone(),
                ids: ids.clone(),
            },
        ),
        TypedAccum::DictF32 { dict, ids, nulls } => (
            present_of(nulls)?,
            PreparedKind::DictF32 {
                dict: dict.clone(),
                ids: ids.clone(),
            },
        ),
        TypedAccum::DictF64 { dict, ids, nulls } => (
            present_of(nulls)?,
            PreparedKind::DictF64 {
                dict: dict.clone(),
                ids: ids.clone(),
            },
        ),
        TypedAccum::DictBytes { dict, ids, nulls } => (
            present_of(nulls)?,
            PreparedKind::DictBytes {
                dict: dict.clone(),
                ids: ids.clone(),
            },
        ),
        TypedAccum::DictFixed { dict, ids, nulls } => (
            present_of(nulls)?,
            PreparedKind::DictFixed {
                width: schema.type_length as u32,
                dict: dict.clone(),
                ids: ids.clone(),
            },
        ),
    };
    Ok(Prepared { present, kind })
}

fn present_bits(nulls: &[u8]) -> Vec<u8> {
    let mut present = vec![0u8; nulls.len().div_ceil(8)];
    for (row, flag) in nulls.iter().copied().enumerate() {
        if flag != 0 {
            present[row >> 3] |= 1 << (row & 7);
        }
    }
    present
}

fn accum_compatible(
    accum: &TypedAccum,
    schema: &SchemaCol,
    col: &WcolColumn<'_>,
) -> Result<bool, String> {
    if matches!(accum, TypedAccum::Unset) {
        return Ok(true);
    }
    Ok(
        std::mem::discriminant(&init_accum(schema, col)?) == std::mem::discriminant(accum)
            && dict_matches(accum, col),
    )
}

fn dict_matches(accum: &TypedAccum, col: &WcolColumn<'_>) -> bool {
    let WcolColumn::DictId { dictionary, .. } = col else {
        return true;
    };
    match (accum, &dictionary.entries) {
        (TypedAccum::DictI32 { dict, .. }, crate::stream::DictEntries::Int(entries)) => {
            dict.len() == entries.len()
                && dict
                    .iter()
                    .zip(entries.iter())
                    .all(|(stored, src)| i64::from(*stored) == *src)
        }
        (TypedAccum::DictI64 { dict, .. }, crate::stream::DictEntries::Int(entries)) => {
            dict.as_slice() == entries.as_ref()
        }
        (TypedAccum::DictF32 { dict, .. }, crate::stream::DictEntries::Float(entries)) => {
            dict.len() == entries.len()
                && dict
                    .iter()
                    .zip(entries.iter())
                    .all(|(stored, src)| stored.to_bits() == (*src as f32).to_bits())
        }
        (TypedAccum::DictF64 { dict, .. }, crate::stream::DictEntries::Float(entries)) => {
            dict.len() == entries.len()
                && dict
                    .iter()
                    .zip(entries.iter())
                    .all(|(stored, src)| stored.to_bits() == src.to_bits())
        }
        (
            TypedAccum::DictBytes { dict, .. } | TypedAccum::DictFixed { dict, .. },
            crate::stream::DictEntries::Bytes { offsets, bytes },
        ) => split_bytes(offsets, bytes).ok().as_deref() == Some(dict.as_slice()),
        _ => false,
    }
}

fn init_accum(schema: &SchemaCol, col: &WcolColumn<'_>) -> Result<TypedAccum, String> {
    match col {
        WcolColumn::I64 { .. } => match schema.physical {
            0 => Ok(TypedAccum::Bool {
                values: Vec::new(),
                nulls: Vec::new(),
            }),
            1 => Ok(TypedAccum::I32 {
                values: Vec::new(),
                nulls: Vec::new(),
            }),
            2 => Ok(TypedAccum::I64 {
                values: Vec::new(),
                nulls: Vec::new(),
            }),
            _ => Err(format!(
                "i64 batch column cannot be written as physical type {}",
                schema.physical
            )),
        },
        WcolColumn::F64 { .. } => match schema.physical {
            4 => Ok(TypedAccum::F32 {
                values: Vec::new(),
                nulls: Vec::new(),
            }),
            5 => Ok(TypedAccum::F64 {
                values: Vec::new(),
                nulls: Vec::new(),
            }),
            _ => Err(format!(
                "f64 batch column cannot be written as physical type {}",
                schema.physical
            )),
        },
        WcolColumn::Utf8 { .. } => match schema.physical {
            6 => Ok(TypedAccum::Bytes {
                values: Vec::new(),
                nulls: Vec::new(),
            }),
            7 => Ok(TypedAccum::Fixed {
                values: Vec::new(),
                nulls: Vec::new(),
            }),
            _ => Err(format!(
                "byte batch column cannot be written as physical type {}",
                schema.physical
            )),
        },
        WcolColumn::DictId { dictionary, .. } => match (&dictionary.entries, schema.physical) {
            (crate::stream::DictEntries::Int(entries), 1) => {
                let mut dict = Vec::with_capacity(entries.len());
                for value in entries.iter() {
                    dict.push(
                        i32::try_from(*value)
                            .map_err(|_| format!("dictionary value {value} does not fit INT32"))?,
                    );
                }
                Ok(TypedAccum::DictI32 {
                    dict,
                    ids: Vec::new(),
                    nulls: Vec::new(),
                })
            }
            (crate::stream::DictEntries::Int(entries), 2) => Ok(TypedAccum::DictI64 {
                dict: entries.to_vec(),
                ids: Vec::new(),
                nulls: Vec::new(),
            }),
            (crate::stream::DictEntries::Float(entries), 4) => Ok(TypedAccum::DictF32 {
                dict: entries.iter().map(|value| *value as f32).collect(),
                ids: Vec::new(),
                nulls: Vec::new(),
            }),
            (crate::stream::DictEntries::Float(entries), 5) => Ok(TypedAccum::DictF64 {
                dict: entries.to_vec(),
                ids: Vec::new(),
                nulls: Vec::new(),
            }),
            (crate::stream::DictEntries::Bytes { offsets, bytes }, 6) => {
                Ok(TypedAccum::DictBytes {
                    dict: split_bytes(offsets, bytes)?,
                    ids: Vec::new(),
                    nulls: Vec::new(),
                })
            }
            (crate::stream::DictEntries::Bytes { offsets, bytes }, 7) => {
                let dict = split_bytes(offsets, bytes)?;
                let width = schema.type_length as usize;
                if dict.iter().any(|value| value.len() != width) {
                    return Err(
                        "dictionary entry length does not match FIXED_LEN_BYTE_ARRAY".to_string(),
                    );
                }
                Ok(TypedAccum::DictFixed {
                    dict,
                    ids: Vec::new(),
                    nulls: Vec::new(),
                })
            }
            _ => Err("dictionary type does not match the parquet column".to_string()),
        },
    }
}

/// Append one cell. Returns the bytes a byte-array value now holds (zero otherwise).
fn push_typed_cell(
    accum: &mut TypedAccum,
    schema: &SchemaCol,
    col: &WcolColumn<'_>,
    row: usize,
) -> Result<usize, String> {
    if matches!(accum, TypedAccum::Unset) {
        *accum = init_accum(schema, col)?;
    }
    let (present, payload) = cell_payload(schema, col, row)?;
    if !schema.optional && present == 0 {
        return Err(format!("null in required column {}", schema.name));
    }
    match accum {
        TypedAccum::Unset => unreachable!("column was initialized"),
        TypedAccum::Bool { values, nulls } => {
            note_null(schema, nulls, present);
            if present != 0 {
                values.push(payload.bool_value()?);
            }
        }
        TypedAccum::I32 { values, nulls } => {
            note_null(schema, nulls, present);
            if present != 0 {
                values.push(payload.i32_value()?);
            }
        }
        TypedAccum::I64 { values, nulls } => {
            note_null(schema, nulls, present);
            if present != 0 {
                values.push(payload.i64_value()?);
            }
        }
        TypedAccum::F32 { values, nulls } => {
            note_null(schema, nulls, present);
            if present != 0 {
                values.push(payload.f32_value()?);
            }
        }
        TypedAccum::F64 { values, nulls } => {
            note_null(schema, nulls, present);
            if present != 0 {
                values.push(payload.f64_value()?);
            }
        }
        TypedAccum::Bytes { values, nulls } | TypedAccum::Fixed { values, nulls } => {
            note_null(schema, nulls, present);
            if present != 0 {
                let value = payload.bytes_value()?;
                let held = value.len() + std::mem::size_of::<Vec<u8>>();
                values.push(value);
                return Ok(held);
            }
        }
        TypedAccum::DictI32 { ids, nulls, .. }
        | TypedAccum::DictI64 { ids, nulls, .. }
        | TypedAccum::DictF32 { ids, nulls, .. }
        | TypedAccum::DictF64 { ids, nulls, .. }
        | TypedAccum::DictBytes { ids, nulls, .. }
        | TypedAccum::DictFixed { ids, nulls, .. } => {
            note_null(schema, nulls, present);
            if present != 0 {
                ids.push(payload.dict_id()?);
            }
        }
    }
    Ok(0)
}

fn note_null(schema: &SchemaCol, nulls: &mut Vec<u8>, present: u8) {
    if schema.optional {
        nulls.push(present);
    }
}

enum Payload {
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bytes(Vec<u8>),
    Id(u32),
    Missing,
}

impl Payload {
    fn bool_value(self) -> Result<bool, String> {
        match self {
            Payload::Bool(value) => Ok(value),
            _ => Err("expected a boolean value".to_string()),
        }
    }
    fn i32_value(self) -> Result<i32, String> {
        match self {
            Payload::I32(value) => Ok(value),
            _ => Err("expected an int32 value".to_string()),
        }
    }
    fn i64_value(self) -> Result<i64, String> {
        match self {
            Payload::I64(value) => Ok(value),
            _ => Err("expected an int64 value".to_string()),
        }
    }
    fn f32_value(self) -> Result<f32, String> {
        match self {
            Payload::F32(value) => Ok(value),
            _ => Err("expected a float value".to_string()),
        }
    }
    fn f64_value(self) -> Result<f64, String> {
        match self {
            Payload::F64(value) => Ok(value),
            _ => Err("expected a double value".to_string()),
        }
    }
    fn bytes_value(self) -> Result<Vec<u8>, String> {
        match self {
            Payload::Bytes(value) => Ok(value),
            _ => Err("expected a byte value".to_string()),
        }
    }
    fn dict_id(self) -> Result<u32, String> {
        match self {
            Payload::Id(value) => Ok(value),
            _ => Err("expected a dictionary id".to_string()),
        }
    }
}

fn cell_payload(
    schema: &SchemaCol,
    col: &WcolColumn<'_>,
    row: usize,
) -> Result<(u8, Payload), String> {
    match col {
        WcolColumn::I64 { values, nulls, .. } => {
            let present = nulls.get(row).copied().unwrap_or(1);
            if present == 0 {
                return Ok((0, Payload::Missing));
            }
            let value = values[row];
            let payload = match schema.physical {
                0 => match value {
                    0 => Payload::Bool(false),
                    1 => Payload::Bool(true),
                    _ => return Err(format!("boolean value {value} is not 0 or 1")),
                },
                1 => Payload::I32(
                    i32::try_from(value)
                        .map_err(|_| format!("value {value} does not fit INT32"))?,
                ),
                2 => Payload::I64(value),
                _ => return Err("i64 value does not match the parquet column".to_string()),
            };
            Ok((1, payload))
        }
        WcolColumn::F64 { values, nulls, .. } => {
            let present = nulls.get(row).copied().unwrap_or(1);
            if present == 0 {
                return Ok((0, Payload::Missing));
            }
            let value = values[row];
            let payload = match schema.physical {
                4 => Payload::F32(value as f32),
                5 => Payload::F64(value),
                _ => return Err("float value does not match the parquet column".to_string()),
            };
            Ok((1, payload))
        }
        WcolColumn::Utf8 {
            offsets,
            bytes,
            nulls,
            ..
        } => {
            let present = nulls.get(row).copied().unwrap_or(1);
            if present == 0 {
                return Ok((0, Payload::Missing));
            }
            let value = bytes[offsets[row] as usize..offsets[row + 1] as usize].to_vec();
            if schema.physical == 7 && value.len() != schema.type_length as usize {
                return Err("value length does not match FIXED_LEN_BYTE_ARRAY".to_string());
            }
            Ok((1, Payload::Bytes(value)))
        }
        WcolColumn::DictId { ids, nulls, .. } => {
            let present = nulls.get(row).copied().unwrap_or(1);
            if present == 0 {
                return Ok((0, Payload::Missing));
            }
            Ok((1, Payload::Id(ids[row])))
        }
    }
}

fn split_bytes(offsets: &[u32], bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if offsets.len() < 1 {
        return Err("dictionary offsets are empty".to_string());
    }
    let mut out = Vec::with_capacity(offsets.len() - 1);
    for pair in offsets.windows(2) {
        let start = pair[0] as usize;
        let end = pair[1] as usize;
        if end < start || end > bytes.len() {
            return Err("dictionary offsets are out of range".to_string());
        }
        out.push(bytes[start..end].to_vec());
    }
    Ok(out)
}

impl Operator for ParquetEncode {
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

        if matches!(self.phase, Phase::NeedMagic) {
            self.write_magic_once()?;
            return self.flush_pending_port(io);
        }

        if let Some(item) = io.input_slot.as_mut().and_then(|s| s.take()) {
            let batch = item.into_batch();
            self.validate_batch(&batch)?;
            self.schema_checked = true;
            self.append_batch(&batch)?;
        } else if let Some(item) = io.input {
            let batch = item.batch();
            self.validate_batch(batch)?;
            self.schema_checked = true;
            self.append_batch(batch)?;
        }

        if !self.pending.is_empty() {
            return self.flush_pending_port(io);
        }

        if io.input_eof {
            self.input_eof = true;
            if !self.schema_checked && self.group.rows == 0 {
                return Err("parquet encode received no row batches".to_string());
            }
            if !matches!(self.phase, Phase::Done) {
                self.write_footer_once()?;
            }
            if !self.pending.is_empty() {
                return self.flush_pending_port(io);
            }
            return Ok(StreamProgress::Finished);
        }

        Ok(StreamProgress::NeedInput)
    }
}

#[cfg(test)]
mod tests {
    use super::{ParquetEncode, ParquetEncodeConfig};

    /// A shard export opens its sink when the reducer starts, long before its first row:
    /// a sink sized for a full 65,536-row group then held 7 MiB of a reducer's memory.
    #[test]
    fn opening_a_sink_reserves_no_row_group() {
        let sink = ParquetEncode::open(
            vec!["Title".into(), "c".into()],
            vec![true, false],
            ParquetEncodeConfig::FULL_FILE,
        )
        .unwrap();
        assert_eq!(sink.page_scratch.capacity(), 0);
        assert!(sink
            .group
            .cols
            .iter()
            .all(|col| col.values.capacity() == 0 && col.nulls.capacity() == 0));
    }
}
