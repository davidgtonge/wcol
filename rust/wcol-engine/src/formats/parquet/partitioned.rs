//! Parquet export split by output values (`partitionBy`): one file per distinct value of the
//! partition columns, under a Hive path such as `EventDate=15887/`.
//!
//! The sink writes several files through one output port, so its bytes are frames that the
//! host splits into objects without reading Parquet. Every integer is little-endian:
//!
//! - `OPEN`  `u8 1, u32 slot, u32 len, path`: a new file under `path` (`Name=value/...`).
//! - `DATA`  `u8 2, u32 slot, u32 len, bytes`: the next bytes of that file.
//! - `CLOSE` `u8 3, u32 slot, u64 rows`: the file is complete.
//!
//! A slot names one file for the life of the task; slots are never reused. At most
//! [`MAX_OPEN_FILES`] are open at once. A new value past that closes the least recently used
//! file, and a later row with the closed value starts another file in the same directory, so
//! clustered data writes few files and scattered data writes more, smaller ones. Partition
//! columns are not stored in the files: their values are in the path, as Hive, Spark and
//! DuckDB write them.

use std::collections::HashMap;

use crate::exec::operator::{Operator, OperatorIo, StreamProgress};
use crate::ops::ProjectOp;
use crate::stream::{OwnedWcolBatch, SelectionMask, WcolColumn};

use super::{ParquetEncode, ParquetEncodeConfig, ParquetSinkColumn};

pub const FRAME_OPEN: u8 = 1;
pub const FRAME_DATA: u8 = 2;
pub const FRAME_CLOSE: u8 = 3;

/// Files open at once. The host stages one 5 MiB multipart part for each.
pub const MAX_OPEN_FILES: usize = 8;
/// Files one task may write. Past this the values are too scattered to export this way.
pub const MAX_FILES: u32 = 4096;
/// Rows held across all open files before the largest is written out as a row group.
const HELD_BUDGET: usize = 16 << 20;

/// One value in a partition path, read from a final output column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionValue<'a> {
    Null,
    Int(i64),
    Bytes(&'a [u8]),
}

/// The path segment for one value: Hive's null marker, an integer in decimal, or bytes with
/// everything but `A-Z a-z 0-9 - _ .` percent-encoded (so `/` is `%2F` and `=` is `%3D`).
pub fn partition_path_value(value: PartitionValue<'_>) -> String {
    match value {
        PartitionValue::Null => "__HIVE_DEFAULT_PARTITION__".to_string(),
        PartitionValue::Int(value) => value.to_string(),
        PartitionValue::Bytes(bytes) => percent_encode(bytes),
    }
}

fn percent_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len());
    for &byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            out.push(byte as char);
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

struct OpenFile {
    slot: u32,
    encode: ParquetEncode,
    rows: u64,
    last_used: u64,
}

pub struct PartitionedParquetEncode {
    file_columns: Vec<ParquetSinkColumn>,
    config: ParquetEncodeConfig,
    /// Batch column ids of the partition columns, in `partitionBy` order, and their names.
    partition_ids: Vec<u32>,
    partition_names: Vec<String>,
    /// Column ids written to each file: the output columns less the partition columns.
    project: ProjectOp,
    open: HashMap<String, OpenFile>,
    next_slot: u32,
    clock: u64,
    frames: Vec<u8>,
    sent: usize,
    finished: bool,
}

impl PartitionedParquetEncode {
    /// `columns` and `ids` are the output columns and their batch column ids, in order;
    /// `partition` holds ordinals into them.
    pub fn open(
        columns: Vec<ParquetSinkColumn>,
        ids: &[u32],
        partition: &[u32],
        config: ParquetEncodeConfig,
    ) -> Result<Self, String> {
        if columns.len() != ids.len() {
            return Err("partitioned sink columns and ids differ in length".to_string());
        }
        let partition: Vec<usize> = partition.iter().map(|&p| p as usize).collect();
        if partition.is_empty() || partition.iter().any(|&p| p >= columns.len()) {
            return Err("partitionBy names no output column".to_string());
        }
        if partition.len() >= columns.len() {
            return Err("partitionBy leaves no column to write".to_string());
        }
        let keep: Vec<usize> = (0..columns.len())
            .filter(|i| !partition.contains(i))
            .collect();
        let file_columns = keep.iter().map(|&i| columns[i].clone()).collect();
        // Validates the file schema once, before any row arrives.
        ParquetEncode::open_typed(Vec::clone(&file_columns), config)?;
        Ok(Self {
            file_columns,
            config,
            partition_ids: partition.iter().map(|&p| ids[p]).collect(),
            partition_names: partition
                .iter()
                .map(|&p| percent_encode(columns[p].name.as_bytes()))
                .collect(),
            project: ProjectOp::from_columns(keep.iter().map(|&i| ids[i]).collect())?,
            open: HashMap::new(),
            next_slot: 0,
            clock: 0,
            frames: Vec::new(),
            sent: 0,
            finished: false,
        })
    }

    fn push_batch(&mut self, batch: &OwnedWcolBatch) -> Result<(), String> {
        if !batch.is_row_batch() {
            return Err("partitioned parquet sink expects row batches".to_string());
        }
        let rows = batch.row_count();
        if rows == 0 {
            return Ok(());
        }
        let paths = self.row_paths(batch)?;
        let mut file_rows = self.project.project_batch(batch)?;
        let mut masks: Vec<(String, SelectionMask)> = Vec::new();
        for (row, path) in paths.into_iter().enumerate() {
            let Some(path) = path else { continue };
            let at = match masks.iter().position(|(seen, _)| *seen == path) {
                Some(at) => at,
                None => {
                    masks.push((path, SelectionMask::with_rows(rows)));
                    masks.len() - 1
                }
            };
            masks[at].1.set_selected(row, true);
        }
        for (path, mask) in masks {
            let selected = mask.selected_count();
            file_rows.set_selection_mask(mask);
            let file = self.file_for(&path)?;
            file.encode.push_rows(&file_rows)?;
            file.rows += selected;
            let slot = file.slot;
            self.take_data(&path, slot);
        }
        self.release_held()
    }

    /// Each selected row's partition path; `None` for a row the batch does not select.
    fn row_paths(&self, batch: &OwnedWcolBatch) -> Result<Vec<Option<String>>, String> {
        let mask = batch.effective_selection_mask();
        let mut col_view = Vec::new();
        let view = batch.view_with(&mut col_view);
        let columns = self
            .partition_ids
            .iter()
            .map(|id| {
                view.columns
                    .iter()
                    .find(|col| column_id(col) == *id)
                    .ok_or_else(|| format!("partition column {id} is not in the batch"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        (0..view.row_count)
            .map(|row| {
                if !mask.is_selected(row) {
                    return Ok(None);
                }
                let mut path = String::new();
                for (name, col) in self.partition_names.iter().zip(&columns) {
                    if !path.is_empty() {
                        path.push('/');
                    }
                    path.push_str(name);
                    path.push('=');
                    path.push_str(&partition_path_value(cell(col, row)?));
                }
                Ok(Some(path))
            })
            .collect()
    }

    /// The open file for `path`, opening one (and closing the least recently used) if needed.
    fn file_for(&mut self, path: &str) -> Result<&mut OpenFile, String> {
        self.clock += 1;
        if !self.open.contains_key(path) {
            if self.open.len() >= MAX_OPEN_FILES {
                let oldest = self
                    .open
                    .iter()
                    .min_by_key(|(_, file)| file.last_used)
                    .map(|(path, _)| path.clone())
                    .ok_or("no open file to close")?;
                self.close(&oldest)?;
            }
            if self.next_slot >= MAX_FILES {
                return Err(format!(
                    "partitionBy needs more than {MAX_FILES} files on one task: its values are too scattered"
                ));
            }
            let slot = self.next_slot;
            self.next_slot += 1;
            let encode = ParquetEncode::open_typed(self.file_columns.clone(), self.config)?;
            self.frame_header(FRAME_OPEN, slot);
            self.frame_bytes(path.as_bytes())?;
            self.open.insert(
                path.to_string(),
                OpenFile {
                    slot,
                    encode,
                    rows: 0,
                    last_used: 0,
                },
            );
        }
        let clock = self.clock;
        let file = self.open.get_mut(path).ok_or("partition file vanished")?;
        file.last_used = clock;
        Ok(file)
    }

    /// While the open files hold more than the budget, write the largest one's rows out.
    fn release_held(&mut self) -> Result<(), String> {
        loop {
            let held: usize = self
                .open
                .values()
                .map(|file| file.encode.held_bytes())
                .sum();
            if held <= HELD_BUDGET {
                return Ok(());
            }
            let Some((path, slot)) = self
                .open
                .iter()
                .max_by_key(|(_, file)| file.encode.held_bytes())
                .map(|(path, file)| (path.clone(), file.slot))
            else {
                return Ok(());
            };
            if let Some(file) = self.open.get_mut(&path) {
                file.encode.write_held_rows()?;
            }
            self.take_data(&path, slot);
        }
    }

    fn close(&mut self, path: &str) -> Result<(), String> {
        let Some(mut file) = self.open.remove(path) else {
            return Ok(());
        };
        file.encode.finish_file()?;
        self.data_frame(&mut file.encode, file.slot);
        self.frame_header(FRAME_CLOSE, file.slot);
        self.frames.extend_from_slice(&file.rows.to_le_bytes());
        Ok(())
    }

    fn close_all(&mut self) -> Result<(), String> {
        let mut paths: Vec<(u32, String)> = self
            .open
            .iter()
            .map(|(path, file)| (file.slot, path.clone()))
            .collect();
        paths.sort_unstable();
        for (_, path) in paths {
            self.close(&path)?;
        }
        Ok(())
    }

    fn take_data(&mut self, path: &str, slot: u32) {
        if let Some(mut file) = self.open.remove(path) {
            self.data_frame(&mut file.encode, slot);
            self.open.insert(path.to_string(), file);
        }
    }

    /// One `DATA` frame with whatever `encode` has written since it was last taken.
    fn data_frame(&mut self, encode: &mut ParquetEncode, slot: u32) {
        let header = self.frames.len();
        self.frame_header(FRAME_DATA, slot);
        let len_at = self.frames.len();
        self.frames.extend_from_slice(&[0; 4]);
        encode.take_output(&mut self.frames);
        let len = self.frames.len() - len_at - 4;
        if len == 0 {
            self.frames.truncate(header);
        } else {
            self.frames[len_at..len_at + 4].copy_from_slice(&(len as u32).to_le_bytes());
        }
    }

    fn frame_header(&mut self, tag: u8, slot: u32) {
        self.frames.push(tag);
        self.frames.extend_from_slice(&slot.to_le_bytes());
    }

    fn frame_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        let len = u32::try_from(bytes.len()).map_err(|_| "partition path too long")?;
        self.frames.extend_from_slice(&len.to_le_bytes());
        self.frames.extend_from_slice(bytes);
        Ok(())
    }

    fn send(&mut self, io: &mut OperatorIo<'_, '_>) -> StreamProgress {
        let Some(port) = io.output_port.as_mut() else {
            return StreamProgress::Blocked;
        };
        if port.is_empty() {
            return StreamProgress::Blocked;
        }
        let rest = &self.frames[self.sent..];
        let n = rest.len().min(port.len());
        port[..n].copy_from_slice(&rest[..n]);
        *io.bytes_written = n;
        self.sent += n;
        if self.sent == self.frames.len() {
            self.frames.clear();
            self.sent = 0;
        }
        StreamProgress::HaveOutput
    }
}

fn column_id(col: &WcolColumn<'_>) -> u32 {
    match col {
        WcolColumn::I64 { column, .. }
        | WcolColumn::F64 { column, .. }
        | WcolColumn::DictId { column, .. }
        | WcolColumn::Utf8 { column, .. } => *column,
    }
}

fn cell<'a>(col: &WcolColumn<'a>, row: usize) -> Result<PartitionValue<'a>, String> {
    let present = |nulls: &[u8]| nulls.get(row).copied().unwrap_or(1) != 0;
    match *col {
        WcolColumn::I64 { values, nulls, .. } => Ok(if present(nulls) {
            PartitionValue::Int(*values.get(row).ok_or("partition row missing")?)
        } else {
            PartitionValue::Null
        }),
        WcolColumn::Utf8 {
            offsets,
            bytes,
            nulls,
            ..
        } => {
            if !present(nulls) {
                return Ok(PartitionValue::Null);
            }
            let start = *offsets.get(row).ok_or("partition row missing")? as usize;
            let end = *offsets.get(row + 1).ok_or("partition row missing")? as usize;
            Ok(PartitionValue::Bytes(
                bytes
                    .get(start..end)
                    .ok_or("partition value out of range")?,
            ))
        }
        WcolColumn::DictId {
            ids,
            nulls,
            dictionary,
            ..
        } => {
            if !present(nulls) {
                return Ok(PartitionValue::Null);
            }
            let id = *ids.get(row).ok_or("partition row missing")? as usize;
            if let Some(entries) = dictionary.int_entries() {
                return Ok(PartitionValue::Int(
                    *entries.get(id).ok_or("dictionary id out of range")?,
                ));
            }
            let (offsets, bytes) = dictionary
                .byte_entries()
                .ok_or("partitionBy on a floating-point column is not supported")?;
            let start = *offsets.get(id).ok_or("dictionary id out of range")? as usize;
            let end = *offsets.get(id + 1).ok_or("dictionary id out of range")? as usize;
            Ok(PartitionValue::Bytes(
                bytes
                    .get(start..end)
                    .ok_or("dictionary value out of range")?,
            ))
        }
        WcolColumn::F64 { .. } => {
            Err("partitionBy on a floating-point column is not supported".to_string())
        }
    }
}

impl Operator for PartitionedParquetEncode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn poll(&mut self, io: &mut OperatorIo<'_, '_>) -> Result<StreamProgress, String> {
        *io.bytes_written = 0;
        if self.sent < self.frames.len() {
            return Ok(self.send(io));
        }
        if self.finished {
            return Ok(StreamProgress::Finished);
        }
        if let Some(item) = io.input_slot.as_mut().and_then(|slot| slot.take()) {
            self.push_batch(&item.into_batch())?;
        } else if let Some(item) = io.input {
            self.push_batch(item.batch())?;
        } else if io.input_eof {
            self.close_all()?;
            self.finished = true;
        } else {
            return Ok(StreamProgress::NeedInput);
        }
        if self.sent < self.frames.len() {
            return Ok(self.send(io));
        }
        Ok(if self.finished {
            StreamProgress::Finished
        } else {
            StreamProgress::NeedInput
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_values_are_hive_segments() {
        assert_eq!(
            partition_path_value(PartitionValue::Null),
            "__HIVE_DEFAULT_PARTITION__"
        );
        assert_eq!(partition_path_value(PartitionValue::Int(-42)), "-42");
        assert_eq!(
            partition_path_value(PartitionValue::Bytes(b"a/b=c d.e_f-g")),
            "a%2Fb%3Dc%20d.e_f-g"
        );
        assert_eq!(
            partition_path_value(PartitionValue::Bytes("é".as_bytes())),
            "%C3%A9"
        );
    }
}
