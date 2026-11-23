use super::selection_mask::SelectionMask;
use super::wcol_batch::{DictEntries, DictionaryValues, WcolBatch, WcolColumn};
use crate::error::{WcolError, WcolResult};
use crate::stream::partials::{PartialSketchColumn, PartialStateSchemaV1, PartialStringKeyColumn};

#[derive(Clone)]
enum OwnedColumn {
    I64 {
        column: u32,
        values: Vec<i64>,
        nulls: Vec<u8>,
    },
    F64 {
        column: u32,
        values: Vec<f64>,
        nulls: Vec<u8>,
    },
    DictId {
        column: u32,
        ids: Vec<u32>,
        nulls: Vec<u8>,
        dict: DictionaryValues,
    },
    Utf8 {
        column: u32,
        offsets: Vec<u32>,
        bytes: Vec<u8>,
        nulls: Vec<u8>,
    },
}

/// Owned batch for one poll step (local WCOL edge).
///
/// Row batches from Parquet/decode have no partial metadata. Aggregate partial state uses the
/// same column layout with [`Self::partial_meta`] set (synthetic `PARTIAL_*` column ids).
#[derive(Clone, Default)]
pub struct OwnedWcolBatch {
    row_count: usize,
    dict_scope: u32,
    columns: Vec<OwnedColumn>,
    partial_schema: Option<PartialStateSchemaV1>,
    partial_passed_rows: u64,
    /// `approx_count_distinct` sketches of a partial-state batch (not WCOL columns).
    partial_sketches: Vec<PartialSketchColumn>,
    partial_string_keys: Vec<PartialStringKeyColumn>,
    /// Set by filter; aggregate reads via [`Self::effective_selection_mask`].
    selection: Option<SelectionMask>,
}

impl OwnedWcolBatch {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.row_count = 0;
        self.columns.clear();
        self.partial_schema = None;
        self.partial_passed_rows = 0;
        self.partial_sketches.clear();
        self.partial_string_keys.clear();
        self.selection = None;
    }

    pub fn clear_selection_mask(&mut self) {
        self.selection = None;
    }

    pub fn set_selection_mask(&mut self, mask: SelectionMask) {
        debug_assert_eq!(mask.rows(), self.row_count);
        if mask.all_selected() {
            self.selection = None;
        } else {
            self.selection = Some(mask);
        }
    }

    pub fn selection_mask(&self) -> Option<&SelectionMask> {
        self.selection.as_ref()
    }

    pub fn effective_selection_mask(&self) -> SelectionMask {
        self.selection
            .clone()
            .unwrap_or_else(|| SelectionMask::all(self.row_count))
    }

    pub fn copy_stream_meta_from(&mut self, other: &OwnedWcolBatch) {
        self.copy_partial_meta_from(other);
        self.selection = other.selection.clone();
    }

    pub fn is_row_batch(&self) -> bool {
        self.partial_schema.is_none()
    }

    pub fn is_partial_batch(&self) -> bool {
        self.partial_schema.is_some()
    }

    pub fn partial_meta(&self) -> Option<(&PartialStateSchemaV1, u64)> {
        self.partial_schema
            .as_ref()
            .map(|schema| (schema, self.partial_passed_rows))
    }

    pub fn set_partial_meta(&mut self, schema: PartialStateSchemaV1, passed_rows: u64) {
        self.partial_schema = Some(schema);
        self.partial_passed_rows = passed_rows;
    }

    pub fn copy_partial_meta_from(&mut self, other: &OwnedWcolBatch) {
        self.partial_schema = other.partial_schema.clone();
        self.partial_passed_rows = other.partial_passed_rows;
        self.partial_sketches = other.partial_sketches.clone();
        self.partial_string_keys = other.partial_string_keys.clone();
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn dict_scope(&self) -> u32 {
        self.dict_scope
    }

    /// Rewrite each dictionary-id column in place: `remap(column, ids, nulls, dictionary)`.
    pub fn remap_dict_ids<E>(
        &mut self,
        mut remap: impl FnMut(u32, &mut [u32], &[u8], &mut DictionaryValues) -> Result<(), E>,
    ) -> Result<(), E> {
        for col in &mut self.columns {
            if let OwnedColumn::DictId {
                column,
                ids,
                nulls,
                dict,
            } = col
            {
                remap(*column, ids, nulls, dict)?;
            }
        }
        Ok(())
    }

    pub fn set_scope(&mut self, scope: u32) {
        self.dict_scope = scope;
    }

    pub fn push_i64_column(&mut self, column: u32, values: Vec<i64>, nulls: Vec<u8>) {
        self.columns.push(OwnedColumn::I64 {
            column,
            values,
            nulls,
        });
    }

    pub fn push_f64_column(&mut self, column: u32, values: Vec<f64>, nulls: Vec<u8>) {
        self.columns.push(OwnedColumn::F64 {
            column,
            values,
            nulls,
        });
    }

    pub fn push_dict_id_column(
        &mut self,
        column: u32,
        ids: Vec<u32>,
        nulls: Vec<u8>,
        dict: DictionaryValues,
    ) {
        self.columns.push(OwnedColumn::DictId {
            column,
            ids,
            nulls,
            dict,
        });
    }

    pub fn push_utf8_column(
        &mut self,
        column: u32,
        offsets: Vec<u32>,
        bytes: Vec<u8>,
        nulls: Vec<u8>,
    ) {
        self.columns.push(OwnedColumn::Utf8 {
            column,
            offsets,
            bytes,
            nulls,
        });
    }

    /// This batch with its large integer and float dictionary columns as plain values, or
    /// `None` when it has none. A hash exchange sends each partition a small share of the
    /// rows but the whole row-group dictionary, which the receiver keeps per producer. A
    /// dictionary with more entries than the batch has rows costs more than its values
    /// (eg. near-unique `WatchID`); a small one (eg. `RegionID`) stays cheaper as ids.
    /// Byte dictionaries stay: their entries can be much wider than an id.
    pub fn with_numeric_dicts_expanded(&self) -> Result<Option<Self>, String> {
        let rows = self.row_count;
        let numeric: Vec<u32> = self
            .columns
            .iter()
            .filter_map(|col| match col {
                OwnedColumn::DictId { column, dict, .. }
                    if (dict.int_entries().is_some() || dict.float_entries().is_some())
                        && dict.entry_count() > rows =>
                {
                    Some(*column)
                }
                _ => None,
            })
            .collect();
        if numeric.is_empty() {
            return Ok(None);
        }
        let mut out = self.clone();
        out.expand_dicts(&numeric)?;
        Ok(Some(out))
    }

    /// Replace the dictionary columns `columns` with their plain values.
    pub fn expand_dicts(&mut self, columns: &[u32]) -> Result<(), String> {
        for col in &mut self.columns {
            let OwnedColumn::DictId {
                column,
                ids,
                nulls,
                dict,
            } = col
            else {
                continue;
            };
            if !columns.contains(column) {
                continue;
            }
            let (column, nulls) = (*column, std::mem::take(nulls));
            // A null row's id is not an index; it expands to zero (or an empty string).
            let expanded = match &dict.entries {
                DictEntries::Int(entries) => OwnedColumn::I64 {
                    column,
                    values: expand_ids(ids, &nulls, entries)?,
                    nulls,
                },
                DictEntries::Float(entries) => OwnedColumn::F64 {
                    column,
                    values: expand_ids(ids, &nulls, entries)?,
                    nulls,
                },
                DictEntries::Bytes { offsets, bytes } => {
                    let (offsets, bytes) = expand_byte_ids(ids, &nulls, offsets, bytes)?;
                    OwnedColumn::Utf8 {
                        column,
                        offsets,
                        bytes,
                        nulls,
                    }
                }
            };
            *col = expanded;
        }
        Ok(())
    }

    /// All columns must be i64 (partial-state batches never use dictionary columns).
    pub fn export_i64_columns(&self) -> Result<Vec<(u32, Vec<i64>, Vec<u8>)>, String> {
        let mut out = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            match col {
                OwnedColumn::I64 {
                    column,
                    values,
                    nulls,
                } => out.push((*column, values.clone(), nulls.clone())),
                OwnedColumn::F64 { .. } => {
                    return Err("partial batch must not contain float columns".to_string());
                }
                OwnedColumn::DictId { .. } => {
                    return Err("partial batch must not contain dictionary columns".to_string());
                }
                OwnedColumn::Utf8 { .. } => {
                    return Err("partial batch must not contain utf8 columns".to_string());
                }
            }
        }
        out.sort_by_key(|(id, _, _)| *id);
        Ok(out)
    }

    pub fn set_partial_sketches(&mut self, sketches: Vec<PartialSketchColumn>) {
        self.partial_sketches = sketches;
    }

    pub fn partial_sketches(&self) -> &[PartialSketchColumn] {
        &self.partial_sketches
    }

    pub fn set_partial_string_keys(&mut self, keys: Vec<PartialStringKeyColumn>) {
        self.partial_string_keys = keys;
    }

    pub fn partial_string_keys(&self) -> &[PartialStringKeyColumn] {
        &self.partial_string_keys
    }

    /// Move out a partial batch's columns and sketches.
    pub fn into_partial_columns(
        mut self,
    ) -> Result<(Vec<(u32, Vec<i64>, Vec<u8>)>, Vec<PartialSketchColumn>), String> {
        let sketches = std::mem::take(&mut self.partial_sketches);
        Ok((self.into_i64_columns()?, sketches))
    }

    /// Like [`Self::export_i64_columns`] but moves the columns out.
    pub fn into_i64_columns(self) -> Result<Vec<(u32, Vec<i64>, Vec<u8>)>, String> {
        let mut out = Vec::with_capacity(self.columns.len());
        for col in self.columns {
            match col {
                OwnedColumn::I64 {
                    column,
                    values,
                    nulls,
                } => out.push((column, values, nulls)),
                OwnedColumn::F64 { .. } => {
                    return Err("partial batch must not contain float columns".to_string());
                }
                OwnedColumn::DictId { .. } => {
                    return Err("partial batch must not contain dictionary columns".to_string());
                }
                OwnedColumn::Utf8 { .. } => {
                    return Err("partial batch must not contain utf8 columns".to_string());
                }
            }
        }
        out.sort_by_key(|(id, _, _)| *id);
        Ok(out)
    }

    pub fn set_row_count(&mut self, rows: usize) -> WcolResult<()> {
        for col in &self.columns {
            let len = match col {
                OwnedColumn::I64 { values, .. } => values.len(),
                OwnedColumn::F64 { values, .. } => values.len(),
                OwnedColumn::DictId { ids, .. } => ids.len(),
                OwnedColumn::Utf8 { nulls, .. } => nulls.len(),
            };
            if len != rows && rows != 0 {
                return Err(WcolError::from("misaligned batch columns"));
            }
        }
        self.row_count = rows;
        Ok(())
    }
}

impl OwnedWcolBatch {
    /// Append row indices from `src` (ignores `src` selection mask; caller filters rows).
    pub fn append_rows_from(&mut self, src: &OwnedWcolBatch, rows: &[usize]) -> Result<(), String> {
        if rows.is_empty() {
            return Ok(());
        }
        if self.columns.is_empty() && self.row_count == 0 {
            self.init_columns_like(src)?;
        }
        let mut col_view = Vec::new();
        let view = src.view_with(&mut col_view);
        for &row in rows {
            if row >= view.row_count {
                return Err("gather row out of range".to_string());
            }
            self.push_row_from_view(&view, row)?;
        }
        Ok(())
    }

    fn init_columns_like(&mut self, template: &OwnedWcolBatch) -> Result<(), String> {
        if !self.columns.is_empty() {
            return Err("gather target already has columns".to_string());
        }
        self.dict_scope = template.dict_scope;
        for col in &template.columns {
            match col {
                OwnedColumn::I64 { column, .. } => {
                    self.push_i64_column(*column, Vec::new(), Vec::new());
                }
                OwnedColumn::F64 { column, .. } => {
                    self.push_f64_column(*column, Vec::new(), Vec::new());
                }
                OwnedColumn::DictId { column, dict, .. } => {
                    self.push_dict_id_column(*column, Vec::new(), Vec::new(), dict.clone());
                }
                OwnedColumn::Utf8 { column, .. } => {
                    self.push_utf8_column(*column, vec![0], Vec::new(), Vec::new());
                }
            }
        }
        Ok(())
    }

    fn push_row_from_view(&mut self, view: &WcolBatch<'_>, row: usize) -> Result<(), String> {
        for col in view.columns {
            match col {
                WcolColumn::I64 {
                    column,
                    values,
                    nulls,
                } => {
                    let dest = self
                        .column_mut_i64(*column)
                        .ok_or("gather column id mismatch")?;
                    dest.1.push(values[row]);
                    dest.2.push(nulls[row]);
                }
                WcolColumn::F64 {
                    column,
                    values,
                    nulls,
                } => {
                    let dest = self
                        .column_mut_f64(*column)
                        .ok_or("gather column id mismatch")?;
                    dest.1.push(values[row]);
                    dest.2.push(nulls[row]);
                }
                WcolColumn::DictId {
                    column, ids, nulls, ..
                } => {
                    let dest = self
                        .column_mut_dict(*column)
                        .ok_or("gather column id mismatch")?;
                    dest.1.push(ids[row]);
                    dest.2.push(nulls[row]);
                }
                WcolColumn::Utf8 {
                    column,
                    offsets,
                    bytes,
                    nulls,
                } => {
                    let dest = self
                        .column_mut_utf8(*column)
                        .ok_or("gather column id mismatch")?;
                    let start = offsets[row] as usize;
                    let end = offsets[row + 1] as usize;
                    dest.2.extend_from_slice(&bytes[start..end]);
                    dest.1.push(
                        u32::try_from(dest.2.len())
                            .map_err(|_| "utf8 gather overflow".to_string())?,
                    );
                    dest.3.push(nulls[row]);
                }
            }
        }
        let next = self.row_count + 1;
        self.set_row_count(next).map_err(|e| e.to_string())
    }

    fn column_mut_i64(&mut self, column: u32) -> Option<(&mut u32, &mut Vec<i64>, &mut Vec<u8>)> {
        for col in &mut self.columns {
            if let OwnedColumn::I64 {
                column: id,
                values,
                nulls,
            } = col
            {
                if *id == column {
                    return Some((id, values, nulls));
                }
            }
        }
        None
    }

    fn column_mut_f64(&mut self, column: u32) -> Option<(&mut u32, &mut Vec<f64>, &mut Vec<u8>)> {
        for col in &mut self.columns {
            if let OwnedColumn::F64 {
                column: id,
                values,
                nulls,
            } = col
            {
                if *id == column {
                    return Some((id, values, nulls));
                }
            }
        }
        None
    }

    fn column_mut_dict(&mut self, column: u32) -> Option<(&mut u32, &mut Vec<u32>, &mut Vec<u8>)> {
        for col in &mut self.columns {
            if let OwnedColumn::DictId {
                column: id,
                ids,
                nulls,
                ..
            } = col
            {
                if *id == column {
                    return Some((id, ids, nulls));
                }
            }
        }
        None
    }

    fn column_mut_utf8(
        &mut self,
        column: u32,
    ) -> Option<(&mut u32, &mut Vec<u32>, &mut Vec<u8>, &mut Vec<u8>)> {
        for col in &mut self.columns {
            if let OwnedColumn::Utf8 {
                column: id,
                offsets,
                bytes,
                nulls,
            } = col
            {
                if *id == column {
                    return Some((id, offsets, bytes, nulls));
                }
            }
        }
        None
    }

    /// Build a batch view whose column slices borrow `self`.
    pub fn view_with<'a>(&'a self, out: &'a mut Vec<WcolColumn<'a>>) -> WcolBatch<'a> {
        out.clear();
        for col in &self.columns {
            out.push(match col {
                OwnedColumn::I64 {
                    column,
                    values,
                    nulls,
                } => WcolColumn::I64 {
                    column: *column,
                    values: values.as_slice(),
                    nulls: nulls.as_slice(),
                },
                OwnedColumn::F64 {
                    column,
                    values,
                    nulls,
                } => WcolColumn::F64 {
                    column: *column,
                    values: values.as_slice(),
                    nulls: nulls.as_slice(),
                },
                OwnedColumn::DictId {
                    column,
                    ids,
                    nulls,
                    dict,
                } => WcolColumn::DictId {
                    column: *column,
                    ids: ids.as_slice(),
                    nulls: nulls.as_slice(),
                    dictionary: dict,
                },
                OwnedColumn::Utf8 {
                    column,
                    offsets,
                    bytes,
                    nulls,
                } => WcolColumn::Utf8 {
                    column: *column,
                    offsets: offsets.as_slice(),
                    bytes: bytes.as_slice(),
                    nulls: nulls.as_slice(),
                },
            });
        }
        WcolBatch {
            row_count: self.row_count,
            dict_scope: self.dict_scope,
            columns: out.as_slice(),
        }
    }
}

/// Offsets and bytes of each row's dictionary string; a null row is empty.
fn expand_byte_ids(
    ids: &[u32],
    nulls: &[u8],
    entry_offsets: &[u32],
    entry_bytes: &[u8],
) -> Result<(Vec<u32>, Vec<u8>), String> {
    let mut offsets = Vec::with_capacity(ids.len() + 1);
    let mut bytes = Vec::new();
    offsets.push(0);
    for (&id, &present) in ids.iter().zip(nulls) {
        if present != 0 {
            let id = id as usize;
            let (start, end) = entry_offsets
                .get(id)
                .zip(entry_offsets.get(id + 1))
                .ok_or_else(|| format!("dictionary id {id} out of range"))?;
            bytes.extend_from_slice(&entry_bytes[*start as usize..*end as usize]);
        }
        let end = u32::try_from(bytes.len()).map_err(|_| "expanded strings exceed 4 GiB")?;
        offsets.push(end);
    }
    Ok((offsets, bytes))
}

fn expand_ids<T: Copy + Default>(
    ids: &[u32],
    nulls: &[u8],
    entries: &[T],
) -> Result<Vec<T>, String> {
    ids.iter()
        .zip(nulls)
        .map(|(&id, &present)| {
            if present == 0 {
                return Ok(T::default());
            }
            entries.get(id as usize).copied().ok_or_else(|| {
                format!(
                    "dictionary id {id} out of range ({} entries)",
                    entries.len()
                )
            })
        })
        .collect()
}
