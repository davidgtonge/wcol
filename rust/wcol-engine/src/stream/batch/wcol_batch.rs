//! Source-agnostic WCOL batch views (Parquet decode, `.wcol` file, WCS decode).

use std::sync::Arc;

/// Dictionary payload installed once per column chunk.
///
/// Storage is `Arc<Vec<_>>` so a stream can append chunks in place when this
/// dictionary is not shared yet. Readers still see a stable slice.
#[derive(Clone, Debug, PartialEq)]
pub enum DictEntries {
    Int(Arc<Vec<i64>>),
    /// FLOAT and DOUBLE dictionary entries, widened to `f64`.
    Float(Arc<Vec<f64>>),
    Bytes {
        /// `len == entry_count + 1`; entry `i` is `bytes[offsets[i]..offsets[i+1]]`.
        offsets: Arc<Vec<u32>>,
        bytes: Arc<Vec<u8>>,
    },
}

/// `(offsets, bytes)` of a byte-array dictionary; see [`DictEntries::Bytes`].
pub type ByteEntries<'a> = (&'a Arc<Vec<u32>>, &'a Arc<Vec<u8>>);

/// One installed dictionary for a `(dict_scope, column_id)` pair.
#[derive(Clone, Debug, PartialEq)]
pub struct DictionaryValues {
    pub scope: u32,
    pub column: u32,
    pub entries: DictEntries,
}

impl DictionaryValues {
    pub fn int_entries(&self) -> Option<&Arc<Vec<i64>>> {
        match &self.entries {
            DictEntries::Int(entries) => Some(entries),
            DictEntries::Float(_) | DictEntries::Bytes { .. } => None,
        }
    }

    pub fn float_entries(&self) -> Option<&Arc<Vec<f64>>> {
        match &self.entries {
            DictEntries::Float(entries) => Some(entries),
            DictEntries::Int(_) | DictEntries::Bytes { .. } => None,
        }
    }

    pub fn byte_entries(&self) -> Option<ByteEntries<'_>> {
        match &self.entries {
            DictEntries::Int(_) | DictEntries::Float(_) => None,
            DictEntries::Bytes { offsets, bytes } => Some((offsets, bytes)),
        }
    }

    pub fn entry_count(&self) -> usize {
        match &self.entries {
            DictEntries::Int(entries) => entries.len(),
            DictEntries::Float(entries) => entries.len(),
            DictEntries::Bytes { offsets, .. } => offsets.len().saturating_sub(1),
        }
    }

    /// The entries at `ids`, in that order, under the same scope and column.
    pub fn subset(&self, ids: &[u32]) -> Result<Self, String> {
        let count = self.entry_count();
        if let Some(bad) = ids.iter().find(|id| **id as usize >= count) {
            return Err(format!(
                "dictionary id {bad} out of range ({count} entries)"
            ));
        }
        let entries = match &self.entries {
            DictEntries::Int(values) => DictEntries::Int(Arc::new(
                ids.iter().map(|id| values[*id as usize]).collect(),
            )),
            DictEntries::Float(values) => DictEntries::Float(Arc::new(
                ids.iter().map(|id| values[*id as usize]).collect(),
            )),
            DictEntries::Bytes { offsets, bytes } => {
                let mut out_offsets = Vec::with_capacity(ids.len() + 1);
                let mut out_bytes = Vec::new();
                out_offsets.push(0u32);
                for id in ids {
                    let at = *id as usize;
                    out_bytes
                        .extend_from_slice(&bytes[offsets[at] as usize..offsets[at + 1] as usize]);
                    out_offsets
                        .push(u32::try_from(out_bytes.len()).map_err(|_| "dictionary too large")?);
                }
                DictEntries::Bytes {
                    offsets: Arc::new(out_offsets),
                    bytes: Arc::new(out_bytes),
                }
            }
        };
        Ok(Self {
            scope: self.scope,
            column: self.column,
            entries,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WcolValueKind {
    I64,
    F64,
    DictId,
    Utf8,
}

/// Borrowed column in a batch. No expansion: dict ids reference `dictionaries`.
#[derive(Debug, Clone, Copy)]
pub enum WcolColumn<'a> {
    I64 {
        column: u32,
        values: &'a [i64],
        nulls: &'a [u8],
    },
    /// FLOAT widened to DOUBLE, and DOUBLE, as real `f64` values.
    F64 {
        column: u32,
        values: &'a [f64],
        nulls: &'a [u8],
    },
    DictId {
        column: u32,
        ids: &'a [u32],
        nulls: &'a [u8],
        dictionary: &'a DictionaryValues,
    },
    /// PLAIN BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY rows in one contiguous arena.
    Utf8 {
        column: u32,
        /// Row `r` is `bytes[offsets[r]..offsets[r+1]]` (valid when `nulls[r] != 0`).
        offsets: &'a [u32],
        bytes: &'a [u8],
        nulls: &'a [u8],
    },
}

/// Row-aligned batch passed between decode, query operators, and sinks.
#[derive(Debug, Clone, Copy)]
pub struct WcolBatch<'a> {
    pub row_count: usize,
    pub dict_scope: u32,
    pub columns: &'a [WcolColumn<'a>],
}

impl<'a> WcolBatch<'a> {
    pub fn slot_i64(&self, column: u32) -> Option<(&'a [i64], &'a [u8])> {
        for col in self.columns {
            if let WcolColumn::I64 {
                column: id,
                values,
                nulls,
            } = col
            {
                if *id == column {
                    return Some((values, nulls));
                }
            }
        }
        None
    }
}
