//! Cached Parquet footer metadata the planner reads instead of files.

use super::*;

/// One column chunk from cached footer metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkMeta {
    pub column: u32,
    pub row_group: u32,
    pub offset: u64,
    pub length: u64,
    pub rows: u64,
    /// The data span already contains the dictionary page.
    pub dictionary_in_span: bool,
    /// Dictionary page that is not covered by [`Self::offset`] and [`Self::length`].
    pub dictionary_page: Option<ByteSpan>,
    /// Parquet compression codec of the column chunk (a decoder input).
    pub codec: i32,
    /// Footer integer `(min, max)` of the chunk's non-null values (see [`int_range`]).
    pub int_range: Option<(i64, i64)>,
}

/// A chunk's integer bounds, read as signed like the engine reads INT32 and INT64. A writer
/// that orders an unsigned column unsigned gives a max below the min when the chunk mixes
/// values either side of the sign bit; such bounds are dropped. Otherwise the two orders
/// agree on the chunk's values.
pub(super) fn int_range(min: Option<i64>, max: Option<i64>) -> Option<(i64, i64)> {
    match (min, max) {
        (Some(min), Some(max)) if min <= max => Some((min, max)),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParquetObject {
    pub object_id: String,
    pub chunks: Vec<ChunkMeta>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableMeta {
    pub table: String,
    pub objects: Vec<ParquetObject>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ParquetMetadataCatalog {
    pub tables: Vec<TableMeta>,
    /// Parquet physical type per leaf, parallel to the footer. Empty means unknown:
    /// a hash exchange is not chosen, because a text group key must not be routed by guess.
    pub leaf_physical: Vec<i32>,
    /// Per-leaf distinct-count upper bound from footer statistics. Missing slots
    /// or `None` mean that leaf has no bound the planner should trust.
    pub leaf_distinct_upper: Vec<Option<u64>>,
    /// Largest dictionary page per leaf, `None` when some chunk has none. See
    /// [`few_valued_key`].
    pub leaf_dictionary_bytes: Vec<Option<u64>>,
    /// Footer chunk statistics: integer min per leaf when available.
    pub leaf_min_i64: Vec<Option<i64>>,
    /// Footer chunk statistics: integer max per leaf when available.
    pub leaf_max_i64: Vec<Option<i64>>,
    /// Footer chunk statistics: lexicographic min per leaf when available.
    pub leaf_min_bytes: Vec<Option<Vec<u8>>>,
    /// Footer chunk statistics: lexicographic max per leaf when available.
    pub leaf_max_bytes: Vec<Option<Vec<u8>>>,
}

impl ParquetMetadataCatalog {
    /// One table backed by one Parquet object, from that object's cached footer.
    pub fn from_footer(
        table: &str,
        object_id: &str,
        footer: &crate::formats::parquet::Plan,
    ) -> Self {
        Self::from_footers(table, &[(object_id, footer)])
    }

    /// One table backed by `objects` (same schema), in job-source order. Column statistics
    /// cover every object's chunks, so a table split into parts plans like the whole file.
    /// The column a scan fills with each row's id: the one after the last leaf. `None` when
    /// the leaves are unknown.
    pub fn row_id_column(&self) -> Option<u32> {
        (!self.leaf_physical.is_empty()).then_some(self.leaf_physical.len() as u32)
    }

    pub fn from_footers(table: &str, objects: &[(&str, &crate::formats::parquet::Plan)]) -> Self {
        let all = crate::formats::parquet::Plan {
            rows: objects.iter().map(|(_, footer)| footer.rows).sum(),
            leaves: objects
                .first()
                .map(|(_, footer)| footer.leaves.clone())
                .unwrap_or_default(),
            chunks: objects
                .iter()
                .flat_map(|(_, footer)| footer.chunks.iter().cloned())
                .collect(),
        };
        let (leaf_min_i64, leaf_max_i64) = crate::formats::parquet::leaf_int_bounds(&all);
        let (leaf_min_bytes, leaf_max_bytes) = crate::formats::parquet::leaf_byte_bounds(&all);
        Self {
            tables: vec![TableMeta {
                table: table.to_string(),
                objects: objects
                    .iter()
                    .map(|(object_id, footer)| ParquetObject {
                        object_id: object_id.to_string(),
                        chunks: footer
                            .chunks
                            .iter()
                            .map(|chunk| ChunkMeta {
                                column: chunk.leaf,
                                row_group: chunk.row_group,
                                offset: chunk.start,
                                length: chunk.compressed_size,
                                rows: chunk.rows.max(0) as u64,
                                dictionary_in_span: chunk.has_dictionary,
                                dictionary_page: None,
                                codec: chunk.codec,
                                int_range: int_range(chunk.min_i64, chunk.max_i64),
                            })
                            .collect(),
                    })
                    .collect(),
            }],
            leaf_physical: all.leaves.iter().map(|leaf| leaf.physical).collect(),
            leaf_distinct_upper: crate::formats::parquet::leaf_distinct_upper(&all),
            leaf_dictionary_bytes: crate::formats::parquet::leaf_dictionary_bytes(&all),
            leaf_min_i64,
            leaf_max_i64,
            leaf_min_bytes,
            leaf_max_bytes,
        }
    }
}
