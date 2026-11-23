//! Physical stream programs (v2 operator byte sequences).

use std::collections::HashSet;

use crate::error::{WcolError, WcolResult};
use crate::formats::parquet::schema::Plan;

/// Decode memory caps (Wasm-safe; no env).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum IngestMemoryProfile {
    #[default]
    ExecutionGroup = 0,
    /// [`ScanMemoryBudget::q3_acceptance_restrictive`] per row group (I-09 gate).
    Q3Restrictive = 1,
}

impl IngestMemoryProfile {
    pub fn decode(byte: u8) -> WcolResult<Self> {
        match byte {
            0 => Ok(Self::ExecutionGroup),
            1 => Ok(Self::Q3Restrictive),
            _ => Err(WcolError::from("unknown ingest memory profile")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IngestGroupChunk {
    pub column: u32,
    pub offset: u64,
    pub codec: i32,
    pub length: u32,
    /// Parquet column chunk includes a dictionary page (preserve `DictId` in decode).
    pub dict_encoded: bool,
}

#[derive(Clone, Debug)]
pub struct IngestGroup {
    pub row_group: u32,
    pub rows: u64,
    pub chunks: Vec<IngestGroupChunk>,
}

/// Column ids that should use dictionary-preserving decode for this ingest plan.
pub fn dict_columns_from_ingest_groups(groups: &[IngestGroup]) -> HashSet<u32> {
    let mut out = HashSet::new();
    for group in groups {
        for chunk in &group.chunks {
            if chunk.dict_encoded {
                out.insert(chunk.column);
            }
        }
    }
    out
}

pub fn decode_groups_from_ingest(
    groups: &[IngestGroup],
) -> Vec<(u32, u64, Vec<(u32, u64, i32, u32)>)> {
    groups
        .iter()
        .map(|group| {
            (
                group.row_group,
                group.rows,
                group
                    .chunks
                    .iter()
                    .map(|chunk| (chunk.column, chunk.offset, chunk.codec, chunk.length))
                    .collect(),
            )
        })
        .collect()
}

pub fn ingest_groups_from_plan(plan: &Plan, column_ids: &[u32]) -> Vec<IngestGroup> {
    if plan.chunks.is_empty() {
        return Vec::new();
    }
    let row_groups = plan
        .chunks
        .iter()
        .map(|c| c.row_group)
        .max()
        .map(|max| max + 1)
        .unwrap_or(0) as usize;
    let mut out = Vec::with_capacity(row_groups);
    for row_group in 0..row_groups {
        let mut rows = 0u64;
        let mut chunks = Vec::new();
        for chunk in plan
            .chunks
            .iter()
            .filter(|c| c.row_group as usize == row_group)
        {
            rows = chunk.rows.max(0) as u64;
            if column_ids.is_empty() || column_ids.contains(&chunk.leaf) {
                if !column_ids.is_empty() {
                    chunks.push(IngestGroupChunk {
                        column: chunk.leaf,
                        offset: chunk.start,
                        codec: chunk.codec,
                        length: chunk.compressed_size as u32,
                        dict_encoded: chunk.has_dictionary,
                    });
                }
            }
        }
        out.push(IngestGroup {
            row_group: row_group as u32,
            rows,
            chunks,
        });
    }
    out
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod dict_plan_tests {
    use super::*;
    use crate::formats::parquet::open_plan;
    use compact_parquet_thrift::encode::{
        scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
    };

    #[test]
    fn ingest_groups_mark_dictionary_columns_from_footer() {
        let dictionary = [10i32, 20, 30];
        let ids: Vec<u32> = vec![0, 1, 2];
        let column = Column {
            name: "v",
            rows: 3,
            codec: SNAPPY,
            present: None,
            values: ColumnValues::DictI32 {
                dictionary: &dictionary,
                ids: &ids,
            },
        };
        let mut places = [ChunkPlace::default()];
        let mut scratch = vec![0u8; scratch_len(3, 64).unwrap()];
        let mut table = vec![0u32; 256];
        let mut file_bytes = vec![0u8; 64 * 1024];
        let n = write_file(
            &mut file_bytes,
            3,
            std::slice::from_ref(&column),
            &mut places,
            &mut scratch,
            &mut table,
        )
        .unwrap();
        file_bytes.truncate(n);
        let path = std::env::temp_dir().join(format!(
            "wcol-ingest-dict-{}-{}.parquet",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, &file_bytes).unwrap();

        let (_file, plan) = open_plan(&path).unwrap();
        let groups = ingest_groups_from_plan(&plan, &[0]);
        assert!(groups[0].chunks[0].dict_encoded);
        let dict = dict_columns_from_ingest_groups(&groups);
        assert!(dict.contains(&0));
        let _ = std::fs::remove_file(path);
    }
}
