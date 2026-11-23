//! Physical page-copy compaction planner (merge row groups, no decode).

use crate::formats::parquet::schema::{Leaf, Plan};
use crate::ops::query::CatalogColumn;
use crate::plan::distributed::{task_id_for_shard, SourceVersion, TaskId};
use crate::plan::sql_parser::ColumnKind;
use crate::plan::stream_program::{encode_catalog, OP_PAGE_COMPACT, PLAN_V2};

const DEFAULT_TARGET_OUTPUT_BYTES: u64 = 600 * 1024 * 1024;
const DEFAULT_TARGET_ROW_GROUP_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct PageCompactionPolicy {
    pub target_output_bytes: u64,
    pub target_row_group_bytes: u64,
    pub output_parts: usize,
}

impl Default for PageCompactionPolicy {
    fn default() -> Self {
        Self {
            target_output_bytes: DEFAULT_TARGET_OUTPUT_BYTES,
            target_row_group_bytes: DEFAULT_TARGET_ROW_GROUP_BYTES,
            output_parts: 4,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CompactionInputFile {
    pub object_key: String,
    pub file_size: u64,
    pub plan: Plan,
}

pub use crate::program::page_copy::{CopySegment, MergedColumnPlan, MergedRowGroupPlan};

#[derive(Clone, Debug)]
pub struct PageCompactionPartPlan {
    pub part_index: usize,
    pub task_id: TaskId,
    pub input_indices: Vec<usize>,
    pub merged_row_groups: Vec<MergedRowGroupPlan>,
    pub rows: u64,
    pub encoded_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct PageCompactionPlan {
    pub source: SourceVersion,
    pub policy: PageCompactionPolicy,
    pub inputs: Vec<CompactionInputFile>,
    pub parts: Vec<PageCompactionPartPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageCompactionPartRecord {
    pub part_index: usize,
    pub object_key: String,
    pub rows: u64,
    pub bytes: u64,
    pub output_row_groups: u32,
    pub task_id: TaskId,
}

struct FlatRowGroup {
    input_index: usize,
    row_group: u32,
    size_bytes: u64,
}

pub fn validate_compaction_inputs(inputs: &[CompactionInputFile]) -> Result<(), String> {
    if inputs.is_empty() {
        return Err("compaction requires at least one input".to_string());
    }
    let expected = &inputs[0].plan.leaves;
    for (index, input) in inputs.iter().enumerate() {
        if input.plan.leaves != *expected {
            return Err(format!("input {index} schema differs from input 0"));
        }
        for chunk in &input.plan.chunks {
            if chunk.has_dictionary {
                return Err(format!(
                    "input {index} row group {} has dictionary pages",
                    chunk.row_group
                ));
            }
            if chunk.has_page_index {
                return Err(format!(
                    "input {index} row group {}: column indexes or bloom filters must be stripped before compaction",
                    chunk.row_group
                ));
            }
            if chunk.codec != 0 {
                return Err(format!(
                    "input {index} row group {} uses unsupported codec {}",
                    chunk.row_group, chunk.codec
                ));
            }
        }
    }
    Ok(())
}

pub fn plan_page_compaction(
    source: SourceVersion,
    job_key: &str,
    inputs: Vec<CompactionInputFile>,
    policy: PageCompactionPolicy,
) -> Result<PageCompactionPlan, String> {
    validate_compaction_inputs(&inputs)?;
    let leaf_count = inputs[0].plan.leaves.len();
    if leaf_count == 0 {
        return Err("empty schema".to_string());
    }
    let mut flat = Vec::new();
    for (input_index, input) in inputs.iter().enumerate() {
        let groups = row_group_count(&input.plan);
        for row_group in 0..groups {
            row_group_rows(&input.plan, row_group as u32)?;
            let size_bytes = row_group_byte_size(&input.plan, row_group as u32)?;
            flat.push(FlatRowGroup {
                input_index,
                row_group: row_group as u32,
                size_bytes,
            });
        }
    }
    let parts_count = policy.output_parts.max(1);
    let mut slices: Vec<Vec<FlatRowGroup>> = (0..parts_count).map(|_| Vec::new()).collect();
    let mut part_bytes = vec![0u64; parts_count];
    let mut part_index = 0usize;
    for group in flat {
        let idx = part_index % parts_count;
        part_bytes[idx] = part_bytes[idx].saturating_add(group.size_bytes);
        slices[idx].push(group);
        if part_bytes[idx] >= policy.target_output_bytes && part_index + 1 < parts_count {
            part_index += 1;
        }
    }
    let mut parts = Vec::with_capacity(parts_count);
    for (part_index, groups) in slices.into_iter().enumerate() {
        if groups.is_empty() {
            continue;
        }
        let mut input_indices = Vec::new();
        for g in &groups {
            if !input_indices.contains(&g.input_index) {
                input_indices.push(g.input_index);
            }
        }
        let merged_row_groups = merge_row_groups(&inputs, &groups, policy.target_row_group_bytes)?;
        let rows = merged_row_groups.iter().try_fold(0u64, |acc, rg| {
            let rows = u64::try_from(rg.num_rows).map_err(|_| "row count overflow".to_string())?;
            acc.checked_add(rows)
                .ok_or_else(|| "row count overflow".to_string())
        })?;
        let encoded_bytes = merged_row_groups
            .iter()
            .flat_map(|rg| rg.columns.iter())
            .try_fold(0u64, |acc, col| {
                acc.checked_add(col.compressed_size)
                    .ok_or_else(|| "size overflow".to_string())
            })?;
        parts.push(PageCompactionPartPlan {
            part_index,
            task_id: task_id_for_shard(&source, job_key, part_index),
            input_indices,
            merged_row_groups,
            rows,
            encoded_bytes,
        });
    }
    if parts.is_empty() {
        return Err("compaction produced no output parts".to_string());
    }
    Ok(PageCompactionPlan {
        source,
        policy,
        inputs,
        parts,
    })
}

fn row_group_count(plan: &Plan) -> usize {
    plan.chunks
        .iter()
        .map(|c| c.row_group as usize + 1)
        .max()
        .unwrap_or(0)
}

fn row_group_rows(plan: &Plan, row_group: u32) -> Result<i64, String> {
    plan.chunks
        .iter()
        .find(|c| c.row_group == row_group)
        .map(|c| c.rows)
        .ok_or_else(|| format!("row group {row_group} missing"))
}

fn row_group_byte_size(plan: &Plan, row_group: u32) -> Result<u64, String> {
    let mut total = 0u64;
    for chunk in plan.chunks.iter().filter(|c| c.row_group == row_group) {
        total = total
            .checked_add(chunk.compressed_size)
            .ok_or_else(|| "row group size overflow".to_string())?;
    }
    Ok(total)
}

fn merge_row_groups(
    inputs: &[CompactionInputFile],
    groups: &[FlatRowGroup],
    target_bytes: u64,
) -> Result<Vec<MergedRowGroupPlan>, String> {
    let leaf_count = inputs[0].plan.leaves.len();
    let mut out = Vec::new();
    let mut batch: Vec<&FlatRowGroup> = Vec::new();
    let mut batch_bytes = 0u64;
    let flush = |batch: &mut Vec<&FlatRowGroup>,
                 batch_bytes: &mut u64,
                 out: &mut Vec<MergedRowGroupPlan>,
                 inputs: &[CompactionInputFile]|
     -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }
        out.push(build_merged_row_group(inputs, batch, leaf_count)?);
        batch.clear();
        *batch_bytes = 0;
        Ok(())
    };
    for group in groups {
        if !batch.is_empty() && batch_bytes.saturating_add(group.size_bytes) > target_bytes {
            flush(&mut batch, &mut batch_bytes, &mut out, inputs)?;
        }
        batch_bytes = batch_bytes.saturating_add(group.size_bytes);
        batch.push(group);
    }
    flush(&mut batch, &mut batch_bytes, &mut out, inputs)?;
    Ok(out)
}

fn build_merged_row_group(
    inputs: &[CompactionInputFile],
    sources: &[&FlatRowGroup],
    leaf_count: usize,
) -> Result<MergedRowGroupPlan, String> {
    let mut num_rows = 0i64;
    for source in sources {
        let rows = inputs[source.input_index]
            .plan
            .chunks
            .iter()
            .find(|c| c.row_group == source.row_group)
            .map(|c| c.rows)
            .ok_or_else(|| "missing row group rows".to_string())?;
        num_rows = num_rows
            .checked_add(rows)
            .ok_or_else(|| "row count overflow".to_string())?;
    }
    let mut columns = Vec::with_capacity(leaf_count);
    for leaf in 0..leaf_count {
        let mut num_values = 0i64;
        let mut uncompressed_size = 0u64;
        let mut compressed_size = 0u64;
        let mut segments = Vec::new();
        for source in sources {
            let plan = &inputs[source.input_index].plan;
            let chunk = plan
                .chunks
                .iter()
                .find(|c| c.row_group == source.row_group && c.leaf as usize == leaf)
                .ok_or_else(|| "missing column chunk during merge".to_string())?;
            num_values = num_values
                .checked_add(chunk.num_values)
                .ok_or_else(|| "num_values overflow".to_string())?;
            uncompressed_size = uncompressed_size
                .checked_add(chunk.uncompressed_size)
                .ok_or_else(|| "uncompressed size overflow".to_string())?;
            compressed_size = compressed_size
                .checked_add(chunk.compressed_size)
                .ok_or_else(|| "compressed size overflow".to_string())?;
            let length = u32::try_from(chunk.compressed_size)
                .map_err(|_| "segment length overflow".to_string())?;
            segments.push(CopySegment {
                input_index: u16::try_from(source.input_index)
                    .map_err(|_| "too many compaction inputs".to_string())?,
                offset: chunk.start,
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
    Ok(MergedRowGroupPlan { num_rows, columns })
}

pub fn catalog_from_leaves(leaves: &[Leaf]) -> Vec<CatalogColumn> {
    leaves
        .iter()
        .map(|leaf| CatalogColumn {
            name: leaf.name.clone(),
            kind: ColumnKind::Number,
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect()
}

pub fn encode_page_compact_wire(
    part: &PageCompactionPartPlan,
    leaves: &[Leaf],
) -> Result<Vec<u8>, String> {
    encode_page_copy_wire(&part.merged_row_groups, leaves, None)
}

/// Opcode-11 program: copy each column's segments in row-group order, then write the
/// footer, either built from those row groups (`None`) or `footer`'s thrift as given.
pub fn encode_page_copy_wire(
    row_groups: &[MergedRowGroupPlan],
    leaves: &[Leaf],
    footer: Option<&[u8]>,
) -> Result<Vec<u8>, String> {
    let catalog = catalog_from_leaves(leaves);
    let mut out = Vec::new();
    out.push(PLAN_V2);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.push(OP_PAGE_COMPACT);
    let max_input = row_groups
        .iter()
        .flat_map(|rg| rg.columns.iter())
        .flat_map(|col| col.segments.iter())
        .map(|seg| seg.input_index as u32)
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    out.extend_from_slice(&max_input.to_le_bytes());
    encode_catalog(&mut out, &catalog);
    out.extend_from_slice(&(row_groups.len() as u32).to_le_bytes());
    for group in row_groups {
        out.extend_from_slice(&group.num_rows.to_le_bytes());
        if group.columns.len() != leaves.len() {
            return Err("merged row group column count mismatch".to_string());
        }
        for column in &group.columns {
            out.extend_from_slice(&column.num_values.to_le_bytes());
            out.extend_from_slice(&column.uncompressed_size.to_le_bytes());
            out.extend_from_slice(&column.compressed_size.to_le_bytes());
            out.extend_from_slice(&(column.segments.len() as u32).to_le_bytes());
            for segment in &column.segments {
                out.extend_from_slice(&segment.input_index.to_le_bytes());
                out.extend_from_slice(&segment.offset.to_le_bytes());
                out.extend_from_slice(&segment.length.to_le_bytes());
            }
        }
    }
    match footer {
        None => out.push(wcol_engine::formats::parquet::page_compact::FOOTER_BUILD),
        Some(thrift) => {
            out.push(wcol_engine::formats::parquet::page_compact::FOOTER_VERBATIM);
            let len = u32::try_from(thrift.len()).map_err(|_| "footer too large".to_string())?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(thrift);
        }
    }
    Ok(out)
}

/// One output part: every source row group becomes one output row group, in input order.
/// Used by query finalisers; does not merge row groups across size targets.
pub fn plan_preserve_row_group_concat(
    source: SourceVersion,
    job_key: &str,
    inputs: Vec<CompactionInputFile>,
) -> Result<(PageCompactionPartPlan, PageCompactionPlan), String> {
    validate_compaction_inputs(&inputs)?;
    let leaf_count = inputs[0].plan.leaves.len();
    let mut merged_row_groups = Vec::new();
    for (input_index, input) in inputs.iter().enumerate() {
        let groups = row_group_count(&input.plan);
        for row_group in 0..groups {
            let flat = FlatRowGroup {
                input_index,
                row_group: row_group as u32,
                size_bytes: row_group_byte_size(&input.plan, row_group as u32)?,
            };
            merged_row_groups.push(build_merged_row_group(&inputs, &[&flat], leaf_count)?);
        }
    }
    if merged_row_groups.is_empty() {
        return Err("concat inputs contain no row groups".to_string());
    }
    let input_indices: Vec<usize> = (0..inputs.len()).collect();
    let rows = merged_row_groups.iter().try_fold(0u64, |acc, rg| {
        let rows = u64::try_from(rg.num_rows).map_err(|_| "row count overflow".to_string())?;
        acc.checked_add(rows)
            .ok_or_else(|| "row count overflow".to_string())
    })?;
    let encoded_bytes = merged_row_groups
        .iter()
        .flat_map(|rg| rg.columns.iter())
        .try_fold(0u64, |acc, col| {
            acc.checked_add(col.compressed_size)
                .ok_or_else(|| "size overflow".to_string())
        })?;
    let part = PageCompactionPartPlan {
        part_index: 0,
        task_id: task_id_for_shard(&source, job_key, 0),
        input_indices,
        merged_row_groups,
        rows,
        encoded_bytes,
    };
    let plan = PageCompactionPlan {
        source,
        policy: PageCompactionPolicy {
            output_parts: 1,
            ..PageCompactionPolicy::default()
        },
        inputs,
        parts: vec![part.clone()],
    };
    Ok((part, plan))
}

pub fn verify_page_compaction_manifest(
    plan: &PageCompactionPlan,
    records: &[PageCompactionPartRecord],
) -> Result<(), String> {
    if records.len() != plan.parts.len() {
        return Err(format!(
            "expected {} parts, manifest has {}",
            plan.parts.len(),
            records.len()
        ));
    }
    let mut seen = vec![false; plan.parts.len()];
    for record in records {
        if record.part_index >= plan.parts.len() {
            return Err("part index out of range".to_string());
        }
        if seen[record.part_index] {
            return Err("duplicate part index".to_string());
        }
        seen[record.part_index] = true;
        let expected = &plan.parts[record.part_index];
        if record.task_id != expected.task_id {
            return Err("task id mismatch".to_string());
        }
        if record.rows != expected.rows {
            return Err("row count mismatch".to_string());
        }
    }
    if seen.iter().any(|v| !*v) {
        return Err("manifest missing parts".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::parquet::schema::{Chunk, Leaf, Plan};

    fn leaf(name: &str) -> Leaf {
        Leaf {
            name: name.to_string(),
            physical: 2,
            type_length: 0,
            max_def: 0,
            max_rep: 0,
        }
    }

    fn chunk(row_group: u32, leaf: u32, start: u64, size: u64, rows: i64) -> Chunk {
        Chunk {
            leaf,
            row_group,
            codec: 0,
            num_values: rows,
            start,
            compressed_size: size,
            uncompressed_size: size,
            has_dictionary: false,
            dictionary_bytes: None,
            has_page_index: false,
            rows,
            min_i64: None,
            max_i64: None,
            min_bytes: None,
            max_bytes: None,
            distinct_count: None,
        }
    }

    #[test]
    fn planner_merges_small_row_groups() {
        let inputs = vec![CompactionInputFile {
            object_key: "a.parquet".to_string(),
            file_size: 300,
            plan: Plan {
                rows: 30,
                leaves: vec![leaf("x")],
                chunks: vec![
                    chunk(0, 0, 4, 100, 10),
                    chunk(1, 0, 104, 100, 10),
                    chunk(2, 0, 204, 100, 10),
                ],
            },
        }];
        let plan = plan_page_compaction(
            SourceVersion { object_key_hash: 1 },
            "compact-test",
            inputs,
            PageCompactionPolicy {
                target_output_bytes: 1_000,
                target_row_group_bytes: 250,
                output_parts: 1,
            },
        )
        .unwrap();
        assert_eq!(plan.parts.len(), 1);
        assert_eq!(plan.parts[0].merged_row_groups.len(), 2);
        assert_eq!(plan.parts[0].rows, 30);
    }
}
