//! Query and standalone Parquet row-group concat planning.

use xxhash_rust::xxh3::xxh3_64;

use crate::plan::distributed::SourceVersion;
use crate::plan::page_compaction::{
    encode_page_compact_wire, plan_page_compaction, plan_preserve_row_group_concat,
    validate_compaction_inputs, CompactionInputFile, PageCompactionPlan, PageCompactionPolicy,
};

pub const DEFAULT_CONCAT_FOOTER_BUDGET_BYTES: usize = 8 * 1024 * 1024;

/// A committed shard object the finaliser reads in planner order.
#[derive(Clone, Debug)]
pub struct CommittedParquetShard {
    pub object_key: String,
    pub file_size: u64,
    pub plan: crate::formats::parquet::Plan,
}

impl CommittedParquetShard {
    pub fn as_compaction_input(&self) -> CompactionInputFile {
        CompactionInputFile {
            object_key: self.object_key.clone(),
            file_size: self.file_size,
            plan: self.plan.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConcatPolicy {
    /// When false, each source row group becomes one output row group (query finaliser default).
    pub merge_row_groups: bool,
    pub target_output_bytes: u64,
    pub target_row_group_bytes: u64,
    pub footer_budget_bytes: usize,
}

impl Default for ConcatPolicy {
    fn default() -> Self {
        Self {
            merge_row_groups: false,
            target_output_bytes: PageCompactionPolicy::default().target_output_bytes,
            target_row_group_bytes: PageCompactionPolicy::default().target_row_group_bytes,
            footer_budget_bytes: DEFAULT_CONCAT_FOOTER_BUDGET_BYTES,
        }
    }
}

impl ConcatPolicy {
    pub fn query_finaliser() -> Self {
        Self {
            merge_row_groups: false,
            ..Self::default()
        }
    }

    pub fn standalone_compaction() -> Self {
        Self {
            merge_row_groups: true,
            ..Self::default()
        }
    }
}

/// Plan a single opcode-11 program that concatenates shards in `inputs` order.
pub fn concat_parquet_row_groups(
    source: SourceVersion,
    output_key: &str,
    shards: &[CommittedParquetShard],
    policy: &ConcatPolicy,
) -> Result<(PageCompactionPlan, Vec<u8>), String> {
    if shards.is_empty() {
        return Err("concat requires at least one shard".to_string());
    }
    let inputs: Vec<CompactionInputFile> = shards.iter().map(|s| s.as_compaction_input()).collect();
    validate_compaction_inputs(&inputs)?;
    if policy.merge_row_groups {
        let compaction_policy = PageCompactionPolicy {
            output_parts: 1,
            target_output_bytes: policy.target_output_bytes,
            target_row_group_bytes: policy.target_row_group_bytes,
        };
        let planned = plan_page_compaction(source, output_key, inputs, compaction_policy)?;
        let part = planned
            .parts
            .first()
            .ok_or_else(|| "compaction produced no part".to_string())?
            .clone();
        let leaves = &planned.inputs[0].plan.leaves;
        let program = encode_page_compact_wire(&part, leaves)?;
        Ok((planned, program))
    } else {
        let (part, planned) = plan_preserve_row_group_concat(source, output_key, inputs)?;
        let leaves = &planned.inputs[0].plan.leaves;
        let program = encode_page_compact_wire(&part, leaves)?;
        Ok((planned, program))
    }
}

/// Build an opcode-11 program that concatenates inputs in order (query finaliser, preserve row groups).
pub fn concat_program_from_footers(
    footers: &[(u64, crate::formats::parquet::Plan)],
) -> Result<Vec<u8>, String> {
    if footers.is_empty() {
        return Err("concat requires at least one footer".into());
    }
    let joined: String = footers
        .iter()
        .enumerate()
        .map(|(index, _)| format!("input-{index}"))
        .collect::<Vec<_>>()
        .join("\0");
    let source = SourceVersion {
        object_key_hash: xxh3_64(joined.as_bytes()),
    };
    let shards: Vec<CommittedParquetShard> = footers
        .iter()
        .enumerate()
        .map(|(index, (file_size, plan))| CommittedParquetShard {
            object_key: format!("input-{index}"),
            file_size: *file_size,
            plan: plan.clone(),
        })
        .collect();
    let (_, program) = concat_parquet_row_groups(
        source,
        "result.parquet",
        &shards,
        &ConcatPolicy::query_finaliser(),
    )?;
    Ok(program)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::parquet::open_plan;
    use std::path::PathBuf;

    /// The fixture's footer as one of our own shards: page compaction only takes the
    /// uncompressed, dictionary-free files the shard writer produces. No page is read here.
    fn shard_plan() -> crate::formats::parquet::Plan {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet");
        let (_, mut plan) = open_plan(&path).unwrap();
        for chunk in &mut plan.chunks {
            chunk.codec = 0;
            chunk.has_dictionary = false;
            chunk.has_page_index = false;
        }
        plan
    }

    #[test]
    fn concat_program_preserves_input_order_in_read_at_sources() {
        let plan = shard_plan();
        let footers = [(100u64, plan.clone()), (200, plan)];
        let program = concat_program_from_footers(&footers).unwrap();
        let max_input = u32::from_le_bytes(program[6..10].try_into().unwrap());
        assert_eq!(max_input, 2);
    }
}
