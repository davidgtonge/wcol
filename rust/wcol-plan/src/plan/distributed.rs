//! Distributed placement: partition row groups in the **physical planner** (Rust-owned).
//!
//! TypeScript (or any host) only runs `N` worker programs and a coordinator merge graph;
//! it does not implement SQL merge logic or ad-hoc sharding.

use std::collections::{HashMap, HashSet};

use xxhash_rust::xxh3::Xxh3;

use crate::exec::operator::WcolStreamItem;
use crate::exec::runtime::drive_operator;
use crate::ops::FinalizeOp;
use crate::ops::ReduceOp;
use crate::plan::physical::IngestGroup;
use crate::stream::partials::PartialStateWcol;

/// Stable logical task identity (same across retries).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskId(pub u64);

/// Identifies the Parquet object / footer snapshot jobs read from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceVersion {
    pub object_key_hash: u64,
}

/// Fixed-width little-endian input to xxh3 so wasm32 and native hosts derive the same id.
pub fn task_id_for_shard(source: &SourceVersion, sql: &str, shard_index: usize) -> TaskId {
    let mut hasher = Xxh3::new();
    hasher.update(&source.object_key_hash.to_le_bytes());
    hasher.update(&(sql.len() as u64).to_le_bytes());
    hasher.update(sql.as_bytes());
    hasher.update(&(shard_index as u64).to_le_bytes());
    TaskId(hasher.digest())
}

/// Hex task id for coordinator logs / DO placement.
pub fn task_id_hex(task_id: &TaskId) -> String {
    format!("{:016x}", task_id.0)
}

/// Build worker + coordinator plans. Does not execute I/O.
/// Merge worker aggregate partials with [`ReduceOp`].
pub fn coordinator_reduce_partials(
    worker_partials: Vec<PartialStateWcol>,
) -> Result<PartialStateWcol, String> {
    if worker_partials.is_empty() {
        return Err("coordinator reduce: no worker partials".into());
    }
    let mut reduce = ReduceOp::new();
    for partial in worker_partials {
        reduce.push(partial)?;
    }
    reduce.finish_state()
}

/// How a row-export worker sizes its Parquet output. File rotation at a row-group
/// boundary belongs in the writer; this local plan still writes one file per worker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParquetOutputPolicy {
    pub target_file_bytes: u64,
    pub target_row_group_rows: usize,
}

impl Default for ParquetOutputPolicy {
    fn default() -> Self {
        Self {
            target_file_bytes: 512 * 1024 * 1024,
            target_row_group_rows: 65_536,
        }
    }
}

/// Point estimate: input rows × selectivity × bytes per output row.
///
/// `selectivity_ppm` is matched rows per million (`990_000` is 99%). This is a
/// planning hint for worker count, not a promise about the finished files.
pub fn estimate_output_bytes(input_rows: u64, selectivity_ppm: u64, bytes_per_row: u64) -> u64 {
    let ppm = selectivity_ppm.min(1_000_000);
    let matched = input_rows.saturating_mul(ppm) / 1_000_000;
    matched.saturating_mul(bytes_per_row)
}

/// Workers to start from an output-size estimate, capped by `max_workers`.
///
/// Advisory. Raising the worker count does not keep a file under
/// `target_file_bytes` when one node or one partition value holds most of the rows.
pub fn workers_for_target_file(
    estimated_bytes: u64,
    target_file_bytes: u64,
    max_workers: usize,
) -> usize {
    let max_workers = max_workers.max(1);
    let target = target_file_bytes.max(1);
    let needed = estimated_bytes.max(1).div_ceil(target) as usize;
    needed.clamp(1, max_workers)
}

/// One successful scan of a planned set of source row groups.
///
/// Coverage is about inputs, not files. The same row group must not be scanned
/// by two accepted tasks. It may still appear in many output objects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanTaskInput {
    pub task_id: TaskId,
    pub source_row_groups: Vec<u32>,
}

/// Every planned source row group is assigned to exactly one accepted scan task.
pub fn verify_scan_task_coverage(
    all_groups: &[IngestGroup],
    tasks: &[ScanTaskInput],
) -> Result<(), String> {
    let expected: HashSet<u32> = all_groups.iter().map(|g| g.row_group).collect();
    let mut got = HashSet::new();
    let mut seen_tasks = HashSet::new();
    for task in tasks {
        if !seen_tasks.insert(task.task_id.clone()) {
            return Err(format!(
                "scan task {} recorded twice",
                task_id_hex(&task.task_id)
            ));
        }
        for rg in &task.source_row_groups {
            if !got.insert(*rg) {
                return Err(format!("row group {rg} assigned to two scan tasks"));
            }
        }
    }
    if expected != got {
        return Err(format!(
            "scan coverage mismatch: expected {} groups, got {}",
            expected.len(),
            got.len()
        ));
    }
    Ok(())
}

/// One committed output object. Several of these may share a task and a source row group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputObjectRecord {
    pub task_id: TaskId,
    pub attempt: u32,
    /// Immutable attempt object, or the key named by the published manifest.
    pub object_key: String,
    pub rows: u64,
    pub bytes: u64,
    /// Canonical partition path values, in `partitionBy` order. Empty when unpartitioned.
    pub partition_values: Vec<String>,
}

/// The published output set: unique keys, and one attempt per task.
///
/// This does not mention source row groups. A task that demultiplexes one row
/// group into three day files publishes three records.
pub fn verify_output_manifest(records: &[OutputObjectRecord]) -> Result<(), String> {
    let mut keys = HashSet::new();
    let mut attempts: HashMap<TaskId, u32> = HashMap::new();
    for record in records {
        if record.object_key.is_empty() {
            return Err("output object key is empty".into());
        }
        if !keys.insert(record.object_key.clone()) {
            return Err(format!(
                "output object {} recorded twice",
                record.object_key
            ));
        }
        if let Some(previous) = attempts.insert(record.task_id.clone(), record.attempt) {
            if previous != record.attempt {
                return Err(format!(
                    "task {} published attempts {previous} and {}",
                    task_id_hex(&record.task_id),
                    record.attempt
                ));
            }
        }
    }
    Ok(())
}

/// Where an attempt writes. The logical task id is not in the committed prefix.
///
/// Readers use the published manifest. Objects under `_jobs/` are attempts, and
/// a retry writes a new `attempt` so two tries never share an R2 key.
pub fn attempt_object_key(job_id: &str, task_id: &TaskId, attempt: u32, file_name: &str) -> String {
    format!(
        "_jobs/{job_id}/{}/{attempt}/{file_name}",
        task_id_hex(task_id)
    )
}

/// Coordinator tail after reduce (row result batch for sinks that expect finalized rows).
pub fn coordinator_reduce_finalize_rows(
    worker_partials: Vec<PartialStateWcol>,
) -> Result<WcolStreamItem, String> {
    let reduced = coordinator_reduce_partials(worker_partials)?;
    let mut finalize = FinalizeOp::new();
    let out = drive_operator(
        &mut finalize,
        vec![WcolStreamItem::from_partial_state(reduced)],
        true,
    )?;
    match out.as_slice() {
        [one] => Ok(one.clone()),
        _ => Err(format!(
            "coordinator finalize expected 1 item, got {}",
            out.len()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wide_numeric_estimate_fits_five_target_files() {
        let estimated = estimate_output_bytes(99_997_497, 990_000, 24);
        assert_eq!(estimated, 2_375_940_528);
        assert_eq!(workers_for_target_file(estimated, 512 * 1024 * 1024, 16), 5);
        assert_eq!(workers_for_target_file(estimated, 512 * 1024 * 1024, 4), 4);
    }

    fn group(row_group: u32) -> IngestGroup {
        IngestGroup {
            row_group,
            rows: 10,
            chunks: Vec::new(),
        }
    }

    #[test]
    fn one_scan_task_can_publish_several_partition_files() {
        let groups = vec![group(42)];
        let task = TaskId(7);
        verify_scan_task_coverage(
            &groups,
            &[ScanTaskInput {
                task_id: task.clone(),
                source_row_groups: vec![42],
            }],
        )
        .unwrap();
        let objects = [
            OutputObjectRecord {
                task_id: task.clone(),
                attempt: 1,
                object_key: attempt_object_key("job", &task, 1, "2013-07-01.parquet"),
                rows: 3,
                bytes: 100,
                partition_values: vec!["2013-07-01".into()],
            },
            OutputObjectRecord {
                task_id: task.clone(),
                attempt: 1,
                object_key: attempt_object_key("job", &task, 1, "2013-07-02.parquet"),
                rows: 4,
                bytes: 120,
                partition_values: vec!["2013-07-02".into()],
            },
        ];
        verify_output_manifest(&objects).unwrap();
        assert!(verify_output_manifest(&[objects[0].clone(), objects[0].clone()]).is_err());
        let mut other_attempt = objects[1].clone();
        other_attempt.attempt = 2;
        other_attempt.object_key = attempt_object_key("job", &task, 2, "2013-07-02.parquet");
        assert!(verify_output_manifest(&[objects[0].clone(), other_attempt]).is_err());
    }

    #[test]
    fn a_source_row_group_is_scanned_once() {
        let groups = vec![group(1), group(2)];
        let err = verify_scan_task_coverage(
            &groups,
            &[
                ScanTaskInput {
                    task_id: TaskId(1),
                    source_row_groups: vec![1, 2],
                },
                ScanTaskInput {
                    task_id: TaskId(2),
                    source_row_groups: vec![2],
                },
            ],
        );
        assert!(err.unwrap_err().contains("assigned to two scan tasks"));
    }

    #[test]
    fn attempt_keys_name_task_and_attempt() {
        assert_eq!(
            attempt_object_key("job-1", &TaskId(0xab), 2, "part.parquet"),
            "_jobs/job-1/00000000000000ab/2/part.parquet"
        );
    }
}
