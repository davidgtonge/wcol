//! Plan a `POST /jobs` request (see `docs/job-api.md`) on the coordinator.
//!
//! The host asks [`job_sources`] which objects to read footers from, fetches them, and calls
//! [`plan_job`]. A `json` or `parquet` job becomes one execution graph: its root prints rows,
//! or every task (or the root) writes a Parquet object. A `concat` job is page compaction:
//! one program that copies row groups from every input into one object. No SQL reaches a node.

use crate::formats::parquet::Plan;
use crate::plan::distributed::{estimate_output_bytes, workers_for_target_file, SourceVersion};
use crate::plan::execution_graph::{
    attach_output_plan, encode_execution_plan_wire, execution_plan_for, ExecutionPlan,
    RootOutputKind,
};
use crate::plan::job::{parse_job_request, JobRequest, ObjectLocation};
use crate::plan::late_rows::{plan_late_pick, plan_late_read, LateRead};
use crate::plan::output_plan::{
    concat_finalizer_for_graph_tasks, default_layout_for_parquet_job, parse_layout_from_job,
    OutputFinalizer, OutputLayout, OutputPlan,
};
use crate::plan::page_compaction::CompactionInputFile;
use crate::plan::parquet_concat::{concat_parquet_row_groups, CommittedParquetShard, ConcatPolicy};
use crate::plan::physical_plan::{OpKind, PhysicalPlannerConfig};
use crate::plan::query_dispatch::{nodes_parquet_finalize_steps, plan_query_job, QueryDispatch};
use crate::plan::result::{encode_result_columns, ResultColumn};
use crate::plan::sql_parser::KMeansOptions;
use crate::program::coordinator::{
    decode_coordinator_steps, encode_coordinator_program, encode_finalize_program, CoordinatorStep,
};
use crate::program::stream_program::{OP_COORDINATOR_FRAGMENT, OP_FINALIZE_FRAGMENT, PLAN_V2};

/// One footer the host read, in [`job_sources`] order.
pub struct JobFooter {
    pub file_size: u64,
    pub plan: Plan,
    /// The raw `FileMetaData` thrift `plan` came from (a row-group split rewrites it).
    pub footer: Vec<u8>,
}

pub enum JobPlan {
    /// Iterative training over a row-producing query graph. The host retains each worker's
    /// partition while the coordinator runs versioned kernel rounds.
    CreateModel {
        exec: ExecutionPlan,
        columns: Vec<ResultColumn>,
        name: String,
        options: KMeansOptions,
    },
    /// Rows back to the caller. `row_cap` bounds the response.
    Json {
        exec: ExecutionPlan,
        columns: Vec<ResultColumn>,
        row_cap: u32,
        /// The planner's estimate of the aggregate's group state (0 without one).
        group_bytes: u64,
    },
    /// Rows back to the caller, read in two graphs (see [`crate::plan::late_rows`]). `exec`
    /// picks the rows: its root prints their ids under the one column in `columns`. The host
    /// then plans the read-back with [`plan_late_read_wire`].
    LateRows {
        exec: ExecutionPlan,
        columns: Vec<ResultColumn>,
        row_cap: u32,
        group_bytes: u64,
    },
    /// Parquet objects under `prefix`. With no edges every task writes one object; otherwise
    /// the root writes the only object.
    Parquet {
        exec: ExecutionPlan,
        columns: Vec<ResultColumn>,
        bucket: String,
        prefix: String,
        group_bytes: u64,
    },
    /// Page copy: each program reads the inputs by index and writes `key`.
    Concat {
        bucket: String,
        key: String,
        programs: Vec<Vec<u8>>,
    },
}

/// The parsed request and the objects whose footers [`plan_job`] needs, in order.
pub fn job_sources(body: &str) -> Result<(JobRequest, Vec<ObjectLocation>), String> {
    let request = parse_job_request(body)?;
    let sources = match &request {
        JobRequest::Json { source, .. }
        | JobRequest::Parquet { source, .. }
        | JobRequest::CreateModel { source, .. } => source.locations.clone(),
        JobRequest::Concat { inputs, .. } => {
            for input in inputs {
                if !matches!(input.location, ObjectLocation::Bucket { .. }) {
                    return Err("concat inputs must be bucket objects".into());
                }
            }
            inputs.iter().map(|input| input.location.clone()).collect()
        }
    };
    Ok((request, sources))
}

pub fn plan_job(
    request: &JobRequest,
    footers: &[JobFooter],
    max_nodes: u32,
    batch_rows: u32,
    min_bytes_per_node: Option<u64>,
    hash_reducers: Option<u32>,
    job_body: &str,
) -> Result<JobPlan, String> {
    let host_stream_cap = wcol_engine::exec::stream_pool::MAX_STREAM_INPUT_PORTS as u32;
    if max_nodes > host_stream_cap {
        return Err(format!(
            "max_nodes {max_nodes} exceeds host limit of {host_stream_cap} input streams per node"
        ));
    }
    if let Some(reducers) = hash_reducers {
        if reducers > host_stream_cap {
            return Err(format!(
                "hashReducers {reducers} exceeds host limit of {host_stream_cap}"
            ));
        }
    }
    let config = job_config(max_nodes, min_bytes_per_node, hash_reducers, job_body)?;
    match request {
        JobRequest::CreateModel {
            source,
            query_sql,
            name,
            options,
            ..
        } => {
            let ids = object_ids(&source.table, footers)?;
            check_from(query_sql, &source.table)?;
            let (dispatch, plan, columns) = plan_query_job(
                query_sql,
                &table_objects(&ids, footers),
                config.max_nodes,
                batch_rows,
                &config,
            )?;
            if matches!(dispatch, QueryDispatch::Partial(_)) {
                return Err("CREATE MODEL query must produce rows".into());
            }
            if plan.operators.iter().any(|op| {
                matches!(
                    op.kind,
                    OpKind::FinalAggregate { .. } | OpKind::Sort { .. } | OpKind::Limit { .. }
                )
            }) {
                return Err(
                    "CREATE MODEL requires an unordered, unaggregated row query without LIMIT"
                        .into(),
                );
            }
            let mut exec = execution_plan_for(&dispatch, &plan, &config, None)?;
            if let ExecutionPlan::Graph {
                tasks,
                edges,
                root_task_id,
                ..
            } = &mut exec
            {
                let resident_ids: Vec<u32> = tasks
                    .iter()
                    .filter(|task| !edges.iter().any(|edge| edge.to == task.id))
                    .map(|task| task.id)
                    .collect();
                tasks.retain(|task| resident_ids.contains(&task.id));
                for task in tasks.iter_mut() {
                    task.program = wcol_engine::program::fragment::resident_node_program(
                        &task.program,
                        wcol_engine::ops::iterative::KMeansConfig {
                            dimensions: columns.len(),
                            clusters: options.clusters as usize,
                            max_iterations: options.max_iterations,
                            tolerance: options.tolerance,
                        },
                    )?;
                }
                edges.clear();
                *root_task_id = tasks.first().ok_or("model has no resident tasks")?.id;
            }
            Ok(JobPlan::CreateModel {
                exec,
                columns,
                name: name.clone(),
                options: options.clone(),
            })
        }
        JobRequest::Json {
            source,
            sql,
            row_cap,
            ..
        } => {
            let ids = object_ids(&source.table, footers)?;
            check_from(sql, &source.table)?;
            let objects = table_objects(&ids, footers);
            if let Some((dispatch, plan, columns)) =
                plan_late_pick(sql, &objects, batch_rows, &config)?
            {
                let mut exec = execution_plan_for(&dispatch, &plan, &config, None)?;
                tighten_root_row_cap(&mut exec, *row_cap)?;
                return Ok(JobPlan::LateRows {
                    exec,
                    columns,
                    row_cap: *row_cap,
                    group_bytes: plan.metrics.estimated_group_bytes,
                });
            }
            let (dispatch, plan, columns) =
                plan_query_job(sql, &objects, config.max_nodes, batch_rows, &config)?;
            let mut exec = execution_plan_for(&dispatch, &plan, &config, None)?;
            // One extra row lets the JSON formatter see that the cap hid more rows.
            // A smaller SQL limit stays in place: the root fetch is min(limit, rowCap + 1).
            tighten_root_row_cap(&mut exec, *row_cap)?;
            Ok(JobPlan::Json {
                exec,
                columns,
                row_cap: *row_cap,
                group_bytes: plan.metrics.estimated_group_bytes,
            })
        }
        JobRequest::Parquet {
            source,
            sql,
            bucket,
            prefix,
            partition_by,
            target_file_bytes,
            target_row_group_rows,
            ..
        } => {
            let ids = object_ids(&source.table, footers)?;
            let footer = &footers[0];
            // No SQL is the identity transform: every column, every row.
            let sql = sql
                .clone()
                .unwrap_or_else(|| format!("SELECT * FROM {}", source.table));
            check_from(&sql, &source.table)?;
            let rows = footers
                .iter()
                .map(|footer| u64::try_from(footer.plan.rows.max(0)).unwrap_or(0))
                .sum();
            let bytes_per_row = 16u64.saturating_mul(footer.plan.leaves.len().max(1) as u64);
            let estimated = estimate_output_bytes(rows, 1_000_000, bytes_per_row);
            let workers =
                workers_for_target_file(estimated, *target_file_bytes, max_nodes as usize) as u32;
            let mut config = PhysicalPlannerConfig::for_coordinator(workers.max(1));
            apply_planner_hints(&mut config, job_body);
            if let Some(reducers) = hash_reducers {
                config.hash_aggregate.reducers = Some(reducers);
            }
            config.parquet_row_group_rows = Some(*target_row_group_rows);
            let objects = table_objects(&ids, footers);
            let mut planned =
                plan_query_job(&sql, &objects, config.max_nodes, batch_rows, &config)?;
            if !partition_by.is_empty() {
                // `partitionBy` names output columns, which only the plan knows: plan once to
                // learn them, then again with the sink told which ordinals split the files.
                let names: Vec<String> = planned.2.iter().map(|col| col.name.clone()).collect();
                config.partition_columns = request.bind_partition_by(&names)?;
                planned = plan_query_job(&sql, &objects, config.max_nodes, batch_rows, &config)?;
            }
            let (dispatch, plan, columns) = planned;
            let finalize = match &dispatch {
                QueryDispatch::Nodes(_) => Some(nodes_parquet_finalize_steps(
                    &plan,
                    &columns,
                    &footer.plan,
                    *target_row_group_rows,
                )?),
                QueryDispatch::Partial(_) => {
                    return Err("a footer-only count has no Parquet export".into());
                }
                _ => None,
            };
            if !plan
                .operators
                .iter()
                .any(|op| matches!(op.kind, OpKind::Sink { .. } | OpKind::FinalAggregate { .. }))
            {
                return Err("export plan has no sink".into());
            }
            let exec = execution_plan_for(&dispatch, &plan, &config, finalize)?;
            let layout = if partition_by.is_empty() {
                parse_layout_from_job(job_body).unwrap_or_else(|| {
                    default_layout_for_parquet_job(&dispatch, &plan, estimated, *target_file_bytes)
                })
            } else {
                OutputLayout::Hive
            };
            let output = OutputPlan::parquet(bucket.clone(), prefix.clone(), layout.clone());
            let finalizer = finalizer_for_single_file_layout(&exec, &layout);
            let exec = attach_output_plan(exec, output, finalizer)?;
            Ok(JobPlan::Parquet {
                exec,
                columns,
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                group_bytes: plan.metrics.estimated_group_bytes,
            })
        }
        JobRequest::Concat {
            bucket,
            key,
            inputs,
            ..
        } => {
            if footers.len() != inputs.len() {
                return Err(format!(
                    "concat has {} inputs, {} footers",
                    inputs.len(),
                    footers.len()
                ));
            }
            if let Some(range) = inputs.iter().find_map(|input| input.row_groups.clone()) {
                // A row-group range is a split: copy those chunks unchanged under the
                // source's own footer, rewritten for the part.
                let [footer] = footers else {
                    return Err("a rowGroups split takes exactly one input".into());
                };
                let split = crate::plan::footer_split::split_row_groups(
                    &footer.footer,
                    &footer.plan,
                    range,
                )?;
                let program = crate::plan::page_compaction::encode_page_copy_wire(
                    &split.row_groups,
                    &footer.plan.leaves,
                    Some(&split.footer),
                )?;
                return Ok(JobPlan::Concat {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    programs: vec![program],
                });
            }
            let mut joined = String::new();
            let files = inputs
                .iter()
                .zip(footers)
                .map(|(input, footer)| {
                    let ObjectLocation::Bucket { key, .. } = &input.location else {
                        return Err("concat inputs must be bucket objects".to_string());
                    };
                    joined.push_str(key);
                    joined.push('\0');
                    Ok(CompactionInputFile {
                        object_key: key.clone(),
                        file_size: footer.file_size,
                        plan: footer.plan.clone(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let source = SourceVersion {
                object_key_hash: xxhash_rust::xxh3::xxh3_64(joined.as_bytes()),
            };
            let shards: Vec<CommittedParquetShard> = files
                .iter()
                .map(|file| CommittedParquetShard {
                    object_key: file.object_key.clone(),
                    file_size: file.file_size,
                    plan: file.plan.clone(),
                })
                .collect();
            let (_planned, program) = concat_parquet_row_groups(
                source,
                key,
                &shards,
                &ConcatPolicy::standalone_compaction(),
            )?;
            Ok(JobPlan::Concat {
                bucket: bucket.clone(),
                key: key.clone(),
                programs: vec![program],
            })
        }
    }
}

/// The planner config of a job on at most `max_nodes` scans.
fn job_config(
    max_nodes: u32,
    min_bytes_per_node: Option<u64>,
    hash_reducers: Option<u32>,
    job_body: &str,
) -> Result<PhysicalPlannerConfig, String> {
    let mut config = PhysicalPlannerConfig::for_coordinator(max_nodes);
    apply_planner_hints(&mut config, job_body);
    if let Some(bytes) = min_bytes_per_node {
        if bytes == 0 {
            return Err("min_bytes_per_node must be at least 1".into());
        }
        config.min_bytes_per_node = bytes;
    }
    if let Some(reducers) = hash_reducers {
        config.hash_aggregate.reducers = Some(reducers);
    }
    Ok(config)
}

fn finalizer_for_single_file_layout(
    exec: &ExecutionPlan,
    layout: &OutputLayout,
) -> OutputFinalizer {
    if !matches!(layout, OutputLayout::SingleFile) {
        return OutputFinalizer::None;
    }
    let ExecutionPlan::Graph {
        tasks,
        edges,
        root_output,
        ..
    } = exec
    else {
        return OutputFinalizer::None;
    };
    if !matches!(root_output, RootOutputKind::Parquet) {
        return OutputFinalizer::None;
    }
    if edges.is_empty() && tasks.len() > 1 {
        let mut ids: Vec<u32> = tasks.iter().map(|task| task.id).collect();
        ids.sort();
        return concat_finalizer_for_graph_tasks(&ids);
    }
    let shard_ids = crate::plan::execution_graph::parquet_export_shard_task_ids(tasks, edges);
    if shard_ids.len() > 1 {
        return concat_finalizer_for_graph_tasks(&shard_ids);
    }
    OutputFinalizer::None
}

/// Planner object ids, in job-source order. One object keeps the table's name.
fn object_ids(table: &str, footers: &[JobFooter]) -> Result<Vec<String>, String> {
    match footers.len() {
        0 => Err("the job's table has no objects".into()),
        1 => Ok(vec![table.to_string()]),
        n => Ok((0..n).map(|index| format!("{table}#{index}")).collect()),
    }
}

fn table_objects<'a>(ids: &'a [String], footers: &'a [JobFooter]) -> Vec<(&'a str, &'a Plan)> {
    ids.iter()
        .map(String::as_str)
        .zip(footers.iter().map(|footer| &footer.plan))
        .collect()
}

/// `FROM` must name the table in `sources`.
fn check_from(sql: &str, table: &str) -> Result<(), String> {
    let query = crate::plan::sql_parser::parse_sql_v0(sql)
        .map_err(|err| format!("sql: {} at {}", err.msg, err.at))?;
    if query.from.0 != table {
        return Err(format!("FROM {} is not a table in sources", query.from.0));
    }
    Ok(())
}

/// `{"kind":..,"sources":[{"bucket":..,"key":..} | {"url":..}]}` for the host.
pub fn job_sources_json(request: &JobRequest, sources: &[ObjectLocation]) -> String {
    let kind = match request {
        JobRequest::Json { .. } => "json",
        JobRequest::Parquet { .. } => "parquet",
        JobRequest::Concat { .. } => "concat",
        JobRequest::CreateModel { .. } => "model",
    };
    let sources: Vec<serde_json::Value> = sources
        .iter()
        .map(|location| match location {
            ObjectLocation::Bucket { bucket, key } => {
                serde_json::json!({ "bucket": bucket, "key": key })
            }
            ObjectLocation::Url(url) => serde_json::json!({ "url": url }),
        })
        .collect();
    let mut value = serde_json::json!({ "kind": kind, "sources": sources });
    if let JobRequest::CreateModel { name, options, .. } = request {
        value["modelName"] = serde_json::Value::from(name.clone());
        value["modelOptions"] = serde_json::json!({
            "numClusters": options.clusters,
            "maxIterations": options.max_iterations,
            "seed": options.seed,
            "tolerance": options.tolerance,
        });
    }
    if let Some(key) = request.idempotency_key() {
        value["idempotencyKey"] = serde_json::Value::from(key);
    }
    value.to_string()
}

/// After SQL `ORDER BY`, `LIMIT`, and offset, ask the root for `row_cap + 1` rows.
/// `row_cap == 0` leaves the plan unchanged. A footer-only answer has no root program.
fn tighten_root_row_cap(exec: &mut ExecutionPlan, row_cap: u32) -> Result<(), String> {
    if row_cap == 0 {
        return Ok(());
    }
    let ExecutionPlan::Graph {
        tasks,
        root_task_id,
        ..
    } = exec
    else {
        return Ok(());
    };
    let want = u64::from(row_cap) + 1;
    let root = tasks
        .iter_mut()
        .find(|task| task.id == *root_task_id)
        .ok_or("json graph has no root task")?;
    root.program = cap_fragment_fetch(&root.program, want)?;
    Ok(())
}

fn cap_fragment_fetch(program: &[u8], want: u64) -> Result<Vec<u8>, String> {
    if program.first() != Some(&PLAN_V2) || program.len() < 14 {
        return Err("json root is not a coordinator fragment".into());
    }
    let opcode = program[5];
    let count = u32::from_le_bytes(program[6..10].try_into().unwrap());
    let body_len = u32::from_le_bytes(program[10..14].try_into().unwrap()) as usize;
    if 14 + body_len != program.len() {
        return Err("json root program length mismatch".into());
    }
    let mut steps = decode_coordinator_steps(&program[14..])
        .map_err(|err| format!("json root steps: {err:?}"))?;
    let mut found = false;
    for step in &mut steps {
        if let CoordinatorStep::Limit { fetch, .. } = step {
            found = true;
            *fetch = Some(fetch.unwrap_or(want).min(want));
        }
    }
    if !found {
        steps.push(CoordinatorStep::Limit {
            skip: 0,
            fetch: Some(want),
        });
    }
    match opcode {
        OP_FINALIZE_FRAGMENT => Ok(encode_finalize_program(count, &steps)),
        OP_COORDINATOR_FRAGMENT => Ok(encode_coordinator_program(count, &steps)),
        _ => Err("json root is not a finalizer".into()),
    }
}

/// Execution graph wire plus its result-column trailer (as `wcol_plan_query` writes it).
pub fn encode_graph_with_columns(exec: &ExecutionPlan, columns: &[ResultColumn]) -> Vec<u8> {
    let mut out = encode_execution_plan_wire(exec);
    let columns = encode_result_columns(columns);
    out.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    out.extend_from_slice(&columns);
    out
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Host wire: `u8` kind, then
/// - `0` json: `u32` row cap, bytes graph;
/// - `1` parquet: str bucket, str prefix, bytes graph;
/// - `2` concat: str bucket, str key, `u32` count, bytes program each;
/// - `3` model: str name, options, bytes graph;
/// - `4` late rows: `u32` row cap, bytes graph (the pick).
///
/// Kinds `0`, `1` and `4` end with `u64` estimated group-state bytes (see
/// [`PlannerMetrics::estimated_group_bytes`]), which the host uses to pick where reducers run.
pub fn encode_job_plan(plan: &JobPlan) -> Vec<u8> {
    let mut out = Vec::new();
    match plan {
        JobPlan::Json {
            exec,
            columns,
            row_cap,
            group_bytes,
        } => {
            out.push(0);
            out.extend_from_slice(&row_cap.to_le_bytes());
            push_bytes(&mut out, &encode_graph_with_columns(exec, columns));
            out.extend_from_slice(&group_bytes.to_le_bytes());
        }
        JobPlan::LateRows {
            exec,
            columns,
            row_cap,
            group_bytes,
        } => {
            out.push(4);
            out.extend_from_slice(&row_cap.to_le_bytes());
            push_bytes(&mut out, &encode_graph_with_columns(exec, columns));
            out.extend_from_slice(&group_bytes.to_le_bytes());
        }
        JobPlan::Parquet {
            exec,
            columns,
            bucket,
            prefix,
            group_bytes,
        } => {
            out.push(1);
            push_bytes(&mut out, bucket.as_bytes());
            push_bytes(&mut out, prefix.as_bytes());
            push_bytes(&mut out, &encode_graph_with_columns(exec, columns));
            out.extend_from_slice(&group_bytes.to_le_bytes());
        }
        JobPlan::Concat {
            bucket,
            key,
            programs,
        } => {
            out.push(2);
            push_bytes(&mut out, bucket.as_bytes());
            push_bytes(&mut out, key.as_bytes());
            out.extend_from_slice(&(programs.len() as u32).to_le_bytes());
            for program in programs {
                push_bytes(&mut out, program);
            }
        }
        JobPlan::CreateModel {
            exec,
            columns,
            name,
            options,
        } => {
            out.push(3);
            push_bytes(&mut out, name.as_bytes());
            out.extend_from_slice(&options.clusters.to_le_bytes());
            out.extend_from_slice(&options.max_iterations.to_le_bytes());
            out.extend_from_slice(&options.seed.to_le_bytes());
            out.extend_from_slice(&options.tolerance.to_le_bytes());
            push_bytes(&mut out, &encode_graph_with_columns(exec, columns));
        }
    }
    out
}

/// A host planning request: the job, its footers, and the host's planner overrides.
struct PlanRequest<'a> {
    max_nodes: u32,
    batch_rows: u32,
    body: &'a str,
    footers: Vec<JobFooter>,
    min_bytes_per_node: Option<u64>,
    hash_reducers: Option<u32>,
}

/// Host input: `u32` max nodes, `u32` batch rows, bytes job JSON, `u32` footer count, then
/// per footer `u64` file size and bytes footer (in [`job_sources`] order). Optional trailing
/// `u32` min bytes per node; a second trailing `u32` is a hash-reducer override (`0` omits).
fn parse_plan_request(input: &[u8]) -> Result<PlanRequest<'_>, String> {
    let mut cur = input;
    let max_nodes = take_u32(&mut cur)?;
    let batch_rows = take_u32(&mut cur)?;
    let body = std::str::from_utf8(take_bytes(&mut cur)?).map_err(|_| "job is not UTF-8")?;
    let count = take_u32(&mut cur)? as usize;
    let mut footers = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if cur.len() < 8 {
            return Err("job plan request is truncated".into());
        }
        let file_size = take_u64(&mut cur)?;
        let footer = take_bytes(&mut cur)?;
        let plan = crate::formats::parquet::plan_footer(footer)
            .map_err(|err| format!("parquet footer: {err:?}"))?;
        footers.push(JobFooter {
            file_size,
            plan,
            footer: footer.to_vec(),
        });
    }
    let min_raw = if cur.is_empty() {
        None
    } else {
        Some(take_u32(&mut cur)?)
    };
    let hash_raw = if cur.is_empty() {
        None
    } else {
        Some(take_u32(&mut cur)?)
    };
    if !cur.is_empty() {
        return Err("trailing bytes after job plan request".into());
    }
    let min_bytes_per_node = match (min_raw, hash_raw) {
        (None, None) => None,
        (Some(0), Some(_)) => None,
        (Some(0), None) => return Err("min_bytes_per_node must be at least 1".into()),
        (Some(bytes), _) => Some(u64::from(bytes)),
        (None, Some(_)) => None,
    };
    Ok(PlanRequest {
        max_nodes,
        batch_rows,
        body,
        footers,
        min_bytes_per_node,
        hash_reducers: hash_raw.filter(|&count| count > 0),
    })
}

/// [`parse_plan_request`] input → [`encode_job_plan`] output.
pub fn plan_job_wire(input: &[u8]) -> Result<Vec<u8>, String> {
    let request = parse_plan_request(input)?;
    let (job, _) = job_sources(request.body)?;
    Ok(encode_job_plan(&plan_job(
        &job,
        &request.footers,
        request.max_nodes,
        request.batch_rows,
        request.min_bytes_per_node,
        request.hash_reducers,
        request.body,
    )?))
}

/// The read-back graph of a [`JobPlan::LateRows`] job. Input: bytes of the pick graph's root
/// row frames, then the job's [`parse_plan_request`] input. Output: `u8` `0` and the result
/// columns when nothing was picked, or `1` and the graph with its result columns.
pub fn plan_late_read_wire(input: &[u8]) -> Result<Vec<u8>, String> {
    let mut cur = input;
    let picked = take_bytes(&mut cur)?;
    let request = parse_plan_request(cur)?;
    let (JobRequest::Json { source, sql, .. }, _) = job_sources(request.body)? else {
        return Err("only a json job reads rows late".into());
    };
    let ids = object_ids(&source.table, &request.footers)?;
    let config = job_config(
        request.max_nodes,
        request.min_bytes_per_node,
        request.hash_reducers,
        request.body,
    )?;
    let read = plan_late_read(
        &sql,
        &table_objects(&ids, &request.footers),
        picked,
        request.batch_rows,
        &config,
    )?;
    let mut out = Vec::new();
    match read {
        LateRead::Nothing(columns) => {
            out.push(0);
            out.extend_from_slice(&encode_result_columns(&columns));
        }
        LateRead::Graph(exec, columns) => {
            out.push(1);
            out.extend_from_slice(&encode_graph_with_columns(&exec, &columns));
        }
    }
    Ok(out)
}

fn apply_planner_hints(config: &mut PhysicalPlannerConfig, body: &str) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return;
    };
    // Spill is the default; `groupSpill: false` keeps reducers in memory and fails on budget.
    if let Some(enabled) = value.get("groupSpill").and_then(|v| v.as_bool()) {
        config.hash_aggregate.group_spill_enabled = enabled;
    }
    if value.get("spillSink").and_then(|v| v.as_str()) == Some("discard") {
        config.hash_aggregate.group_spill_enabled = true;
        config.hash_aggregate.spill_discard = true;
    }
    let mut target_set = false;
    if let Some(bytes) = value.get("targetGroupBytes").and_then(|v| v.as_u64()) {
        if bytes > 0 {
            config.hash_aggregate.target_group_bytes = bytes;
            target_set = true;
        }
    }
    if let Some(bytes) = value.get("hardGroupBytes").and_then(|v| v.as_u64()) {
        if bytes > 0 {
            config.hash_aggregate.hard_group_bytes = bytes;
        }
    } else if target_set {
        // An explicit budget is a single ceiling. The 16/32 pair is the default only.
        config.hash_aggregate.hard_group_bytes = config.hash_aggregate.target_group_bytes;
    }
    if config.hash_aggregate.hard_group_bytes < config.hash_aggregate.target_group_bytes {
        config.hash_aggregate.hard_group_bytes = config.hash_aggregate.target_group_bytes;
    }
    // Lower it to read a wide row top-k late on a small file.
    if let Some(bytes) = value.get("lateWideBytes").and_then(|v| v.as_u64()) {
        config.late_rows.wide_bytes = bytes;
    }
    if let Some(reducers) = value.get("hashReducers").and_then(|v| v.as_u64()) {
        if reducers > 0 {
            config.hash_aggregate.reducers = Some(reducers as u32);
        }
    }
}

/// Host input: `u32` footer count, then per footer `u64` file_size + bytes thrift footer.
pub fn concat_program_wire(input: &[u8]) -> Result<Vec<u8>, String> {
    let mut cur = input;
    let count = take_u32(&mut cur)? as usize;
    let mut footers = Vec::with_capacity(count);
    for _ in 0..count {
        let file_size = take_u64(&mut cur)?;
        let footer_bytes = take_bytes(&mut cur)?;
        let plan = crate::formats::parquet::plan_footer(footer_bytes)
            .map_err(|err| format!("footer: {err}"))?;
        footers.push((file_size, plan));
    }
    crate::plan::parquet_concat::concat_program_from_footers(&footers)
}

fn take_u64(cur: &mut &[u8]) -> Result<u64, String> {
    if cur.len() < 8 {
        return Err("truncated u64".into());
    }
    let (head, tail) = cur.split_at(8);
    *cur = tail;
    Ok(u64::from_le_bytes(head.try_into().unwrap()))
}

fn take_u32(cur: &mut &[u8]) -> Result<u32, String> {
    if cur.len() < 4 {
        return Err("job plan request is truncated".into());
    }
    let value = u32::from_le_bytes(cur[..4].try_into().unwrap());
    *cur = &cur[4..];
    Ok(value)
}

fn take_bytes<'a>(cur: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let len = take_u32(cur)? as usize;
    if cur.len() < len {
        return Err("job plan request is truncated".into());
    }
    let (head, tail) = cur.split_at(len);
    *cur = tail;
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spill_after(body: &str) -> bool {
        let mut config = PhysicalPlannerConfig::default();
        apply_planner_hints(&mut config, body);
        config.hash_aggregate.group_spill_enabled
    }

    #[test]
    fn group_spill_defaults_on_and_job_can_turn_it_off() {
        assert!(spill_after(r#"{"sql":"SELECT 1"}"#));
        assert!(spill_after(r#"{"groupSpill":true}"#));
        assert!(!spill_after(r#"{"groupSpill":false}"#));
    }
}
