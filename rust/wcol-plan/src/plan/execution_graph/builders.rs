//! Execution graphs per plan shape: aggregate, hash, row, Parquet parts and range sort.

use super::*;

/// Node partials stream into one finalizer task, which merges them and runs the plan's sort
/// and limit over the result rows; with a Parquet step it writes the file instead of rows.
pub(super) fn build_aggregate_graph(
    programs: &[NodeProgram],
    plan: &WcolPhysicalPlan,
    config: &PhysicalPlannerConfig,
    finalize: Option<Vec<crate::program::CoordinatorStep>>,
) -> Result<ExecutionPlan, String> {
    if programs.is_empty() {
        return Err("aggregate graph has no worker programs".into());
    }
    let producer_count = programs.len() as u32;
    check_stream_pool_layout(0, 1, "gather scan")?;
    check_stream_pool_layout(producer_count, 1, "gather finalizer")?;
    let single = programs.len() == 1;
    let mut tasks = Vec::with_capacity(programs.len() + 1);
    for (index, node) in programs.iter().enumerate() {
        tasks.push(ExecutionTask {
            id: index as u32,
            slot: if single {
                COORDINATOR_SLOT
            } else {
                worker_slot(node.node)
            },
            program: node.program.clone(),
            request_count: node.request_count,
            fetched_bytes: node.fetched_bytes,
            work: node.work.clone(),
        });
    }
    let reduce_id = tasks.len() as u32;
    let exported = plan.operators.iter().any(|op| {
        op.node.is_none()
            && matches!(
                op.kind,
                crate::plan::physical_plan::OpKind::Sink {
                    kind: crate::plan::physical_plan::SinkKind::Parquet { .. }
                }
            )
    });
    let steps = match finalize {
        Some(steps) => steps,
        None if exported => {
            return Err("an aggregate export needs its finalizer export steps".into());
        }
        None => crate::plan::result::finalize_steps(plan)
            .ok_or("aggregate plan has no aggregate operator")??,
    };
    let root_output = if steps
        .iter()
        .any(|step| matches!(step, crate::program::CoordinatorStep::Parquet { .. }))
    {
        RootOutputKind::Parquet
    } else {
        RootOutputKind::Rows
    };
    let program = crate::program::encode_finalize_program(producer_count, &steps);
    let reduce_slot = root_slot(programs);
    tasks.push(ExecutionTask {
        id: reduce_id,
        slot: reduce_slot,
        program,
        request_count: 0,
        fetched_bytes: 0,
        work: None,
    });
    let mut edges = Vec::with_capacity(programs.len());
    for (index, node) in programs.iter().enumerate() {
        let from_slot = if single {
            COORDINATOR_SLOT
        } else {
            worker_slot(node.node)
        };
        edges.push(ExecutionEdge {
            from: index as u32,
            to: reduce_id,
            transport: if single {
                EdgeTransport::Colocated
            } else {
                edge_transport(from_slot, reduce_slot, config)
            },
            partition: WHOLE_STREAM,
        });
    }
    Ok(execution_graph(tasks, edges, reduce_id, root_output))
}

pub(super) fn plan_hash(plan: &WcolPhysicalPlan) -> Option<(Vec<u32>, u32)> {
    plan.operators.iter().find_map(|op| match &op.kind {
        crate::plan::physical_plan::OpKind::HashExchange {
            keys, partitions, ..
        } => Some((keys.clone(), *partitions)),
        _ => None,
    })
}

pub(super) fn catalog_of(program: &[u8]) -> Result<Vec<crate::ir::CatalogColumn>, String> {
    let body = program.get(1..).ok_or("scan program is empty")?;
    let mut cur = crate::program::stream_program::PlanCursor::new(body);
    let _count = cur
        .u32()
        .map_err(|err| format!("scan program header: {err}"))?;
    let _opcode = cur
        .u8()
        .map_err(|err| format!("scan program opcode: {err}"))?;
    let _batch = cur
        .u32()
        .map_err(|err| format!("scan program batch: {err}"))?;
    cur.catalog()
        .map_err(|err| format!("scan program catalog: {err}"))
}

/// Scan tasks emit hash-partitioned rows. One reducer task owns each partition.
/// The finalizer merges reducer partials; it does not see the scan rows.
pub(super) fn build_hash_graph(
    programs: &[NodeProgram],
    plan: &WcolPhysicalPlan,
    config: &PhysicalPlannerConfig,
    finalize: Option<Vec<crate::program::CoordinatorStep>>,
) -> Result<ExecutionPlan, String> {
    let (keys, partitions) = plan_hash(plan).ok_or("hash graph without a hash exchange")?;
    if programs.is_empty() || partitions == 0 {
        return Err("hash graph has no scans or reducers".into());
    }
    let scan_count = programs.len() as u32;
    check_stream_pool_layout(0, partitions, "hash scan")?;
    check_stream_pool_layout(scan_count, 1, "hash reducer")?;
    check_stream_pool_layout(partitions, 1, "hash finalizer")?;
    let catalog = catalog_of(&programs[0].program)?;
    let aggregate = plan
        .operators
        .iter()
        .find_map(|op| match &op.kind {
            crate::plan::physical_plan::OpKind::FinalAggregate {
                group_by,
                aggregates,
            } => Some((group_by.clone(), aggregates.clone())),
            _ => None,
        })
        .ok_or("hash graph has no final aggregate")?;
    // A COUNT(DISTINCT) partitions on (group, value), or on the group alone when there are
    // enough groups (`hash_exchange_for`).
    if keys != aggregate.0
        && keys != crate::plan::physical_plan::exchange_keys(&aggregate.0, &aggregate.1)
    {
        return Err("hash exchange keys are not the group by and distinct columns".into());
    }
    let mut skip = 0u64;
    let mut fetch = None;
    for op in plan.operators.iter().filter(|op| op.node.is_none()) {
        if let crate::plan::physical_plan::OpKind::Limit {
            skip: n,
            fetch: limit,
        } = &op.kind
        {
            skip = *n;
            fetch = *limit;
        }
    }
    let rank = crate::plan::node_fragment::rank_count_aggregate(plan);
    let target = config.hash_aggregate.target_group_bytes;
    let hard = config.hash_aggregate.hard_group_bytes.max(target);
    let last_scan = programs.iter().map(|node| node.node.0).max().unwrap_or(0);
    let mut tasks = Vec::with_capacity(programs.len() + partitions as usize + 1);
    for (index, node) in programs.iter().enumerate() {
        tasks.push(ExecutionTask {
            id: index as u32,
            slot: worker_slot(node.node),
            program: node.program.clone(),
            request_count: node.request_count,
            fetched_bytes: node.fetched_bytes,
            work: node.work.clone(),
        });
    }
    let spill = config.hash_aggregate.group_spill_enabled;
    let reducer_base = tasks.len() as u32;
    for partition in 0..partitions {
        tasks.push(ExecutionTask {
            id: reducer_base + partition,
            slot: SlotId(last_scan + 1 + partition),
            program: crate::program::encode_hash_reducer_program(
                &crate::program::HashReducerSpec {
                    producer_count: programs.len() as u32,
                    catalog: &catalog,
                    group_by: &aggregate.0,
                    aggregates: &aggregate.1,
                    skip,
                    fetch,
                    rank,
                    group_budget: hard,
                    group_spill: spill,
                    spill_combined_budget: if spill { target } else { 0 },
                    partition,
                },
            ),
            request_count: 0,
            fetched_bytes: 0,
            work: None,
        });
    }
    let steps = match finalize {
        Some(steps) => steps,
        None => crate::plan::result::finalize_steps(plan)
            .ok_or("aggregate plan has no aggregate operator")??,
    };
    let root_output = if steps
        .iter()
        .any(|step| matches!(step, crate::program::CoordinatorStep::Parquet { .. }))
    {
        RootOutputKind::Parquet
    } else {
        RootOutputKind::Rows
    };
    let mut edges = Vec::with_capacity(programs.len() * partitions as usize + partitions as usize);
    for (index, node) in programs.iter().enumerate() {
        for partition in 0..partitions {
            edges.push(ExecutionEdge {
                from: index as u32,
                to: reducer_base + partition,
                transport: edge_transport(
                    worker_slot(node.node),
                    SlotId(last_scan + 1 + partition),
                    config,
                ),
                partition,
            });
        }
    }
    if hash_parquet_shard_export(&steps, root_output) {
        let export_base = tasks.len() as u32;
        for partition in 0..partitions {
            tasks.push(ExecutionTask {
                id: export_base + partition,
                slot: SlotId(last_scan + 1 + partition),
                program: crate::program::coordinator::encode_shard_export_program(&steps),
                request_count: 0,
                fetched_bytes: 0,
                work: None,
            });
            edges.push(ExecutionEdge {
                from: reducer_base + partition,
                to: export_base + partition,
                transport: EdgeTransport::Colocated,
                partition: WHOLE_STREAM,
            });
        }
        return Ok(execution_graph(tasks, edges, export_base, root_output));
    }
    let reduce_id = tasks.len() as u32;
    let reduce_slot = COORDINATOR_SLOT;
    tasks.push(ExecutionTask {
        id: reduce_id,
        slot: reduce_slot,
        program: crate::program::encode_finalize_program(partitions, &steps),
        request_count: 0,
        fetched_bytes: 0,
        work: None,
    });
    for partition in 0..partitions {
        edges.push(ExecutionEdge {
            from: reducer_base + partition,
            to: reduce_id,
            transport: edge_transport(SlotId(last_scan + 1 + partition), reduce_slot, config),
            partition: WHOLE_STREAM,
        });
    }
    Ok(execution_graph(tasks, edges, reduce_id, root_output))
}

pub(super) fn build_row_graph(
    nodes: &[NodeProgram],
    coordinator: &[u8],
    config: &PhysicalPlannerConfig,
    plan: &WcolPhysicalPlan,
) -> Result<ExecutionPlan, String> {
    if nodes.is_empty() {
        return Err("row graph has no worker programs".into());
    }
    let producer_count = nodes.len() as u32;
    check_stream_pool_layout(0, 1, "row scan")?;
    check_stream_pool_layout(producer_count, 1, "row coordinator")?;
    let single = nodes.len() == 1;
    let mut tasks = Vec::with_capacity(nodes.len() + 1);
    for (index, node) in nodes.iter().enumerate() {
        tasks.push(ExecutionTask {
            id: index as u32,
            slot: if single {
                COORDINATOR_SLOT
            } else {
                worker_slot(node.node)
            },
            program: node.program.clone(),
            request_count: node.request_count,
            fetched_bytes: node.fetched_bytes,
            work: node.work.clone(),
        });
    }
    let coord_id = tasks.len() as u32;
    let coord_slot = root_slot(nodes);
    tasks.push(ExecutionTask {
        id: coord_id,
        slot: coord_slot,
        program: coordinator.to_vec(),
        request_count: 0,
        fetched_bytes: 0,
        work: None,
    });
    let mut edges = Vec::with_capacity(nodes.len());
    for (index, node) in nodes.iter().enumerate() {
        let from_slot = if single {
            COORDINATOR_SLOT
        } else {
            worker_slot(node.node)
        };
        edges.push(ExecutionEdge {
            from: index as u32,
            to: coord_id,
            transport: if single {
                EdgeTransport::Colocated
            } else {
                edge_transport(from_slot, coord_slot, config)
            },
            partition: WHOLE_STREAM,
        });
    }
    let root_output = if plan.operators.iter().any(|op| {
        op.node.is_none()
            && matches!(
                op.kind,
                crate::plan::physical_plan::OpKind::Sink {
                    kind: crate::plan::physical_plan::SinkKind::Parquet { .. }
                }
            )
    }) {
        RootOutputKind::Parquet
    } else {
        RootOutputKind::Rows
    };
    Ok(execution_graph(tasks, edges, coord_id, root_output))
}

pub(super) fn build_parquet_parts(nodes: &[NodeProgram]) -> Result<ExecutionPlan, String> {
    if nodes.is_empty() {
        return Err("parquet export has no parts".into());
    }
    let tasks = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| ExecutionTask {
            id: index as u32,
            slot: if nodes.len() == 1 {
                COORDINATOR_SLOT
            } else {
                worker_slot(node.node)
            },
            program: node.program.clone(),
            request_count: node.request_count,
            fetched_bytes: node.fetched_bytes,
            work: node.work.clone(),
        })
        .collect();
    Ok(execution_graph(
        tasks,
        Vec::new(),
        0,
        RootOutputKind::Parquet,
    ))
}

/// Phase 2: range-partitioned global sort export (scan → range tasks → concat finaliser).
pub fn build_range_sort_graph(
    programs: &[NodeProgram],
    _plan: &WcolPhysicalPlan,
    config: &PhysicalPlannerConfig,
    steps: Vec<crate::program::CoordinatorStep>,
    partitions: u32,
    _sort_keys: &[SortKey],
) -> Result<ExecutionPlan, String> {
    if programs.is_empty() || partitions < 2 {
        return Err("range sort graph requires scans and at least two partitions".into());
    }
    let scan_count = programs.len() as u32;
    check_stream_pool_layout(0, partitions, "range scan")?;
    check_stream_pool_layout(scan_count, 1, "range worker")?;
    let last_scan = programs.iter().map(|node| node.node.0).max().unwrap_or(0);
    let mut tasks = Vec::with_capacity(programs.len() + partitions as usize);
    for (index, node) in programs.iter().enumerate() {
        tasks.push(ExecutionTask {
            id: index as u32,
            slot: worker_slot(node.node),
            program: node.program.clone(),
            request_count: node.request_count,
            fetched_bytes: node.fetched_bytes,
            work: node.work.clone(),
        });
    }
    let worker_base = tasks.len() as u32;
    for partition in 0..partitions {
        tasks.push(ExecutionTask {
            id: worker_base + partition,
            slot: SlotId(last_scan + 1 + partition),
            program: crate::program::encode_coordinator_program(1, &steps),
            request_count: 0,
            fetched_bytes: 0,
            work: None,
        });
    }
    let mut edges = Vec::with_capacity(programs.len() * partitions as usize);
    for (index, node) in programs.iter().enumerate() {
        for partition in 0..partitions {
            edges.push(ExecutionEdge {
                from: index as u32,
                to: worker_base + partition,
                transport: edge_transport(
                    worker_slot(node.node),
                    SlotId(last_scan + 1 + partition),
                    config,
                ),
                partition,
            });
        }
    }
    Ok(execution_graph(
        tasks,
        edges,
        worker_base,
        RootOutputKind::Parquet,
    ))
}
