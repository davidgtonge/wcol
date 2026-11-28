use super::*;

#[test]
fn q1_sized_scan_is_one_node_and_a_full_file_uses_several() {
    let config = PhysicalPlannerConfig::for_coordinator(32);
    assert_eq!(planned_node_slots(25_000, &config), 1);
    assert!(planned_node_slots(256 * 1024, &config) > 1);
    let allowed: Vec<NodeId> = (0..32).map(NodeId).collect();
    let q1 = lower_to_physical(
        &sum_column(),
        &catalog(vec![chunk(0, 0, 0, 25_000)]),
        &allowed,
        &config,
    )
    .unwrap();
    assert_eq!(
        q1.metrics.nodes.len(),
        1,
        "Q1 is about 25 KB and stays on one node"
    );
    assert_eq!(q1.metrics.logical_bytes, 25_000);
    let full = lower_to_physical(
        &sum_column(),
        &catalog(
            (0..8)
                .map(|row_group| chunk(0, row_group, u64::from(row_group) * 200_000, 64 * 1024))
                .collect(),
        ),
        &allowed,
        &config,
    )
    .unwrap();
    assert!(
        full.metrics.nodes.len() > 1,
        "full file planned {} nodes",
        full.metrics.nodes.len()
    );
}

#[test]
fn logical_plan_has_no_storage_fields() {
    let text = example().display();
    assert_eq!(
        text,
        "Aggregate(group=[0], avg(1))\n  Filter(2 gt i64:30)\n    Scan(respondents, [0, 1, 2])\n"
    );
    assert!(!text.contains("offset"));
    assert!(!text.contains("node"));
}

#[test]
fn distributed_aggregate_coalesces_inside_each_node() {
    let logical = example();
    let meta = catalog(nearby_groups());
    let plan = lower_to_physical(
        &logical,
        &meta,
        &nodes(2),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(plan.metrics.request_count, 2);
    assert_eq!(plan.metrics.logical_bytes, 64 * 6);
    assert_eq!(plan.metrics.fetched_bytes, 264 * 2);
    assert_eq!(plan.metrics.over_read_bytes, 72 * 2);
    assert!((plan.metrics.over_read_percent() - 27.272727).abs() < 0.001);
    assert_eq!(plan.metrics.nodes.len(), 2);
    for node in &plan.metrics.nodes {
        assert_eq!(node.request_count, 1);
        assert_eq!(node.logical_bytes, 192);
        assert_eq!(node.fetched_bytes, 264);
    }
    for fetch in &plan.fetches {
        assert!(fetch.length <= PhysicalPlannerConfig::default().max_request_bytes);
        assert_eq!(fetch.logical_reads.len(), 3);
    }
    for read in &plan.logical_reads {
        let fetch = &plan.fetches[read.fetch_id as usize];
        let end = read.offset + read.length;
        assert!(read.offset >= fetch.offset && end <= fetch.offset + fetch.length);
        assert!(fetch.logical_reads.contains(&read.id));
    }

    let root = &plan.operators[plan.root as usize];
    let OpKind::FinalAggregate {
        group_by,
        aggregates,
    } = &root.kind
    else {
        panic!("root is not the final aggregate");
    };
    assert_eq!(group_by, &vec![0]);
    assert_eq!(aggregates[0].kind, AggKind::Avg);
    let exchange = &plan.operators[root.inputs[0] as usize];
    assert!(matches!(exchange.kind, OpKind::Exchange));
    assert_eq!(exchange.inputs.len(), 2);
    for input in &exchange.inputs {
        assert!(chain_has(plan.operators.as_slice(), *input, |kind| {
            matches!(kind, OpKind::PartialAggregate { .. })
        }));
        assert!(chain_has(plan.operators.as_slice(), *input, |kind| {
            matches!(kind, OpKind::Filter { .. })
        }));
        assert!(chain_has(plan.operators.as_slice(), *input, |kind| {
            matches!(kind, OpKind::Decode { groups } if groups.iter().all(|group| group.columns == vec![0, 1, 2]))
        }));
    }

    let again = lower_to_physical(
        &logical,
        &meta,
        &nodes(2),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(plan.canonical_text(), again.canonical_text());
    let mut reversed = nearby_groups();
    reversed.reverse();
    let shuffled = lower_to_physical(
        &logical,
        &catalog(reversed),
        &nodes(2),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(plan, shuffled);
}

#[test]
fn node_shares_give_every_object_a_node_and_the_rest_by_weight() {
    assert_eq!(node_shares(&[10, 10, 10], 3), vec![1, 1, 1]);
    assert_eq!(node_shares(&[30, 10], 6), vec![4, 2]);
    assert_eq!(node_shares(&[1, 1, 100], 5), vec![1, 1, 3]);
    assert_eq!(node_shares(&[7], 4), vec![4]);
    assert_eq!(node_shares(&[0, 0], 3).iter().sum::<usize>(), 3);
}

#[test]
fn each_scan_node_reads_one_object_of_a_split_table() {
    let logical = WcolLogicalPlan {
        root: LogicalOp::Scan {
            table: "respondents".into(),
            columns: vec![0],
        },
    };
    let config = PhysicalPlannerConfig {
        min_bytes_per_node: 1,
        ..PhysicalPlannerConfig::for_coordinator(16)
    };
    // More nodes than objects: objects split across nodes, never shared by one.
    let plan = lower_to_physical(&logical, &split_catalog(3, 4), &nodes(6), &config).unwrap();
    let placed = objects_per_node(&plan);
    assert_eq!(placed.len(), 6);
    assert!(
        placed.values().all(|objects| objects.len() == 1),
        "{placed:?}"
    );
    let rows: u64 = plan.logical_reads.len() as u64;
    assert_eq!(rows, 12);
    // As many nodes as objects: one each.
    let plan = lower_to_physical(&logical, &split_catalog(4, 2), &nodes(4), &config).unwrap();
    assert!(objects_per_node(&plan)
        .values()
        .all(|objects| objects.len() == 1));
    // A tiny table still gets a node per object.
    let plan = lower_to_physical(
        &logical,
        &split_catalog(3, 1),
        &nodes(8),
        &PhysicalPlannerConfig::for_coordinator(16),
    )
    .unwrap();
    assert_eq!(objects_per_node(&plan).len(), 3);
}

#[test]
fn more_objects_than_scan_nodes_is_refused() {
    let logical = WcolLogicalPlan {
        root: LogicalOp::Scan {
            table: "respondents".into(),
            columns: vec![0],
        },
    };
    let err = lower_to_physical(
        &logical,
        &split_catalog(5, 1),
        &nodes(4),
        &PhysicalPlannerConfig::for_coordinator(16),
    );
    assert!(
        matches!(err, Err(PlanError::Unsupported(msg)) if msg.contains("more objects than scan nodes")),
        "{err:?}"
    );
}

#[test]
fn gap_and_request_cap_bound_fetches() {
    let logical = WcolLogicalPlan {
        root: LogicalOp::Scan {
            table: "respondents".into(),
            columns: vec![0],
        },
    };
    let wide = catalog(vec![
        chunk(0, 0, 0, 64),
        chunk(0, 1, 64 + 128 * 1024 + 1, 64),
    ]);
    let split = lower_to_physical(
        &logical,
        &wide,
        &nodes(1),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(split.metrics.request_count, 2);
    assert_eq!(split.metrics.over_read_bytes, 0);

    let touching = catalog(vec![chunk(0, 0, 0, 64), chunk(0, 1, 64 + 128 * 1024, 64)]);
    let merged = lower_to_physical(
        &logical,
        &touching,
        &nodes(1),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(merged.metrics.request_count, 1);
    assert_eq!(merged.metrics.over_read_bytes, 128 * 1024);

    let fat = catalog(vec![
        chunk(0, 0, 0, 1_500_000),
        chunk(0, 1, 1_500_000 + 100, 1_500_000),
    ]);
    let capped =
        lower_to_physical(&logical, &fat, &nodes(1), &PhysicalPlannerConfig::default()).unwrap();
    assert_eq!(capped.metrics.request_count, 2);
    assert!(capped
        .fetches
        .iter()
        .all(|fetch| fetch.length <= 2 * 1024 * 1024));

    let huge = catalog(vec![chunk(0, 0, 0, 3 * 1024 * 1024)]);
    let one = lower_to_physical(
        &logical,
        &huge,
        &nodes(1),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(one.fetches.len(), 2);
    assert!(one
        .fetches
        .iter()
        .all(|fetch| fetch.length <= 2 * 1024 * 1024));
    assert_eq!(
        one.fetches.iter().map(|fetch| fetch.length).sum::<u64>(),
        3 * 1024 * 1024
    );
    let read = &one.logical_reads[0];
    assert_eq!(read.pieces.len(), 2);
    assert_eq!(
        read.pieces.iter().map(|piece| piece.length).sum::<u64>(),
        read.length
    );
}

#[test]
fn metadata_count_star_without_columns_is_footer_only() {
    let logical = WcolLogicalPlan {
        root: LogicalOp::Aggregate {
            group_by: vec![],
            aggregates: vec![AggExpr {
                kind: AggKind::CountStar,
                column: None,
                offset: 0,
            }],
            input: Box::new(LogicalOp::Scan {
                table: "respondents".into(),
                columns: vec![],
            }),
        },
    };
    let meta = catalog(vec![chunk(0, 0, 0, 64), chunk(0, 1, 10_000, 64)]);
    let plan = lower_to_physical(
        &logical,
        &meta,
        &nodes(2),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(plan.operators.len(), 1);
    assert!(matches!(
        &plan.operators[0].kind,
        OpKind::MetadataAggregate { rows: 20, bounds, .. } if bounds == &[None]
    ));
    assert!(plan.fetches.is_empty());
}

#[test]
fn min_max_need_bounds_on_every_chunk() {
    let min_max = WcolLogicalPlan {
        root: LogicalOp::Aggregate {
            group_by: vec![],
            aggregates: [AggKind::Min, AggKind::Max]
                .into_iter()
                .map(|kind| AggExpr {
                    kind,
                    column: Some(0),
                    offset: 0,
                })
                .collect(),
            input: Box::new(LogicalOp::Scan {
                table: "respondents".into(),
                columns: vec![0],
            }),
        },
    };
    let plan_with = |ranges: [Option<(i64, i64)>; 2]| {
        let mut meta = catalog(
            ranges
                .iter()
                .enumerate()
                .map(|(group, range)| ChunkMeta {
                    int_range: *range,
                    ..chunk(0, group as u32, group as u64 * 10_000, 64)
                })
                .collect(),
        );
        meta.leaf_physical = vec![1];
        lower_to_physical(
            &min_max,
            &meta,
            &nodes(2),
            &PhysicalPlannerConfig::default(),
        )
        .unwrap()
    };
    let both = plan_with([Some((5, 9)), Some((-3, 7))]);
    assert!(matches!(
        &both.operators[0].kind,
        OpKind::MetadataAggregate { rows: 20, bounds, .. }
            if bounds == &[Some((-3, 9)), Some((-3, 9))]
    ));
    assert!(both.fetches.is_empty());
    // One chunk without bounds may hold anything: read the column.
    let partial = plan_with([Some((5, 9)), None]);
    assert!(!partial
        .operators
        .iter()
        .any(|op| matches!(op.kind, OpKind::MetadataAggregate { .. })));
    assert!(!partial.fetches.is_empty());
}

#[test]
fn balanced_assign_minimises_max_load() {
    let weights = [6u64, 12, 12, 6];
    for node_count in 1..=4 {
        let slots = assign(&weights, node_count);
        let mut loads = vec![0u64; node_count];
        for (&weight, &slot) in weights.iter().zip(slots.iter()) {
            loads[slot] += weight;
        }
        let max_load = *loads.iter().max().unwrap_or(&0);
        let brute = brute_min_max_load(&weights, node_count);
        assert_eq!(max_load, brute, "node_count={node_count}");
    }
}

#[test]
fn dependent_columns_stay_on_the_same_node() {
    let mut chunks = Vec::new();
    for row_group in 0..2 {
        let base = u64::from(row_group) * 10_000_000;
        chunks.push(chunk(0, row_group, base, 32));
        chunks.push(chunk(1, row_group, base + 5_000_000, 32));
    }
    let plan = lower_to_physical(
        &example_two_columns(),
        &catalog(chunks),
        &nodes(2),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(plan.metrics.request_count, 4);
    let mut seen = [0u32, 0];
    for op in &plan.operators {
        let OpKind::Decode { groups } = &op.kind else {
            continue;
        };
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].columns, vec![0, 1]);
        seen[groups[0].row_group as usize] += 1;
    }
    assert_eq!(seen, [1, 1]);
}

#[test]
fn placement_keeps_adjacent_row_groups_and_balances_bytes() {
    let chunks = (0..4)
        .map(|row_group| chunk(0, row_group, u64::from(row_group) * 10_000, 100))
        .collect();
    let logical = WcolLogicalPlan {
        root: LogicalOp::Scan {
            table: "respondents".into(),
            columns: vec![0],
        },
    };
    let plan = lower_to_physical(
        &logical,
        &catalog(chunks),
        &nodes(2),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    let groups = decode_row_groups(&plan);
    assert_eq!(groups, vec![vec![0, 1], vec![2, 3]]);
    assert_eq!(
        plan.metrics.nodes[0].logical_bytes,
        plan.metrics.nodes[1].logical_bytes
    );
}

#[test]
fn dictionary_page_outside_the_span_stays_with_its_row_group() {
    let mut data = chunk(0, 0, 2_000, 100);
    data.dictionary_page = Some(ByteSpan {
        offset: 0,
        length: 40,
    });
    let logical = WcolLogicalPlan {
        root: LogicalOp::Scan {
            table: "respondents".into(),
            columns: vec![0],
        },
    };
    let plan = lower_to_physical(
        &logical,
        &catalog(vec![data]),
        &nodes(2),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(plan.logical_reads.len(), 2);
    assert!(plan.logical_reads.iter().any(|read| read.dictionary));
    assert!(plan.logical_reads.iter().all(|read| read.node == NodeId(0)));
    assert_eq!(plan.fetches.len(), 1);
    assert_eq!(plan.fetches[0].offset, 0);
    assert_eq!(plan.fetches[0].length, 2_100);
    assert_eq!(plan.metrics.over_read_bytes, 1_960);
}
