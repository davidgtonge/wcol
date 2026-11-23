use super::*;

#[test]
fn a_combiner_partitions_its_partials_by_partial_key_position() {
    let agg = |kind, column| AggExpr {
        column,
        kind,
        offset: 0,
    };
    let hash = |keys: Vec<u32>| FragmentStep::HashPartition {
        input: PartitionInput::Rows { keys },
        partitions: 64,
        hash_version: 1,
    };
    let partials = |keys: Vec<(u32, u32)>| FragmentStep::HashPartition {
        input: PartitionInput::Partials { keys },
        partitions: 64,
        hash_version: 1,
    };
    // GROUP BY 7, 5 with COUNT(DISTINCT 9): the partial key is [7, 5, 9].
    let combiner = vec![FragmentStep::PartialAggregate {
        group_by: vec![7, 5],
        aggregates: vec![
            agg(AggKind::CountStar, None),
            agg(AggKind::CountDistinct, Some(9)),
        ],
    }];
    assert_eq!(
        partition_step(&hash(vec![5, 7, 9]), &combiner).unwrap(),
        partials(vec![(1, 5), (0, 7), (2, 9)])
    );
    // Group-only distinct routing leaves the counted value out.
    assert_eq!(
        partition_step(&hash(vec![7, 5]), &combiner).unwrap(),
        partials(vec![(0, 7), (1, 5)])
    );
    assert!(matches!(
        partition_step(&hash(vec![4]), &combiner),
        Err(FragmentError::ExchangeKeyNotGrouped { column: 4 })
    ));
    // Without a partial aggregate the step still splits rows.
    assert_eq!(partition_step(&hash(vec![5]), &[]).unwrap(), hash(vec![5]));
}
use crate::plan::physical_plan::{
    lower_to_physical, AggExpr, ChunkMeta, CmpOp, Compare, Literal, LogicalOp,
    ParquetMetadataCatalog, ParquetObject, PhysicalPlannerConfig, Predicate, SortKey, SortTarget,
    TableMeta, WcolLogicalPlan,
};
use crate::program::IngestGroup;

fn sample_meta() -> ParquetMetadataCatalog {
    let chunk = |column, row_group, offset| ChunkMeta {
        column,
        row_group,
        offset,
        length: 64,
        rows: 10,
        dictionary_in_span: column == 1,
        dictionary_page: None,
        codec: 1,
        int_range: None,
    };
    ParquetMetadataCatalog {
        leaf_physical: Vec::new(),
        leaf_distinct_upper: Vec::new(),
        leaf_dictionary_bytes: Vec::new(),
        leaf_min_i64: Vec::new(),
        leaf_max_i64: Vec::new(),
        leaf_min_bytes: Vec::new(),
        leaf_max_bytes: Vec::new(),
        tables: vec![TableMeta {
            table: "hits".into(),
            objects: vec![ParquetObject {
                object_id: "hits.parquet".into(),
                chunks: vec![
                    chunk(0, 0, 0),
                    chunk(1, 0, 64),
                    chunk(0, 1, 128),
                    chunk(1, 1, 192),
                ],
            }],
        }],
    }
}

fn grouped_plan(nodes: &[NodeId]) -> WcolPhysicalPlan {
    let logical = WcolLogicalPlan {
        root: LogicalOp::Limit {
            skip: 1,
            fetch: Some(5),
            input: Box::new(LogicalOp::Sort {
                keys: vec![SortKey {
                    target: SortTarget::Aggregate(0),
                    descending: true,
                    nulls_first: false,
                }],
                input: Box::new(LogicalOp::Aggregate {
                    input: Box::new(LogicalOp::Filter {
                        predicates: vec![Predicate::Compare(Compare {
                            column: 0,
                            op: CmpOp::Gt,
                            value: Literal::Int(3),
                        })],
                        input: Box::new(LogicalOp::Scan {
                            table: "hits".into(),
                            columns: vec![0, 1],
                        }),
                    }),
                    group_by: vec![1],
                    aggregates: vec![AggExpr {
                        kind: crate::plan::physical_plan::AggKind::CountStar,
                        column: None,
                        offset: 0,
                    }],
                }),
            }),
        },
    };
    lower_to_physical(
        &logical,
        &sample_meta(),
        nodes,
        &PhysicalPlannerConfig::default(),
    )
    .unwrap()
}

#[test]
fn whole_group_distinct_is_ranked_on_reducers() {
    // Q13 on the full hits footer: each phrase is whole on one reducer, so the reducers
    // rank its distinct count; without a rank they would send every phrase.
    let path =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/hits.footer.bin");
    let Ok(bytes) = std::fs::read(&path) else {
        return;
    };
    let footer = crate::formats::parquet::plan_footer(&bytes).unwrap();
    let mut config = crate::plan::PhysicalPlannerConfig::for_coordinator(32);
    config.hash_aggregate.reducers = Some(64);
    let rank = |sql: &str| {
        let (_, plan) = crate::plan::plan_query_dispatch(
            sql,
            "hits",
            &footer,
            32,
            1024,
            &config,
            &[crate::plan::Frontend::Sql],
        )
        .unwrap();
        rank_count_aggregate(&plan)
    };
    assert_eq!(
        rank(
            "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits \
              WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10"
        ),
        Some(0)
    );
    // RegionID groups are split across reducers: their counts are partial there.
    assert_eq!(
        rank(
            "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits \
              GROUP BY RegionID ORDER BY u DESC LIMIT 10"
        ),
        None
    );
}

#[test]
fn one_fragment_is_the_final_count_rank() {
    let fragments = node_fragments(&grouped_plan(&[NodeId(0)]), 1).unwrap();
    assert_eq!(fragments.len(), 1);
    let tail = fragments.values().next().unwrap().tail;
    assert!(tail.final_aggregate);
    assert_eq!(tail.rank_aggregate, Some(0));
    assert_eq!(tail.fetch, Some(5));
    assert_eq!(tail.skip, 1);
}

#[test]
fn fragments_round_trip_through_their_wire_format() {
    let plan = grouped_plan(&[NodeId(0), NodeId(1)]);
    let fragments = node_fragments(&plan, 3).unwrap();
    assert_eq!(fragments.len(), 2);
    for fragment in fragments.values() {
        assert_eq!(fragment.fetch_depth, 3);
        assert_eq!(
            fragment.tail,
            FragmentTail {
                skip: 1,
                fetch: Some(5),
                order_groups_by_count: true,
                final_aggregate: false,
                rank_aggregate: None,
            }
        );
        assert!(matches!(fragment.steps[0], FragmentStep::Filter(_)));
        assert!(matches!(
            fragment.steps[1],
            FragmentStep::PartialAggregate { .. }
        ));
        let bytes = encode_node_fragment(fragment);
        assert_eq!(&decode_node_fragment(&bytes).unwrap(), fragment);
        assert!(decode_node_fragment(&bytes[..bytes.len() - 1]).is_err());
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(decode_node_fragment(&extra).is_err());
    }
}

#[test]
fn ingest_groups_come_from_the_fragment_alone() {
    let plan = grouped_plan(&[NodeId(0), NodeId(1)]);
    let fragments = node_fragments(&plan, 2).unwrap();
    let groups: Vec<IngestGroup> = fragments
        .values()
        .flat_map(|fragment| fragment.ingest_groups().unwrap())
        .collect();
    assert_eq!(groups.len(), 2, "one group per row group across both nodes");
    for group in &groups {
        assert_eq!(group.chunks.len(), 2);
        for chunk in &group.chunks {
            assert_eq!(chunk.codec, 1);
            assert_eq!(chunk.length, 64);
            assert_eq!(chunk.dict_encoded, chunk.column == 1);
        }
    }
}

#[test]
fn a_partial_program_splits_per_row_group() {
    use crate::ops::query::CatalogColumn;
    use crate::plan::sql_parser::ColumnKind;

    let plan = grouped_plan(&[NodeId(0), NodeId(1)]);
    let fragments = node_fragments(&plan, 3).unwrap();
    let mut fragment = fragments.values().next().unwrap().clone();
    let mut second = fragment.decode[0].clone();
    second.row_group = second.row_group.wrapping_add(7);
    fragment.decode.push(second);
    let catalog = vec![CatalogColumn {
        name: "a".into(),
        kind: ColumnKind::Number,
        physical: 1,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }];
    let program = encode_node_fragment_program(32, &catalog, &fragment);
    let parts = split_node_program_per_row_group(&program).unwrap();
    assert_eq!(parts.len(), 2);
    let row_groups: Vec<u32> = parts
        .iter()
        .map(|part| {
            let again = split_node_program_per_row_group(part).unwrap();
            assert_eq!(again.len(), 1, "one row group does not split again");
            decode_program_fragment(part).decode[0].row_group
        })
        .collect();
    assert_eq!(row_groups.len(), 2);
    assert_ne!(row_groups[0], row_groups[1]);
}

fn decode_program_fragment(program: &[u8]) -> NodeFragment {
    use crate::plan::stream_program::PlanCursor;
    let mut cur = PlanCursor::new(program);
    assert_eq!(cur.u8().unwrap(), crate::plan::stream_program::PLAN_V2);
    assert_eq!(cur.u32().unwrap(), 1);
    assert_eq!(
        cur.u8().unwrap(),
        crate::plan::stream_program::OP_NODE_FRAGMENT
    );
    let _batch = cur.u32().unwrap();
    let _catalog = cur.catalog().unwrap();
    let len = cur.u32().unwrap() as usize;
    decode_node_fragment(cur.bytes(len).unwrap()).unwrap()
}

#[test]
fn fragments_point_at_their_objects_source_index() {
    use crate::program::types::DecodeGroup;
    let fragment = |node: u32, objects: &[&str]| NodeFragment {
        node: NodeId(node),
        decode: objects
            .iter()
            .map(|object| DecodeGroup {
                object_id: object.to_string(),
                row_group: 0,
                rows: 1,
                columns: vec![0],
                reads: Vec::new(),
            })
            .collect(),
        logical_reads: Vec::new(),
        fetches: Vec::new(),
        steps: Vec::new(),
        output_op: 0,
        tail: FragmentTail::default(),
        fetch_depth: 1,
        source: 0,
        row_id: None,
        claim_units: Vec::new(),
        own_units: Vec::new(),
    };
    let mut fragments = BTreeMap::from([
        (NodeId(0), fragment(0, &["t#2"])),
        (NodeId(1), fragment(1, &["t#0", "t#0"])),
    ]);
    assign_sources(&mut fragments, &["t#0", "t#1", "t#2"]).unwrap();
    assert_eq!(
        (fragments[&NodeId(0)].source, fragments[&NodeId(1)].source),
        (2, 0)
    );
    let mut mixed = BTreeMap::from([(NodeId(0), fragment(0, &["t#0", "t#1"]))]);
    assert!(assign_sources(&mut mixed, &["t#0", "t#1"]).is_err());
}

#[test]
fn hash_scans_of_one_object_share_a_unit_catalog_and_name_their_own() {
    use crate::program::types::{DecodeGroup, FetchRequest, LogicalRead};
    let read = |id: u32, row_group: u32| LogicalRead {
        id,
        scan: 0,
        node: NodeId(0),
        object_id: "t".into(),
        row_group,
        column: 0,
        offset: u64::from(row_group) * 100,
        length: 10,
        dictionary: false,
        codec: 0,
        fetch_id: id,
        pieces: Vec::new(),
    };
    let fetch = |id: u32, reads: Vec<u32>| FetchRequest {
        id,
        node: NodeId(0),
        object_id: "t".into(),
        offset: 0,
        length: 10,
        logical_reads: reads,
    };
    // Scan 0 holds row groups 0..4; one fetch serves 1 and 2, so they form one unit.
    // Scan 1 holds 4..6.
    let scan = |node: u32, groups: std::ops::Range<u32>, fetches: Vec<FetchRequest>| NodeFragment {
        node: NodeId(node),
        decode: groups
            .clone()
            .map(|row_group| DecodeGroup {
                object_id: "t".into(),
                row_group,
                rows: 1,
                columns: vec![0],
                reads: vec![row_group],
            })
            .collect(),
        logical_reads: groups.map(|row_group| read(row_group, row_group)).collect(),
        fetches,
        steps: vec![FragmentStep::HashPartition {
            input: PartitionInput::Rows { keys: vec![0] },
            partitions: 2,
            hash_version: 1,
        }],
        output_op: 0,
        tail: FragmentTail::default(),
        fetch_depth: 1,
        source: 0,
        row_id: None,
        claim_units: Vec::new(),
        own_units: Vec::new(),
    };
    let mut fragments = BTreeMap::from([
        (
            NodeId(0),
            scan(
                0,
                0..4,
                vec![
                    fetch(10, vec![0]),
                    fetch(11, vec![1, 2]),
                    fetch(12, vec![3]),
                ],
            ),
        ),
        (
            NodeId(1),
            scan(1, 4..6, vec![fetch(13, vec![4]), fetch(14, vec![5])]),
        ),
    ]);
    // By cost: 3 (9), 0 (5), {1, 2} (4), 4 (1), 5 (1). Dealt 0, 1, 1, 0, 0: each scan
    // starts on a heavy unit, though scan 0 planned all three.
    let cost = |_: &str, row_group: u32, _: u32| [5, 2, 2, 9, 1, 1][row_group as usize];
    add_claim_units(&mut fragments, &cost);
    let units = |node: u32| -> Vec<Vec<u32>> {
        fragments[&NodeId(node)]
            .claim_units
            .iter()
            .map(|unit| unit.row_groups.clone())
            .collect()
    };
    let catalog = vec![vec![0], vec![1, 2], vec![3], vec![4], vec![5]];
    assert_eq!((units(0), units(1)), (catalog.clone(), catalog));
    assert_eq!(fragments[&NodeId(0)].own_units, vec![3, 4, 5]);
    assert_eq!(fragments[&NodeId(1)].own_units, vec![0, 1]);
    // Each scan can decode every unit it may claim.
    assert_eq!(fragments[&NodeId(0)].decode.len(), 6);
    assert_eq!(fragments[&NodeId(1)].fetches.len(), 5);
    // Scan 1 lists scan 0's units after its own: ids step backwards on the wire. (Reads
    // copied from scan 0 still name node 0; the wire gives every read its fragment's node.)
    for fragment in fragments.values() {
        let bytes = encode_node_fragment(fragment);
        let decoded = decode_node_fragment(&bytes).unwrap();
        assert_eq!(encode_node_fragment(&decoded), bytes);
        assert_eq!(
            (&decoded.claim_units, &decoded.own_units),
            (&fragment.claim_units, &fragment.own_units)
        );
    }
}
