use super::*;

#[test]
fn estimated_requests_follow_coalescing() {
    let config = PhysicalPlannerConfig::default();
    // Within the 128 KiB gap: one request.
    assert_eq!(
        estimated_requests(&[draft(0, 1000), draft(64 * 1024, 1000)], &config),
        1
    );
    // Far apart: two.
    assert_eq!(
        estimated_requests(&[draft(0, 1000), draft(10 << 20, 1000)], &config),
        2
    );
    // 5 MiB chunk: split at the 2 MiB request cap.
    assert_eq!(estimated_requests(&[draft(0, 5 << 20)], &config), 3);
    assert_eq!(estimated_requests(&[], &config), 0);
}

#[test]
fn tiny_chunks_cost_their_requests_not_just_their_bytes() {
    let config = PhysicalPlannerConfig::default();
    let tiny = unit_cost(&[draft(0, 100)], &config);
    let big = unit_cost(&[draft(0, 2 << 20)], &config);
    // A 100-byte read still costs a request: within 2x of a full 2 MiB fetch.
    assert!(
        tiny >= REQUEST_COST_BYTES && big < 4 * tiny,
        "tiny {tiny} big {big}"
    );
    // Placement, shaped like full-hits Q1: 40 row groups whose column compressed to
    // almost nothing, then 4 full ones. Each unit is one request.
    let sizes: Vec<u64> = std::iter::repeat_n(100u64, 40)
        .chain([2 << 20; 4])
        .collect();
    let requests_per_node = |weights: &[u64]| {
        let mut per_node = [0usize; 4];
        for slot in assign(weights, 4) {
            per_node[slot] += 1;
        }
        per_node
    };
    let by_bytes = requests_per_node(&sizes);
    let by_cost = requests_per_node(
        &sizes
            .iter()
            .map(|&n| unit_cost(&[draft(0, n)], &config))
            .collect::<Vec<_>>(),
    );
    assert!(
        by_bytes.iter().max().unwrap() >= &40,
        "bytes-only should pile tiny units: {by_bytes:?}"
    );
    assert!(
        by_cost.iter().max().unwrap() <= &14,
        "cost weights should spread requests: {by_cost:?}"
    );
}

#[test]
fn few_groups_stay_on_gather_whatever_the_input_size() {
    // 9k groups fit one reducer: scans pre-aggregate on the gather plan.
    assert_eq!(
        auto_reducer_count(9_000, 100_000_000, 128, &spill(true)).unwrap(),
        1
    );
    // Full-hits RegionID: a 131K-group footer bound is over one reducer's 16 MiB, but its
    // partials still merge in one place for less than shuffling 100M rows.
    assert_eq!(
        auto_reducer_count(131_070, 100_000_000, 384, &spill(true)).unwrap(),
        1
    );
    // Past the gather budget, partition.
    assert!(auto_reducer_count(200_000, 100_000_000, 384, &spill(true)).unwrap() > 1);
}

#[test]
fn partitioned_aggregates_scale_with_input_rows() {
    // Memory alone asks for 8; 100M rows at 8M per reducer asks for 16.
    let groups = 8 * 16 * 1024 * 1024 / 128;
    assert_eq!(
        auto_reducer_count(groups, 100_000_000, 128, &spill(true)).unwrap(),
        16
    );
    // Small input keeps the memory count.
    assert_eq!(
        auto_reducer_count(groups, 2_000_000, 128, &spill(true)).unwrap(),
        8
    );
}

#[test]
fn a_row_count_fallback_estimate_is_capped_at_the_host_limit() {
    // Keys without a footer bound fall back to the row count: 100M groups exceed every
    // reducer count, but the bound says nothing about the real (filtered) group count.
    assert_eq!(
        auto_reducer_count(100_000_000, 100_000_000, 128, &spill(true)).unwrap(),
        128
    );
    assert_eq!(
        auto_reducer_count(100_000_000, 100_000_000, 128, &spill(false)).unwrap(),
        128
    );
}

#[test]
fn a_bounded_estimate_above_the_host_limit_is_refused_without_spill() {
    // Footer bounds put 50M groups in 100M rows: more state than 128 reducers hold.
    assert_eq!(
        auto_reducer_count(50_000_000, 100_000_000, 128, &spill(true)).unwrap(),
        128
    );
    assert!(auto_reducer_count(50_000_000, 100_000_000, 128, &spill(false)).is_err());
}

#[test]
fn the_cap_is_the_largest_power_of_two_within_the_stream_limit() {
    assert_eq!(largest_reducer_count(128), 128);
    assert_eq!(largest_reducer_count(100), 64);
    assert_eq!(largest_reducer_count(64), 64);
    assert_eq!(largest_reducer_count(5), 4);
    assert_eq!(largest_reducer_count(0), 1);
}

#[test]
fn row_groups_whose_bounds_exclude_the_filter_are_not_read() {
    // Column 0 is sorted: row group N holds 10N..=10N+9. Column 1 has no bounds.
    let chunks = (0..4)
        .flat_map(|group| {
            [
                bounded(0, group, i64::from(group) * 10, i64::from(group) * 10 + 9),
                chunk(1, group, u64::from(group) * 100 + 50, 10),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        pruned_scan(vec![compare(0, CmpOp::Eq, 25)], chunks.clone()),
        vec![2]
    );
    assert_eq!(
        pruned_scan(vec![compare(0, CmpOp::GtEq, 19)], chunks.clone()),
        vec![1, 2, 3]
    );
    assert_eq!(
        pruned_scan(vec![compare(0, CmpOp::Lt, 10)], chunks.clone()),
        vec![0]
    );
    let in_list = Predicate::In {
        column: 0,
        values: vec![Literal::Int(5), Literal::Int(35)],
        negated: false,
    };
    assert_eq!(pruned_scan(vec![in_list], chunks.clone()), vec![0, 3]);
    assert_eq!(
        pruned_scan(vec![compare(1, CmpOp::Eq, 25)], chunks.clone()),
        vec![0, 1, 2, 3],
        "a column without bounds prunes nothing"
    );
    assert_eq!(
        pruned_scan(vec![compare(0, CmpOp::Eq, 99)], chunks),
        vec![0],
        "with every row group pruned one stays, and it filters to nothing"
    );
}

#[test]
fn bounds_across_the_sign_bit_are_not_trusted() {
    assert_eq!(int_range(Some(5), Some(-1)), None);
    assert_eq!(int_range(Some(-3), Some(7)), Some((-3, 7)));
    assert!(int_compare_may_pass(CmpOp::NotEq, 4, 4, 5));
    assert!(!int_compare_may_pass(CmpOp::NotEq, 4, 4, 4));
}

#[test]
fn a_hash_scan_combines_unless_its_key_is_provably_near_unique() {
    let mut meta = catalog(Vec::new());
    // Column 0 has at most 40 values, column 1 a million, column 2 no footer bound.
    meta.leaf_distinct_upper = vec![Some(40), Some(1_000_000), None];
    let agg = |kind, column| AggExpr {
        kind,
        column,
        offset: 0,
    };
    let spec = |group_by: Vec<u32>, aggregates: Vec<AggExpr>| AggSpec {
        group_by,
        aggregates,
    };
    let count = || vec![agg(AggKind::CountStar, None)];
    let rows = 1_000;
    assert!(scan_combines(&spec(vec![0], count()), &meta, rows));
    assert!(!scan_combines(&spec(vec![1], count()), &meta, rows));
    assert!(
        scan_combines(&spec(vec![2], count()), &meta, rows),
        "unbounded text combines"
    );
    assert!(!scan_combines(&spec(vec![0, 1], count()), &meta, rows));
    // The COUNT(DISTINCT) column is part of the partial key.
    let distinct = |column| vec![agg(AggKind::CountDistinct, Some(column))];
    assert!(!scan_combines(&spec(vec![0], distinct(1)), &meta, rows));
    assert!(scan_combines(&spec(vec![], distinct(2)), &meta, rows));
}
