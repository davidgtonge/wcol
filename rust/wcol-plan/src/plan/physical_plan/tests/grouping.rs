use super::*;

#[test]
fn unknown_cardinality_uses_source_rows() {
    let meta = million_row_keys();
    assert_eq!(
        estimated_distinct_groups(&[0, 1], &meta, 1_000_000),
        1_000_000
    );
    let plan = lower_to_physical(
        &grouped_count(vec![0, 1]),
        &meta,
        &nodes(1),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(plan.metrics.estimated_distinct_groups, 1_000_000);
    // 1M groups at ESTIMATED_GROUP_BYTES (384) over the 16 MiB default target need 23
    // reducers, rounded up to a power of two.
    assert_eq!(plan.metrics.hash_reducers, 32);
    assert!(plan
        .operators
        .iter()
        .any(|op| matches!(op.kind, OpKind::HashExchange { partitions: 32, .. })));
}

#[test]
fn small_domain_product_stays_on_gather() {
    let mut meta = million_row_keys();
    meta.leaf_distinct_upper = vec![Some(20), Some(50)];
    assert_eq!(estimated_distinct_groups(&[0, 1], &meta, 1_000_000), 1_000);
    let plan = lower_to_physical(
        &grouped_count(vec![0, 1]),
        &meta,
        &nodes(1),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(plan.metrics.estimated_distinct_groups, 1_000);
    assert_eq!(plan.metrics.hash_reducers, 1);
    assert!(plan
        .operators
        .iter()
        .all(|op| !matches!(op.kind, OpKind::HashExchange { .. })));
    assert!(plan
        .operators
        .iter()
        .any(|op| matches!(op.kind, OpKind::Exchange)));
}

#[test]
fn boolean_key_is_a_known_small_domain() {
    let mut meta = million_row_keys();
    meta.leaf_physical = vec![0, 1];
    meta.leaf_distinct_upper = vec![None, Some(4)];
    assert_eq!(estimated_distinct_groups(&[0, 1], &meta, 1_000_000), 8);
    let plan = lower_to_physical(
        &grouped_count(vec![0, 1]),
        &meta,
        &nodes(1),
        &PhysicalPlannerConfig::default(),
    )
    .unwrap();
    assert_eq!(plan.metrics.hash_reducers, 1);
    assert!(plan
        .operators
        .iter()
        .all(|op| !matches!(op.kind, OpKind::HashExchange { .. })));
}

#[test]
fn grouped_count_distinct_hashes_on_group_keys() {
    let mut config = PhysicalPlannerConfig::default();
    config.max_nodes = 4;
    config.hash_aggregate.reducers = Some(4);
    let plan = lower_to_physical(
        &grouped_distinct(vec![0], false),
        &few_groups(),
        &nodes(1),
        &config,
    )
    .unwrap();
    assert!(
        plan.operators.iter().any(|op| matches!(
            &op.kind,
            OpKind::HashExchange { keys, partitions: 4, .. } if keys == &vec![0, 1]
        )),
        "{}",
        plan.canonical_text()
    );
}

#[test]
fn global_count_distinct_hashes_on_the_value() {
    let mut config = PhysicalPlannerConfig::default();
    config.max_nodes = 4;
    config.hash_aggregate.reducers = Some(4);
    let plan = lower_to_physical(
        &grouped_distinct(vec![], false),
        &million_row_keys(),
        &nodes(1),
        &config,
    )
    .unwrap();
    assert!(
        plan.operators.iter().any(|op| matches!(
            &op.kind,
            OpKind::HashExchange { keys, partitions: 4, .. } if keys == &vec![1]
        )),
        "{}",
        plan.canonical_text()
    );
}

#[test]
fn mixed_distinct_and_count_hash_on_the_group_and_the_value() {
    let mut config = PhysicalPlannerConfig::default();
    config.max_nodes = 4;
    config.hash_aggregate.reducers = Some(4);
    let plan = lower_to_physical(
        &grouped_distinct(vec![0], true),
        &few_groups(),
        &nodes(1),
        &config,
    )
    .unwrap();
    assert!(
        plan.operators.iter().any(|op| matches!(
            &op.kind,
            OpKind::HashExchange { keys, partitions: 4, .. } if keys == &vec![0, 1]
        )),
        "{}",
        plan.canonical_text()
    );
}

#[test]
fn a_small_dictionary_keeps_the_value_in_the_keys() {
    // Q10: MobilePhoneModel has no bound, but its dictionary pages are tiny.
    let mut config = PhysicalPlannerConfig::default();
    config.max_nodes = 4;
    config.hash_aggregate.reducers = Some(4);
    let keys_for = |dictionary: u64| {
        let mut meta = million_row_keys();
        meta.leaf_dictionary_bytes = vec![Some(dictionary), Some(dictionary)];
        let plan = lower_to_physical(&grouped_distinct(vec![0], false), &meta, &nodes(1), &config)
            .unwrap();
        plan.operators
            .iter()
            .find_map(|op| match &op.kind {
                OpKind::HashExchange { keys, .. } => Some(keys.clone()),
                _ => None,
            })
            .unwrap()
    };
    assert_eq!(keys_for(829), vec![0, 1]);
    assert_eq!(keys_for(600_000), vec![0]);
}

#[test]
fn many_groups_partition_a_distinct_on_the_group_alone() {
    // Q13: an unbounded SearchPhrase has far more groups than reducers. Each group then
    // sits whole on one reducer, so its distinct count is final there.
    let mut config = PhysicalPlannerConfig::default();
    config.max_nodes = 4;
    config.hash_aggregate.reducers = Some(4);
    for extra_count in [false, true] {
        let plan = lower_to_physical(
            &grouped_distinct(vec![0], extra_count),
            &million_row_keys(),
            &nodes(1),
            &config,
        )
        .unwrap();
        assert!(
            plan.operators.iter().any(|op| matches!(
                &op.kind,
                OpKind::HashExchange { keys, partitions: 4, .. } if keys == &vec![0]
            )),
            "{}",
            plan.canonical_text()
        );
    }
}

#[test]
fn mixed_distinct_partitions_when_the_value_has_no_bound() {
    // A footer bound on the GROUP BY column alone used to keep Q9 on the gather plan.
    // The distinct value is part of the state, so a missing bound partitions.
    let meta = few_groups();
    let mut config = PhysicalPlannerConfig::default();
    config.max_nodes = 4;
    let plan =
        lower_to_physical(&grouped_distinct(vec![0], true), &meta, &nodes(1), &config).unwrap();
    assert!(plan.metrics.hash_reducers > 1);
    assert!(plan.operators.iter().any(|op| matches!(
        &op.kind,
        OpKind::HashExchange { keys, .. } if keys == &vec![0, 1]
    )));
}

#[test]
fn small_mixed_distinct_stays_on_gather() {
    let mut meta = million_row_keys();
    meta.leaf_distinct_upper = vec![Some(100), Some(100)];
    let mut config = PhysicalPlannerConfig::default();
    config.max_nodes = 4;
    let plan =
        lower_to_physical(&grouped_distinct(vec![0], true), &meta, &nodes(1), &config).unwrap();
    assert_eq!(plan.metrics.hash_reducers, 1);
    assert!(plan
        .operators
        .iter()
        .all(|op| !matches!(op.kind, OpKind::HashExchange { .. })));
}

#[test]
fn string_group_key_uses_hash_exchange() {
    let mut meta = million_row_keys();
    meta.leaf_physical = vec![6, 1];
    let mut config = PhysicalPlannerConfig::default();
    config.hash_aggregate.reducers = Some(2);
    let plan = lower_to_physical(&grouped_count(vec![0]), &meta, &nodes(1), &config).unwrap();
    assert!(plan
        .operators
        .iter()
        .any(|op| matches!(op.kind, OpKind::HashExchange { partitions: 2, .. })));
}
