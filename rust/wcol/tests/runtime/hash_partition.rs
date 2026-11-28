//! Hash exchange routing: row conservation and deterministic partitions.

use std::sync::Arc;

use wcol::ops::exchange::{
    hash_row_keys_at, partition_index, split_owned_batch, BatchPartitionPlan, HashPartitionPlan,
    WCOL_EXCHANGE_HASH_VERSION_DEFAULT,
};
use wcol::stream::{DictionaryValues, OwnedWcolBatch};

#[test]
fn composite_keys_differ_from_single_column() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1], vec![1]);
    batch.push_i64_column(1, vec![2], vec![1]);
    batch.set_row_count(1).unwrap();
    let mut view = Vec::new();
    let b = batch.view_with(&mut view);
    let h0 = hash_row_keys_at(&b, &[0], 0, 1).unwrap();
    let h1 = hash_row_keys_at(&b, &[0, 1], 0, 1).unwrap();
    assert_ne!(h0, h1);
}

fn split(plan: &HashPartitionPlan, batch: &OwnedWcolBatch) -> Vec<OwnedWcolBatch> {
    split_owned_batch(&BatchPartitionPlan::Hash(plan.clone()), batch).unwrap()
}

#[test]
fn row_conservation_multiple_batches() {
    let plan = HashPartitionPlan::new(vec![0], 4, WCOL_EXCHANGE_HASH_VERSION_DEFAULT).unwrap();
    let mut per_partition = [0usize; 4];
    for chunk in 0..5 {
        let mut batch = OwnedWcolBatch::empty();
        let base = chunk * 10;
        batch.push_i64_column(0, (base..base + 10).collect(), vec![1; 10]);
        batch.set_row_count(10).unwrap();
        for (part, out) in split(&plan, &batch).iter().enumerate() {
            per_partition[part] += out.row_count();
        }
    }
    assert_eq!(per_partition.iter().sum::<usize>(), 50);
}

#[test]
fn dict_encoded_key_matches_plain_i64_partition() {
    let key = 99_i64;
    let mut plain = OwnedWcolBatch::empty();
    plain.push_i64_column(7, vec![key], vec![1]);
    plain.set_row_count(1).unwrap();

    let mut dict = OwnedWcolBatch::empty();
    dict.push_dict_id_column(
        7,
        vec![0],
        vec![1],
        DictionaryValues {
            scope: 0,
            column: 7,
            entries: wcol::stream::DictEntries::Int(Arc::new(vec![key, key + 1])),
        },
    );
    dict.set_row_count(1).unwrap();

    let plan = HashPartitionPlan::new(vec![7], 8, WCOL_EXCHANGE_HASH_VERSION_DEFAULT).unwrap();
    let occupied = |batch: &OwnedWcolBatch| {
        split(&plan, batch)
            .iter()
            .position(|out| out.row_count() > 0)
            .unwrap()
    };
    assert_eq!(occupied(&plain), occupied(&dict));
}

#[test]
fn partition_index_is_stable_modulo() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![12345], vec![1]);
    batch.set_row_count(1).unwrap();
    let mut view = Vec::new();
    let b = batch.view_with(&mut view);
    let hash = hash_row_keys_at(&b, &[0], 0, 1).unwrap();
    assert_eq!(partition_index(hash, 4), partition_index(hash, 4));
}
