//! Milestone A: hash partition row conservation and deterministic routing.

use std::sync::Arc;

use wcol::exec::operator::{StreamProgress, WcolStreamItem};
use wcol::ops::exchange::{
    hash_row_keys_at, partition_index, HashPartitionIo, HashPartitionOp, HashPartitionPlan,
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

fn run_batches(op: &mut HashPartitionOp, batches: Vec<OwnedWcolBatch>) -> u64 {
    let parts = op.stats().row_counts.len();
    let mut outs = vec![None; parts];
    let mut iter = batches.into_iter();
    let mut pending = iter.next().map(WcolStreamItem::from_row_batch);
    let mut input_eof = false;
    let mut emitted = 0u64;
    loop {
        let blocked = vec![false; parts];
        let mut io = HashPartitionIo {
            input: None,
            input_slot: Some(&mut pending),
            input_eof,
            outputs: &mut outs,
            outputs_blocked: &blocked,
        };
        match op.poll(&mut io).unwrap() {
            StreamProgress::HaveOutput => {
                for slot in outs.iter_mut() {
                    if let Some(item) = slot.take() {
                        emitted += item.batch().row_count() as u64;
                    }
                }
            }
            StreamProgress::NeedInput => {
                if pending.is_none() && !input_eof {
                    pending = iter.next().map(WcolStreamItem::from_row_batch);
                    if pending.is_none() {
                        input_eof = true;
                    }
                }
            }
            StreamProgress::Finished => break,
            StreamProgress::Blocked => panic!("unexpected blocked"),
            _ => {}
        }
    }
    emitted
}

#[test]
fn row_conservation_multiple_batches() {
    let plan = HashPartitionPlan::new(vec![0], 4, WCOL_EXCHANGE_HASH_VERSION_DEFAULT).unwrap();
    let mut op = HashPartitionOp::with_limits(plan, 8, 32).unwrap();
    let inputs: Vec<OwnedWcolBatch> = (0..5)
        .map(|chunk| {
            let mut b = OwnedWcolBatch::empty();
            let base = chunk * 10;
            b.push_i64_column(0, (base..base + 10).collect(), vec![1; 10]);
            b.set_row_count(10).unwrap();
            b
        })
        .collect();
    let emitted = run_batches(&mut op, inputs);
    assert_eq!(emitted, 50);
    assert_eq!(op.stats().row_counts.iter().sum::<u64>(), 50);
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

    let mut op1 = HashPartitionOp::for_keys(vec![7], 8).unwrap();
    let mut op2 = HashPartitionOp::for_keys(vec![7], 8).unwrap();
    run_batches(&mut op1, vec![plain]);
    run_batches(&mut op2, vec![dict]);

    let p_plain = op1.stats().row_counts.iter().position(|&c| c > 0).unwrap();
    let p_dict = op2.stats().row_counts.iter().position(|&c| c > 0).unwrap();
    assert_eq!(p_plain, p_dict);
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
