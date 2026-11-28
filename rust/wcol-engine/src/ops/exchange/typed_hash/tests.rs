use super::*;
use crate::stream::{DictEntries, DictionaryValues, OwnedWcolBatch};
use std::sync::Arc;

#[test]
fn same_logical_dict_and_i64_key_same_partition() {
    let mut plain = OwnedWcolBatch::empty();
    plain.push_i64_column(1, vec![42], vec![1]);
    plain.set_row_count(1).unwrap();

    let mut dict = OwnedWcolBatch::empty();
    dict.push_dict_id_column(
        1,
        vec![0],
        vec![1],
        DictionaryValues {
            scope: 0,
            column: 1,
            entries: DictEntries::Int(Arc::new(vec![42_i64])),
        },
    );
    dict.set_row_count(1).unwrap();

    let plan_keys = [1];
    let mut v1 = Vec::new();
    let mut v2 = Vec::new();
    let b1 = plain.view_with(&mut v1);
    let b2 = dict.view_with(&mut v2);
    let h1 = hash_row_keys_at(&b1, &plan_keys, 0, WCOL_EXCHANGE_HASH_VERSION_DEFAULT).unwrap();
    let h2 = hash_row_keys_at(&b2, &plan_keys, 0, WCOL_EXCHANGE_HASH_VERSION_DEFAULT).unwrap();
    assert_eq!(h1, h2);
}

#[test]
fn null_keys_share_one_hash() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1, 2], vec![0, 0]);
    batch.set_row_count(2).unwrap();
    let keys = [0];
    let mut view = Vec::new();
    let b = batch.view_with(&mut view);
    let h0 = hash_row_keys_at(&b, &keys, 0, 1).unwrap();
    let h1 = hash_row_keys_at(&b, &keys, 1, 1).unwrap();
    assert_eq!(h0, h1);
}

#[test]
fn hash_version_changes_assignment() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![7], vec![1]);
    batch.set_row_count(1).unwrap();
    let keys = [0];
    let mut view = Vec::new();
    let b = batch.view_with(&mut view);
    let a = hash_row_keys_at(&b, &keys, 0, 1).unwrap();
    let b_hash = hash_row_keys_at(&b, &keys, 0, 2).unwrap();
    assert_ne!(a, b_hash);
}

fn byte_dict(column: u32, scope: u32, ids: Vec<u32>, values: &[&[u8]]) -> OwnedWcolBatch {
    let mut offsets = vec![0u32];
    let mut bytes = Vec::new();
    for value in values {
        bytes.extend_from_slice(value);
        offsets.push(bytes.len() as u32);
    }
    let rows = ids.len();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_dict_id_column(
        column,
        ids,
        vec![1; rows],
        DictionaryValues {
            scope,
            column,
            entries: DictEntries::Bytes {
                offsets: Arc::from(offsets),
                bytes: Arc::from(bytes),
            },
        },
    );
    batch.set_row_count(rows).unwrap();
    batch
}

#[test]
fn same_string_different_dictionary_ids_hash_equal() {
    let left = byte_dict(0, 1, vec![2], &[b"/home", b"/products", b"/checkout"]);
    let right = byte_dict(0, 2, vec![0], &[b"/checkout"]);
    let keys = [0];
    let mut v1 = Vec::new();
    let mut v2 = Vec::new();
    let b1 = left.view_with(&mut v1);
    let b2 = right.view_with(&mut v2);
    let h1 = hash_row_keys_at(&b1, &keys, 0, 1).unwrap();
    let h2 = hash_row_keys_at(&b2, &keys, 0, 1).unwrap();
    assert_eq!(h1, h2);
    assert_eq!(
        key_fingerprint_at(&b1, &keys, 0).unwrap(),
        key_fingerprint_at(&b2, &keys, 0).unwrap()
    );
    let cache = ExchangeHashCache::prepare(&b1, &keys, 1).unwrap();
    assert_eq!(cache.lookup(&b1, &keys, 0).unwrap(), Some(h1));
    assert_eq!(
        partition_for_row(&b1, 0, &keys, 4, 1, &cache).unwrap(),
        partition_index(h1, 4)
    );
}

#[test]
fn plain_utf8_matches_dictionary_bytes() {
    let mut plain = OwnedWcolBatch::empty();
    plain.push_utf8_column(3, vec![0, 9], b"/checkout".to_vec(), vec![1]);
    plain.set_row_count(1).unwrap();
    let dict = byte_dict(3, 0, vec![0], &[b"/checkout"]);
    let keys = [3];
    let mut v1 = Vec::new();
    let mut v2 = Vec::new();
    let b1 = plain.view_with(&mut v1);
    let b2 = dict.view_with(&mut v2);
    assert_eq!(
        hash_row_keys_at(&b1, &keys, 0, 1).unwrap(),
        hash_row_keys_at(&b2, &keys, 0, 1).unwrap()
    );
}

#[test]
fn distinct_strings_stay_distinct_keys() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_utf8_column(0, vec![0, 2, 3], b"abc".to_vec(), vec![1, 1]);
    batch.set_row_count(2).unwrap();
    let keys = [0];
    let mut view = Vec::new();
    let b = batch.view_with(&mut view);
    assert_ne!(
        key_fingerprint_at(&b, &keys, 0).unwrap(),
        key_fingerprint_at(&b, &keys, 1).unwrap()
    );
}

#[test]
fn composite_string_tuple_is_length_prefixed() {
    let mut ab_c = OwnedWcolBatch::empty();
    ab_c.push_utf8_column(0, vec![0, 2], b"ab".to_vec(), vec![1]);
    ab_c.push_utf8_column(1, vec![0, 1], b"c".to_vec(), vec![1]);
    ab_c.set_row_count(1).unwrap();
    let mut a_bc = OwnedWcolBatch::empty();
    a_bc.push_utf8_column(0, vec![0, 1], b"a".to_vec(), vec![1]);
    a_bc.push_utf8_column(1, vec![0, 2], b"bc".to_vec(), vec![1]);
    a_bc.set_row_count(1).unwrap();
    let keys = [0, 1];
    let mut v1 = Vec::new();
    let mut v2 = Vec::new();
    let left = ab_c.view_with(&mut v1);
    let right = a_bc.view_with(&mut v2);
    assert_ne!(
        key_fingerprint_at(&left, &keys, 0).unwrap(),
        key_fingerprint_at(&right, &keys, 0).unwrap()
    );
}

#[test]
fn null_string_is_not_the_empty_string() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_utf8_column(0, vec![0, 0, 0], Vec::new(), vec![0, 1]);
    batch.set_row_count(2).unwrap();
    let keys = [0];
    let mut view = Vec::new();
    let b = batch.view_with(&mut view);
    assert_ne!(
        hash_row_keys_at(&b, &keys, 0, 1).unwrap(),
        hash_row_keys_at(&b, &keys, 1, 1).unwrap()
    );
}
