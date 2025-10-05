use rustc_hash::FxHashMap;

use super::*;

fn bools_to_mask(bools: &[bool]) -> Vec<u32> {
    let mut mask = vec![0u32; MASK_WORDS];
    for (idx, on) in bools.iter().enumerate() {
        if *on {
            set_bit(&mut mask, idx);
        }
    }
    mask
}

fn mask_to_bools(mask: &[u32], rows: usize) -> Vec<bool> {
    (0..rows).map(|idx| get_bit(mask, idx)).collect()
}

fn bools_to_bitmap(bools: &[bool]) -> Vec<u8> {
    let mut bytes = vec![0u8; bools.len().div_ceil(8)];
    for (idx, on) in bools.iter().enumerate() {
        if *on {
            let byte = idx >> 3;
            let bit = idx & 7;
            bytes[byte] |= 1 << bit;
        }
    }
    bytes
}

fn dummy_col(id: u32, physical_type: u8, flags: u8) -> Column {
    Column {
        id,
        name: String::new(),
        logical_type: physical_type,
        physical_type,
        flags,
        encoding: 0,
        dict_id: 0,
        dict_index_width: 0,
        scale: 0,
    }
}

fn dummy_col_scaled(id: u32, physical_type: u8, flags: u8, scale: i32) -> Column {
    Column {
        id,
        name: String::new(),
        logical_type: physical_type,
        physical_type,
        flags,
        encoding: 0,
        dict_id: 0,
        dict_index_width: 0,
        scale,
    }
}

fn encode_raw_strings(values: &[&str]) -> Vec<u8> {
    let rows = values.len();
    let mut sorted: Vec<usize> = (0..rows).collect();
    sorted.sort_by(|a, b| values[*a].as_bytes().cmp(values[*b].as_bytes()));

    let mut row_to_unique: Vec<u16> = vec![0u16; rows];
    let mut lcps: Vec<u16> = Vec::new();
    let mut suffix_lens: Vec<u32> = Vec::new();
    let mut data_blob: Vec<u8> = Vec::new();
    let mut prev: Vec<u8> = Vec::new();

    for &orig_idx in &sorted {
        let bytes = values[orig_idx].as_bytes();
        if !lcps.is_empty() && bytes == prev.as_slice() {
            row_to_unique[orig_idx] = (lcps.len() - 1) as u16;
            continue;
        }
        let lcp = if lcps.is_empty() {
            0
        } else {
            bytes
                .iter()
                .zip(prev.iter())
                .take_while(|(a, b)| a == b)
                .count()
        };
        let suffix = &bytes[lcp..];
        lcps.push(lcp as u16);
        suffix_lens.push(suffix.len() as u32);
        data_blob.extend_from_slice(suffix);
        prev.clear();
        prev.extend_from_slice(bytes);
        row_to_unique[orig_idx] = (lcps.len() - 1) as u16;
    }

    let unique_count = lcps.len();
    let suffix_len_width = if suffix_lens.iter().copied().max().unwrap_or(0) <= u16::MAX as u32 {
        2u8
    } else {
        4u8
    };
    let header_size = 32usize;
    let row_id_width = 2u8;
    let row_off = header_size as u32;
    let lcp_off = row_off + (rows as u32) * row_id_width as u32;
    let len_off = lcp_off + (unique_count as u32) * 2;
    let dict_off = len_off + (unique_count as u32) * suffix_len_width as u32;
    let data_off = dict_off;
    let data_len = data_blob.len() as u32;

    let mut out = vec![0u8; header_size];
    out[0..2].copy_from_slice(&(rows as u16).to_le_bytes());
    out[2] = row_id_width;
    out[3] = suffix_len_width | 0x80;
    out[4..8].copy_from_slice(&row_off.to_le_bytes());
    out[8..12].copy_from_slice(&lcp_off.to_le_bytes());
    out[12..16].copy_from_slice(&len_off.to_le_bytes());
    out[16..20].copy_from_slice(&data_off.to_le_bytes());
    out[20..24].copy_from_slice(&data_len.to_le_bytes());
    out[24..28].copy_from_slice(&dict_off.to_le_bytes());
    out[28..32].copy_from_slice(&0u32.to_le_bytes());

    for value in row_to_unique {
        out.extend_from_slice(&value.to_le_bytes());
    }
    for value in lcps {
        out.extend_from_slice(&value.to_le_bytes());
    }
    for value in suffix_lens {
        if suffix_len_width == 2 {
            out.extend_from_slice(&(value as u16).to_le_bytes());
        } else {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    out.extend_from_slice(&data_blob);
    out
}

#[test]
fn empty_string_mask_mixed_rows() {
    let rows = 8usize;
    let nulls = vec![true, false, true, true, true, true, false, true];
    let empties = vec![false, false, true, false, false, true, false, false];
    let null_bitmap = bools_to_bitmap(&nulls);
    let empty_bitmap = bools_to_bitmap(&empties);

    let entry = IndexEntry {
        data_off: 0,
        data_comp_len: 0,
        data_raw_len: 0,
        null_off: 0,
        null_comp_len: 0,
        null_raw_len: null_bitmap.len() as u32,
        empty_mode: EMPTY_MODE_MIXED,
        empty_count: 2,
        empty_off: 0,
        empty_comp_len: 0,
        empty_raw_len: empty_bitmap.len() as u32,
        min: 0.0,
        max: 0.0,
        presence: 0,
    };
    let col = dummy_col(0, TYPE_STRING, FLAG_NULLABLE);
    let filter_eq = Filter {
        col_id: 0,
        op: OP_EQ,
        value: 0.0,
        value2: 0.0,
        in_list: None,
        value_str: Some("".to_string()),
        in_list_str: None,
        like_ids: None,
    };
    let filter_neq = Filter {
        op: OP_NEQ,
        ..filter_eq.clone()
    };

    let eq_mask = build_empty_string_mask(
        &entry,
        &col,
        &filter_eq,
        rows,
        Some(&empty_bitmap),
        Some(&null_bitmap),
    )
    .expect("eq mask");
    assert_eq!(mask_to_bools(&eq_mask, rows), empties);

    let neq_mask = build_empty_string_mask(
        &entry,
        &col,
        &filter_neq,
        rows,
        Some(&empty_bitmap),
        Some(&null_bitmap),
    )
    .expect("neq mask");
    let expected_neq = vec![true, false, false, true, true, false, false, true];
    assert_eq!(mask_to_bools(&neq_mask, rows), expected_neq);
}

#[test]
fn empty_string_mask_all_one() {
    let rows = 4usize;
    let entry = IndexEntry {
        data_off: 0,
        data_comp_len: 0,
        data_raw_len: 0,
        null_off: 0,
        null_comp_len: 0,
        null_raw_len: 0,
        empty_mode: EMPTY_MODE_ALL_ONE,
        empty_count: rows as u32,
        empty_off: 0,
        empty_comp_len: 0,
        empty_raw_len: 0,
        min: 0.0,
        max: 0.0,
        presence: 0,
    };
    let col = dummy_col(0, TYPE_STRING, 0);
    let filter_eq = Filter {
        col_id: 0,
        op: OP_EQ,
        value: 0.0,
        value2: 0.0,
        in_list: None,
        value_str: Some("".to_string()),
        in_list_str: None,
        like_ids: None,
    };
    let filter_neq = Filter {
        op: OP_NEQ,
        ..filter_eq.clone()
    };

    let eq_mask =
        build_empty_string_mask(&entry, &col, &filter_eq, rows, None, None).expect("eq mask");
    assert_eq!(mask_to_bools(&eq_mask, rows), vec![true; rows]);

    let neq_mask =
        build_empty_string_mask(&entry, &col, &filter_neq, rows, None, None).expect("neq mask");
    assert_eq!(mask_to_bools(&neq_mask, rows), vec![false; rows]);
}

#[test]
fn empty_string_mask_all_zero_with_nulls() {
    let rows = 6usize;
    let nulls = vec![true, false, true, true, false, true];
    let null_bitmap = bools_to_bitmap(&nulls);
    let entry = IndexEntry {
        data_off: 0,
        data_comp_len: 0,
        data_raw_len: 0,
        null_off: 0,
        null_comp_len: 0,
        null_raw_len: null_bitmap.len() as u32,
        empty_mode: EMPTY_MODE_ALL_ZERO,
        empty_count: 0,
        empty_off: 0,
        empty_comp_len: 0,
        empty_raw_len: 0,
        min: 0.0,
        max: 0.0,
        presence: 0,
    };
    let col = dummy_col(0, TYPE_STRING, FLAG_NULLABLE);
    let filter_neq = Filter {
        col_id: 0,
        op: OP_NEQ,
        value: 0.0,
        value2: 0.0,
        in_list: None,
        value_str: Some("".to_string()),
        in_list_str: None,
        like_ids: None,
    };
    let neq_mask =
        build_empty_string_mask(&entry, &col, &filter_neq, rows, None, Some(&null_bitmap))
            .expect("neq mask");
    assert_eq!(mask_to_bools(&neq_mask, rows), nulls);
}

#[test]
fn raw_string_decode_builds_dict_ids() {
    let values = ["alpha", "beta", "alpha", ""];
    let raw = encode_raw_strings(&values);
    let mut dict = Dictionary::new();
    let ids = decode_raw_string_ids(&raw, values.len(), &mut dict, None).expect("decode");
    assert_eq!(ids, vec![1, 2, 1, 0]);
    assert_eq!(dict.values, vec!["", "alpha", "beta"]);
}

#[test]
fn raw_string_hash_ids_mode() {
    std::env::set_var("WCOL_STRING_HASH_IDS", "1");
    let values = ["alpha", "beta", "alpha", ""];
    let raw = encode_raw_strings(&values);
    let mut dict = Dictionary::new();
    let ids = decode_raw_string_ids(&raw, values.len(), &mut dict, None).expect("decode");
    assert_eq!(ids.len(), values.len());
    assert_eq!(ids[0], ids[2]);
    assert_eq!(dict.values.len(), 0);
    assert_eq!(dict.hash_cache.len(), 3);
    std::env::remove_var("WCOL_STRING_HASH_IDS");
}

#[test]
fn raw_string_group_by_counts() {
    let values = ["alpha", "beta", "alpha", "gamma", "beta"];
    let raw = encode_raw_strings(&values);
    let mut dict = Dictionary::new();
    let ids = decode_raw_string_ids(&raw, values.len(), &mut dict, None).expect("decode");
    let col = dummy_col(0, TYPE_U32, 0);
    let data = ColumnData::U32(ids);
    let key_data = vec![(&col, &data)];
    let mut counts: FxHashMap<crate::types::GroupKey, u32> = FxHashMap::default();
    for row in 0..values.len() {
        let key = build_group_key(&key_data, row);
        *counts.entry(key).or_insert(0) += 1;
    }
    let mut observed: FxHashMap<String, u32> = FxHashMap::default();
    for (key, count) in counts {
        let id = key.a as usize;
        let value = dict.values.get(id).expect("dict value");
        observed.insert(value.clone(), count);
    }
    let mut expected: FxHashMap<String, u32> = FxHashMap::default();
    for value in values {
        *expected.entry(value.to_string()).or_insert(0) += 1;
    }
    assert_eq!(observed, expected);
}

#[test]
fn raw_string_like_mask_matches_sorted_values() {
    let values = ["alpha", "beta", "alpha", "gamma", "", "delta"];
    let raw = encode_raw_strings(&values);
    let rows = values.len();
    let mask = decode_raw_string_like_mask(&raw, rows, "pha", false, None, None).expect("decode");
    let observed = mask_to_bools(&mask, rows);
    let expected = vec![true, false, true, false, false, false];
    assert_eq!(observed, expected);

    let negated = decode_raw_string_like_mask(&raw, rows, "pha", true, None, None).expect("decode");
    let observed_neg = mask_to_bools(&negated, rows);
    let expected_neg = expected.iter().map(|v| !v).collect::<Vec<bool>>();
    assert_eq!(observed_neg, expected_neg);
}

#[test]
fn raw_string_like_boundary_match() {
    let values = ["hello", "helloworld"];
    let raw = encode_raw_strings(&values);
    let rows = values.len();
    let mask = decode_raw_string_like_mask(&raw, rows, "ow", false, None, None).expect("decode");
    let observed = mask_to_bools(&mask, rows);
    assert_eq!(observed, vec![false, true]);
}

fn pred_for_op(op: u8, value: f64, a: f64, b: f64) -> bool {
    match op {
        OP_EQ => value == a,
        OP_NEQ => value != a,
        OP_LT => value < a,
        OP_LTE => value <= a,
        OP_GT => value > a,
        OP_GTE => value >= a,
        OP_BETWEEN => value >= a && value <= b,
        _ => false,
    }
}

#[test]
fn rows_in_chunk_boundaries() {
    let chunk = ROWS_PER_CHUNK;
    assert_eq!(rows_in_chunk(0, ROWS_PER_CHUNK, 0), 0);
    assert_eq!(rows_in_chunk(0, ROWS_PER_CHUNK, 4), 0);

    assert_eq!(rows_in_chunk(1, ROWS_PER_CHUNK, 0), 1);
    assert_eq!(rows_in_chunk(1, ROWS_PER_CHUNK, 1), 0);

    assert_eq!(rows_in_chunk(chunk as u64, chunk, 0), chunk);
    assert_eq!(rows_in_chunk(chunk as u64, chunk, 1), 0);

    assert_eq!(rows_in_chunk(chunk as u64 + 1, chunk, 0), chunk);
    assert_eq!(rows_in_chunk(chunk as u64 + 1, chunk, 1), 1);

    let total = chunk as u64 * 3 + 17;
    assert_eq!(rows_in_chunk(total, chunk, 2), chunk);
    assert_eq!(rows_in_chunk(total, chunk, 3), 17);
    assert_eq!(rows_in_chunk(total, chunk, 4), 0);
}

#[test]
fn clear_tail_masks_correctly() {
    let mut mask = vec![0xffff_ffff; MASK_WORDS];
    clear_tail(&mut mask, ROWS_PER_CHUNK);
    assert!(mask.iter().all(|word| *word == 0xffff_ffff));

    let mut mask = vec![0xffff_ffff; MASK_WORDS];
    clear_tail(&mut mask, ROWS_PER_CHUNK - 1);
    assert_eq!(mask[MASK_WORDS - 1], 0x7fff_ffff);
    assert!(mask[..MASK_WORDS - 1]
        .iter()
        .all(|word| *word == 0xffff_ffff));

    let mut mask = vec![0xffff_ffff; MASK_WORDS];
    clear_tail(&mut mask, 0);
    assert!(mask.iter().all(|word| *word == 0));

    let mut mask = vec![0xffff_ffff; MASK_WORDS];
    clear_tail(&mut mask, 33);
    assert_eq!(mask[0], 0xffff_ffff);
    assert_eq!(mask[1], 0x0000_0001);
    assert!(mask[2..].iter().all(|word| *word == 0));
}

#[test]
fn mask_from_bitmap_length_guard() {
    let rows = 9;
    let short = vec![0x09];
    assert!(mask_from_bitmap(&short, rows).is_none());

    let full = vec![0x09, 0x01];
    let mask = mask_from_bitmap(&full, rows).expect("mask should be built");
    let bools = mask_to_bools(&mask, rows);
    let mut expected = vec![false; rows];
    expected[0] = true;
    expected[3] = true;
    expected[8] = true;
    assert_eq!(bools, expected);
}

#[test]
fn iter_mask_enumerates_set_bits() {
    let rows = 128;
    let mut mask = vec![0u32; MASK_WORDS];
    let set = vec![0, 1, 31, 32, 63, rows - 1];
    for idx in &set {
        set_bit(&mut mask, *idx);
    }
    let out: Vec<usize> = iter_mask(&mask, rows).collect();
    assert_eq!(out, set);

    let rows = 64;
    let mut mask = vec![0u32; MASK_WORDS];
    set_bit(&mut mask, 63);
    set_bit(&mut mask, 127);
    let out: Vec<usize> = iter_mask(&mask, rows).collect();
    assert_eq!(out, vec![63]);
}

#[test]
fn combine_masks_rpn_logic() {
    let rows = 128;
    let a: Vec<bool> = (0..rows).map(|i| i % 2 == 0).collect();
    let b: Vec<bool> = (0..rows).map(|i| i % 3 == 0).collect();
    let c: Vec<bool> = (0..rows).map(|i| i % 5 == 0).collect();
    let masks = vec![bools_to_mask(&a), bools_to_mask(&b), bools_to_mask(&c)];

    let and_mask = combine_masks(&[0, 1, COMB_AND], &masks).unwrap();
    let and_ref: Vec<bool> = (0..rows).map(|i| a[i] && b[i]).collect();
    assert_eq!(mask_to_bools(&and_mask, rows), and_ref);

    let or_mask = combine_masks(&[0, 1, COMB_OR], &masks).unwrap();
    let or_ref: Vec<bool> = (0..rows).map(|i| a[i] || b[i]).collect();
    assert_eq!(mask_to_bools(&or_mask, rows), or_ref);

    let not_mask = combine_masks(&[0, COMB_NOT], &masks).unwrap();
    let not_ref: Vec<bool> = (0..rows).map(|i| !a[i]).collect();
    assert_eq!(mask_to_bools(&not_mask, rows), not_ref);

    let expr1 = combine_masks(&[0, 1, COMB_AND, 2, COMB_OR], &masks).unwrap();
    let expr1_ref: Vec<bool> = (0..rows).map(|i| (a[i] && b[i]) || c[i]).collect();
    assert_eq!(mask_to_bools(&expr1, rows), expr1_ref);

    let expr2 = combine_masks(&[0, 1, 2, COMB_OR, COMB_AND], &masks).unwrap();
    let expr2_ref: Vec<bool> = (0..rows).map(|i| a[i] && (b[i] || c[i])).collect();
    assert_eq!(mask_to_bools(&expr2, rows), expr2_ref);

    assert!(combine_masks(&[3, 0, COMB_AND], &masks).is_err());
    assert!(combine_masks(&[COMB_AND], &masks).is_err());
    assert!(combine_masks(&[0, 1], &masks).is_err());
}

#[test]
fn build_single_mask_numeric_ops() {
    let rows = 64;
    let u8_values: Vec<u8> = (0..rows).map(|i| i as u8).collect();
    let u16_values: Vec<u16> = (0..rows).map(|i| i as u16).collect();
    let u32_values: Vec<u32> = (0..rows).map(|i| i as u32).collect();
    let i32_values: Vec<i32> = (0..rows).map(|i| i as i32 - 32).collect();
    let mut f64_values: Vec<f64> = (0..rows).map(|i| i as f64).collect();
    f64_values[10] = f64::NAN;

    let cases = [
        (OP_EQ, 10.0, 10.0),
        (OP_NEQ, 10.0, 10.0),
        (OP_LT, 10.0, 10.0),
        (OP_LTE, 10.0, 10.0),
        (OP_GT, 10.0, 10.0),
        (OP_GTE, 10.0, 10.0),
        (OP_BETWEEN, 10.0, 20.0),
    ];

    let col = dummy_col(0, TYPE_U8, 0);
    for (op, a, b) in cases {
        let mask = build_single_mask(&col, &ColumnData::U8(u8_values.clone()), op, a, b, rows);
        let expected: Vec<bool> = (0..rows)
            .map(|i| pred_for_op(op, u8_values[i] as f64, a, b))
            .collect();
        assert_eq!(mask_to_bools(&mask, rows), expected);
    }

    let col = dummy_col(0, TYPE_U16, 0);
    for (op, a, b) in cases {
        let mask = build_single_mask(&col, &ColumnData::U16(u16_values.clone()), op, a, b, rows);
        let expected: Vec<bool> = (0..rows)
            .map(|i| pred_for_op(op, u16_values[i] as f64, a, b))
            .collect();
        assert_eq!(mask_to_bools(&mask, rows), expected);
    }

    let col = dummy_col(0, TYPE_U32, 0);
    for (op, a, b) in cases {
        let mask = build_single_mask(&col, &ColumnData::U32(u32_values.clone()), op, a, b, rows);
        let expected: Vec<bool> = (0..rows)
            .map(|i| pred_for_op(op, u32_values[i] as f64, a, b))
            .collect();
        assert_eq!(mask_to_bools(&mask, rows), expected);
    }

    let col = dummy_col(0, TYPE_I32, 0);
    for (op, a, b) in cases {
        let mask = build_single_mask(&col, &ColumnData::I32(i32_values.clone()), op, a, b, rows);
        let expected: Vec<bool> = (0..rows)
            .map(|i| pred_for_op(op, i32_values[i] as f64, a, b))
            .collect();
        assert_eq!(mask_to_bools(&mask, rows), expected);
    }

    let col = dummy_col(0, TYPE_F64, 0);
    for (op, a, b) in cases {
        let mask = build_single_mask(&col, &ColumnData::F64(f64_values.clone()), op, a, b, rows);
        let expected: Vec<bool> = (0..rows)
            .map(|i| {
                let value = f64_values[i];
                if value.is_nan() {
                    false
                } else {
                    pred_for_op(op, value, a, b)
                }
            })
            .collect();
        assert_eq!(mask_to_bools(&mask, rows), expected);
    }
}

#[test]
fn build_filter_mask_in_list_semantics() {
    let rows = 6;
    let data = ColumnData::U32(vec![1, 2, 3, 4, 5, 6]);
    let col = dummy_col(0, TYPE_U32, 0);

    let filter = Filter {
        col_id: 0,
        op: OP_EQ,
        value: 0.0,
        value2: 0.0,
        in_list: Some(vec![2.0, 5.0]),
        value_str: None,
        in_list_str: None,
        like_ids: None,
    };
    let mask = build_filter_mask(&col, &data, &filter, rows, None);
    assert_eq!(
        mask_to_bools(&mask, rows),
        vec![false, true, false, false, true, false]
    );

    let filter_dup = Filter {
        in_list: Some(vec![2.0, 2.0, 5.0]),
        ..filter.clone()
    };
    let mask_dup = build_filter_mask(&col, &data, &filter_dup, rows, None);
    assert_eq!(
        mask_to_bools(&mask_dup, rows),
        vec![false, true, false, false, true, false]
    );

    let filter_empty = Filter {
        in_list: Some(vec![]),
        ..filter.clone()
    };
    let mask_empty = build_filter_mask(&col, &data, &filter_empty, rows, None);
    assert_eq!(mask_to_bools(&mask_empty, rows), vec![false; rows]);
}

#[test]
fn group_key_stability() {
    let rows = 3;
    let col = dummy_col(0, TYPE_U32, 0);
    let data = ColumnData::U32(vec![10, 20, 30]);
    let key_data = vec![(&col, &data)];
    for row in 0..rows {
        let key = build_group_key(&key_data, row);
        assert_eq!(key.a, 10 + (row as u64) * 10);
        assert_eq!(key.b, 0);
    }

    let col_a = dummy_col(0, TYPE_U32, 0);
    let col_b = dummy_col(1, TYPE_U32, 0);
    let data_a = ColumnData::U32(vec![1, 2, 3]);
    let data_b = ColumnData::U32(vec![4, 5, 6]);
    let key_data = vec![(&col_a, &data_a), (&col_b, &data_b)];
    let expected = [(1u64, 4u64), (2u64, 5u64), (3u64, 6u64)];
    for (row, &exp) in expected.iter().enumerate().take(rows) {
        let key = build_group_key(&key_data, row);
        assert_eq!(key.a, exp.0);
        assert_eq!(key.b, exp.1);
    }

    let col_i = dummy_col(0, TYPE_I32, 0);
    let data_i = ColumnData::I32(vec![-1, 0, 1]);
    let key_data = vec![(&col_i, &data_i)];
    let key0 = build_group_key(&key_data, 0);
    let key1 = build_group_key(&key_data, 1);
    let key2 = build_group_key(&key_data, 2);
    assert_eq!(key0.a, u32::MAX as u64);
    assert_eq!(key1.a, 0);
    assert_eq!(key2.a, 1);
}

#[test]
fn plan_required_columns_includes_expected() {
    let plan = Plan {
        runtime: 1,
        filters: vec![
            Filter {
                col_id: 1,
                op: OP_EQ,
                value: 0.0,
                value2: 0.0,
                in_list: None,
                value_str: None,
                in_list_str: None,
                like_ids: None,
            },
            Filter {
                col_id: 3,
                op: OP_EQ,
                value: 0.0,
                value2: 0.0,
                in_list: None,
                value_str: None,
                in_list_str: None,
                like_ids: None,
            },
        ],
        combine: vec![],
        group_by: Some(GroupBy {
            keys: vec![2, 4],
            value_col: Some(6),
            value_kind: crate::constants::AGG_KIND_SUM,
            count_kind: crate::constants::AGG_KIND_COUNT_STAR,
        }),
        aggregates: vec![crate::runtime::agg_key_make(
            5,
            crate::constants::AGG_KIND_SUM,
            0,
        )],
        limit: 0,
        offset: 0,
        rows: Vec::new(),
        agg_state: FxHashMap::default(),
        group_state: FxHashMap::default(),
        group_keys: Vec::new(),
        group_key_repr: FxHashMap::default(),
        group_order_by_count: false,
        group_aggs: Vec::new(),
        row_order_by: Vec::new(),
        row_heap: std::collections::BinaryHeap::new(),
        row_order_lex_ranks: FxHashMap::default(),
        hll_state: FxHashMap::default(),
        group_emit_raw: false,
        group_rows_raw_with_keys: Vec::new(),
        group_dict_hist_dict_len: 0,
        group_dict_hist_counts: None,
        group_dict_hist_sums: None,
        select_cols: Vec::new(),
        row_projection: RowProjectionBuf::default(),
        timing: PlanTiming::default(),
        filter_timing: FilterTiming::default(),
    };
    let required = plan_required_columns(&plan, 8);
    assert_eq!(required, vec![1, 2, 3, 4, 5, 6]);

    let empty_plan = Plan {
        runtime: 1,
        filters: vec![],
        combine: vec![],
        group_by: None,
        aggregates: vec![],
        limit: 0,
        offset: 0,
        rows: Vec::new(),
        agg_state: FxHashMap::default(),
        group_state: FxHashMap::default(),
        group_keys: Vec::new(),
        group_key_repr: FxHashMap::default(),
        group_order_by_count: false,
        group_aggs: Vec::new(),
        row_order_by: Vec::new(),
        row_heap: std::collections::BinaryHeap::new(),
        row_order_lex_ranks: FxHashMap::default(),
        hll_state: FxHashMap::default(),
        group_emit_raw: false,
        group_rows_raw_with_keys: Vec::new(),
        group_dict_hist_dict_len: 0,
        group_dict_hist_counts: None,
        group_dict_hist_sums: None,
        select_cols: Vec::new(),
        row_projection: RowProjectionBuf::default(),
        timing: PlanTiming::default(),
        filter_timing: FilterTiming::default(),
    };
    let required = plan_required_columns(&empty_plan, 8);
    assert!(required.is_empty());
}

#[test]
fn null_filtering_mask_and_validity() {
    let rows = 16;
    let mut filter_mask = vec![0u32; MASK_WORDS];
    for i in 0..rows {
        set_bit(&mut filter_mask, i);
    }
    let bitmap = vec![0b0101_0101, 0b0101_0101];
    let valid = mask_from_bitmap(&bitmap, rows).expect("valid bitmap");
    let combined = mask_and(&filter_mask, &valid);
    let expected: Vec<bool> = (0..rows).map(|i| i % 2 == 0).collect();
    assert_eq!(mask_to_bools(&combined, rows), expected);
}

#[test]
fn scaled_int_filters_use_logical_values() {
    let rows = 3;
    let col = dummy_col_scaled(0, TYPE_I32, 0, 1000);
    let data = ColumnData::I32(vec![9083, 9083, 11783]);

    let mask_eq = build_single_mask(&col, &data, OP_EQ, 9.083, 9.083, rows);
    assert_eq!(mask_to_bools(&mask_eq, rows), vec![true, true, false]);

    let mask_between = build_single_mask(&col, &data, OP_BETWEEN, 9.083, 11.783, rows);
    assert_eq!(mask_to_bools(&mask_between, rows), vec![true, true, true]);

    let mask_miss = build_single_mask(&col, &data, OP_BETWEEN, 9.084, 11.782, rows);
    assert_eq!(mask_to_bools(&mask_miss, rows), vec![false, false, false]);
}

#[test]
fn scaled_int_aggregates_apply_scale() {
    let rows = 3;
    let col = dummy_col_scaled(0, TYPE_I32, 0, 1000);
    let data = ColumnData::I32(vec![9083, 9083, 11783]);
    let mask = full_mask(rows);
    let state = aggregate_column(&col, &data, &mask, rows);

    assert_eq!(state.count, 3);
    assert!((state.min - 9.083).abs() < 1e-9);
    assert!((state.max - 11.783).abs() < 1e-9);
    assert!((state.sum - 29.949).abs() < 1e-9);
}

#[test]
fn nullable_row_bitmap_skips_all_valid_pages() {
    let rows = 10;
    let all_valid = bools_to_bitmap(&vec![true; rows]);
    let mixed = bools_to_bitmap(&[true, false, true, true, true, true, true, true, true, true]);

    assert!(nullable_row_bitmap(Some(&all_valid), rows).is_none());
    assert!(nullable_row_bitmap(Some(&mixed), rows).is_some());
    assert!(nullable_row_bitmap(None, rows).is_none());
}

#[test]
fn maybe_apply_validity_mask_keeps_mask_when_all_valid() {
    let rows = 12;
    let mut mask = vec![0u32; MASK_WORDS];
    for i in [0usize, 2, 4, 8, 11] {
        set_bit(&mut mask, i);
    }
    let all_valid = bools_to_bitmap(&vec![true; rows]);

    let out =
        maybe_apply_validity_mask(mask.clone(), Some(&all_valid), rows).expect("valid null bitmap");
    assert_eq!(out, mask);
}

#[test]
fn maybe_apply_validity_mask_applies_intersection() {
    let rows = 12;
    let mut mask = vec![0u32; MASK_WORDS];
    for i in [0usize, 1, 2, 3, 8, 9, 10, 11] {
        set_bit(&mut mask, i);
    }
    let valid = bools_to_bitmap(&[
        true, false, true, false, true, true, true, true, false, true, true, false,
    ]);

    let out = maybe_apply_validity_mask(mask, Some(&valid), rows).expect("valid null bitmap");
    assert_eq!(
        mask_to_bools(&out, rows),
        vec![true, false, true, false, false, false, false, false, false, true, true, false]
    );
}
