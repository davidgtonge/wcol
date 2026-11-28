use super::*;

#[test]
fn a_partition_stream_sends_only_the_entries_its_rows_reference() {
    let scan_dict = dict_bytes(4, 3, &[b"a", b"bb", b"ccc", b"dddd", b"eeeee", b"ffffff"]);
    let batch = |scope: u32, ids: Vec<u32>, nulls: Vec<u8>| {
        let rows = ids.len();
        let mut dict = scan_dict.clone();
        dict.scope = scope;
        let mut batch = OwnedWcolBatch::empty();
        batch.set_scope(scope);
        batch.push_dict_id_column(3, ids, nulls, dict);
        batch.set_row_count(rows).unwrap();
        batch
    };
    let sent = vec![
        batch(4, vec![4, 1, 4], vec![1, 1, 1]),
        // Row 2 is null; entry 2 is new, entry 1 was already sent.
        batch(4, vec![2, 1, 5], vec![1, 1, 0]),
        // The next row group starts its own local numbering.
        batch(5, vec![0, 0], vec![0, 1]),
    ];
    let mut egress = RowBatchWcsEgress::new();
    for batch in &sent {
        egress.push_batch_partition(batch.clone(), 1).unwrap();
    }
    egress.push_end_partition(1).unwrap();
    let wire = egress.drain_to_vec().unwrap();

    let back = decode_partition(&wire);
    assert_eq!(back.len(), sent.len());
    for (sent, back) in sent.iter().zip(&back) {
        assert_eq!(dict_values(back), dict_values(sent));
    }
    // Scope 4 installed entries 4, 1, then 2: three of six, never the whole dictionary.
    let installed = |batch: &OwnedWcolBatch| {
        let mut cols = Vec::new();
        let view = batch.view_with(&mut cols);
        match &view.columns[0] {
            WcolColumn::DictId { dictionary, .. } => dictionary.entry_count(),
            _ => unreachable!(),
        }
    };
    assert_eq!(installed(&back[1]), 3);
    assert_eq!(installed(&back[2]), 1);
}

#[test]
fn typed_batches_round_trip_one_byte_at_a_time() {
    let mut batch = OwnedWcolBatch::empty();
    batch.set_scope(7);
    batch.push_i64_column(0, vec![i64::MIN, 0, 42, 7], vec![1, 0, 1, 1]);
    batch.push_f64_column(
        1,
        vec![
            0.0,
            -0.0,
            f64::from_bits(0x7ff0_0000_0000_0001),
            f64::INFINITY,
        ],
        vec![1, 1, 1, 0],
    );
    let mut bytes = Vec::new();
    let mut offsets = vec![0u32];
    for value in [b"".as_slice(), "héllo 你好".as_bytes(), b"a", b"x"] {
        bytes.extend_from_slice(value);
        offsets.push(bytes.len() as u32);
    }
    batch.push_utf8_column(2, offsets, bytes, vec![1, 1, 0, 1]);
    batch.push_dict_id_column(
        3,
        vec![0, 1, 2, 1],
        vec![1, 1, 1, 0],
        dict_bytes(7, 3, &[b"aa", "héllo".as_bytes(), b""]),
    );
    batch.set_row_count(4).unwrap();

    let back = round_trip(&[batch.clone()]);
    assert_eq!(back.len(), 1);
    assert_same_batch(&batch, &back[0]);
}

#[test]
fn dictionary_is_sent_once_and_scopes_do_not_mix() {
    let mut first = OwnedWcolBatch::empty();
    first.set_scope(1);
    first.push_dict_id_column(0, vec![0, 1], vec![1, 1], dict_bytes(1, 0, &[b"a", b"b"]));
    first.set_row_count(2).unwrap();
    let mut second = OwnedWcolBatch::empty();
    second.set_scope(1);
    second.push_dict_id_column(0, vec![1], vec![1], dict_bytes(1, 0, &[b"a", b"b"]));
    second.set_row_count(1).unwrap();
    let mut other = OwnedWcolBatch::empty();
    other.set_scope(2);
    other.push_dict_id_column(0, vec![0, 1], vec![1, 1], dict_int(2, 0, &[9, 8]));
    other.set_row_count(2).unwrap();

    let mut egress = RowBatchWcsEgress::new();
    egress.push_batch(first.clone()).unwrap();
    egress.push_batch(second.clone()).unwrap();
    egress.push_batch(other.clone()).unwrap();
    egress.push_end().unwrap();
    let wire = egress.drain_to_vec().unwrap();
    let mut dict_frames = 0usize;
    let mut at = 0usize;
    while at + crate::stream::core::WCS_HEADER_BYTES <= wire.len() {
        let kind = wire[at + 5];
        let len = u32::from_le_bytes(wire[at + 8..at + 12].try_into().unwrap()) as usize;
        if kind == FRAME_DICTIONARY {
            dict_frames += 1;
        }
        at += crate::stream::core::WCS_HEADER_BYTES + len;
    }
    assert_eq!(
        dict_frames, 2,
        "one dictionary per scope, reused across frames"
    );

    let back = round_trip(&[first.clone(), second.clone(), other.clone()]);
    assert_same_batch(&first, &back[0]);
    assert_same_batch(&second, &back[1]);
    assert_same_batch(&other, &back[2]);
    let mut first_cols = Vec::new();
    let first_view = back[0].view_with(&mut first_cols);
    match first_view.columns[0] {
        WcolColumn::DictId { dictionary, .. } => {
            let (offsets, bytes) = dictionary.byte_entries().unwrap();
            assert_eq!(&bytes[offsets[0] as usize..offsets[1] as usize], b"a");
        }
        _ => panic!("dict"),
    }
    let mut other_cols = Vec::new();
    let other_view = back[2].view_with(&mut other_cols);
    match other_view.columns[0] {
        WcolColumn::DictId { dictionary, .. } => {
            assert_eq!(dictionary.int_entries().unwrap().as_ref(), &[9, 8]);
        }
        _ => panic!("int dict"),
    }
}

/// Rows go as plain values until they would number as many as the dictionary's
/// entries; then the dictionary goes once and later rows send ids.
#[test]
fn a_dictionary_is_sent_once_its_rows_would_cost_as_much() {
    let dict = dict_bytes(1, 0, &[b"a", b"b", b"c", b"d", b"e"]);
    let batch = |ids: Vec<u32>| {
        let mut batch = OwnedWcolBatch::empty();
        batch.set_scope(1);
        let rows = ids.len();
        batch.push_dict_id_column(0, ids, vec![1; rows], dict.clone());
        batch.set_row_count(rows).unwrap();
        batch
    };
    let batches = [batch(vec![4, 0]), batch(vec![1, 1]), batch(vec![2, 3])];
    let back = round_trip(&batches);
    let kinds: Vec<bool> = back
        .iter()
        .map(|batch| {
            let mut cols = Vec::new();
            matches!(
                batch.view_with(&mut cols).columns[0],
                WcolColumn::DictId { .. }
            )
        })
        .collect();
    assert_eq!(kinds, [false, false, true]);
    for (sent, got) in batches.iter().zip(&back) {
        assert_same_batch(sent, got);
    }
}

#[test]
fn random_mixed_batches_round_trip() {
    let samples: &[&[u8]] = &[b"", b"a", "héllo".as_bytes(), "你好".as_bytes(), b"\0x"];
    let mut state = 0x1234_5678_9abc_def0u64;
    let mut next = || {
        state = state
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .wrapping_add(0x6a09_e667_f3bc_c909);
        state
    };
    for _ in 0..24 {
        let rows = (next() % 6) as usize;
        let mut batch = OwnedWcolBatch::empty();
        let scope = (next() % 3) as u32 + 1;
        batch.set_scope(scope);
        let mut i64s = Vec::with_capacity(rows);
        let mut i_nulls = Vec::with_capacity(rows);
        let mut f64s = Vec::with_capacity(rows);
        let mut f_nulls = Vec::with_capacity(rows);
        let mut offsets = vec![0u32];
        let mut bytes = Vec::new();
        let mut u_nulls = Vec::with_capacity(rows);
        let mut ids = Vec::with_capacity(rows);
        let mut d_nulls = Vec::with_capacity(rows);
        for _ in 0..rows {
            i64s.push(next() as i64);
            i_nulls.push(if next() % 5 == 0 { 0 } else { 1 });
            let bits = match next() % 5 {
                0 => 0.0f64.to_bits(),
                1 => (-0.0f64).to_bits(),
                2 => f64::INFINITY.to_bits(),
                3 => 0x7ff0_0000_0000_0001,
                _ => next(),
            };
            f64s.push(f64::from_bits(bits));
            f_nulls.push(if next() % 4 == 0 { 0 } else { 1 });
            let sample = samples[(next() as usize) % samples.len()];
            bytes.extend_from_slice(sample);
            offsets.push(bytes.len() as u32);
            u_nulls.push(if next() % 3 == 0 { 0 } else { 1 });
            ids.push((next() % 3) as u32);
            d_nulls.push(if next() % 6 == 0 { 0 } else { 1 });
        }
        batch.push_i64_column(0, i64s, i_nulls);
        batch.push_f64_column(1, f64s, f_nulls);
        batch.push_utf8_column(2, offsets, bytes, u_nulls);
        batch.push_dict_id_column(
            3,
            ids,
            d_nulls,
            dict_bytes(scope, 3, &[b"a", "é".as_bytes(), b""]),
        );
        batch.set_row_count(rows).unwrap();
        let back = round_trip(&[batch.clone()]);
        assert_eq!(back.len(), 1);
        assert_same_batch(&batch, &back[0]);
    }
}
