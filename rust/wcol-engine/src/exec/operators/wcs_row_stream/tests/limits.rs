use super::*;

#[test]
fn large_batch_splits_on_row_boundaries_and_round_trips() {
    const ROWS: usize = 1500;
    let mut batch = OwnedWcolBatch::empty();
    let mut bytes = Vec::new();
    let mut offsets = vec![0u32];
    for row in 0..ROWS {
        let text = format!("row-{row:04}-{}", "x".repeat(48));
        bytes.extend(text.into_bytes());
        offsets.push(bytes.len() as u32);
    }
    batch.push_utf8_column(0, offsets, bytes, vec![1; ROWS]);
    batch.set_row_count(ROWS).unwrap();
    let mut egress = RowBatchWcsEgress::new();
    egress.push_batch(batch.clone()).unwrap();
    egress.push_end().unwrap();
    let wire = egress.drain_to_vec().unwrap();
    let mut off = 0;
    let mut data_frames = 0usize;
    while off + crate::stream::core::WCS_HEADER_BYTES <= wire.len() {
        let payload = u32::from_le_bytes(wire[off + 8..off + 12].try_into().unwrap()) as usize;
        assert!(
            payload <= MAX_WCS_PAYLOAD,
            "payload {payload} exceeds {MAX_WCS_PAYLOAD}"
        );
        if wire[off + 5] == FRAME_DATA_BATCH {
            data_frames += 1;
        }
        off += crate::stream::core::WCS_HEADER_BYTES + payload;
    }
    assert_eq!(off, wire.len());
    assert!(
        data_frames > 1,
        "expected several data frames, got {data_frames}"
    );
    let mut ingress = RowBatchWcsIngress::new();
    let mut at = 0;
    let mut width = 1usize;
    while at < wire.len() {
        let end = (at + width).min(wire.len());
        let n = ingress.feed(&wire[at..end]).unwrap();
        assert!(n > 0, "ingress stalled at {at}");
        at += n;
        width = width % 13 + 1;
    }
    ingress.finish_feed().unwrap();
    let rows: usize = ingress
        .take_batches()
        .iter()
        .map(|item| item.row_count())
        .sum();
    assert_eq!(rows, ROWS);
}

#[test]
fn large_string_dictionary_installs_in_multiple_frames() {
    let strings: Vec<Vec<u8>> = (0..2_000)
        .map(|index| format!("phrase-{index:05}").into_bytes())
        .collect();
    let refs: Vec<&[u8]> = strings.iter().map(Vec::as_slice).collect();
    let dict = dict_bytes(0, 2, &refs);
    let mut batch = OwnedWcolBatch::empty();
    batch.push_dict_id_column(2, (0..2_000).collect(), vec![1; 2_000], dict);
    batch.set_row_count(2_000).unwrap();
    let back = round_trip(&[batch.clone()]);
    assert_eq!(back.len(), 1);
    assert_same_batch(&batch, &back[0]);
}

#[test]
fn composite_key_columns_round_trip() {
    let dict = dict_bytes(0, 0, &[b"engine-a", b"engine-b"]);
    let mut batch = OwnedWcolBatch::empty();
    batch.push_dict_id_column(0, vec![0, 1, 0], vec![1; 3], dict);
    batch.push_i64_column(1, vec![10, 20, 30], vec![1; 3]);
    batch.set_row_count(3).unwrap();
    let back = round_trip(&[batch.clone()]);
    assert_same_batch(&batch, &back[0]);
}

#[test]
fn single_row_over_the_frame_limit_is_rejected() {
    let mut batch = OwnedWcolBatch::empty();
    let bytes = vec![b'a'; MAX_WCS_PAYLOAD];
    batch.push_utf8_column(0, vec![0, bytes.len() as u32], bytes, vec![1]);
    batch.set_row_count(1).unwrap();
    let err = RowBatchWcsEgress::new()
        .push_batch(batch.clone())
        .unwrap_err();
    assert!(
        err.as_str().contains("single_row_too_large"),
        "{}",
        err.as_str()
    );
}

#[test]
fn dictionary_egress_holds_one_frame() {
    let strings: Vec<Vec<u8>> = (0..20_000)
        .map(|index| format!("k{index:06}").into_bytes())
        .collect();
    let refs: Vec<&[u8]> = strings.iter().map(Vec::as_slice).collect();
    let dict = dict_bytes(0, 1, &refs);
    let mut batch = OwnedWcolBatch::empty();
    batch.push_dict_id_column(1, (0..20_000).collect(), vec![1; 20_000], dict);
    batch.set_row_count(20_000).unwrap();
    let mut egress = RowBatchWcsEgress::new();
    egress.push_batch(batch.clone()).unwrap();
    let header = crate::stream::core::WCS_HEADER_BYTES;
    assert!(egress.pending_bytes() > 0);
    assert!(
        egress.pending_bytes() <= header + MAX_WCS_PAYLOAD,
        "pending {}",
        egress.pending_bytes()
    );
    assert!(egress.has_pending_output());
    let wire = egress.drain_to_vec().unwrap();
    assert!(wire.len() > header + MAX_WCS_PAYLOAD);
    assert_eq!(egress.pending_bytes(), 0);
}

#[test]
fn oversized_dictionary_entry_fails_before_a_frame_is_queued() {
    let huge = vec![b'x'; MAX_WCS_PAYLOAD];
    let dict = dict_bytes(0, 1, &[&huge]);
    let mut batch = OwnedWcolBatch::empty();
    batch.push_dict_id_column(1, vec![0], vec![1], dict);
    batch.set_row_count(1).unwrap();
    let mut egress = RowBatchWcsEgress::new();
    let err = egress.push_batch(batch.clone()).unwrap_err();
    assert!(err.as_str().contains("dictionary_entry_too_large"), "{err}");
    assert!(err.as_str().contains("index=0"), "{err}");
    assert_eq!(egress.pending_bytes(), 0);
    assert!(!egress.has_pending_output());
}

#[test]
fn later_oversized_dictionary_entry_keeps_failing() {
    let huge = vec![b'y'; MAX_WCS_PAYLOAD];
    let dict = dict_bytes(0, 1, &[b"ok", &huge]);
    let mut batch = OwnedWcolBatch::empty();
    batch.push_dict_id_column(1, vec![0, 0], vec![1, 1], dict);
    batch.set_row_count(2).unwrap();
    let mut egress = RowBatchWcsEgress::new();
    egress.push_batch(batch.clone()).unwrap();
    let err = egress.drain_to_vec().unwrap_err();
    assert!(err.as_str().contains("dictionary_entry_too_large"), "{err}");
    assert!(err.as_str().contains("index=1"), "{err}");
    let again = egress.poll_write(&mut [0u8; 16]).unwrap_err();
    assert!(
        again.as_str().contains("dictionary_entry_too_large"),
        "{again}"
    );
    assert!(egress.has_pending_output());
}

/// 100 streams each keep their dictionary. Encoded output stays one frame per stream.
#[test]
fn high_cardinality_streams_bound_frames_and_retain_dictionaries() {
    const STREAMS: usize = 100;
    const ENTRIES: usize = 2_500;
    let header = crate::stream::core::WCS_HEADER_BYTES;
    let frame_cap = header + MAX_WCS_PAYLOAD;
    let mut egresses = Vec::with_capacity(STREAMS);
    let mut peak_one = 0usize;
    let mut simultaneous = 0usize;
    for stream in 0..STREAMS {
        let values: Vec<Vec<u8>> = (0..ENTRIES)
            .map(|index| format!("s{stream:03}-e{index:04}-{}", "u".repeat(32)).into_bytes())
            .collect();
        let refs: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
        let dict = dict_bytes(0, 1, &refs);
        let mut batch = OwnedWcolBatch::empty();
        batch.push_dict_id_column(1, vec![0; ENTRIES], vec![1; ENTRIES], dict);
        batch.set_row_count(ENTRIES).unwrap();
        let mut egress = RowBatchWcsEgress::new();
        egress.push_batch(batch.clone()).unwrap();
        egress.push_end().unwrap();
        peak_one = peak_one.max(egress.pending_bytes());
        simultaneous = simultaneous.saturating_add(egress.pending_bytes());
        egresses.push(egress);
    }
    assert!(peak_one <= frame_cap, "one stream pending {peak_one}");
    assert!(
        simultaneous <= STREAMS * frame_cap,
        "queued frames {simultaneous}"
    );
    let mut ingresses = Vec::with_capacity(STREAMS);
    let mut host_held = 0usize;
    for egress in &mut egresses {
        let mut ingress = RowBatchWcsIngress::new();
        let mut chunk = [0u8; 4096];
        while egress.has_pending_output() {
            peak_one = peak_one.max(egress.pending_bytes());
            host_held = host_held.max(egress.pending_bytes());
            let n = egress.poll_write(&mut chunk).unwrap();
            if n == 0 {
                break;
            }
            ingress.feed(&chunk[..n]).unwrap();
        }
        ingress.finish_feed().unwrap();
        assert_eq!(egress.pending_bytes(), 0);
        ingresses.push(ingress);
    }
    drop(egresses);
    let per_stream: Vec<usize> = ingresses.iter().map(retained_dictionary_bytes).collect();
    let total: usize = per_stream.iter().sum();
    let snap = crate::formats::parquet::alloc_track::snapshot();
    eprintln!(
        "dict_streams={STREAMS} entries={ENTRIES} dict_bytes_per_stream={} dict_bytes_total={total} peak_frame_bytes={peak_one} frame_cap={frame_cap} simultaneous_frames_after_push={simultaneous} rust_live_bytes={} rust_peak_bytes={} wasm_linear_bytes={}",
        per_stream[0],
        snap.live_bytes,
        snap.peak_bytes,
        snap.wasm_linear_bytes
    );
    assert!(per_stream.iter().all(|bytes| *bytes > 80_000));
    assert_eq!(per_stream.iter().min(), per_stream.iter().max());
    assert!(peak_one <= frame_cap);
    assert!(host_held <= frame_cap);
    if snap.live_bytes > 0 {
        assert!(
            snap.live_bytes >= total / 2,
            "live {} is below retained dictionaries {total}",
            snap.live_bytes
        );
        assert!(
            snap.live_bytes < total.saturating_mul(4).saturating_add(32 * 1024 * 1024),
            "live {} is far above retained dictionaries {total}",
            snap.live_bytes
        );
        assert!(
            snap.peak_bytes < 128 * 1024 * 1024,
            "peak rust {} exceeds 128 MiB for {} dictionary bytes",
            snap.peak_bytes,
            total
        );
    }
}
