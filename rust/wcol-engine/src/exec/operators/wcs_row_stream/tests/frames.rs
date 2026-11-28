use super::*;

#[test]
fn partial_states_go_out_as_partition_stamped_partial_frames() {
    use crate::stream::partial_wcs::partial_state_from_wire_payload;
    use crate::stream::partials::{
        PartialColumn, PartialStateSchemaV1, PartialStateWcol, PARTIAL_SCHEMA_VERSION,
    };
    // Enough distinct rows that the state needs several frames.
    let rows = 400_000usize;
    let state = PartialStateWcol {
        schema: PartialStateSchemaV1 {
            version: PARTIAL_SCHEMA_VERSION,
            grouped: true,
            order_groups_by_count: false,
            limit: 0,
            offset: 0,
            group_key_len: 1,
            group_key_float_mask: 0,
            group_key_string_mask: 0,
            aggs: Vec::new(),
        },
        passed_rows: 7,
        row_count: rows,
        columns: vec![PartialColumn {
            id: crate::stream::partials::PARTIAL_KEY_COL_BASE,
            values: (0..rows as i64)
                .map(|v| v.wrapping_mul(0x1E37_79B9_7F4A_7C15))
                .collect(),
            nulls: vec![1; rows],
        }],
        sketches: Vec::new(),
        string_keys: Vec::new(),
    };
    let mut egress = RowBatchWcsEgress::new();
    egress.push_partial_partition(&state, 5).unwrap();
    egress.push_end_partition(5).unwrap();
    let wire = egress.drain_to_vec().unwrap();
    let mut ingress = crate::exec::operators::wcs_frame::WcsIngress::new();
    let (used, frames) = ingress.feed(&wire).unwrap();
    assert_eq!(used, wire.len());
    let (partials, ends): (Vec<_>, Vec<_>) = frames
        .iter()
        .partition(|frame| frame.frame_type == FRAME_PARTIAL);
    assert!(partials.len() > 1, "a large state splits across frames");
    assert_eq!(ends.len(), 1);
    let mut total = 0;
    let mut passed = 0;
    for frame in partials {
        let part = partial_state_from_wire_payload(&frame.payload).unwrap();
        total += part.row_count;
        passed += part.passed_rows;
    }
    assert_eq!((total, passed), (rows, 7));
    // Every frame header names the partition.
    let mut at = 0;
    while at < wire.len() {
        assert_eq!(
            u32::from_le_bytes(wire[at + 12..at + 16].try_into().unwrap()),
            5
        );
        let len = u32::from_le_bytes(wire[at + 8..at + 12].try_into().unwrap()) as usize;
        at += crate::stream::core::WCS_HEADER_BYTES + len;
    }
}

#[test]
fn wcs_egress_releases_completed_frames() {
    const PARTITIONS: usize = 16;
    const BATCHES: usize = 12;
    const ROWS: usize = 64;
    let mut egresses: Vec<RowBatchWcsEgress> =
        (0..PARTITIONS).map(|_| RowBatchWcsEgress::new()).collect();
    let mut wires = vec![Vec::new(); PARTITIONS];
    let mut peak_pending = 0usize;
    let mut delivered = 0usize;
    for batch in 0..BATCHES {
        for (part, egress) in egresses.iter_mut().enumerate() {
            let mut rows = Vec::with_capacity(ROWS);
            for row in 0..ROWS {
                rows.push((batch * PARTITIONS * ROWS + part * ROWS + row) as i64);
            }
            let mut owned = OwnedWcolBatch::empty();
            owned.push_i64_column(0, rows, vec![1; ROWS]);
            owned.set_row_count(ROWS).unwrap();
            egress.push_batch_partition(owned, part as u32).unwrap();
            let staged = egress.pending_bytes();
            assert!(staged > 0, "a pushed batch must be retained until copied");
            peak_pending = peak_pending.max(staged);
            let mut buf = [0u8; 5];
            let mut saw_partial = false;
            while egress.has_pending_output() {
                let before = egress.pending_bytes();
                let n = egress.poll_write(&mut buf).unwrap();
                assert!(n > 0 && n <= buf.len());
                wires[part].extend_from_slice(&buf[..n]);
                delivered += n;
                if egress.mid_frame() {
                    saw_partial = true;
                    assert!(egress.has_pending_output());
                    assert!(egress.pending_bytes() < before);
                    assert_eq!(egress.pending_bytes(), before - n);
                } else {
                    assert_eq!(egress.pending_bytes(), 0);
                }
            }
            assert!(saw_partial, "a 5-byte buffer must split a frame");
            assert_eq!(egress.pending_bytes(), 0);
            assert!(!egress.has_pending_output());
        }
    }
    for (part, egress) in egresses.iter_mut().enumerate() {
        egress.push_end_partition(part as u32).unwrap();
        let mut buf = [0u8; 5];
        while egress.has_pending_output() {
            let n = egress.poll_write(&mut buf).unwrap();
            wires[part].extend_from_slice(&buf[..n]);
            delivered += n;
        }
        assert_eq!(egress.pending_bytes(), 0);
    }
    assert!(
        delivered > peak_pending * 4,
        "peak pending {peak_pending} tracked the current batch, delivered {delivered}"
    );
    for wire in &wires {
        let mut at = 0usize;
        let mut sequence = 0u32;
        let mut saw_end = false;
        while at < wire.len() {
            assert!(wire.len() - at >= 32, "truncated header");
            let payload = u32::from_le_bytes(wire[at + 8..at + 12].try_into().unwrap()) as usize;
            let got = u32::from_le_bytes(wire[at + 24..at + 28].try_into().unwrap());
            assert_eq!(got, sequence);
            sequence += 1;
            let frame_type = wire[at + 5];
            at += 32 + payload;
            if frame_type == FRAME_END {
                saw_end = true;
                break;
            }
        }
        assert!(saw_end);
        assert_eq!(at, wire.len());
    }
}

#[test]
fn direct_feed_includes_end() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![9], vec![1]);
    batch.set_row_count(1).unwrap();
    let mut egress = RowBatchWcsEgress::new();
    egress.push_batch(batch.clone()).unwrap();
    egress.push_end().unwrap();
    let wire = egress.drain_to_vec().unwrap();
    assert!(wire.len() >= crate::stream::core::WCS_HEADER_BYTES * 2);

    let mut ingress = RowBatchWcsIngress::new();
    let mut off = 0usize;
    while off < wire.len() {
        off += ingress.feed(&wire[off..]).unwrap();
    }
    ingress.finish_feed().unwrap();
    assert_eq!(ingress.rows_received, 1);
}

#[test]
fn fragmented_chunks_round_trip_one_batch() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1, 2, 3], vec![1; 3]);
    batch.set_row_count(3).unwrap();

    let mut egress = RowBatchWcsEgress::new();
    egress.push_batch(batch.clone()).unwrap();
    egress.push_end().unwrap();
    let wire = egress.drain_to_vec().unwrap();

    let mut ingress = RowBatchWcsIngress::new();
    let mut off = 0usize;
    let mut width = 1usize;
    while off < wire.len() {
        let end = (off + width).min(wire.len());
        let n = ingress.feed(&wire[off..end]).unwrap();
        assert!(n > 0, "ingress stalled at {off}");
        off += n;
        width = (width % 13) + 1;
    }
    ingress.finish_feed().unwrap();
    assert_eq!(ingress.rows_received, 3);
    assert!(ingress.stream_complete());
}
