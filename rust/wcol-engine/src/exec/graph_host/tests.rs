use super::*;
use crate::ops::WCOL_EXCHANGE_HASH_VERSION_DEFAULT;
use crate::program::stream_program::PlanCursor;

#[test]
fn a_shared_isolate_divides_the_spill_budget() {
    assert_eq!(shared_spill_budget(16 << 20, 1), 16 << 20);
    assert_eq!(shared_spill_budget(16 << 20, 2), 8 << 20);
    assert_eq!(
        shared_spill_budget(16 << 20, 0),
        16 << 20,
        "no share is the whole isolate"
    );
}

#[test]
fn empty_hash_partitions_do_not_queue_end_frames() {
    let plan = BatchPartitionPlan::Hash(
        HashPartitionPlan::new(vec![0], 2, WCOL_EXCHANGE_HASH_VERSION_DEFAULT).unwrap(),
    );
    let mut fanout = HashFanout {
        plan: FanoutPlan::Rows(plan),
        egress: vec![RowBatchWcsEgress::new(), RowBatchWcsEgress::new()],
        cursor: 0,
        ends_queued: false,
        opened: vec![true, false],
    };
    fanout.egress[0].push_end_partition(0).unwrap();
    assert!(fanout.egress[0].has_pending_output());
    assert!(!fanout.egress[1].has_pending_output());
    for (index, egress) in fanout.egress.iter_mut().enumerate() {
        if !fanout.opened[index] {
            continue;
        }
        if !egress.has_pending_output() {
            egress.push_end_partition(index as u32).unwrap();
        }
    }
    assert!(fanout.egress[0].has_pending_output());
    assert!(!fanout.egress[1].has_pending_output());
}

#[test]
fn mark_absent_rejects_duplicate_notification() {
    let body = 2u32.to_le_bytes();
    let mut cur = PlanCursor::new(&body);
    let mut host = GraphHost::open_wcs_partial_reduce(&mut cur, 4096).unwrap();
    host.mark_input_absent(0).unwrap();
    assert!(host.mark_input_absent(0).is_ok());
}

fn encoded_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0u8; 32 + payload.len()];
    let n = crate::stream::wcs::encode_frame(3, 1, payload, &mut frame).unwrap();
    frame.truncate(n);
    frame
}

fn partial_host() -> GraphHost {
    let body = 1u32.to_le_bytes();
    let mut cur = PlanCursor::new(&body);
    GraphHost::open_wcs_partial_reduce(&mut cur, 4096).unwrap()
}

#[test]
fn frame_over_the_maximum_payload_is_rejected_from_the_header() {
    let mut host = partial_host();
    let mut header = [0u8; 32];
    header[..4].copy_from_slice(&crate::stream::core::WCS_MAGIC.to_le_bytes());
    header[4] = 1;
    header[5] = 3;
    header[8..12].copy_from_slice(&(64 * 1024u32 + 1).to_le_bytes());
    let err = host.try_feed_input_port(0, &header).unwrap_err();
    assert!(err.as_str().contains("maximum size"), "{}", err.as_str());
}

#[test]
fn one_chunk_stops_after_the_first_frame_until_it_is_taken() {
    let mut host = partial_host();
    let first = encoded_frame(b"one");
    let second = encoded_frame(b"two");
    let mut chunk = first.clone();
    chunk.extend_from_slice(&second);
    let n = host.try_feed_input_port(0, &chunk).unwrap();
    assert_eq!(n, first.len());
    assert_eq!(host.try_feed_input_port(0, &chunk[n..]).unwrap(), 0);
    host.poll();
    let rest = host.try_feed_input_port(0, &chunk[n..]).unwrap();
    assert_eq!(rest, second.len());
}

#[test]
fn fragmented_frame_completes_across_arbitrary_chunks() {
    let mut host = partial_host();
    let frame = encoded_frame(&vec![9u8; 1000]);
    let mut off = 0;
    let mut width = 1usize;
    while off < frame.len() {
        let end = (off + width).min(frame.len());
        let n = host.try_feed_input_port(0, &frame[off..end]).unwrap();
        if n == 0 {
            host.poll();
            continue;
        }
        off += n;
        width = width % 17 + 1;
    }
}

fn hash_reducer(producers: u32) -> GraphHost {
    use crate::ir::{CatalogColumn, ColumnKind};
    use crate::program::types::{AggExpr, AggKind};
    use crate::program::{encode_hash_reducer_program, HashReducerSpec};
    let catalog = vec![CatalogColumn {
        name: "k".into(),
        kind: ColumnKind::Text,
        physical: 6,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }];
    let program = encode_hash_reducer_program(&HashReducerSpec {
        producer_count: producers,
        catalog: &catalog,
        group_by: &[],
        aggregates: &[AggExpr {
            kind: AggKind::CountStar,
            column: None,
            offset: 0,
        }],
        skip: 0,
        fetch: None,
        rank: None,
        group_budget: 0,
        group_spill: false,
        spill_combined_budget: 0,
        partition: 0,
    });
    let mut cur = PlanCursor::new(&program[6..]);
    GraphHost::open_hash_reducer(&mut cur, 64 * 1024, 1).unwrap()
}

fn dictionary_frames(values: &[&str]) -> Vec<Vec<u8>> {
    use crate::stream::{DictEntries, DictionaryValues};
    use std::sync::Arc;
    let mut offsets = vec![0u32];
    let mut bytes = Vec::new();
    for value in values {
        bytes.extend_from_slice(value.as_bytes());
        offsets.push(bytes.len() as u32);
    }
    let dict = DictionaryValues {
        scope: 0,
        column: 0,
        entries: DictEntries::Bytes {
            offsets: Arc::new(offsets),
            bytes: Arc::new(bytes),
        },
    };
    let payloads = crate::formats::parquet::stream_wcs::encode_dictionary_frames(
        &dict,
        0,
        crate::stream::wcs::MAX_WCS_PAYLOAD,
    )
    .unwrap();
    payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            let mut frame = vec![0u8; 32 + payload.len()];
            let n = crate::stream::wcs::encode_frame(
                crate::stream::wcs::FRAME_DICTIONARY,
                index as u32,
                &payload,
                &mut frame,
            )
            .unwrap();
            frame.truncate(n);
            frame
        })
        .collect()
}

fn end_frame() -> Vec<u8> {
    let mut frame = vec![0u8; 64];
    let n = crate::stream::wcs::encode_frame(crate::stream::wcs::FRAME_END, 1, &[], &mut frame)
        .unwrap();
    frame.truncate(n);
    frame
}

fn pump(host: &mut GraphHost) {
    for _ in 0..16 {
        match host.poll() {
            Action::Failed => panic!("poll failed: {}", host.error().unwrap_or("")),
            Action::HaveOutput => {
                let n = host.output().len();
                if n == 0 {
                    break;
                }
                host.consume_output(n).unwrap();
            }
            Action::NeedInput
            | Action::Yield
            | Action::ReadAt
            | Action::SpillExec
            | Action::Finished => {}
        }
    }
}

/// A reducer grouping text column 0 by COUNT(*).
fn grouped_text_reducer(producers: u32) -> GraphHost {
    use crate::ir::{CatalogColumn, ColumnKind};
    use crate::program::types::{AggExpr, AggKind};
    use crate::program::{encode_hash_reducer_program, HashReducerSpec};
    let catalog = vec![CatalogColumn {
        name: "k".into(),
        kind: ColumnKind::Text,
        physical: 6,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }];
    let program = encode_hash_reducer_program(&HashReducerSpec {
        producer_count: producers,
        catalog: &catalog,
        group_by: &[0],
        aggregates: &[AggExpr {
            kind: AggKind::CountStar,
            column: None,
            offset: 0,
        }],
        skip: 0,
        fetch: None,
        rank: None,
        group_budget: 0,
        group_spill: false,
        spill_combined_budget: 0,
        partition: 0,
    });
    let mut cur = PlanCursor::new(&program[6..]);
    GraphHost::open_hash_reducer(&mut cur, 64 * 1024, 1).unwrap()
}

#[test]
fn a_reducer_merges_partial_frames_like_the_rows_they_sum() {
    use crate::stream::partial_wcs::partial_state_from_wire_payload;
    use crate::stream::partials::partial_snapshot_from_state;
    let text_batch = |values: &[String]| {
        let (mut offsets, mut bytes) = (vec![0u32], Vec::new());
        for value in values {
            bytes.extend_from_slice(value.as_bytes());
            offsets.push(bytes.len() as u32);
        }
        let mut batch = crate::stream::OwnedWcolBatch::empty();
        batch.push_utf8_column(0, offsets, bytes, vec![1; values.len()]);
        batch.set_row_count(values.len()).unwrap();
        batch
    };
    let catalog = vec![crate::ir::CatalogColumn {
        name: "k".into(),
        kind: crate::ir::ColumnKind::Text,
        physical: 6,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }];
    let count = [crate::ir::AggExpr {
        column: None,
        kind: crate::ir::AggKind::CountStar,
        offset: 0,
    }];
    let partial_of = |values: &[String]| {
        let mut query = crate::ops::query::batch::BatchQuery::from_ir(
            &[],
            &[0],
            &count,
            None,
            0,
            false,
            &catalog,
        )
        .unwrap();
        let batch = text_batch(values);
        let mut cols = Vec::new();
        let view = batch.view_with(&mut cols);
        query
            .consume(&view, &crate::stream::SelectionMask::all(values.len()))
            .unwrap();
        query.finish_partial_state()
    };
    // Two scans whose keys overlap.
    let scan_rows = |scan: usize| -> Vec<String> {
        (0..3000)
            .map(|i| format!("https://site/{}", (i * 7 + scan * 500) % 900))
            .collect()
    };
    let mut host = grouped_text_reducer(2);
    for scan in 0..2 {
        let mut egress = RowBatchWcsEgress::new();
        egress
            .push_partial_partition(&partial_of(&scan_rows(scan)), 0)
            .unwrap();
        egress.push_end_partition(0).unwrap();
        let wire = egress.drain_to_vec().unwrap();
        let mut off = 0;
        while off < wire.len() {
            let n = host.try_feed_input_port(scan as u32, &wire[off..]).unwrap();
            if n == 0 {
                pump(&mut host);
            }
            off += n;
        }
    }
    for port in 0..2 {
        host.end_input_port(port).unwrap();
    }
    let mut out = Vec::new();
    let mut last = None;
    for _ in 0..10_000 {
        let action = host.poll();
        match action {
            Action::Failed => panic!("poll failed: {}", host.error().unwrap_or("")),
            Action::HaveOutput => {
                let n = host.output().len();
                out.extend_from_slice(host.output());
                host.consume_output(n).unwrap();
            }
            Action::Finished => {
                last = Some("finished");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(
        last,
        Some("finished"),
        "reducer did not finish; {} bytes out",
        out.len()
    );
    let mut ingress = crate::exec::operators::WcsIngress::new();
    let (_, frames) = ingress.feed(&out).unwrap();
    let mut got: Vec<(Vec<u8>, u64)> = Vec::new();
    for frame in frames.iter().filter(|f| f.frame_type == FRAME_PARTIAL) {
        let state = partial_state_from_wire_payload(&frame.payload).unwrap();
        for group in partial_snapshot_from_state(&state).unwrap().groups {
            let crate::ops::query::PartialKeyPart::Bytes(key) = &group.key[0] else {
                panic!("expected a text key");
            };
            got.push((key.clone(), group.stats[0].count));
        }
    }
    got.sort();
    let mut expected = std::collections::BTreeMap::<Vec<u8>, u64>::new();
    for row in scan_rows(0).into_iter().chain(scan_rows(1)) {
        *expected.entry(row.into_bytes()).or_default() += 1;
    }
    let expected: Vec<(Vec<u8>, u64)> = expected.into_iter().collect();
    assert_eq!(got, expected);
}

#[test]
fn dictionaries_leave_with_the_stream_that_installed_them() {
    let mut host = hash_reducer(2);
    let values: Vec<String> = (0..4_000)
        .map(|i| format!("key-{i:05}-{:x<40}", i))
        .collect();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();
    for frame in dictionary_frames(&refs) {
        host.feed_input_port(0, &frame).unwrap();
    }
    pump(&mut host);
    assert_eq!(host.retained_dictionary_count(), 1);
    let held = crate::formats::parquet::alloc_track::snapshot().live_bytes;
    host.feed_input_port(0, &end_frame()).unwrap();
    host.end_input_port(0).unwrap();
    // FRAME_END is queued but not decoded, so this stream still owns the dictionary.
    assert_eq!(host.retained_dictionary_count(), 1);
    pump(&mut host);
    assert_eq!(
        host.retained_dictionary_count(),
        0,
        "stream 0 dictionaries stayed after EOF"
    );
    let released = crate::formats::parquet::alloc_track::snapshot().live_bytes;
    if held > 100_000 {
        assert!(
            released + 64 * 1024 < held,
            "live bytes {released} after release, {held} while the dictionary was installed"
        );
    }

    for frame in dictionary_frames(&["only-the-second-stream"]) {
        host.feed_input_port(1, &frame).unwrap();
    }
    pump(&mut host);
    assert_eq!(host.retained_dictionary_count(), 1);
    host.feed_input_port(1, &end_frame()).unwrap();
    host.end_input_port(1).unwrap();
    pump(&mut host);
    assert_eq!(host.retained_dictionary_count(), 0);
}
