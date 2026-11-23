use super::*;
use crate::program::types::{FetchPiece, NodeId};

fn read(id: u32, offset: u64, pieces: Vec<FetchPiece>) -> LogicalRead {
    let length = pieces.iter().map(|piece| piece.length).sum();
    LogicalRead {
        id,
        scan: 0,
        node: NodeId(0),
        object_id: "f".into(),
        row_group: 0,
        column: id,
        offset,
        length,
        dictionary: false,
        codec: 0,
        fetch_id: pieces[0].fetch_id,
        pieces,
    }
}

fn piece(fetch_id: u32, fetch_offset: u64, read_offset: u64, length: u64) -> FetchPiece {
    FetchPiece {
        fetch_id,
        fetch_offset,
        read_offset,
        length,
    }
}

fn fetch(id: u32, offset: u64, length: u64, reads: Vec<u32>) -> FetchRequest {
    FetchRequest {
        id,
        node: NodeId(0),
        object_id: "f".into(),
        offset,
        length,
        logical_reads: reads,
    }
}

fn load(stage: &mut FetchStage, demand: bool, bytes: &[u8]) -> HostFetchRead {
    let host = stage.poll_host_fetch(demand).expect("a fetch to load");
    stage
        .complete_host_fetch(host.ticket, bytes.to_vec())
        .unwrap();
    host
}

#[test]
fn slices_across_two_pieces_and_frees_after_the_last_byte() {
    let pieces = vec![piece(0, 0, 0, 4), piece(0, 4, 4, 6)];
    let mut stage = FetchStage::new(
        vec![fetch(0, 100, 10, vec![0])],
        vec![read(0, 100, pieces)],
        1,
        u64::MAX,
    );
    assert_eq!(load(&mut stage, false, b"0123456789").offset, 100);
    assert_eq!(stage.slice_for_range(102, 5).unwrap(), b"23456");
    assert_eq!(
        stage.resident_bytes(),
        10,
        "5 bytes of the read are still unconsumed"
    );
    assert_eq!(stage.slice_for_range(100, 2).unwrap(), b"01");
    assert_eq!(stage.slice_for_range(107, 3).unwrap(), b"789");
    assert_eq!(stage.resident_bytes(), 0);
}

#[test]
fn coalesced_gap_bytes_do_not_keep_a_buffer_alive() {
    // One fetch [0, 10): read 0 is [0, 3), read 1 is [7, 10); bytes 3..7 are a gap.
    let reads = vec![
        read(0, 0, vec![piece(0, 0, 0, 3)]),
        read(1, 7, vec![piece(0, 7, 0, 3)]),
    ];
    let mut stage = FetchStage::new(vec![fetch(0, 0, 10, vec![0, 1])], reads, 1, u64::MAX);
    load(&mut stage, false, b"abcdefghij");
    assert_eq!(stage.slice_for_range(0, 3).unwrap(), b"abc");
    assert_eq!(stage.slice_for_range(7, 3).unwrap(), b"hij");
    assert_eq!(stage.resident_bytes(), 0);
}

#[test]
fn prefetch_stays_within_the_byte_cap_but_a_lone_demand_load_passes_it() {
    let reads: Vec<LogicalRead> = (0..5)
        .map(|id| read(id, u64::from(id) * 10, vec![piece(id, 0, 0, 10)]))
        .collect();
    let fetches = (0..5)
        .map(|id| fetch(id, u64::from(id) * 10, 10, vec![id]))
        .collect();
    let mut stage = FetchStage::new(fetches, reads, 2, 40);
    let first = stage.poll_host_fetch(false).unwrap();
    let second = stage.poll_host_fetch(false).unwrap();
    assert!(
        stage.poll_host_fetch(false).is_none(),
        "two in flight: depth is full"
    );
    stage
        .complete_host_fetch(first.ticket, vec![0; 10])
        .unwrap();
    stage
        .complete_host_fetch(second.ticket, vec![1; 10])
        .unwrap();
    load(&mut stage, false, &[2; 10]);
    let fourth = stage.poll_host_fetch(false).unwrap();
    assert!(
        stage.poll_host_fetch(true).is_none(),
        "at the cap, a demand load waits for the fetch in flight"
    );
    stage
        .complete_host_fetch(fourth.ticket, vec![3; 10])
        .unwrap();
    assert!(
        stage.poll_host_fetch(false).is_none(),
        "40 bytes resident: no more prefetch"
    );
    // The decoder needs the fifth column before it can finish the first four.
    stage.prioritize_fetch_covering(45);
    assert_eq!(load(&mut stage, true, &[4; 10]).offset, 40);
    assert_eq!(stage.metrics().peak_resident_bytes, 50);
    for column in 0..5u64 {
        stage.slice_for_range(column * 10, 10).unwrap();
    }
    assert_eq!(stage.resident_bytes(), 0);
}

#[test]
fn fetches_load_in_the_order_the_decoder_reaches_them() {
    // Row group 0: a long column in fetches 0..3 at offsets 0..30, then a short column
    // in fetch 3 at offset 30. Row group 1: one fetch at offset 40.
    let long = LogicalRead {
        length: 30,
        ..read(
            0,
            0,
            (0..3)
                .map(|id| piece(id, 0, u64::from(id) * 10, 10))
                .collect(),
        )
    };
    let short = read(1, 30, vec![piece(3, 0, 0, 10)]);
    let next = LogicalRead {
        row_group: 1,
        ..read(2, 40, vec![piece(4, 0, 0, 10)])
    };
    let fetches = (0..5)
        .map(|id| fetch(id, u64::from(id) * 10, 10, vec![id.min(2)]))
        .collect();
    let mut stage = FetchStage::new(fetches, vec![long, short, next], 8, u64::MAX);
    let offsets: Vec<u64> = std::iter::from_fn(|| stage.poll_host_fetch(false))
        .map(|host| host.offset)
        .collect();
    assert_eq!(offsets, vec![0, 30, 10, 20, 40]);
}

#[test]
fn loading_row_groups_are_granted_ones_still_queued_or_in_flight() {
    // Row group N is read N (one fetch each); row group 3 is never granted.
    let reads: Vec<LogicalRead> = (0..4)
        .map(|id| LogicalRead {
            row_group: id,
            ..read(id, u64::from(id) * 10, vec![piece(id, 0, 0, 10)])
        })
        .collect();
    let fetches = (0..4)
        .map(|id| fetch(id, u64::from(id) * 10, 10, vec![id]))
        .collect();
    let mut stage = FetchStage::new(fetches, reads, 1, u64::MAX);
    stage.gate_by_row_group();
    stage.grant(&[0, 1, 2]);
    load(&mut stage, false, &[0; 10]);
    stage.poll_host_fetch(false).expect("row group 1 in flight");
    // 0 is loaded, 1 in flight, 2 queued, 3 not granted.
    assert_eq!(stage.loading_row_groups(), BTreeSet::from([1, 2]));
}

#[test]
fn a_fetch_can_arrive_in_ring_sized_parts() {
    let reads = vec![read(0, 0, vec![piece(0, 0, 0, 10)])];
    let mut stage = FetchStage::new(vec![fetch(0, 0, 10, vec![0])], reads, 1, u64::MAX);
    let host = stage.poll_host_fetch(true).unwrap();
    stage
        .complete_host_fetch(host.ticket, b"0123".to_vec())
        .unwrap();
    assert!(
        stage.slice_for_range(0, 4).is_err(),
        "not usable until every part is in"
    );
    stage
        .complete_host_fetch(host.ticket, b"456".to_vec())
        .unwrap();
    stage
        .complete_host_fetch(host.ticket, b"789".to_vec())
        .unwrap();
    assert_eq!(stage.slice_for_range(0, 10).unwrap(), b"0123456789");
    assert!(
        stage
            .complete_host_fetch(host.ticket, b"x".to_vec())
            .is_err(),
        "ticket is finished"
    );
}

#[test]
fn a_cancelled_fetch_can_be_requested_again() {
    let reads = vec![read(0, 0, vec![piece(0, 0, 0, 4)])];
    let mut stage = FetchStage::new(vec![fetch(0, 0, 4, vec![0])], reads, 1, u64::MAX);
    let first = stage.poll_host_fetch(true).unwrap();
    stage
        .complete_host_fetch(first.ticket, b"01".to_vec())
        .unwrap();
    stage.cancel_host_fetch(first.ticket).unwrap();
    let again = stage.poll_host_fetch(true).expect("fetch re-queued");
    assert_ne!(again.ticket, first.ticket);
    stage
        .complete_host_fetch(again.ticket, b"0123".to_vec())
        .unwrap();
    assert_eq!(stage.slice_for_range(0, 4).unwrap(), b"0123");
}
