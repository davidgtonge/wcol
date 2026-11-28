//! After a pressure flush the empty table must not keep a footprint above the flush lock.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::ir::{AggExpr, AggKind, CatalogColumn, ColumnKind};
use crate::ops::query::PartialKeyPart;
use crate::spill::{
    decode_run_put_open, decode_run_read_open, HostR2SortedRunStore, HostSpillBridge, SpillConfig,
    SpillProgress, SPILL_OP_RUN_COMMIT, SPILL_OP_RUN_PUT, SPILL_OP_RUN_READ,
};
use crate::stream::partials::{partial_snapshot_from_state, PartialStateWcol};
use crate::stream::{OwnedWcolBatch, SelectionMask};

use super::finish_stream::BatchQueryFinish;
use super::BatchQuery;

const RETAINED_CAP: u64 = 8 * 1024 * 1024;
const HARD_CEILING: u64 = 32 * 1024 * 1024;
const FLUSH_TARGET: u64 = 16 * 1024 * 1024;
const SCRATCH: u64 = 1024 * 1024;
/// Flush lock: resident state at or above this triggers another pressure flush.
const FLUSH_THRESHOLD: u64 = FLUSH_TARGET - SCRATCH;

fn catalog() -> Vec<CatalogColumn> {
    vec![CatalogColumn {
        name: "k".into(),
        kind: ColumnKind::Number,
        physical: 2,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    }]
}

fn count_star() -> BatchQuery {
    BatchQuery::from_ir(
        &[],
        &[0],
        &[AggExpr {
            column: None,
            kind: AggKind::CountStar,
            offset: 0,
        }],
        None,
        0,
        false,
        &catalog(),
    )
    .unwrap()
}

fn i64_batch(keys: Vec<i64>) -> OwnedWcolBatch {
    let n = keys.len();
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, keys, vec![1; n]);
    batch.set_row_count(n).unwrap();
    batch
}

fn service_host(
    query: &mut BatchQuery,
    bridge: &Rc<RefCell<HostSpillBridge>>,
    bodies: &mut BTreeMap<String, Vec<u8>>,
) {
    let request = bridge
        .borrow()
        .pending_request()
        .expect("spill host request");
    match request.op {
        SPILL_OP_RUN_PUT => {
            let plan = decode_run_put_open(&request.append_bytes).unwrap();
            let body = bridge.borrow().run_upload.clone();
            assert_eq!(body.len() as u64, plan.byte_length);
            bodies.entry(plan.object_key).or_default().extend(body);
            bridge.borrow_mut().complete_run_put().unwrap();
            query.on_spill_run_put_handoff().unwrap();
        }
        SPILL_OP_RUN_COMMIT => {
            bridge.borrow_mut().complete_run_commit().unwrap();
            query.on_spill_run_commit_host_complete().unwrap();
        }
        SPILL_OP_RUN_READ => {
            let plan = decode_run_read_open(&request.append_bytes).unwrap();
            let body = bodies.get(&plan.object_key).expect("uploaded run");
            let start = plan.offset as usize;
            let end = start.saturating_add(plan.length as usize).min(body.len());
            let chunk = if start >= body.len() {
                Vec::new()
            } else {
                body[start..end].to_vec()
            };
            bridge.borrow_mut().fill_run_read(&chunk).unwrap();
            query.ingest_spill_host_read().unwrap();
        }
        op => panic!("unexpected spill op {op}"),
    }
}

fn drive_consume(
    query: &mut BatchQuery,
    bridge: &Rc<RefCell<HostSpillBridge>>,
    bodies: &mut BTreeMap<String, Vec<u8>>,
    batch: &OwnedWcolBatch,
    suspended: &mut bool,
) {
    let mask = SelectionMask::all(batch.row_count());
    for _ in 0..64 {
        let mut cols = Vec::new();
        let view = batch.view_with(&mut cols);
        match query.consume(&view, &mask).unwrap() {
            SpillProgress::Ready(()) => return,
            SpillProgress::AwaitHost => {
                *suspended = true;
                service_host(query, bridge, bodies);
            }
        }
    }
    panic!("consume did not resume");
}

fn counts(state: &PartialStateWcol) -> BTreeMap<i128, u64> {
    let snap = partial_snapshot_from_state(state).unwrap();
    snap.groups
        .iter()
        .map(|group| {
            let PartialKeyPart::Int(key) = group.key[0] else {
                panic!("expected an integer group key");
            };
            (key, group.stats[0].count)
        })
        .collect()
}

fn finish_spilled(
    query: &mut BatchQuery,
    bridge: &Rc<RefCell<HostSpillBridge>>,
    bodies: &mut BTreeMap<String, Vec<u8>>,
) -> PartialStateWcol {
    // Production finish flushes the live table into a run before merging.
    for _ in 0..64 {
        match query.flush_live_run().unwrap() {
            SpillProgress::Ready(()) => break,
            SpillProgress::AwaitHost => service_host(query, bridge, bodies),
        }
    }
    let mut finish = BatchQueryFinish::begin(query).unwrap();
    let mut chunks = Vec::new();
    for _ in 0..100_000 {
        match finish.next_chunk(query).unwrap() {
            SpillProgress::AwaitHost => service_host(query, bridge, bodies),
            SpillProgress::Ready(Some(chunk)) => chunks.push(chunk),
            SpillProgress::Ready(None) => break,
        }
    }
    assert!(!chunks.is_empty(), "spill finish produced no groups");
    let mut iter = chunks.into_iter();
    let first = iter.next().unwrap();
    iter.try_fold(first, crate::stream::partials::partial_state_concat)
        .unwrap()
}

fn count_mismatches(left: &BTreeMap<i128, u64>, right: &BTreeMap<i128, u64>) -> String {
    let mut only_left = 0usize;
    let mut only_right = 0usize;
    let mut different = 0usize;
    let mut sample = Vec::new();
    for (key, count) in left {
        match right.get(key) {
            Some(other) if other == count => {}
            Some(other) => {
                different += 1;
                if sample.len() < 8 {
                    sample.push(format!("{key}: {count} vs {other}"));
                }
            }
            None => {
                only_left += 1;
                if sample.len() < 8 {
                    sample.push(format!("{key}: {count} only left"));
                }
            }
        }
    }
    for key in right.keys() {
        if !left.contains_key(key) {
            only_right += 1;
            if sample.len() < 8 {
                sample.push(format!("{key}: {} only right", right[key]));
            }
        }
    }
    format!(
        "left={} right={} only_left={only_left} only_right={only_right} different={different} sample={sample:?}",
        left.len(),
        right.len()
    )
}

fn open_spilled(
    session: u64,
) -> (
    BatchQuery,
    Rc<RefCell<HostSpillBridge>>,
    BTreeMap<String, Vec<u8>>,
) {
    let bridge = Rc::new(RefCell::new(HostSpillBridge::new(session)));
    let mut spilled = count_star();
    spilled
        .enable_spill(
            SpillConfig {
                combined_budget_bytes: FLUSH_TARGET,
                scratch_reserve_bytes: SCRATCH,
                ..SpillConfig::default()
            },
            Box::new(HostR2SortedRunStore::new(bridge.clone())),
        )
        .unwrap();
    (spilled, bridge, BTreeMap::new())
}

fn grow_until_first_flush(
    spilled: &mut BatchQuery,
    bridge: &Rc<RefCell<HostSpillBridge>>,
    bodies: &mut BTreeMap<String, Vec<u8>>,
    mut baseline: Option<&mut BatchQuery>,
) -> (bool, u64, i64) {
    let mut suspended = false;
    let mut next = 0i64;
    let mut peak = 0u64;
    while spilled
        .spill_flush_stats()
        .map(|stats| stats.runs)
        .unwrap_or(0)
        == 0
    {
        let batch = i64_batch((next..next + 4096).collect());
        next += 4096;
        drive_consume(spilled, bridge, bodies, &batch, &mut suspended);
        if let Some(baseline) = baseline.as_mut() {
            let mut cols = Vec::new();
            baseline
                .consume(
                    &batch.view_with(&mut cols),
                    &SelectionMask::all(batch.row_count()),
                )
                .unwrap();
        }
        peak = peak.max(spilled.resident_state_bytes());
        peak = peak.max(
            spilled
                .spill_flush_stats()
                .map(|stats| stats.peak_live_bytes)
                .unwrap_or(0),
        );
        assert!(
            next < 2_000_000,
            "table did not flush before {next} distinct keys"
        );
    }
    (suspended, peak, next)
}

fn flush_until_empty(
    spilled: &mut BatchQuery,
    bridge: &Rc<RefCell<HostSpillBridge>>,
    bodies: &mut BTreeMap<String, Vec<u8>>,
) {
    for _ in 0..64 {
        if spilled.live_group_len() == 0 && !spilled.spill_flush_incomplete() {
            return;
        }
        match spilled.flush_live_run().unwrap() {
            SpillProgress::Ready(()) => {}
            SpillProgress::AwaitHost => service_host(spilled, bridge, bodies),
        }
    }
    panic!(
        "live table still has {} groups after flush drain",
        spilled.live_group_len()
    );
}

/// After a completed pressure flush, an empty grouping table must sit under the
/// flush threshold. Retaining more capacity than that recreates the micro-run bug.
#[test]
fn empty_table_stays_under_flush_threshold_after_pressure_flush() {
    let (mut spilled, bridge, mut bodies) = open_spilled(17);
    let (suspended, peak, _) = grow_until_first_flush(&mut spilled, &bridge, &mut bodies, None);

    assert!(suspended, "pressure flush did not suspend on the host");
    assert!(spilled.spill_flush_stats().unwrap().runs >= 1);
    // A mid-batch flush can leave leftover rows in the live table. Drain them so
    // the invariant is checked on an empty grouping structure.
    flush_until_empty(&mut spilled, &bridge, &mut bodies);

    assert_eq!(spilled.live_group_len(), 0);
    let retained = spilled.resident_state_bytes();
    assert!(
        retained < FLUSH_THRESHOLD,
        "empty table retained {retained} bytes at or above the flush threshold {FLUSH_THRESHOLD}"
    );
    assert!(
        retained < RETAINED_CAP,
        "empty table retained {retained} bytes above the 8 MiB low-water mark"
    );
    assert!(
        (FLUSH_THRESHOLD..=HARD_CEILING).contains(&peak),
        "peak group memory {peak} never crossed the flush lock"
    );
}

#[test]
fn pressure_flush_releases_capacity_across_host_suspend() {
    let (mut spilled, bridge, mut bodies) = open_spilled(11);
    let mut baseline = count_star();
    let (mut suspended, peak, _) =
        grow_until_first_flush(&mut spilled, &bridge, &mut bodies, Some(&mut baseline));

    assert!(
        suspended,
        "first pressure flush did not suspend on the host"
    );
    assert_eq!(
        spilled.spill_flush_stats().unwrap().runs,
        1,
        "first pressure flush should commit one run"
    );
    assert!(
        spilled.resident_state_bytes() < FLUSH_THRESHOLD,
        "empty table retained {} bytes at or above the flush threshold",
        spilled.resident_state_bytes()
    );
    assert!(
        spilled.resident_state_bytes() < RETAINED_CAP,
        "empty table retained {} bytes",
        spilled.resident_state_bytes()
    );
    assert!(
        peak > FLUSH_TARGET / 2 && peak <= HARD_CEILING,
        "peak group memory {peak} left the 16/32 MiB band"
    );

    let repeated = i64_batch(vec![0]);
    for _ in 0..4_000 {
        drive_consume(
            &mut spilled,
            &bridge,
            &mut bodies,
            &repeated,
            &mut suspended,
        );
        let mut cols = Vec::new();
        baseline
            .consume(&repeated.view_with(&mut cols), &SelectionMask::all(1))
            .unwrap();
        assert_eq!(
            spilled.spill_flush_stats().unwrap().runs,
            1,
            "a one-row batch committed another run"
        );
        assert!(spilled.resident_state_bytes() <= HARD_CEILING);
    }

    let spilled_state = finish_spilled(&mut spilled, &bridge, &mut bodies);
    let baseline_state = baseline.finish_partial_state();
    let left = counts(&spilled_state);
    let right = counts(&baseline_state);
    assert_eq!(left, right, "{}", count_mismatches(&left, &right));
}

/// After PUT completes but before COMMIT, flush must still report incomplete so
/// the host keeps driving finish_run instead of parking on NeedInput.
#[test]
fn put_in_flight_counts_as_incomplete_until_commit() {
    let (mut spilled, bridge, mut bodies) = open_spilled(19);
    let mut next = 0i64;
    while spilled.spill_flush_stats().unwrap().runs == 0 {
        let batch = i64_batch((next..next + 4096).collect());
        next += 4096;
        assert!(next < 2_000_000, "table did not flush before {next} keys");
        let mask = SelectionMask::all(batch.row_count());
        for _ in 0..64 {
            let mut cols = Vec::new();
            match spilled.consume(&batch.view_with(&mut cols), &mask).unwrap() {
                SpillProgress::Ready(()) => break,
                SpillProgress::AwaitHost => {
                    let op = bridge.borrow().pending_request().expect("pending").op;
                    if op == SPILL_OP_RUN_PUT {
                        service_host(&mut spilled, &bridge, &mut bodies);
                        assert_eq!(spilled.spill_flush_stats().unwrap().runs, 0);
                        assert!(
                            spilled.spill_flush_incomplete(),
                            "PutInFlight must keep spill_flush_incomplete so COMMIT is driven"
                        );
                        // Drive COMMIT next and confirm the run commits.
                        match spilled.flush_live_run().unwrap() {
                            SpillProgress::Ready(()) => {}
                            SpillProgress::AwaitHost => {
                                assert_eq!(
                                    bridge.borrow().pending_request().unwrap().op,
                                    SPILL_OP_RUN_COMMIT
                                );
                                service_host(&mut spilled, &bridge, &mut bodies);
                            }
                        }
                        assert!(spilled.spill_flush_stats().unwrap().runs >= 1);
                        return;
                    }
                    service_host(&mut spilled, &bridge, &mut bodies);
                }
            }
        }
    }
    panic!("committed a run without observing RUN_PUT");
}
