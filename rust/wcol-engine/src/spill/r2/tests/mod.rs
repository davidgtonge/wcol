use super::super::host::{
    await_host, HostSpillRequest, SPILL_OP_RUN_COMMIT, SPILL_OP_RUN_PUT, SPILL_OP_RUN_READ,
};
use super::super::sorted_run::{encode_run, RunStreamDecoder};
use super::codec::{run_read_chunk, MAX_RUN_READ_CHUNK, MERGE_READ_BUDGET};
use super::*;

mod merges;
mod uploads;

pub(super) fn drive_put_commit(
    store: &mut HostR2SortedRunStore,
    bridge: &Rc<RefCell<HostSpillBridge>>,
) {
    for _ in 0..32 {
        match store.finalize_run() {
            Ok(SpillProgress::Ready(())) if !store.needs_finish_run() => return,
            Ok(SpillProgress::Ready(())) => continue,
            Ok(SpillProgress::AwaitHost) => {
                let req = bridge.borrow().pending_request().expect("pending spill");
                match req.op {
                    SPILL_OP_RUN_PUT => {
                        bridge.borrow_mut().complete_run_put().unwrap();
                        store.on_run_put_handoff().unwrap();
                    }
                    SPILL_OP_RUN_COMMIT => {
                        bridge.borrow_mut().complete_run_commit().unwrap();
                        store.on_run_commit_host_complete().unwrap();
                    }
                    op => panic!("unexpected spill op {op}"),
                }
            }
            Err(e) => panic!("{e}"),
        }
    }
    panic!("drive_put_commit iteration cap");
}

pub(super) fn commit_captured_run(
    store: &mut HostR2SortedRunStore,
    bridge: &Rc<RefCell<HostSpillBridge>>,
) -> (String, Vec<u8>) {
    let mut key = None;
    let mut body = None;
    for _ in 0..8 {
        match store.finalize_run().unwrap() {
            SpillProgress::Ready(()) if !store.needs_finish_run() => {
                return (key.expect("run key"), body.expect("run body"));
            }
            SpillProgress::Ready(()) => continue,
            SpillProgress::AwaitHost => {
                let req = bridge.borrow().pending_request().expect("pending spill");
                match req.op {
                    SPILL_OP_RUN_PUT => {
                        let plan = decode_run_put_open(&req.append_bytes).unwrap();
                        assert_eq!(store.run_records, 0);
                        assert!(store.run_body.is_empty());
                        let staged = bridge.borrow().run_upload.clone();
                        assert_eq!(staged.len() as u64, plan.byte_length);
                        key = Some(plan.object_key);
                        body = Some(staged);
                        bridge.borrow_mut().complete_run_put().unwrap();
                        store.on_run_put_handoff().unwrap();
                    }
                    SPILL_OP_RUN_COMMIT => {
                        bridge.borrow_mut().complete_run_commit().unwrap();
                        store.on_run_commit_host_complete().unwrap();
                    }
                    op => panic!("unexpected spill op {op}"),
                }
            }
        }
    }
    panic!("run commit did not finish");
}

pub(super) fn retained_merge_bytes(store: &HostR2SortedRunStore) -> usize {
    store
        .merge_readers
        .iter()
        .map(|reader| {
            let head = reader
                .head
                .as_ref()
                .map_or(0, |(key, payload)| key.len().saturating_add(payload.len()));
            head.saturating_add(reader.decoder.retained_bytes())
        })
        .sum()
}

pub(super) fn merge_r2(
    store: &mut HostR2SortedRunStore,
    bridge: &Rc<RefCell<HostSpillBridge>>,
    bodies: &std::collections::HashMap<String, Vec<u8>>,
) -> (Vec<(Vec<u8>, i64)>, usize) {
    merge_r2_counting(store, bridge, bodies, &std::cell::Cell::new(0))
}

/// `merge_r2`, counting the run reads it serves in `reads`.
pub(super) fn merge_r2_counting(
    store: &mut HostR2SortedRunStore,
    bridge: &Rc<RefCell<HostSpillBridge>>,
    bodies: &std::collections::HashMap<String, Vec<u8>>,
    reads: &std::cell::Cell<u32>,
) -> (Vec<(Vec<u8>, i64)>, usize) {
    let mut peak = 0usize;
    let mut opened = false;
    for _ in 0..bodies.len().saturating_mul(4).max(8) {
        match store.begin_ordered_scan().unwrap() {
            SpillProgress::Ready(()) => {
                peak = peak.max(retained_merge_bytes(store));
                opened = true;
                break;
            }
            SpillProgress::AwaitHost => {
                let req = bridge.borrow().pending_request().expect("pending read");
                let plan = decode_run_read_open(&req.append_bytes).unwrap();
                reads.set(reads.get() + 1);
                let body = bodies.get(&plan.object_key).expect("run body");
                let offset = plan.offset as usize;
                let end = offset.saturating_add(plan.length as usize).min(body.len());
                bridge
                    .borrow_mut()
                    .fill_run_read(&body[offset..end])
                    .unwrap();
            }
        }
    }
    assert!(opened, "ordered scan did not finish bootstrapping");
    let mut resume = SpillScanResume::default();
    let mut rows = Vec::new();
    for _ in 0..100_000 {
        match store
            .next_ordered_batch(&mut resume, 32, 1024 * 1024)
            .unwrap()
        {
            SpillProgress::Ready(batch) => {
                rows.extend(batch.rows.into_iter().map(|row| (row.1, row.0)));
                peak = peak.max(retained_merge_bytes(store));
                if batch.exhausted {
                    return (rows, peak);
                }
            }
            SpillProgress::AwaitHost => {
                let req = bridge.borrow().pending_request().expect("pending read");
                let plan = decode_run_read_open(&req.append_bytes).unwrap();
                reads.set(reads.get() + 1);
                let body = bodies.get(&plan.object_key).expect("run body");
                let offset = plan.offset as usize;
                let end = offset.saturating_add(plan.length as usize).min(body.len());
                bridge
                    .borrow_mut()
                    .fill_run_read(&body[offset..end])
                    .unwrap();
            }
        }
    }
    panic!("merge did not finish");
}
