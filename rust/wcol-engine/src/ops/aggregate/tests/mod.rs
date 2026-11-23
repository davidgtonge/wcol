use super::*;
use crate::ir::AggKind;
use crate::stream::partial_wcs::{partial_state_from_wire_payload, partial_state_to_wcs_payload};

mod budget;
mod distinct;
mod partials;
mod ranking;
mod spill;

pub(super) fn spill_ready<T>(progress: SpillProgress<T>) -> T {
    progress.expect_ready("test spill await")
}

pub(super) fn finish_state(agg: &mut AggregateOp) -> PartialStateWcol {
    spill_ready(agg.finish_partial_state().unwrap())
}

pub(super) fn consume_all(agg: &mut AggregateOp, batch: &OwnedWcolBatch) {
    spill_ready(
        agg.consume_row_batch(batch, &SelectionMask::all(batch.row_count()))
            .unwrap(),
    );
}

pub(super) fn distinct_count(state: &PartialStateWcol) -> u64 {
    let snap = crate::stream::partials::partial_snapshot_from_state(state).unwrap();
    let groups = crate::ops::query::finalize_groups(&snap).unwrap();
    assert_eq!(groups.len(), 1);
    groups[0].stats[0].count
}

/// Per-group distinct counts the way a hash reducer emits them: every finish chunk becomes
/// counts, and the counts are summed (as the finalizer does).
pub(super) fn reducer_counts(
    agg: &mut AggregateOp,
) -> Vec<(Vec<crate::ops::query::PartialKeyPart>, u64)> {
    reducer_counts_served(agg, &mut |_| {})
}

/// `serve` answers the spill host op an `AwaitHost` is waiting on.
pub(super) fn reducer_counts_served(
    agg: &mut AggregateOp,
    serve: &mut dyn FnMut(&mut AggregateOp),
) -> Vec<(Vec<crate::ops::query::PartialKeyPart>, u64)> {
    let mut chunks = Vec::new();
    if agg.has_group_spill() && agg.needs_spill_chunk_finish() {
        for _ in 0..1024 {
            match agg.begin_finish_chunks().unwrap() {
                SpillProgress::Ready(()) => break,
                SpillProgress::AwaitHost => serve(agg),
            }
        }
        loop {
            match agg.finish_next_chunk().unwrap() {
                SpillProgress::Ready(Some(chunk)) => chunks.push(chunk),
                SpillProgress::Ready(None) => break,
                SpillProgress::AwaitHost => serve(agg),
            }
        }
    } else {
        chunks.push(finish_state(agg));
    }
    let mut merged: Option<crate::ops::query::PartialSnapshot> = None;
    for chunk in &chunks {
        let snap = crate::stream::partials::partial_snapshot_from_state(chunk).unwrap();
        let counts = crate::ops::query::distinct_counts_snapshot(&snap).unwrap();
        merged = Some(match merged {
            None => counts,
            Some(left) => crate::ops::query::merge_partial_snapshots(&left, &counts).unwrap(),
        });
    }
    let mut rows: Vec<_> = crate::ops::query::finalize_groups(&merged.unwrap())
        .unwrap()
        .into_iter()
        .map(|group| (group.key, group.stats[0].count))
        .collect();
    rows.sort_by(|a, b| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)));
    rows
}

/// A fake R2 host: keeps uploaded run bodies and serves ranged reads from them.
pub(super) fn serve_r2(
    agg: &mut AggregateOp,
    bridge: &std::rc::Rc<std::cell::RefCell<crate::spill::HostSpillBridge>>,
    bodies: &mut std::collections::HashMap<String, Vec<u8>>,
) {
    let Some(req) = bridge.borrow().pending_request() else {
        return;
    };
    match req.op {
        crate::spill::SPILL_OP_RUN_PUT => {
            let plan = crate::spill::decode_run_put_open(&req.append_bytes).unwrap();
            let body = std::mem::take(&mut bridge.borrow_mut().run_upload);
            assert_eq!(body.len() as u64, plan.byte_length);
            bodies.entry(plan.object_key).or_default().extend(body);
            bridge.borrow_mut().complete_run_put().unwrap();
            agg.on_spill_run_put_handoff().unwrap();
        }
        crate::spill::SPILL_OP_RUN_COMMIT => {
            bridge.borrow_mut().complete_run_commit().unwrap();
            agg.on_spill_run_commit_host_complete().unwrap();
        }
        crate::spill::SPILL_OP_RUN_READ => {
            let plan = crate::spill::decode_run_read_open(&req.append_bytes).unwrap();
            let body = &bodies[&plan.object_key];
            let start = plan.offset as usize;
            let end = start.saturating_add(plan.length as usize).min(body.len());
            bridge
                .borrow_mut()
                .fill_run_read(&body[start..end])
                .unwrap();
            agg.ingest_spill_host_read().unwrap();
        }
        op => panic!("unexpected spill op {op}"),
    }
}

/// Every group of a finished table, partial chunk by chunk, merged and sorted by key.
pub(super) fn finished_groups_served(
    agg: &mut AggregateOp,
    serve: &mut dyn FnMut(&mut AggregateOp),
) -> Vec<crate::ops::query::PartialGroup> {
    let mut chunks = Vec::new();
    if agg.has_group_spill() && agg.needs_spill_chunk_finish() {
        for _ in 0..1024 {
            match agg.begin_finish_chunks().unwrap() {
                SpillProgress::Ready(()) => break,
                SpillProgress::AwaitHost => serve(agg),
            }
        }
        loop {
            match agg.finish_next_chunk().unwrap() {
                SpillProgress::Ready(Some(chunk)) => chunks.push(chunk),
                SpillProgress::Ready(None) => break,
                SpillProgress::AwaitHost => serve(agg),
            }
        }
    } else {
        chunks.push(finish_state(agg));
    }
    let mut merged: Option<crate::ops::query::PartialSnapshot> = None;
    for chunk in &chunks {
        let snap = crate::stream::partials::partial_snapshot_from_state(chunk).unwrap();
        merged = Some(match merged {
            None => snap,
            Some(left) => crate::ops::query::merge_partial_snapshots(&left, &snap).unwrap(),
        });
    }
    let mut groups = crate::ops::query::finalize_groups(&merged.unwrap()).unwrap();
    groups.sort_by(|a, b| format!("{:?}", a.key).cmp(&format!("{:?}", b.key)));
    groups
}

/// The first differing group, not a dump of every group.
pub(super) fn assert_same_groups(
    got: &[crate::ops::query::PartialGroup],
    expected: &[crate::ops::query::PartialGroup],
    label: &str,
) {
    for (got, want) in got.iter().zip(expected) {
        assert_eq!(got.key, want.key, "{label}: group keys differ");
        for (agg, (g, w)) in got.stats.iter().zip(&want.stats).enumerate() {
            // An empty stat's float min and max are NaN, which never equals itself.
            let same = |a: f64, b: f64| a == b || (a.is_nan() && b.is_nan());
            let equal = (g.count, g.flags, g.sum_i, g.min_i, g.max_i, &g.sketch)
                == (w.count, w.flags, w.sum_i, w.min_i, w.max_i, &w.sketch)
                && same(g.sum_f, w.sum_f)
                && same(g.min_f, w.min_f)
                && same(g.max_f, w.max_f);
            assert!(
                equal,
                "{label}: key {:?} aggregate {agg}: {g:?} vs {w:?}",
                got.key
            );
        }
    }
    assert_eq!(got.len(), expected.len(), "{label}: group count");
}
