//! Milestone B: hash partition outputs over WCS1 `DATA_BATCH` streams (2×2 topology in-process).

use wcol::exec::operator::{StreamProgress, WcolStreamItem};
use wcol::ops::exchange::{
    HashPartitionIo, HashPartitionOp, HashPartitionPlan, RowBatchWcsEgress, RowBatchWcsIngress,
    WCOL_EXCHANGE_HASH_VERSION_DEFAULT,
};
use wcol::stream::OwnedWcolBatch;

fn partition_into_batches(
    op: &mut HashPartitionOp,
    input: OwnedWcolBatch,
) -> Result<Vec<Vec<OwnedWcolBatch>>, String> {
    let parts = op.stats().row_counts.len();
    let mut per_part: Vec<Vec<OwnedWcolBatch>> = (0..parts).map(|_| Vec::new()).collect();
    let mut outs: Vec<Option<WcolStreamItem>> = vec![None; parts];
    let mut pending = Some(WcolStreamItem::from_row_batch(input));
    let mut input_eof = false;
    loop {
        let blocked = vec![false; parts];
        let mut io = HashPartitionIo {
            input: None,
            input_slot: Some(&mut pending),
            input_eof,
            outputs: &mut outs,
            outputs_blocked: &blocked,
        };
        match op.poll(&mut io)? {
            StreamProgress::HaveOutput => {
                for (p, slot) in outs.iter_mut().enumerate() {
                    if let Some(item) = slot.take() {
                        per_part[p].push(item.into_batch());
                    }
                }
            }
            StreamProgress::NeedInput => {
                if pending.is_none() && !input_eof {
                    input_eof = true;
                }
            }
            StreamProgress::Finished => break,
            StreamProgress::Blocked => return Err("partition blocked".to_string()),
            _ => {}
        }
    }
    Ok(per_part)
}

fn encode_partition_stream(batches: &[OwnedWcolBatch]) -> Result<Vec<u8>, String> {
    let mut egress = RowBatchWcsEgress::new();
    for batch in batches {
        egress.push_batch(batch).map_err(|e| e.to_string())?;
    }
    egress.push_end().map_err(|e| e.to_string())?;
    egress.drain_to_vec().map_err(|e| e.to_string())
}

fn feed_chunks(ingress: &mut RowBatchWcsIngress, wire: &[u8]) -> Result<(), String> {
    let mut off = 0usize;
    while off < wire.len() {
        let end = (off + 7).min(wire.len());
        let n = ingress.feed(&wire[off..end]).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("ingress stalled".into());
        }
        off += n;
    }
    ingress.finish_feed().map_err(|e| e.to_string())
}

fn consume_stream(wire: &[u8]) -> Result<u64, String> {
    let mut ingress = RowBatchWcsIngress::new();
    feed_chunks(&mut ingress, wire)?;
    Ok(ingress.rows_received)
}

#[test]
fn two_partitions_over_wcs1_preserve_rows() {
    let plan = HashPartitionPlan::new(vec![0], 2, WCOL_EXCHANGE_HASH_VERSION_DEFAULT).unwrap();
    let mut op = HashPartitionOp::with_limits(plan, 16, 64).unwrap();
    let mut input = OwnedWcolBatch::empty();
    input.push_i64_column(0, (0..100).collect(), vec![1; 100]);
    input.set_row_count(100).unwrap();

    let parts = partition_into_batches(&mut op, input).unwrap();
    let mut total = 0u64;
    for part_batches in &parts {
        if part_batches.is_empty() {
            continue;
        }
        total += consume_stream(&encode_partition_stream(part_batches).unwrap()).unwrap();
    }
    assert_eq!(total, 100);
    assert_eq!(op.stats().row_counts.iter().sum::<u64>(), 100);
}

#[test]
fn two_producers_fan_in_per_partition() {
    let plan = HashPartitionPlan::new(vec![0], 2, WCOL_EXCHANGE_HASH_VERSION_DEFAULT).unwrap();

    let make_batch = |start: i64, len: usize| {
        let mut b = OwnedWcolBatch::empty();
        b.push_i64_column(0, (start..start + len as i64).collect(), vec![1; len]);
        b.set_row_count(len).unwrap();
        b
    };

    let mut op_a = HashPartitionOp::with_limits(plan.clone(), 8, 32).unwrap();
    let mut op_b = HashPartitionOp::with_limits(plan, 8, 32).unwrap();
    let parts_a = partition_into_batches(&mut op_a, make_batch(0, 50)).unwrap();
    let parts_b = partition_into_batches(&mut op_b, make_batch(50, 50)).unwrap();

    for p in 0..2 {
        let mut rows = 0u64;
        for batches in [&parts_a[p], &parts_b[p]] {
            if batches.is_empty() {
                continue;
            }
            rows += consume_stream(&encode_partition_stream(batches).unwrap()).unwrap();
        }
        let expected = op_a.stats().row_counts[p] + op_b.stats().row_counts[p];
        assert_eq!(rows, expected);
    }

    assert_eq!(
        op_a.stats().row_counts.iter().sum::<u64>() + op_b.stats().row_counts.iter().sum::<u64>(),
        100
    );
}

#[test]
fn truncated_stream_rejected_without_end() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1], vec![1]);
    batch.set_row_count(1).unwrap();
    let mut egress = RowBatchWcsEgress::new();
    egress.push_batch(&batch).unwrap();
    let wire = egress.drain_to_vec().unwrap();
    let mut ingress = RowBatchWcsIngress::new();
    assert!(feed_chunks(&mut ingress, &wire).is_err());
}
