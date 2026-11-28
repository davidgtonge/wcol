//! Hash exchange outputs over WCS1 `DATA_BATCH` streams (2×2 topology in-process).

use wcol::ops::exchange::{
    split_owned_batch, BatchPartitionPlan, HashPartitionPlan, RowBatchWcsEgress,
    RowBatchWcsIngress, WCOL_EXCHANGE_HASH_VERSION_DEFAULT,
};
use wcol::stream::OwnedWcolBatch;

fn partition_rows(plan: &HashPartitionPlan, input: &OwnedWcolBatch) -> Vec<OwnedWcolBatch> {
    split_owned_batch(&BatchPartitionPlan::Hash(plan.clone()), input).unwrap()
}

fn rows_over_wcs1(batch: &OwnedWcolBatch) -> u64 {
    if batch.row_count() == 0 {
        return 0;
    }
    consume_stream(&encode_partition_stream(std::slice::from_ref(batch)).unwrap()).unwrap()
}

fn encode_partition_stream(batches: &[OwnedWcolBatch]) -> Result<Vec<u8>, String> {
    let mut egress = RowBatchWcsEgress::new();
    for batch in batches {
        egress
            .push_batch(batch.clone())
            .map_err(|e| e.to_string())?;
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
    let mut input = OwnedWcolBatch::empty();
    input.push_i64_column(0, (0..100).collect(), vec![1; 100]);
    input.set_row_count(100).unwrap();

    let total: u64 = partition_rows(&plan, &input)
        .iter()
        .map(rows_over_wcs1)
        .sum();
    assert_eq!(total, 100);
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

    let parts_a = partition_rows(&plan, &make_batch(0, 50));
    let parts_b = partition_rows(&plan, &make_batch(50, 50));

    let mut total = 0u64;
    for p in 0..2 {
        let rows = rows_over_wcs1(&parts_a[p]) + rows_over_wcs1(&parts_b[p]);
        let expected = (parts_a[p].row_count() + parts_b[p].row_count()) as u64;
        assert_eq!(rows, expected);
        total += rows;
    }
    assert_eq!(total, 100);
}

#[test]
fn truncated_stream_rejected_without_end() {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1], vec![1]);
    batch.set_row_count(1).unwrap();
    let mut egress = RowBatchWcsEgress::new();
    egress.push_batch(batch.clone()).unwrap();
    let wire = egress.drain_to_vec().unwrap();
    let mut ingress = RowBatchWcsIngress::new();
    assert!(feed_chunks(&mut ingress, &wire).is_err());
}
