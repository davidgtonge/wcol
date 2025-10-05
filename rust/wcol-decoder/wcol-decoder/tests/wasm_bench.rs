#![cfg(feature = "bench")]

use std::hint::black_box;
use wasm_bindgen_test::{wasm_bindgen_bench, Criterion};
use wcol_decoder::bench::{
    build_group_key, iter_mask_count, mask_and, mask_count, mask_is_zero, mask_not, mask_or,
    mask_words, op_between, op_eq, op_gt, op_lt, rows_per_chunk, BenchColumn,
};

fn lcg_u32(seed: &mut u32) -> u32 {
    *seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
    *seed
}

fn make_u32_values(rows: usize) -> Vec<u32> {
    let mut out = Vec::with_capacity(rows);
    let mut seed = 1u32;
    for _ in 0..rows {
        out.push(lcg_u32(&mut seed));
    }
    out
}

fn make_i32_values(rows: usize) -> Vec<i32> {
    let mut out = Vec::with_capacity(rows);
    let mut seed = 7u32;
    for _ in 0..rows {
        out.push(lcg_u32(&mut seed) as i32);
    }
    out
}

fn make_f64_values(rows: usize) -> Vec<f64> {
    let mut out = Vec::with_capacity(rows);
    let mut seed = 19u32;
    for _ in 0..rows {
        let value = (lcg_u32(&mut seed) % 1000) as f64;
        out.push(value * 0.25);
    }
    out
}

fn set_bit(mask: &mut [u32], idx: usize) {
    let word = idx >> 5;
    let bit = idx & 31;
    mask[word] |= 1 << bit;
}

fn make_sparse_mask(rows: usize, step: usize) -> Vec<u32> {
    let mut mask = vec![0u32; mask_words()];
    let mut i = 0;
    while i < rows {
        set_bit(&mut mask, i);
        i += step;
    }
    mask
}

fn make_full_mask(rows: usize) -> Vec<u32> {
    let mut mask = vec![0u32; mask_words()];
    let full_words = rows / 32;
    let tail_bits = rows % 32;
    for word in mask.iter_mut().take(full_words) {
        *word = u32::MAX;
    }
    if tail_bits > 0 {
        mask[full_words] = (1u32 << tail_bits) - 1;
    }
    mask
}

#[wasm_bindgen_bench]
fn bench_mask_ops(c: &mut Criterion) {
    let rows = rows_per_chunk();
    let mask_a = make_sparse_mask(rows, 3);
    let mask_b = make_sparse_mask(rows, 5);
    let zero_mask = vec![0u32; mask_words()];

    c.bench_function("mask_and", |b| {
        b.iter(|| black_box(mask_and(black_box(&mask_a), black_box(&mask_b))))
    });
    c.bench_function("mask_or", |b| {
        b.iter(|| black_box(mask_or(black_box(&mask_a), black_box(&mask_b))))
    });
    c.bench_function("mask_not", |b| {
        b.iter(|| black_box(mask_not(black_box(&mask_a))))
    });
    c.bench_function("mask_count", |b| {
        b.iter(|| black_box(mask_count(black_box(&mask_a))))
    });
    c.bench_function("mask_is_zero/zero", |b| {
        b.iter(|| black_box(mask_is_zero(black_box(&zero_mask))))
    });
    c.bench_function("mask_is_zero/sparse", |b| {
        b.iter(|| black_box(mask_is_zero(black_box(&mask_a))))
    });
    c.bench_function("iter_mask_count/sparse", |b| {
        b.iter(|| black_box(iter_mask_count(black_box(&mask_a), rows)))
    });
}

#[wasm_bindgen_bench]
fn bench_filter_masks(c: &mut Criterion) {
    let rows = rows_per_chunk();
    let col_i32 = BenchColumn::i32(make_i32_values(rows), 0);
    let col_u32 = BenchColumn::u32(make_u32_values(rows), 0);

    c.bench_function("filter_i32_eq", |b| {
        b.iter(|| black_box(col_i32.build_mask(op_eq(), 123.0, 123.0, rows)))
    });
    c.bench_function("filter_i32_lt", |b| {
        b.iter(|| black_box(col_i32.build_mask(op_lt(), 400.0, 0.0, rows)))
    });
    c.bench_function("filter_i32_between", |b| {
        b.iter(|| black_box(col_i32.build_mask(op_between(), 200.0, 800.0, rows)))
    });
    c.bench_function("filter_u32_eq", |b| {
        b.iter(|| black_box(col_u32.build_mask(op_eq(), 123.0, 123.0, rows)))
    });
    c.bench_function("filter_u32_gt", |b| {
        b.iter(|| black_box(col_u32.build_mask(op_gt(), 900.0, 0.0, rows)))
    });
    c.bench_function("filter_u32_between", |b| {
        b.iter(|| black_box(col_u32.build_mask(op_between(), 100.0, 900.0, rows)))
    });
}

#[wasm_bindgen_bench]
fn bench_agg(c: &mut Criterion) {
    let rows = rows_per_chunk();
    let col_f64 = BenchColumn::f64(make_f64_values(rows), 0);
    let full_mask = make_full_mask(rows);
    let sparse_mask = make_sparse_mask(rows, 7);

    c.bench_function("aggregate_f64_full_mask", |b| {
        b.iter(|| black_box(col_f64.aggregate_sum(&full_mask, rows)))
    });
    c.bench_function("aggregate_f64_sparse_mask", |b| {
        b.iter(|| black_box(col_f64.aggregate_sum(&sparse_mask, rows)))
    });
}

#[wasm_bindgen_bench]
fn bench_group(c: &mut Criterion) {
    let rows = rows_per_chunk();
    let col_a = BenchColumn::u32(make_u32_values(rows), 0);
    let col_b = BenchColumn::u32(make_u32_values(rows), 0);

    c.bench_function("group_key_single", |b| {
        b.iter(|| {
            let mut acc = 0u64;
            for row in 0..rows {
                acc ^= build_group_key(&col_a, None, row);
            }
            black_box(acc);
        })
    });
    c.bench_function("group_key_pair", |b| {
        b.iter(|| {
            let mut acc = 0u64;
            for row in 0..rows {
                acc ^= build_group_key(&col_a, Some(&col_b), row);
            }
            black_box(acc);
        })
    });
}
