//! HyperLogLog implementation for approx_count_distinct.
//!
//! Uses xxHash (via xxhash-rust) for fast, high-quality hashing.
//! Default precision p=14 gives 16384 registers and ~0.8% error.

use crate::constants::HLL_DEFAULT_PRECISION;
use crate::query::mask::{is_valid, iter_mask, mask_is_full};
use crate::query::scan::for_each_value;
use crate::types::{Column, ColumnData, HllState};

/// Create a new HLL state with the given precision.
pub(crate) fn hll_new(p: u8) -> HllState {
    let m = 1usize << p;
    HllState {
        p,
        registers: vec![0u8; m],
    }
}

/// Create a new HLL state with default precision (p=14).
pub(crate) fn hll_new_default() -> HllState {
    hll_new(HLL_DEFAULT_PRECISION)
}

/// Insert a 64-bit hash value into the HLL.
#[inline]
pub(crate) fn hll_insert(state: &mut HllState, hash: u64) {
    let m = 1usize << state.p;
    // Use top p bits for register index
    let idx = (hash >> (64 - state.p)) as usize;
    // Use remaining bits to count leading zeros
    let w = hash << state.p;
    // rho = number of leading zeros + 1 (capped at 64 - p + 1)
    let rho = if w == 0 {
        (64 - state.p + 1) as u8
    } else {
        (w.leading_zeros() + 1) as u8
    };
    if rho > state.registers[idx % m] {
        state.registers[idx % m] = rho;
    }
}

/// Hash an i64 value and insert into HLL.
#[inline]
pub(crate) fn hll_insert_i64(state: &mut HllState, value: i64) {
    let hash = xxhash_rust::xxh3::xxh3_64(&value.to_le_bytes());
    hll_insert(state, hash);
}

/// Hash a u64 value and insert into HLL.
#[inline]
#[allow(dead_code)]
pub(crate) fn hll_insert_u64(state: &mut HllState, value: u64) {
    let hash = xxhash_rust::xxh3::xxh3_64(&value.to_le_bytes());
    hll_insert(state, hash);
}

/// Hash an i32 value and insert into HLL.
#[inline]
pub(crate) fn hll_insert_i32(state: &mut HllState, value: i32) {
    let hash = xxhash_rust::xxh3::xxh3_64(&value.to_le_bytes());
    hll_insert(state, hash);
}

/// Hash a u32 value and insert into HLL.
#[inline]
pub(crate) fn hll_insert_u32(state: &mut HllState, value: u32) {
    let hash = xxhash_rust::xxh3::xxh3_64(&value.to_le_bytes());
    hll_insert(state, hash);
}

/// Hash an f64 value and insert into HLL.
/// Skips NaN values, treats -0 as 0.
#[inline]
pub(crate) fn hll_insert_f64(state: &mut HllState, value: f64) {
    if value.is_nan() {
        return;
    }
    // Canonicalize: treat -0 as 0
    let canonical = if value == 0.0 { 0.0 } else { value };
    let hash = xxhash_rust::xxh3::xxh3_64(&canonical.to_bits().to_le_bytes());
    hll_insert(state, hash);
}

/// Hash bytes (for strings) and insert into HLL.
#[inline]
#[allow(dead_code)]
pub(crate) fn hll_insert_bytes(state: &mut HllState, bytes: &[u8]) {
    let hash = xxhash_rust::xxh3::xxh3_64(bytes);
    hll_insert(state, hash);
}

/// Merge another HLL state into this one (max of registers).
pub(crate) fn hll_merge(target: &mut HllState, other: &HllState) {
    if target.p != other.p {
        return; // Cannot merge different precisions
    }
    for (t, o) in target.registers.iter_mut().zip(other.registers.iter()) {
        if *o > *t {
            *t = *o;
        }
    }
}

/// Estimate the cardinality from HLL state.
/// Uses the standard HLL estimator with small/large range corrections.
pub(crate) fn hll_estimate(state: &HllState) -> f64 {
    let m = state.registers.len() as f64;
    let p = state.p;

    // Compute raw estimate: alpha_m * m^2 / sum(2^(-M[j]))
    let alpha_m = match p {
        4 => 0.673,
        5 => 0.697,
        6 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / m),
    };

    let mut sum = 0.0;
    let mut zeros = 0u32;
    for &reg in &state.registers {
        sum += 2.0_f64.powi(-(reg as i32));
        if reg == 0 {
            zeros += 1;
        }
    }

    let raw_estimate = alpha_m * m * m / sum;

    // Small range correction (linear counting)
    if raw_estimate <= 2.5 * m {
        if zeros > 0 {
            // Linear counting
            return m * (m / zeros as f64).ln();
        }
    }

    // Large range correction (for very high cardinalities)
    let two_pow_32 = (1u64 << 32) as f64;
    if raw_estimate > two_pow_32 / 30.0 {
        return -two_pow_32 * (1.0 - raw_estimate / two_pow_32).ln();
    }

    raw_estimate
}

/// Standard error estimate for HLL: ~1.04 / sqrt(m)
pub(crate) fn hll_error_estimate(state: &HllState) -> f64 {
    let m = state.registers.len() as f64;
    1.04 / m.sqrt()
}

#[inline]
fn hll_each<T, V, F, I>(
    values: &[T],
    mask: &[u32],
    rows: usize,
    state: &mut HllState,
    cast: F,
    insert: I,
)
where
    T: Copy,
    V: Copy,
    F: Fn(T) -> V,
    I: Fn(&mut HllState, V),
{
    for_each_value(values, mask, rows, |val| insert(state, cast(val)));
}

/// Aggregate a column into an HLL state, respecting the mask.
pub(crate) fn hll_aggregate_column(
    _col: &Column,
    data: &ColumnData,
    mask: &[u32],
    rows: usize,
    state: &mut HllState,
) {
    match data {
        ColumnData::U8(values) => {
            hll_each(values, mask, rows, state, |v: u8| v as u32, hll_insert_u32)
        }
        ColumnData::U16(values) => {
            hll_each(values, mask, rows, state, |v: u16| v as u32, hll_insert_u32)
        }
        ColumnData::U32(values) => hll_each(values, mask, rows, state, |v: u32| v, hll_insert_u32),
        ColumnData::I8(values) => {
            hll_each(values, mask, rows, state, |v: i8| v as i32, hll_insert_i32)
        }
        ColumnData::I16(values) => {
            hll_each(values, mask, rows, state, |v: i16| v as i32, hll_insert_i32)
        }
        ColumnData::I32(values) => hll_each(values, mask, rows, state, |v: i32| v, hll_insert_i32),
        ColumnData::I64(values) => {
            for_each_value(values, mask, rows, |val| hll_insert_i64(state, val));
        }
        ColumnData::F64(values) => {
            for_each_value(values, mask, rows, |val| hll_insert_f64(state, val));
        }
        ColumnData::Bool(values) => {
            let mut seen_true = false;
            let mut seen_false = false;
            if mask_is_full(mask, rows) {
                for row in 0..rows {
                    if is_valid(values, row) {
                        seen_true = true;
                    } else {
                        seen_false = true;
                    }
                    if seen_true && seen_false {
                        break;
                    }
                }
            } else {
                for row in iter_mask(mask, rows) {
                    if is_valid(values, row) {
                        seen_true = true;
                    } else {
                        seen_false = true;
                    }
                    if seen_true && seen_false {
                        break;
                    }
                }
            }
            if seen_true {
                hll_insert_u32(state, 1);
            }
            if seen_false {
                hll_insert_u32(state, 0);
            }
        }
    }
}

/// For dictionary-encoded columns: aggregate dict IDs directly.
/// This is fast because dict IDs are globally unique per file.
pub(crate) fn hll_aggregate_dict_ids(
    data: &ColumnData,
    mask: &[u32],
    rows: usize,
    state: &mut HllState,
) {
    match data {
        ColumnData::U8(values) => {
            hll_each(values, mask, rows, state, |v: u8| v as u32, hll_insert_u32)
        }
        ColumnData::U16(values) => {
            hll_each(values, mask, rows, state, |v: u16| v as u32, hll_insert_u32)
        }
        ColumnData::U32(values) => hll_each(values, mask, rows, state, |v: u32| v, hll_insert_u32),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_hll() {
        let state = hll_new_default();
        let estimate = hll_estimate(&state);
        assert!(estimate < 1.0, "empty HLL should estimate ~0");
    }

    #[test]
    fn test_single_value() {
        let mut state = hll_new_default();
        hll_insert_i64(&mut state, 42);
        let estimate = hll_estimate(&state);
        assert!(
            estimate >= 0.5 && estimate <= 2.0,
            "single value should estimate ~1, got {}",
            estimate
        );
    }

    #[test]
    fn test_same_value_repeated() {
        let mut state = hll_new_default();
        for _ in 0..1000 {
            hll_insert_i64(&mut state, 42);
        }
        let estimate = hll_estimate(&state);
        assert!(
            estimate >= 0.5 && estimate <= 2.0,
            "same value repeated should estimate ~1, got {}",
            estimate
        );
    }

    #[test]
    fn test_known_cardinality() {
        let mut state = hll_new_default();
        let n = 10000;
        for i in 0..n {
            hll_insert_i64(&mut state, i);
        }
        let estimate = hll_estimate(&state);
        let error = (estimate - n as f64).abs() / n as f64;
        assert!(
            error < 0.05,
            "10000 distinct values: estimate={}, error={:.2}%",
            estimate,
            error * 100.0
        );
    }

    #[test]
    fn test_merge() {
        let mut state1 = hll_new_default();
        let mut state2 = hll_new_default();

        for i in 0..5000 {
            hll_insert_i64(&mut state1, i);
        }
        for i in 5000..10000 {
            hll_insert_i64(&mut state2, i);
        }

        let estimate1 = hll_estimate(&state1);
        let estimate2 = hll_estimate(&state2);

        hll_merge(&mut state1, &state2);
        let merged_estimate = hll_estimate(&state1);

        // Merged should be close to 10000
        let error = (merged_estimate - 10000.0).abs() / 10000.0;
        assert!(
            error < 0.05,
            "merged estimate={}, error={:.2}%",
            merged_estimate,
            error * 100.0
        );

        // Each half should be close to 5000
        let error1 = (estimate1 - 5000.0).abs() / 5000.0;
        let error2 = (estimate2 - 5000.0).abs() / 5000.0;
        assert!(
            error1 < 0.05,
            "state1 estimate={}, error={:.2}%",
            estimate1,
            error1 * 100.0
        );
        assert!(
            error2 < 0.05,
            "state2 estimate={}, error={:.2}%",
            estimate2,
            error2 * 100.0
        );
    }

    #[test]
    fn test_nan_skipped() {
        let mut state = hll_new_default();
        hll_insert_f64(&mut state, f64::NAN);
        hll_insert_f64(&mut state, f64::NAN);
        let estimate = hll_estimate(&state);
        assert!(estimate < 1.0, "NaN values should be skipped");
    }

    #[test]
    fn test_negative_zero() {
        let mut state = hll_new_default();
        hll_insert_f64(&mut state, 0.0);
        hll_insert_f64(&mut state, -0.0);
        let estimate = hll_estimate(&state);
        assert!(
            estimate >= 0.5 && estimate <= 2.0,
            "-0 and 0 should be treated as same, got {}",
            estimate
        );
    }
}
