//! DuckDB DOUBLE total order. One encoding for keys, hashes, and range predicates.
//!
//! `-0.0` and `0.0` share a key. Every NaN shares one key that sorts above `+inf`.
//! Comparing those keys as `u64` matches DuckDB's order, and `u64::min` of the keys is
//! DuckDB's `MIN` (NaN loses unless it is the only value) because the NaN key is the greatest.

/// Canonical quiet NaN. Every NaN maps here so equality and grouping agree.
pub const CANON_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

#[inline]
pub fn canonical_f64(v: f64) -> f64 {
    if v.is_nan() {
        f64::from_bits(CANON_NAN_BITS)
    } else if v == 0.0 {
        0.0
    } else {
        v
    }
}

/// Bits that order like DuckDB's DOUBLE and make equal values identical.
#[inline]
pub fn float_key(v: f64) -> u64 {
    let bits = canonical_f64(v).to_bits();
    if bits & 0x8000_0000_0000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000_0000_0000
    }
}

/// Inverse of [`float_key`]. The result is canonical (`+0.0`, one NaN).
#[inline]
pub fn key_to_f64(key: u64) -> f64 {
    let bits = if key & 0x8000_0000_0000_0000 != 0 {
        key & !0x8000_0000_0000_0000
    } else {
        !key
    };
    canonical_f64(f64::from_bits(bits))
}

/// Greatest [`float_key`]. `MIN` starts here so a NaN does not replace a real value.
#[inline]
pub fn nan_key() -> u64 {
    float_key(f64::NAN)
}

#[inline]
/// The float a text literal names (`'NaN'`, `'inf'`, `'-Infinity'`), as DuckDB reads them.
pub fn float_text_literal(text: &str) -> Option<f64> {
    match text.trim().to_ascii_lowercase().as_str() {
        "nan" => Some(f64::NAN),
        "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
        "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

pub fn column_is_float(physical: i32) -> bool {
    matches!(physical, 4 | 5)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_matches_duckdb_double() {
        let values = [
            f64::NEG_INFINITY,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f64::INFINITY,
            f64::NAN,
            f64::from_bits(0x7ff0_0000_0000_0001),
        ];
        let mut keys: Vec<u64> = values.into_iter().map(float_key).collect();
        let sorted = keys.clone();
        keys.sort_unstable();
        assert_eq!(keys, sorted);
        assert_eq!(float_key(-0.0), float_key(0.0));
        assert_eq!(
            float_key(f64::NAN),
            float_key(f64::from_bits(0x7ff0_0000_0000_0001))
        );
        assert!(float_key(f64::INFINITY) < float_key(f64::NAN));
        assert!(float_key(-1.0) < float_key(0.0));
        assert!(float_key(1.0) < float_key(f64::INFINITY));
        assert_eq!(key_to_f64(float_key(-2.5)), -2.5);
        assert_eq!(key_to_f64(float_key(-0.0)).to_bits(), 0.0f64.to_bits());
        assert!(key_to_f64(float_key(f64::NAN)).is_nan());
    }

    #[test]
    fn min_skips_nan_and_max_keeps_it() {
        let nan = nan_key();
        let one = float_key(1.0);
        let neg = float_key(-1.0);
        assert_eq!(nan.min(one).min(neg), neg);
        assert_eq!(one.max(nan).max(neg), nan);
        assert_eq!(nan.min(nan), nan);
    }
}
