//! Merge bounded composite-key samples from scan nodes into range split points.

use crate::plan::physical_plan::ParquetMetadataCatalog;
use crate::plan::range_sort::SampledSortBoundaries;
use crate::program::types::{SortKey, SortTarget};
use wcol_engine::ir::Scalar;

/// Reservoir sample size per scan fragment.
pub const DEFAULT_SAMPLE_SIZE: usize = 8192;

/// Plan range boundaries: footer-derived quantile samples, optional node samples, then
/// [`boundaries_from_samples`].
pub fn boundaries_for_range_exchange(
    keys: &[SortKey],
    partitions: u32,
    metadata: &ParquetMetadataCatalog,
    node_samples: Option<Vec<Vec<Vec<Scalar>>>>,
) -> Result<SampledSortBoundaries, String> {
    let mut batches = vec![footer_composite_samples(
        keys,
        metadata,
        DEFAULT_SAMPLE_SIZE,
    )?];
    if let Some(nodes) = node_samples {
        batches.extend(nodes);
    }
    boundaries_from_samples(keys.to_vec(), partitions, batches)
}

/// Footer-only planning (no runtime node samples yet).
pub fn boundaries_from_catalog(
    keys: &[SortKey],
    partitions: u32,
    metadata: &ParquetMetadataCatalog,
) -> Result<SampledSortBoundaries, String> {
    boundaries_for_range_exchange(keys, partitions, metadata, None)
}

/// Merge sample batches from one or more scan nodes and derive interior split keys.
pub fn boundaries_from_samples(
    keys: Vec<SortKey>,
    partitions: u32,
    node_samples: Vec<Vec<Vec<Scalar>>>,
) -> Result<SampledSortBoundaries, String> {
    if partitions < 2 {
        return Err("range sort requires at least two partitions".into());
    }
    let mut merged: Vec<Vec<Scalar>> = node_samples.into_iter().flatten().collect();
    if merged.is_empty() {
        return Err("sort-key sample is empty".into());
    }
    merged.sort_by(|left, right| compare_composite(keys.as_slice(), left, right));
    let splits_needed = (partitions - 1) as usize;
    let mut splits = Vec::with_capacity(splits_needed);
    for index in 1..=splits_needed {
        let rank = index * merged.len() / (partitions as usize);
        let pick = rank.min(merged.len().saturating_sub(1));
        splits.push(merged[pick].clone());
    }
    Ok(SampledSortBoundaries { keys, splits })
}

/// Approximate sort-key rows from footer min/max statistics (int, float, text).
fn footer_composite_samples(
    keys: &[SortKey],
    metadata: &ParquetMetadataCatalog,
    sample_size: usize,
) -> Result<Vec<Vec<Scalar>>, String> {
    if sample_size == 0 {
        return Err("sort-key sample size must be > 0".into());
    }
    let mut rows = Vec::with_capacity(sample_size);
    for rank in 0..sample_size {
        let t = (rank + 1) as f64 / (sample_size as f64 + 1.0);
        let mut composite = Vec::with_capacity(keys.len());
        for key in keys {
            composite.push(scalar_at_quantile(key, t, metadata)?);
        }
        rows.push(composite);
    }
    Ok(rows)
}

fn scalar_at_quantile(
    key: &SortKey,
    t: f64,
    metadata: &ParquetMetadataCatalog,
) -> Result<Scalar, String> {
    let SortTarget::Column(column) = key.target else {
        return Err("range sort keys must be scan columns".into());
    };
    let column = column as usize;
    let physical = metadata.leaf_physical.get(column).copied().unwrap_or(-1);

    if let (Some(min), Some(max)) = (
        metadata.leaf_min_i64.get(column).and_then(|v| *v),
        metadata.leaf_max_i64.get(column).and_then(|v| *v),
    ) {
        if max >= min {
            let span = max - min;
            let value = min.saturating_add((span as f64 * t).floor() as i64);
            return Ok(Scalar::Int(value));
        }
    }

    if crate::ops::query::physical_type::is_float(physical) {
        if let (Some(min_b), Some(max_b)) = (
            metadata.leaf_min_bytes.get(column).and_then(|v| v.as_ref()),
            metadata.leaf_max_bytes.get(column).and_then(|v| v.as_ref()),
        ) {
            let min_f = decode_float(physical, min_b)?;
            let max_f = decode_float(physical, max_b)?;
            if max_f >= min_f {
                return Ok(Scalar::Number(min_f + (max_f - min_f) * t));
            }
        }
    }

    if crate::ops::query::physical_type::is_bytes(physical) {
        if let (Some(min_b), Some(max_b)) = (
            metadata.leaf_min_bytes.get(column).and_then(|v| v.as_ref()),
            metadata.leaf_max_bytes.get(column).and_then(|v| v.as_ref()),
        ) {
            return Ok(Scalar::Text(interpolate_bytes(min_b, max_b, t)));
        }
        return Ok(Scalar::Text(text_quantile_from_ndv(column, t, metadata)));
    }

    Err(format!(
        "footer has no usable statistics for sort column {column} (physical={physical})"
    ))
}

fn decode_float(physical: i32, bytes: &[u8]) -> Result<f64, String> {
    match physical {
        4 if bytes.len() >= 4 => {
            Ok(f32::from_le_bytes(bytes[..4].try_into().map_err(|_| "float stat length")?) as f64)
        }
        5 if bytes.len() >= 8 => Ok(f64::from_le_bytes(
            bytes[..8].try_into().map_err(|_| "double stat length")?,
        )),
        _ => Err("float column stat has unexpected encoding".into()),
    }
}

fn interpolate_bytes(min: &[u8], max: &[u8], t: f64) -> String {
    if min == max {
        return String::from_utf8_lossy(min).into_owned();
    }
    let len = min.len().max(max.len());
    let mut out = Vec::with_capacity(len);
    for index in 0..len {
        let a = *min.get(index).unwrap_or(&0);
        let b = *max.get(index).unwrap_or(&0xff);
        let value = a as f64 + (b as f64 - a as f64) * t;
        out.push(value.round().clamp(0.0, 255.0) as u8);
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Deterministic lexicographic placeholders when the footer has no byte min/max.
fn text_quantile_from_ndv(column: usize, t: f64, metadata: &ParquetMetadataCatalog) -> String {
    let ndv = metadata
        .leaf_distinct_upper
        .get(column)
        .and_then(|v| *v)
        .unwrap_or(256)
        .max(2);
    let rank = ((t * ndv as f64).floor() as u64).min(ndv - 1);
    format!("wcol/q/{rank:016x}")
}

fn compare_composite(keys: &[SortKey], left: &[Scalar], right: &[Scalar]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (index, key) in keys.iter().enumerate() {
        let ordering = scalar_key_cmp(key, left.get(index), right.get(index));
        if ordering != Ordering::Equal {
            return if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    Ordering::Equal
}

fn scalar_key_cmp(
    key: &SortKey,
    left: Option<&Scalar>,
    right: Option<&Scalar>,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if key.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if key.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => scalar_cmp(left, right),
    }
}

fn scalar_cmp(left: &Scalar, right: &Scalar) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    use std::cmp::PartialOrd;
    match (left, right) {
        (Scalar::Int(a), Scalar::Int(b)) => a.cmp(b),
        (Scalar::Number(a), Scalar::Number(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Scalar::Text(a), Scalar::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program::types::SortTarget;

    fn catalog_int(min: i64, max: i64) -> ParquetMetadataCatalog {
        ParquetMetadataCatalog {
            tables: Vec::new(),
            leaf_physical: vec![2],
            leaf_distinct_upper: vec![None],
            leaf_dictionary_bytes: Vec::new(),
            leaf_min_i64: vec![Some(min)],
            leaf_max_i64: vec![Some(max)],
            leaf_min_bytes: vec![None],
            leaf_max_bytes: vec![None],
        }
    }

    #[test]
    fn approximate_splits_follow_sort_order() {
        let keys = vec![SortKey {
            target: SortTarget::Column(0),
            descending: false,
            nulls_first: false,
        }];
        let samples = vec![vec![
            vec![Scalar::Int(10)],
            vec![Scalar::Int(30)],
            vec![Scalar::Int(20)],
        ]];
        let boundaries = boundaries_from_samples(keys, 2, samples).unwrap();
        assert_eq!(boundaries.splits.len(), 1);
        assert_eq!(boundaries.splits[0][0], Scalar::Int(20));
    }

    #[test]
    fn footer_int_samples_feed_boundaries_from_samples() {
        let keys = vec![SortKey {
            target: SortTarget::Column(0),
            descending: false,
            nulls_first: false,
        }];
        let boundaries = boundaries_from_catalog(&keys, 3, &catalog_int(0, 100)).unwrap();
        assert_eq!(boundaries.splits.len(), 2);
        assert_eq!(boundaries.splits[0][0], Scalar::Int(33));
        assert_eq!(boundaries.splits[1][0], Scalar::Int(66));
    }

    #[test]
    fn text_bounds_use_lexicographic_footer_bytes() {
        let keys = vec![SortKey {
            target: SortTarget::Column(0),
            descending: false,
            nulls_first: false,
        }];
        let metadata = ParquetMetadataCatalog {
            tables: Vec::new(),
            leaf_physical: vec![6],
            leaf_distinct_upper: vec![Some(100)],
            leaf_dictionary_bytes: Vec::new(),
            leaf_min_i64: vec![None],
            leaf_max_i64: vec![None],
            leaf_min_bytes: vec![Some(b"a".to_vec())],
            leaf_max_bytes: vec![Some(b"z".to_vec())],
        };
        let samples = footer_composite_samples(&keys, &metadata, 3).unwrap();
        assert_eq!(samples.len(), 3);
        assert!(match (&samples[0][0], &samples[2][0]) {
            (Scalar::Text(a), Scalar::Text(b)) => a.as_bytes() < b.as_bytes(),
            _ => false,
        });
        let boundaries = boundaries_from_samples(keys, 2, vec![samples]).unwrap();
        assert_eq!(boundaries.splits.len(), 1);
    }
}
