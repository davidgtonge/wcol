//! Float and double kernels against a row-wise oracle. NaN equals NaN and sorts above +inf.
//! -0.0 and 0.0 are one value. NULL stays out of both.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::ir::{ColumnKind, SortKey};
use crate::ops::query::float_key::{canonical_f64, float_key};
use crate::ops::query::{
    finalize_groups, merge_partial_snapshots, CatalogColumn, PartialKeyPart, PartialSnapshot,
};
use crate::ops::sort::sort_row_batches;
use crate::stream::partial_wcs::{partial_state_from_wire_payload, partial_state_to_wcs_payload};
use crate::stream::{DictEntries, DictionaryValues, OwnedWcolBatch, SelectionMask, WcolColumn};

fn catalog() -> Vec<CatalogColumn> {
    let mut cols = Vec::new();
    for (name, physical) in [("a", 2), ("b", 2), ("f", 4), ("g", 5)] {
        cols.push(CatalogColumn {
            name: name.into(),
            kind: ColumnKind::Number,
            physical,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        });
    }
    cols
}

fn f_batch(values: &[f64], nulls: &[u8]) -> OwnedWcolBatch {
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1; values.len()], vec![1; values.len()]);
    batch.push_i64_column(1, vec![0; values.len()], vec![1; values.len()]);
    batch.push_f64_column(2, values.to_vec(), nulls.to_vec());
    batch.push_f64_column(3, values.to_vec(), nulls.to_vec());
    batch.set_row_count(values.len()).unwrap();
    batch
}

fn dict_batch(entries: &[f64], ids: &[u32], nulls: &[u8]) -> OwnedWcolBatch {
    let entries: Arc<Vec<f64>> = Arc::new(entries.to_vec());
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, vec![1; ids.len()], vec![1; ids.len()]);
    batch.push_i64_column(1, vec![2; ids.len()], vec![1; ids.len()]);
    batch.push_dict_id_column(
        2,
        ids.to_vec(),
        nulls.to_vec(),
        DictionaryValues {
            scope: 0,
            column: 2,
            entries: DictEntries::Float(Arc::clone(&entries)),
        },
    );
    batch.push_dict_id_column(
        3,
        ids.to_vec(),
        nulls.to_vec(),
        DictionaryValues {
            scope: 0,
            column: 3,
            entries: DictEntries::Float(entries),
        },
    );
    batch.set_row_count(ids.len()).unwrap();
    batch
}

fn snapshot(sql: &str, batches: &[OwnedWcolBatch]) -> PartialSnapshot {
    let catalog = catalog();
    let mut filter = crate::plan::bind::open_batch_filter(sql, &catalog)
        .expect(sql)
        .expect(sql);
    let mut query = crate::plan::bind::open_batch_query(sql, &catalog).unwrap();
    for batch in batches {
        let mut view = Vec::new();
        let view = batch.view_with(&mut view);
        let selection = filter.selection(&view).unwrap();
        query.consume(&view, &selection).unwrap();
    }
    query.finish_snapshot()
}

fn same_f64(left: f64, right: f64) {
    if left.is_nan() {
        assert!(right.is_nan());
    } else {
        assert_eq!(left.to_bits(), right.to_bits(), "{left} vs {right}");
    }
}

fn stat_f64(stat: &crate::ops::query::PartialStat) -> (u64, f64, f64, f64) {
    assert_eq!(stat.flags & 1, 0, "float stat was marked integer");
    (stat.count, stat.sum_f, stat.min_f, stat.max_f)
}

struct Oracle {
    count: u64,
    sum: f64,
    min_key: u64,
    max_key: u64,
    seen: bool,
}

impl Oracle {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min_key: 0,
            max_key: 0,
            seen: false,
        }
    }

    fn add(&mut self, v: f64) {
        let key = float_key(v);
        if !self.seen {
            self.min_key = key;
            self.max_key = key;
            self.seen = true;
        } else {
            self.min_key = self.min_key.min(key);
            self.max_key = self.max_key.max(key);
        }
        self.count += 1;
        self.sum += v;
    }
}

fn values_of(batch: &OwnedWcolBatch, column: u32) -> Vec<Option<f64>> {
    let mut buf = Vec::new();
    let view = batch.view_with(&mut buf);
    let col = view.columns.iter().find(|col| match col {
        WcolColumn::F64 { column: id, .. } | WcolColumn::DictId { column: id, .. } => *id == column,
        _ => false,
    });
    let mut out = Vec::with_capacity(view.row_count);
    for row in 0..view.row_count {
        let value = match col.expect("float column") {
            WcolColumn::F64 { values, nulls, .. } => (nulls[row] != 0).then_some(values[row]),
            WcolColumn::DictId {
                ids,
                nulls,
                dictionary,
                ..
            } => (nulls[row] != 0).then(|| dictionary.float_entries().unwrap()[ids[row] as usize]),
            _ => None,
        };
        out.push(value);
    }
    out
}

#[test]
fn filters_match_a_row_oracle() {
    let samples = [
        f64::NEG_INFINITY,
        -1.0,
        -0.0,
        0.0,
        1.0,
        1.5,
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        1e308,
        f64::INFINITY,
        f64::NAN,
        f64::from_bits(0x7ff0_0000_0000_0001),
    ];
    let mut values = Vec::new();
    let mut nulls = Vec::new();
    for v in samples {
        values.push(v);
        nulls.push(1);
        values.push(v);
        nulls.push(0);
    }
    let batch = f_batch(&values, &nulls);
    let rows = values_of(&batch, 3);
    let cases: &[(&str, fn(f64) -> bool)] = &[
        ("SELECT COUNT(*) FROM t WHERE g > 1", |v| {
            float_key(v) > float_key(1.0)
        }),
        ("SELECT COUNT(*) FROM t WHERE g >= 1.5", |v| {
            float_key(v) >= float_key(1.5)
        }),
        ("SELECT COUNT(*) FROM t WHERE g < 0", |v| {
            float_key(v) < float_key(0.0)
        }),
        ("SELECT COUNT(*) FROM t WHERE g <= -0.0", |v| {
            float_key(v) <= float_key(-0.0)
        }),
        ("SELECT COUNT(*) FROM t WHERE g = 0", |v| v == 0.0),
        ("SELECT COUNT(*) FROM t WHERE g <> 1", |v| v != 1.0),
        ("SELECT COUNT(*) FROM t WHERE g = 'NaN'", |v| v.is_nan()),
        ("SELECT COUNT(*) FROM t WHERE g > 'inf'", |v| v.is_nan()),
        ("SELECT COUNT(*) FROM t WHERE g < 'NaN'", |v| !v.is_nan()),
        ("SELECT COUNT(*) FROM t WHERE g > 1e308", |v| {
            float_key(v) > float_key(1e308)
        }),
        ("SELECT COUNT(*) FROM t WHERE g IN (1, 1.5, 0)", |v| {
            v == 1.0 || v == 1.5 || v == 0.0
        }),
        ("SELECT COUNT(*) FROM t WHERE g NOT IN (1, 'NaN')", |v| {
            v != 1.0 && !v.is_nan()
        }),
        ("SELECT COUNT(*) FROM t WHERE f = -1", |v| v == -1.0),
    ];
    for (sql, pred) in cases {
        let snap = snapshot(sql, &[batch.clone()]);
        let got = snap.groups.first().map(|g| g.stats[0].count).unwrap_or(0);
        let expect = rows.iter().filter(|v| v.is_some_and(pred)).count() as u64;
        assert_eq!(got, expect, "{sql}");
    }
}

#[test]
fn aggregates_match_a_row_oracle_including_shards() {
    let values = [
        1.0,
        -0.0,
        0.0,
        f64::NAN,
        2.5,
        f64::INFINITY,
        f64::NEG_INFINITY,
        4.0,
        f64::NAN,
    ];
    let nulls = [1u8, 1, 1, 1, 0, 1, 1, 1, 1];
    let whole = f_batch(&values, &nulls);
    let left = f_batch(&values[..4], &nulls[..4]);
    let right = f_batch(&values[4..], &nulls[4..]);
    let sql = "SELECT SUM(g), MIN(g), MAX(g), AVG(g), COUNT(g) FROM t";
    let mut oracle = Oracle::new();
    for (v, present) in values.iter().zip(nulls) {
        if present != 0 {
            oracle.add(*v);
        }
    }
    let check = |snap: &PartialSnapshot| {
        let stat = &snap.groups[0].stats;
        let (count, sum, min, max) = stat_f64(&stat[0]);
        assert_eq!(count, oracle.count);
        same_f64(sum, oracle.sum);
        same_f64(
            min,
            canonical_f64(crate::ops::query::float_key::key_to_f64(oracle.min_key)),
        );
        // SUM's min/max are the same accumulator.
        let _ = (min, max);
        let (_, _, min, _) = stat_f64(&stat[1]);
        let (_, _, _, max) = stat_f64(&stat[2]);
        same_f64(
            min,
            canonical_f64(crate::ops::query::float_key::key_to_f64(oracle.min_key)),
        );
        same_f64(
            max,
            canonical_f64(crate::ops::query::float_key::key_to_f64(oracle.max_key)),
        );
        let (count, sum, _, _) = stat_f64(&stat[3]);
        same_f64(sum / count as f64, oracle.sum / oracle.count as f64);
        assert_eq!(stat[4].count, oracle.count);
    };
    check(&snapshot(sql, &[whole.clone()]));
    let merged =
        merge_partial_snapshots(&snapshot(sql, &[left]), &snapshot(sql, &[right])).unwrap();
    let (count, sum, min, max) = stat_f64(&merged.groups[0].stats[0]);
    assert_eq!(count, oracle.count);
    if sum.is_nan() || oracle.sum.is_nan() {
        same_f64(sum, oracle.sum);
    } else {
        let denom = oracle.sum.abs().max(1.0);
        assert!(
            (sum - oracle.sum).abs() / denom < 1e-9,
            "{sum} vs {}",
            oracle.sum
        );
    }
    same_f64(
        min,
        canonical_f64(crate::ops::query::float_key::key_to_f64(oracle.min_key)),
    );
    same_f64(
        max,
        canonical_f64(crate::ops::query::float_key::key_to_f64(oracle.max_key)),
    );
    assert_eq!(merged.groups[0].stats[4].count, oracle.count);
}

#[test]
fn group_by_collapses_nan_and_signed_zero_across_shards() {
    let values = [
        0.0,
        -0.0,
        f64::NAN,
        f64::from_bits(0x7ff8_0000_0000_0001),
        1.0,
        f64::NAN,
    ];
    let nulls = [1u8, 1, 1, 1, 1, 0];
    let left = f_batch(&values[..3], &nulls[..3]);
    let right = f_batch(&values[3..], &nulls[3..]);
    let sql = "SELECT g, COUNT(*) FROM t GROUP BY g";
    let merged = merge_partial_snapshots(
        &snapshot(sql, &[left.clone()]),
        &snapshot(sql, &[right.clone()]),
    )
    .unwrap();
    let whole = snapshot(sql, &[f_batch(&values, &nulls)]);
    let key = |part: &PartialKeyPart| match part {
        PartialKeyPart::Null => None,
        PartialKeyPart::Float(bits) => Some(canonical_f64(f64::from_bits(*bits)).to_bits()),
        other => panic!("unexpected key {other:?}"),
    };
    let mut expect: BTreeMap<Option<u64>, u64> = BTreeMap::new();
    for (v, present) in values.iter().zip(nulls) {
        if present == 0 {
            *expect.entry(None).or_default() += 1;
        } else {
            *expect.entry(Some(canonical_f64(*v).to_bits())).or_default() += 1;
        }
    }
    assert_eq!(expect.len(), 4);
    for snap in [&merged, &whole] {
        assert_eq!(snap.groups.len(), expect.len());
        for group in &snap.groups {
            assert_eq!(group.stats[0].count, expect[&key(&group.key[0])]);
        }
    }
    let state = {
        let catalog = catalog();
        let mut query = crate::plan::bind::open_batch_query(sql, &catalog).unwrap();
        for batch in [left, right] {
            let mut view = Vec::new();
            let view = batch.view_with(&mut view);
            query
                .consume(&view, &SelectionMask::all(view.row_count))
                .unwrap();
        }
        query.finish_partial_state()
    };
    let wire = partial_state_to_wcs_payload(&state).unwrap();
    assert_eq!(partial_state_from_wire_payload(&wire).unwrap(), state);
}

#[test]
fn count_distinct_and_approx_share_the_float_key() {
    let values = [1.0, 1.0, -0.0, 0.0, f64::NAN, f64::NAN, f64::INFINITY];
    let nulls = [1u8, 1, 1, 1, 1, 1, 0];
    let batch = f_batch(&values, &nulls);
    let distinct = finalized("SELECT COUNT(DISTINCT g) FROM t", &[batch.clone()]);
    let mut set = BTreeSet::new();
    for (v, present) in values.iter().zip(nulls) {
        if present != 0 {
            set.insert(float_key(*v));
        }
    }
    assert_eq!(distinct[0].stats[0].count, set.len() as u64);
    let approx = finalized("SELECT approx_count_distinct(g) FROM t", &[batch]);
    let estimate = approx[0].stats[0].count;
    assert!((1..=8).contains(&estimate), "{estimate}");
}

fn finalized(sql: &str, batches: &[OwnedWcolBatch]) -> Vec<crate::ops::query::PartialGroup> {
    finalize_groups(&snapshot(sql, batches)).unwrap()
}

#[test]
fn dictionary_floats_filter_and_aggregate_like_plain_values() {
    let entries = [1.5, -0.0, f64::NAN, 4.0];
    let ids = [0u32, 1, 1, 2, 2, 3];
    let nulls = [1u8, 1, 0, 1, 1, 1];
    let batch = dict_batch(&entries, &ids, &nulls);
    let rows = values_of(&batch, 2);
    let snap = snapshot(
        "SELECT SUM(f), MIN(f), MAX(f), COUNT(*) FROM t WHERE f <> 4",
        &[batch],
    );
    let mut oracle = Oracle::new();
    for value in &rows {
        if let Some(v) = value {
            if *v != 4.0 {
                oracle.add(*v);
            }
        }
    }
    assert_eq!(snap.passed_rows, oracle.count);
    let (count, sum, min, max) = stat_f64(&snap.groups[0].stats[0]);
    assert_eq!(count, oracle.count);
    same_f64(sum, oracle.sum);
    same_f64(
        min,
        canonical_f64(crate::ops::query::float_key::key_to_f64(oracle.min_key)),
    );
    same_f64(
        max,
        canonical_f64(crate::ops::query::float_key::key_to_f64(oracle.max_key)),
    );
}

#[test]
fn order_by_float_puts_nulls_last_and_nan_after_infinity() {
    let values = [1.0, f64::NAN, -0.0, f64::INFINITY, f64::NEG_INFINITY, 0.0];
    let nulls = [1u8, 1, 1, 1, 0, 1];
    let batch = f_batch(&values, &nulls);
    let sorted = sort_row_batches(std::slice::from_ref(&batch), &[asc(3)]).unwrap();
    let got = values_of(&sorted, 3);
    let mut expect: Vec<Option<f64>> = values
        .iter()
        .zip(nulls)
        .map(|(v, present)| (present != 0).then_some(*v))
        .collect();
    expect.sort_by(|a, b| match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(a), Some(b)) => float_key(*a).cmp(&float_key(*b)),
    });
    assert_eq!(got.len(), expect.len());
    for (g, e) in got.iter().zip(&expect) {
        match (g, e) {
            (None, None) => {}
            (Some(g), Some(e)) => same_f64(*g, *e),
            _ => panic!("{got:?} vs {expect:?}"),
        }
    }
}

#[test]
fn the_queries_from_the_old_float_guard_run() {
    use crate::plan::{columns_for_sql, output_columns_for_rows};
    let catalog = catalog();
    for sql in [
        "SELECT SUM(f) FROM t",
        "SELECT MIN(g), COUNT(*) FROM t",
        "SELECT COUNT(*) FROM t WHERE f > 1",
        "SELECT g, COUNT(*) FROM t GROUP BY g",
        "SELECT COUNT(DISTINCT f) FROM t",
    ] {
        assert!(
            crate::plan::bind::open_batch_query(sql, &catalog).is_ok(),
            "{sql}"
        );
        assert!(columns_for_sql(sql, &catalog).is_ok(), "{sql}");
    }
    assert!(
        crate::plan::bind::open_batch_filter("SELECT a FROM t WHERE g < 0", &catalog)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        output_columns_for_rows("SELECT a, f FROM t", &catalog).unwrap(),
        vec![0, 2]
    );
    assert_eq!(
        output_columns_for_rows("SELECT f FROM t ORDER BY f", &catalog).unwrap(),
        vec![2]
    );
    assert_eq!(
        output_columns_for_rows("SELECT a FROM t ORDER BY a", &catalog).unwrap(),
        vec![0]
    );
    assert!(
        crate::plan::bind::open_batch_query("SELECT SUM(a) FROM t WHERE b > 1", &catalog).is_ok()
    );
}

fn asc(column: u32) -> SortKey {
    SortKey {
        column,
        descending: false,
        nulls_first: false,
    }
}

fn desc(column: u32) -> SortKey {
    SortKey {
        column,
        descending: true,
        nulls_first: false,
    }
}

/// Row ids (column 0) in the order DuckDB returns them: nulls last for every key, NaN above
/// +inf, -0.0 tied with 0.0, ties broken by input order.
fn oracle_order(columns: &[Vec<Option<f64>>], keys: &[(usize, bool)]) -> Vec<i64> {
    let mut rows: Vec<usize> = (0..columns[0].len()).collect();
    rows.sort_by(|&a, &b| {
        for &(slot, desc) in keys {
            let ord = match (columns[slot][a], columns[slot][b]) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(_), None) => std::cmp::Ordering::Less,
                (Some(x), Some(y)) if desc => float_key(y).cmp(&float_key(x)),
                (Some(x), Some(y)) => float_key(x).cmp(&float_key(y)),
            };
            if ord.is_ne() {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    rows.into_iter().map(|row| row as i64).collect()
}

fn row_ids(batch: &OwnedWcolBatch) -> Vec<i64> {
    let mut buf = Vec::new();
    let view = batch.view_with(&mut buf);
    match view
        .columns
        .iter()
        .find(|col| matches!(col, WcolColumn::I64 { column: 0, .. }))
    {
        Some(WcolColumn::I64 { values, .. }) => values.to_vec(),
        _ => panic!("row id column missing"),
    }
}

#[test]
fn order_by_float_honours_each_key_direction() {
    // Column 2 has many ties so the second key decides; column 3 has every special value.
    let specials = [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        0.0,
        -0.0,
        1.5,
        -2.25,
    ];
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let rows = 400;
    let mut c2 = Vec::new();
    let mut c3 = Vec::new();
    for _ in 0..rows {
        let r = next();
        c2.push((r % 9 != 0).then(|| [1.0, -1.0, f64::NAN, 0.0][(r >> 8) as usize % 4]));
        c3.push((r % 11 != 0).then(|| specials[(r >> 16) as usize % specials.len()]));
    }
    let as_column = |cells: &[Option<f64>]| -> (Vec<f64>, Vec<u8>) {
        (
            cells.iter().map(|v| v.unwrap_or(0.0)).collect(),
            cells.iter().map(|v| u8::from(v.is_some())).collect(),
        )
    };
    let mut batch = OwnedWcolBatch::empty();
    batch.push_i64_column(0, (0..rows as i64).collect(), vec![1; rows]);
    let (v2, n2) = as_column(&c2);
    let (v3, n3) = as_column(&c3);
    batch.push_f64_column(2, v2, n2);
    batch.push_f64_column(3, v3, n3);
    batch.set_row_count(rows).unwrap();

    let columns = vec![c2.clone(), c3.clone()];
    for keys in [
        vec![desc(3)],
        vec![asc(3)],
        vec![asc(2), desc(3)],
        vec![desc(2), asc(3)],
        vec![desc(2), desc(3)],
    ] {
        let slots: Vec<(usize, bool)> = keys
            .iter()
            .map(|key| ((key.column - 2) as usize, key.descending))
            .collect();
        let sorted = sort_row_batches(std::slice::from_ref(&batch), &keys).unwrap();
        assert_eq!(row_ids(&sorted), oracle_order(&columns, &slots), "{keys:?}");
    }

    // DESC puts NaN first and nulls still last.
    let sorted = sort_row_batches(std::slice::from_ref(&batch), &[desc(3)]).unwrap();
    let got = values_of(&sorted, 3);
    assert!(got.first().unwrap().unwrap().is_nan());
    assert!(got.last().unwrap().is_none());
}

#[test]
fn row_order_keeps_every_key_and_its_direction() {
    use crate::plan::row_order_columns;
    let catalog = catalog();
    assert_eq!(
        row_order_columns("SELECT a FROM t ORDER BY f DESC", &catalog).unwrap(),
        vec![desc(2)]
    );
    assert_eq!(
        row_order_columns("SELECT a FROM t ORDER BY g ASC, f DESC, g", &catalog).unwrap(),
        vec![asc(3), desc(2), asc(3)]
    );
    assert!(row_order_columns("SELECT a FROM t", &catalog)
        .unwrap()
        .is_empty());
    // A key that is not a plain column is refused rather than dropped.
    assert!(row_order_columns("SELECT a FROM t ORDER BY f, g + 1", &catalog).is_err());
    assert_eq!(
        row_order_columns("SELECT a FROM t ORDER BY f, a", &catalog).unwrap(),
        vec![asc(2), asc(0)]
    );
}
