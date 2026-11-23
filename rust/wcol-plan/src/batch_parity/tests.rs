//! Randomised parity: the batch engine must match groups computed directly from each row.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::ir::ColumnKind;
use crate::ops::query::{CatalogColumn, PartialKeyPart, PartialSnapshot, PartialStat};
use crate::stream::partials::partial_state_from_snapshot;
use crate::stream::{DictEntries, DictionaryValues, OwnedWcolBatch, SelectionMask};

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn catalog() -> Vec<CatalogColumn> {
    ["a", "b", "c", "d"]
        .iter()
        .map(|name| CatalogColumn {
            name: name.to_string(),
            kind: ColumnKind::Number,
            physical: 2,
            type_length: 0,
            max_def: 1,
            max_rep: 0,
        })
        .collect()
}

/// `a`, `b`: plain i64 (a nullable, wide range); `c`, `d`: dictionary ids (c nullable).
/// Dictionaries change every few batches, like a new row group.
fn batches(seed: u64, count: usize) -> Vec<OwnedWcolBatch> {
    let mut rng = Lcg(seed);
    let mut dict_c: Arc<Vec<i64>> = Arc::new(vec![0i64]);
    let mut dict_d: Arc<Vec<i64>> = Arc::new(vec![0i64]);
    let mut out = Vec::new();
    for batch_index in 0..count {
        if batch_index % 3 == 0 {
            let len_c = 1 + rng.below(12) as usize;
            dict_c = (0..len_c)
                .map(|_| rng.below(20) as i64 - 5)
                .collect::<Vec<_>>()
                .into();
            let len_d = 1 + rng.below(6) as usize;
            dict_d = (0..len_d)
                .map(|_| rng.below(4) as i64)
                .collect::<Vec<_>>()
                .into();
        }
        let rows = rng.below(90) as usize;
        let mut a = Vec::with_capacity(rows);
        let mut a_nulls = Vec::with_capacity(rows);
        let mut b = Vec::with_capacity(rows);
        let mut c = Vec::with_capacity(rows);
        let mut c_nulls = Vec::with_capacity(rows);
        let mut d = Vec::with_capacity(rows);
        for _ in 0..rows {
            let null_a = rng.below(7) == 0;
            a.push(if null_a {
                0
            } else {
                rng.below(2000) as i64 - 1000
            });
            a_nulls.push(u8::from(!null_a));
            b.push(rng.below(6) as i64);
            let null_c = rng.below(5) == 0;
            c.push(if null_c {
                0
            } else {
                rng.below(dict_c.len() as u64) as u32
            });
            c_nulls.push(u8::from(!null_c));
            d.push(rng.below(dict_d.len() as u64) as u32);
        }
        let mut batch = OwnedWcolBatch::empty();
        batch.set_scope(batch_index as u32 / 3);
        batch.push_i64_column(0, a, a_nulls);
        batch.push_i64_column(1, b, vec![1; rows]);
        batch.push_dict_id_column(
            2,
            c,
            c_nulls,
            DictionaryValues {
                scope: batch_index as u32 / 3,
                column: 2,
                entries: DictEntries::Int(Arc::clone(&dict_c)),
            },
        );
        batch.push_dict_id_column(
            3,
            d,
            vec![1; rows],
            DictionaryValues {
                scope: batch_index as u32 / 3,
                column: 3,
                entries: DictEntries::Int(Arc::clone(&dict_d)),
            },
        );
        batch.set_row_count(rows).unwrap();
        out.push(batch);
    }
    out
}

const QUERIES: &[&str] = &[
    "SELECT COUNT(*) FROM t WHERE b > 2",
    "SELECT SUM(a), COUNT(*), MIN(b), MAX(b), AVG(c), COUNT(c), COUNT(a) FROM t",
    "SELECT SUM(a), MIN(c), MAX(c) FROM t WHERE a > 3 AND b <> 2",
    "SELECT SUM(c), COUNT(*) FROM t WHERE c IN (1, 2, 3, 14)",
    "SELECT MIN(a), MAX(a) FROM t WHERE b < 3 AND a >= 2",
    "SELECT COUNT(*) FROM t WHERE c = 3 AND d <> 1",
    "SELECT SUM(a) FROM t WHERE b = 'x'",
    "SELECT d, COUNT(*), SUM(a) FROM t GROUP BY d",
    "SELECT c, COUNT(*), AVG(a), COUNT(a) FROM t GROUP BY c ORDER BY COUNT(*) DESC LIMIT 3",
    "SELECT b, d, COUNT(*), MIN(c), MAX(a) FROM t WHERE a <> 0 GROUP BY b, d",
    "SELECT a, COUNT(*) FROM t WHERE d = 2 GROUP BY a",
    "SELECT c, b, COUNT(*), SUM(d) FROM t WHERE b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) GROUP BY c, b",
    "SELECT a, c, COUNT(*) FROM t GROUP BY a, c",
    "SELECT b, c, d, COUNT(*), SUM(a), MIN(a) FROM t WHERE a <> 3 GROUP BY b, c, d",
    "SELECT a, b, c, d, COUNT(*) FROM t GROUP BY a, b, c, d",
];

#[test]
fn batch_engine_matches_rows_on_random_batches() {
    for seed in 1..=40u64 {
        let input = batches(seed, 1 + (seed as usize % 9));
        for sql in QUERIES {
            assert_query_matches_rows(sql, &input);
        }
    }
}

#[test]
fn empty_input_matches_rows() {
    for sql in QUERIES {
        assert_query_matches_rows(sql, &[]);
    }
}

#[test]
fn rejects_like_and_folds_repeated_group_keys() {
    let catalog = catalog();
    assert!(crate::plan::bind::open_batch_filter(
        "SELECT COUNT(*) FROM t WHERE a LIKE '%x'",
        &catalog
    )
    .is_err());
    // A repeated key groups the same rows: four keys, within the limit.
    let repeated = "SELECT a, b, c, d, a, COUNT(*) FROM t GROUP BY a, b, c, d, a";
    assert!(crate::plan::bind::open_batch_query(repeated, &catalog).is_ok());
}

#[test]
fn metadata_count_uses_the_row_count() {
    let catalog = catalog();
    let mut query =
        crate::plan::bind::open_batch_query("SELECT COUNT(*) FROM t", &catalog).unwrap();
    assert!(query.metadata_only());
    query.add_metadata_rows(41).unwrap();
    let snap = query.finish_snapshot();
    assert_eq!(snap.passed_rows, 41);
    assert_eq!(snap.groups.len(), 1);
    assert_eq!(snap.groups[0].stats[0].count, 41);
}

#[test]
fn selection_rows_must_match_batch() {
    let catalog = catalog();
    let mut query = crate::plan::bind::open_batch_query("SELECT SUM(a) FROM t", &catalog).unwrap();
    let input = batches(7, 1);
    let mut view = Vec::new();
    let view = input[0].view_with(&mut view);
    let wrong = SelectionMask::all(view.row_count + 1);
    assert!(query.consume(&view, &wrong).is_err());
}

fn batch_snapshot(sql: &str, batches: &[OwnedWcolBatch]) -> crate::ops::query::PartialSnapshot {
    let catalog = catalog();
    let mut filter = crate::plan::bind::open_batch_filter(sql, &catalog)
        .unwrap()
        .unwrap();
    let mut query = crate::plan::bind::open_batch_query(sql, &catalog).unwrap();
    for batch in batches {
        let mut view = Vec::new();
        let view = batch.view_with(&mut view);
        let selection = filter.selection(&view).unwrap();
        query.consume(&view, &selection).unwrap();
    }
    let snapshot = query.finish_snapshot();
    assert_eq!(
        query.finish_partial_state(),
        partial_state_from_snapshot(&snapshot).unwrap(),
        "direct partial columns differ from the snapshot conversion: {sql}"
    );
    snapshot
}

#[test]
fn aggregate_offsets_shift_sum_min_max() {
    for seed in 1..=10u64 {
        let input = batches(seed, 4);
        for (shifted, plain) in [
            (
                "SELECT SUM(a + 3), MIN(a + 3), MAX(c + 7), COUNT(a), SUM(c + 7) FROM t WHERE b <> 1",
                "SELECT SUM(a), MIN(a), MAX(c), COUNT(a), SUM(c), COUNT(c) FROM t WHERE b <> 1",
            ),
            (
                "SELECT d, SUM(a + 5), MAX(a + 5), COUNT(*) FROM t GROUP BY d",
                "SELECT d, SUM(a), MAX(a), COUNT(a) FROM t GROUP BY d",
            ),
        ] {
            let s = batch_snapshot(shifted, &input);
            let p = batch_snapshot(plain, &input);
            assert_eq!(s.groups.len(), p.groups.len(), "seed {seed}: {shifted}");
            for (sg, pg) in s.groups.iter().zip(&p.groups) {
                assert_eq!(sg.key, pg.key);
                if shifted.contains("GROUP BY") {
                    let n = i128::from(pg.stats[2].count);
                    assert_eq!(sg.stats[0].sum_i, pg.stats[0].sum_i + 5 * n, "seed {seed}");
                    if n > 0 {
                        assert_eq!(sg.stats[1].max_i, pg.stats[1].max_i + 5, "seed {seed}");
                    }
                } else {
                    let n_a = i128::from(pg.stats[3].count);
                    let n_c = i128::from(pg.stats[5].count);
                    assert_eq!(sg.stats[0].sum_i, pg.stats[0].sum_i + 3 * n_a, "seed {seed}");
                    assert_eq!(sg.stats[4].sum_i, pg.stats[4].sum_i + 7 * n_c, "seed {seed}");
                    if n_a > 0 {
                        assert_eq!(sg.stats[1].min_i, pg.stats[1].min_i + 3, "seed {seed}");
                    }
                    if n_c > 0 {
                        assert_eq!(sg.stats[2].max_i, pg.stats[2].max_i + 7, "seed {seed}");
                    }
                }
            }
        }
    }
}

#[test]
fn utf8_ne_empty_filter_selects_nonempty_rows() {
    let mut catalog = catalog();
    catalog.push(CatalogColumn {
        name: "s".into(),
        kind: ColumnKind::Text,
        physical: 6,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    });
    let mut batch = OwnedWcolBatch::empty();
    batch.push_utf8_column(4, vec![0, 0, 2, 2], b"ab".to_vec(), vec![1, 1, 0]);
    batch.set_row_count(3).unwrap();
    let mut filter =
        crate::plan::bind::open_batch_filter("SELECT COUNT(*) FROM t WHERE s <> ''", &catalog)
            .unwrap()
            .expect("text filter compiles");
    let mut view_buf = Vec::new();
    let view = batch.view_with(&mut view_buf);
    let mask = filter.selection(&view).unwrap();
    assert_eq!(mask.selected_count(), 1);
    assert!(mask.is_selected(1));
}

#[test]
fn batch_accepts_offsets_and_distinct() {
    let catalog = catalog();
    assert!(crate::plan::bind::open_batch_query("SELECT SUM(a + 1) FROM t", &catalog).is_ok());
    assert!(
        crate::plan::bind::open_batch_query("SELECT COUNT(DISTINCT a) FROM t", &catalog).is_ok()
    );
}

/// Every row of the test batches as resolved `[a, b, c, d]` values (`None` = NULL).
fn rows_of(batches: &[OwnedWcolBatch]) -> Vec<[Option<i64>; 4]> {
    use crate::stream::WcolColumn;
    let mut out = Vec::new();
    for batch in batches {
        let mut view = Vec::new();
        let view = batch.view_with(&mut view);
        for row in 0..view.row_count {
            let mut cells = [None; 4];
            for col in view.columns {
                match *col {
                    WcolColumn::I64 {
                        column,
                        values,
                        nulls,
                    } => {
                        cells[column as usize] = (nulls[row] != 0).then_some(values[row]);
                    }
                    WcolColumn::DictId {
                        column,
                        ids,
                        nulls,
                        dictionary,
                    } => {
                        cells[column as usize] = (nulls[row] != 0).then(|| {
                            dictionary.int_entries().expect("numeric dict")[ids[row] as usize]
                        });
                    }
                    WcolColumn::Utf8 { .. } | WcolColumn::F64 { .. } => {
                        unreachable!("test batches are integer only")
                    }
                }
            }
            out.push(cells);
        }
    }
    out
}

#[derive(Clone, Default)]
struct ColAgg {
    n: u64,
    sum: i128,
    min: i64,
    max: i64,
}

impl ColAgg {
    fn add(&mut self, value: i64) {
        if self.n == 0 {
            self.min = value;
            self.max = value;
        } else {
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
        self.n += 1;
        self.sum += i128::from(value);
    }
}

#[derive(Clone, Default)]
struct RowAgg {
    star: u64,
    cols: [ColAgg; 4],
}

impl RowAgg {
    fn push(&mut self, row: &[Option<i64>; 4]) {
        self.star += 1;
        for (index, cell) in row.iter().enumerate() {
            if let Some(value) = *cell {
                self.cols[index].add(value);
            }
        }
    }
}

fn part(value: Option<i64>) -> PartialKeyPart {
    value.map_or(PartialKeyPart::Null, |value| {
        PartialKeyPart::Int(i128::from(value))
    })
}

fn assert_sum(stat: &PartialStat, col: &ColAgg) {
    assert_eq!(stat.count, col.n);
    if col.n > 0 {
        assert_eq!(stat.sum_i, col.sum);
    }
}

fn assert_min(stat: &PartialStat, col: &ColAgg) {
    assert_eq!(stat.count, col.n);
    if col.n > 0 {
        assert_eq!(stat.min_i, i128::from(col.min));
    }
}

fn assert_max(stat: &PartialStat, col: &ColAgg) {
    assert_eq!(stat.count, col.n);
    if col.n > 0 {
        assert_eq!(stat.max_i, i128::from(col.max));
    }
}

fn fold_rows(
    rows: impl Iterator<Item = [Option<i64>; 4]>,
    key_of: impl Fn(&[Option<i64>; 4]) -> Vec<PartialKeyPart>,
) -> (u64, BTreeMap<Vec<PartialKeyPart>, RowAgg>) {
    let mut passed = 0u64;
    let mut groups: BTreeMap<Vec<PartialKeyPart>, RowAgg> = BTreeMap::new();
    for row in rows {
        passed += 1;
        groups.entry(key_of(&row)).or_default().push(&row);
    }
    (passed, groups)
}

fn assert_folded(
    snap: &PartialSnapshot,
    passed: u64,
    groups: &BTreeMap<Vec<PartialKeyPart>, RowAgg>,
    check: impl Fn(&RowAgg, &[PartialStat]),
) {
    assert_eq!(snap.passed_rows, passed);
    assert_eq!(snap.groups.len(), groups.len());
    for group in &snap.groups {
        check(&groups[&group.key], &group.stats);
    }
}

fn assert_query_matches_rows(sql: &str, input: &[OwnedWcolBatch]) {
    let rows = rows_of(input);
    let snap = batch_snapshot(sql, input);
    let some = |value: Option<i64>, pred: fn(i64) -> bool| value.is_some_and(pred);
    match sql {
        "SELECT COUNT(*) FROM t WHERE b > 2" => {
            let (passed, groups) = fold_rows(rows.into_iter().filter(|r| some(r[1], |b| b > 2)), |_| vec![]);
            assert!(!snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| assert_eq!(stats[0].count, agg.star));
        }
        "SELECT SUM(a), COUNT(*), MIN(b), MAX(b), AVG(c), COUNT(c), COUNT(a) FROM t" => {
            let (passed, groups) = fold_rows(rows.into_iter(), |_| vec![]);
            assert!(!snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| {
                assert_sum(&stats[0], &agg.cols[0]);
                assert_eq!(stats[1].count, agg.star);
                assert_min(&stats[2], &agg.cols[1]);
                assert_max(&stats[3], &agg.cols[1]);
                assert_sum(&stats[4], &agg.cols[2]);
                assert_eq!(stats[5].count, agg.cols[2].n);
                assert_eq!(stats[6].count, agg.cols[0].n);
            });
        }
        "SELECT SUM(a), MIN(c), MAX(c) FROM t WHERE a > 3 AND b <> 2" => {
            let (passed, groups) = fold_rows(
                rows.into_iter().filter(|r| some(r[0], |a| a > 3) && some(r[1], |b| b != 2)),
                |_| vec![],
            );
            assert!(!snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| {
                assert_sum(&stats[0], &agg.cols[0]);
                assert_min(&stats[1], &agg.cols[2]);
                assert_max(&stats[2], &agg.cols[2]);
            });
        }
        "SELECT SUM(c), COUNT(*) FROM t WHERE c IN (1, 2, 3, 14)" => {
            let (passed, groups) = fold_rows(
                rows.into_iter().filter(|r| some(r[2], |c| matches!(c, 1 | 2 | 3 | 14))),
                |_| vec![],
            );
            assert!(!snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| {
                assert_sum(&stats[0], &agg.cols[2]);
                assert_eq!(stats[1].count, agg.star);
            });
        }
        "SELECT MIN(a), MAX(a) FROM t WHERE b < 3 AND a >= 2" => {
            let (passed, groups) = fold_rows(
                rows.into_iter().filter(|r| some(r[1], |b| b < 3) && some(r[0], |a| a >= 2)),
                |_| vec![],
            );
            assert!(!snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| {
                assert_min(&stats[0], &agg.cols[0]);
                assert_max(&stats[1], &agg.cols[0]);
            });
        }
        "SELECT COUNT(*) FROM t WHERE c = 3 AND d <> 1" => {
            let (passed, groups) = fold_rows(
                rows.into_iter().filter(|r| r[2] == Some(3) && some(r[3], |d| d != 1)),
                |_| vec![],
            );
            assert!(!snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| assert_eq!(stats[0].count, agg.star));
        }
        "SELECT SUM(a) FROM t WHERE b = 'x'" => {
            assert_eq!(snap.passed_rows, 0);
            assert!(snap.groups.is_empty());
        }
        "SELECT d, COUNT(*), SUM(a) FROM t GROUP BY d" => {
            let (passed, groups) = fold_rows(rows.into_iter(), |r| vec![part(r[3])]);
            assert!(snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| {
                assert_eq!(stats[0].count, agg.star);
                assert_sum(&stats[1], &agg.cols[0]);
            });
        }
        "SELECT c, COUNT(*), AVG(a), COUNT(a) FROM t GROUP BY c ORDER BY COUNT(*) DESC LIMIT 3" => {
            let (passed, groups) = fold_rows(rows.into_iter(), |r| vec![part(r[2])]);
            assert!(snap.grouped);
            assert!(snap.order_groups_by_count);
            assert_eq!(snap.limit, 3);
            assert_folded(&snap, passed, &groups, |agg, stats| {
                assert_eq!(stats[0].count, agg.star);
                assert_sum(&stats[1], &agg.cols[0]);
                assert_eq!(stats[2].count, agg.cols[0].n);
            });
        }
        "SELECT b, d, COUNT(*), MIN(c), MAX(a) FROM t WHERE a <> 0 GROUP BY b, d" => {
            let (passed, groups) = fold_rows(
                rows.into_iter().filter(|r| some(r[0], |a| a != 0)),
                |r| vec![part(r[1]), part(r[3])],
            );
            assert!(snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| {
                assert_eq!(stats[0].count, agg.star);
                assert_min(&stats[1], &agg.cols[2]);
                assert_max(&stats[2], &agg.cols[0]);
            });
        }
        "SELECT a, COUNT(*) FROM t WHERE d = 2 GROUP BY a" => {
            let (passed, groups) = fold_rows(rows.into_iter().filter(|r| r[3] == Some(2)), |r| vec![part(r[0])]);
            assert!(snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| assert_eq!(stats[0].count, agg.star));
        }
        "SELECT c, b, COUNT(*), SUM(d) FROM t WHERE b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) GROUP BY c, b" => {
            let (passed, groups) = fold_rows(
                rows.into_iter().filter(|r| some(r[1], |b| (0..=9).contains(&b))),
                |r| vec![part(r[2]), part(r[1])],
            );
            assert!(snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| {
                assert_eq!(stats[0].count, agg.star);
                assert_sum(&stats[1], &agg.cols[3]);
            });
        }
        "SELECT a, c, COUNT(*) FROM t GROUP BY a, c" => {
            let (passed, groups) = fold_rows(rows.into_iter(), |r| vec![part(r[0]), part(r[2])]);
            assert!(snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| assert_eq!(stats[0].count, agg.star));
        }
        "SELECT b, c, d, COUNT(*), SUM(a), MIN(a) FROM t WHERE a <> 3 GROUP BY b, c, d" => {
            let (passed, groups) = fold_rows(
                rows.into_iter().filter(|r| some(r[0], |a| a != 3)),
                |r| vec![part(r[1]), part(r[2]), part(r[3])],
            );
            assert!(snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| {
                assert_eq!(stats[0].count, agg.star);
                assert_sum(&stats[1], &agg.cols[0]);
                assert_min(&stats[2], &agg.cols[0]);
            });
        }
        "SELECT a, b, c, d, COUNT(*) FROM t GROUP BY a, b, c, d" => {
            let (passed, groups) = fold_rows(rows.into_iter(), |r| vec![part(r[0]), part(r[1]), part(r[2]), part(r[3])]);
            assert!(snap.grouped);
            assert_folded(&snap, passed, &groups, |agg, stats| assert_eq!(stats[0].count, agg.star));
        }
        other => panic!("no row oracle for {other}"),
    }
}

fn finalized(sql: &str, batches: &[OwnedWcolBatch]) -> Vec<crate::ops::query::PartialGroup> {
    crate::ops::query::finalize_groups(&batch_snapshot(sql, batches)).unwrap()
}

#[test]
fn count_distinct_matches_brute_force() {
    use crate::ops::query::PartialKeyPart;
    use std::collections::{BTreeMap, BTreeSet};
    let key_part =
        |v: Option<i64>| v.map_or(PartialKeyPart::Null, |v| PartialKeyPart::Int(v.into()));
    for seed in 1..=25u64 {
        let input = batches(seed, 1 + (seed as usize % 6));
        let rows = rows_of(&input);

        // Ungrouped: SELECT COUNT(DISTINCT a), COUNT(*) FROM t
        let got = finalized("SELECT COUNT(DISTINCT a), COUNT(*) FROM t", &input);
        let distinct: BTreeSet<i64> = rows.iter().filter_map(|r| r[0]).collect();
        assert_eq!(got.len(), 1, "seed {seed}");
        assert_eq!(got[0].stats[0].count, distinct.len() as u64, "seed {seed}");
        assert_eq!(got[0].stats[1].count, rows.len() as u64, "seed {seed}");

        // Grouped with a filter and other aggregates.
        let sql =
            "SELECT d, COUNT(DISTINCT c), COUNT(*), SUM(a), MIN(a) FROM t WHERE b <> 2 GROUP BY d";
        let mut expected: BTreeMap<Vec<PartialKeyPart>, (BTreeSet<i64>, u64, i128, Option<i64>)> =
            BTreeMap::new();
        for r in rows.iter().filter(|r| r[1] != Some(2)) {
            let e = expected.entry(vec![key_part(r[3])]).or_default();
            if let Some(c) = r[2] {
                e.0.insert(c);
            }
            e.1 += 1;
            if let Some(a) = r[0] {
                e.2 += i128::from(a);
                e.3 = Some(e.3.map_or(a, |m| m.min(a)));
            }
        }
        let got = finalized(sql, &input);
        assert_eq!(got.len(), expected.len(), "seed {seed}");
        for group in &got {
            let (set, count, sum, min) = &expected[&group.key];
            assert_eq!(
                group.stats[0].count,
                set.len() as u64,
                "seed {seed} {:?}",
                group.key
            );
            assert_eq!(group.stats[1].count, *count, "seed {seed}");
            if let Some(min) = min {
                assert_eq!(group.stats[2].sum_i, *sum, "seed {seed}");
                assert_eq!(group.stats[3].min_i, i128::from(*min), "seed {seed}");
            } else {
                assert_eq!(group.stats[2].count, 0, "seed {seed}");
            }
        }
    }
}

#[test]
fn count_distinct_merges_across_shards() {
    use crate::ops::query::{finalize_groups, merge_partial_snapshots};
    for seed in 1..=10u64 {
        let input = batches(seed, 6);
        let (left, right) = input.split_at(3);
        for sql in [
            "SELECT COUNT(DISTINCT a), SUM(b) FROM t",
            "SELECT d, COUNT(DISTINCT c), COUNT(*) FROM t GROUP BY d",
        ] {
            let merged =
                merge_partial_snapshots(&batch_snapshot(sql, left), &batch_snapshot(sql, right))
                    .unwrap();
            let whole = batch_snapshot(sql, &input);
            let mut a = finalize_groups(&merged).unwrap();
            let mut b = finalize_groups(&whole).unwrap();
            a.sort_by(|x, y| x.key.cmp(&y.key));
            b.sort_by(|x, y| x.key.cmp(&y.key));
            assert_eq!(encode_groups(&a), encode_groups(&b), "seed {seed}: {sql}");
        }
    }
}

/// Groups as comparable text (stats contain NaN for empty min/max).
fn encode_groups(groups: &[crate::ops::query::PartialGroup]) -> String {
    format!("{:?}", groups).replace("NaN", "nan")
}

#[test]
fn count_distinct_on_two_columns_is_rejected() {
    let catalog = catalog();
    let err = crate::plan::bind::open_batch_query(
        "SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t",
        &catalog,
    )
    .err();
    assert!(err.is_some_and(|e| e.contains("more than one column")));
}

#[test]
fn approx_count_distinct_estimates_and_merges() {
    use crate::ops::query::{finalize_groups, merge_partial_snapshots, PartialKeyPart};
    use std::collections::{BTreeMap, BTreeSet};
    for seed in 1..=10u64 {
        let input = batches(seed, 8);
        let rows = rows_of(&input);
        let sql = "SELECT d, approx_count_distinct(a), COUNT(*) FROM t GROUP BY d";
        let mut expected: BTreeMap<Vec<PartialKeyPart>, BTreeSet<i64>> = BTreeMap::new();
        for r in &rows {
            let key = vec![r[3].map_or(PartialKeyPart::Null, |v| PartialKeyPart::Int(v.into()))];
            let set = expected.entry(key).or_default();
            if let Some(a) = r[0] {
                set.insert(a);
            }
        }
        let whole = batch_snapshot(sql, &input);
        let got = finalize_groups(&whole).unwrap();
        assert_eq!(got.len(), expected.len());
        for group in &got {
            let exact = expected[&group.key].len() as f64;
            let estimate = group.stats[0].count as f64;
            assert!(
                (estimate - exact).abs() <= (0.03 * exact).max(1.0),
                "seed {seed}: {estimate} vs {exact}"
            );
        }
        // A union of sketches is exact, so two shards give the same estimate as one pass.
        let (left, right) = input.split_at(4);
        let merged =
            merge_partial_snapshots(&batch_snapshot(sql, left), &batch_snapshot(sql, right))
                .unwrap();
        let mut a = finalize_groups(&merged).unwrap();
        a.sort_by(|x, y| x.key.cmp(&y.key));
        assert_eq!(encode_groups(&a), encode_groups(&got), "seed {seed}");
    }
}

#[test]
fn approx_count_distinct_survives_pwcl() {
    use crate::stream::partial_wcs::{
        partial_state_from_wire_payload, partial_state_to_wcs_payload,
    };
    use crate::stream::partials::{partial_state_from_batch, partial_state_to_batch};
    let catalog = catalog();
    let input = batches(5, 6);
    for sql in [
        "SELECT approx_count_distinct(c), SUM(a) FROM t",
        "SELECT b, approx_count_distinct(a) FROM t GROUP BY b",
    ] {
        let mut query = crate::plan::bind::open_batch_query(sql, &catalog).unwrap();
        for batch in &input {
            let mut view = Vec::new();
            let view = batch.view_with(&mut view);
            query
                .consume(&view, &SelectionMask::all(view.row_count))
                .unwrap();
        }
        let state = query.finish_partial_state();
        assert_eq!(
            state,
            partial_state_from_snapshot(&query.finish_snapshot()).unwrap(),
            "{sql}"
        );
        assert!(!state.sketches.is_empty());
        let wire = partial_state_to_wcs_payload(&state).unwrap();
        assert_eq!(
            partial_state_from_wire_payload(&wire).unwrap(),
            state,
            "{sql}"
        );
        assert_eq!(
            partial_state_from_batch(&partial_state_to_batch(state.clone())).unwrap(),
            state
        );
    }
}

#[test]
fn text_aggregates_are_rejected_except_counts() {
    let mut catalog = catalog();
    catalog.push(CatalogColumn {
        name: "s".into(),
        kind: ColumnKind::Text,
        physical: 6,
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    });
    for sql in [
        "SELECT SUM(s) FROM t",
        "SELECT MIN(s), COUNT(*) FROM t",
        "SELECT d, MAX(s) FROM t GROUP BY d",
    ] {
        let err = crate::plan::bind::open_batch_query(sql, &catalog)
            .err()
            .unwrap_or_default();
        assert!(err.contains("on text is not supported"), "{sql}: {err}");
    }
    assert!(
        crate::plan::bind::open_batch_query("SELECT COUNT(s), COUNT(*) FROM t", &catalog).is_ok()
    );
}
