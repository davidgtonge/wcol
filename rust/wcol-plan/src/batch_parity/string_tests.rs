//! Randomised string coverage: filters, keys and counts against a brute-force evaluation of the
//! same rows, with PLAIN (`Utf8`) and dictionary batches mixed and dictionaries changing between
//! batches the way they do between row groups. Partials are read back through PWCL wire bytes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::ir::ColumnKind;
use crate::ops::query::{
    finalize_groups, merge_partial_snapshots, CatalogColumn, PartialGroup, PartialKeyPart,
    PartialSnapshot,
};
use crate::stream::partial_wcs::partial_state_to_wcs_payload;
use crate::stream::partials::partial_snapshot_from_wire;
use crate::stream::{DictEntries, DictionaryValues, OwnedWcolBatch, WcolColumn};

const POOL: &[&str] = &[
    "",
    "a",
    "ab",
    "ведь",
    "ведь это",
    "е",
    "google.com",
    "www.google.ru",
    "a_b",
    "50%",
    "Ж",
];

fn catalog() -> Vec<CatalogColumn> {
    let column = |name: &str, kind| CatalogColumn {
        name: name.to_string(),
        kind,
        physical: if kind == ColumnKind::Text { 6 } else { 2 },
        type_length: 0,
        max_def: 1,
        max_rep: 0,
    };
    vec![
        column("a", ColumnKind::Number),
        column("s", ColumnKind::Text),
        column("d", ColumnKind::Number),
    ]
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

/// `a`: nullable i64; `s`: nullable string, PLAIN on odd batches and dictionary on even ones
/// (a fresh dictionary every other batch); `d`: small i64.
fn batches(seed: u64, count: usize) -> Vec<OwnedWcolBatch> {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut dict: Vec<&str> = Vec::new();
    let mut out = Vec::new();
    for index in 0..count {
        let rows = rng.below(70);
        let mut a = Vec::new();
        let mut a_nulls = Vec::new();
        let mut d = Vec::new();
        let mut strings: Vec<Option<&str>> = Vec::new();
        for _ in 0..rows {
            let null_a = rng.below(6) == 0;
            a.push(if null_a { 0 } else { rng.below(50) as i64 });
            a_nulls.push(u8::from(!null_a));
            d.push(rng.below(3) as i64);
            strings.push((rng.below(7) != 0).then(|| POOL[rng.below(POOL.len())]));
        }
        let mut batch = OwnedWcolBatch::empty();
        batch.set_scope(index as u32);
        batch.push_i64_column(0, a, a_nulls);
        let nulls: Vec<u8> = strings.iter().map(|s| u8::from(s.is_some())).collect();
        if index % 2 == 1 {
            let mut offsets = vec![0u32];
            let mut bytes = Vec::new();
            for s in &strings {
                bytes.extend_from_slice(s.unwrap_or("").as_bytes());
                offsets.push(bytes.len() as u32);
            }
            batch.push_utf8_column(1, offsets, bytes, nulls);
        } else {
            if index % 4 == 0 || dict.is_empty() {
                // Shuffled pool subset, so ids never mean the same string twice.
                dict = POOL.iter().copied().filter(|_| rng.below(3) != 0).collect();
                if dict.is_empty() {
                    dict.push("a");
                }
                let swaps = dict.len();
                for i in 0..swaps {
                    let j = rng.below(swaps);
                    dict.swap(i, j);
                }
            }
            let mut offsets = vec![0u32];
            let mut blob = Vec::new();
            for entry in &dict {
                blob.extend_from_slice(entry.as_bytes());
                offsets.push(blob.len() as u32);
            }
            let ids: Vec<u32> = strings
                .iter_mut()
                .map(|s| match s {
                    Some(value) => match dict.iter().position(|e| e == value) {
                        Some(id) => id as u32,
                        None => {
                            *value = dict[0];
                            0
                        }
                    },
                    None => 0,
                })
                .collect();
            batch.push_dict_id_column(
                1,
                ids,
                nulls,
                DictionaryValues {
                    scope: index as u32,
                    column: 1,
                    entries: DictEntries::Bytes {
                        offsets: Arc::from(offsets),
                        bytes: Arc::from(blob),
                    },
                },
            );
        }
        batch.push_i64_column(2, d, vec![1; rows]);
        batch.set_row_count(rows).unwrap();
        out.push(batch);
    }
    out
}

type Row = (Option<i64>, Option<String>, i64);

fn rows_of(batches: &[OwnedWcolBatch]) -> Vec<Row> {
    let mut out = Vec::new();
    for batch in batches {
        let mut view = Vec::new();
        let view = batch.view_with(&mut view);
        for row in 0..view.row_count {
            let mut cells: Row = (None, None, 0);
            for col in view.columns {
                match *col {
                    WcolColumn::I64 {
                        column: 0,
                        values,
                        nulls,
                    } => cells.0 = (nulls[row] != 0).then_some(values[row]),
                    WcolColumn::I64 { values, .. } => cells.2 = values[row],
                    WcolColumn::Utf8 {
                        offsets,
                        bytes,
                        nulls,
                        ..
                    } => {
                        let text = &bytes[offsets[row] as usize..offsets[row + 1] as usize];
                        cells.1 =
                            (nulls[row] != 0).then(|| String::from_utf8(text.to_vec()).unwrap());
                    }
                    WcolColumn::F64 { .. } => unreachable!("string tests have no float columns"),
                    WcolColumn::DictId {
                        ids,
                        nulls,
                        dictionary,
                        ..
                    } => {
                        let DictEntries::Bytes { offsets, bytes } = &dictionary.entries else {
                            unreachable!()
                        };
                        let id = ids[row] as usize;
                        let text = &bytes[offsets[id] as usize..offsets[id + 1] as usize];
                        cells.1 =
                            (nulls[row] != 0).then(|| String::from_utf8(text.to_vec()).unwrap());
                    }
                }
            }
            out.push(cells);
        }
    }
    out
}

/// Run `sql` on the batches and read the partial back from PWCL wire bytes, as a coordinator
/// would receive it.
fn wire_snapshot(sql: &str, batches: &[OwnedWcolBatch]) -> PartialSnapshot {
    let catalog = catalog();
    let mut filter = crate::plan::bind::open_batch_filter(sql, &catalog)
        .unwrap()
        .expect("batch filter");
    let mut query = crate::plan::bind::open_batch_query(sql, &catalog).unwrap();
    for batch in batches {
        let mut view = Vec::new();
        let view = batch.view_with(&mut view);
        let selection = filter.selection(&view).unwrap();
        query.consume(&view, &selection).unwrap();
    }
    let wire = partial_state_to_wcs_payload(&query.finish_partial_state()).unwrap();
    partial_snapshot_from_wire(&wire).unwrap()
}

fn finalized(sql: &str, batches: &[OwnedWcolBatch]) -> Vec<PartialGroup> {
    finalize_groups(&wire_snapshot(sql, batches)).unwrap()
}

fn key(s: &Option<String>) -> PartialKeyPart {
    s.as_ref().map_or(PartialKeyPart::Null, |s| {
        PartialKeyPart::Bytes(s.as_bytes().to_vec())
    })
}

/// Reference LIKE over characters.
fn like(pattern: &[char], text: &[char]) -> bool {
    match pattern.split_first() {
        None => text.is_empty(),
        Some(('%', rest)) => (0..=text.len()).any(|skip| like(rest, &text[skip..])),
        Some(('_', rest)) => !text.is_empty() && like(rest, &text[1..]),
        Some((ch, rest)) => text.first() == Some(ch) && like(rest, &text[1..]),
    }
}

#[test]
fn string_filters_match_brute_force() {
    let patterns = [
        "%", "%google%", "_едь%", "%ь_э%", "a_b", "a\\_b", "50%", "%.ru", "%а%", "_", "",
    ];
    for seed in 1..=12u64 {
        let input = batches(seed, 7);
        let rows = rows_of(&input);
        let count = |pred: &dyn Fn(&str) -> bool| {
            rows.iter()
                .filter(|r| r.1.as_deref().is_some_and(pred))
                .count() as u64
        };
        let mut cases: Vec<(String, u64)> = vec![
            ("s = 'ведь'".into(), count(&|s| s == "ведь")),
            ("s <> ''".into(), count(&|s| !s.is_empty())),
            ("s = ''".into(), count(&|s| s.is_empty())),
            ("s <> 'a'".into(), count(&|s| s != "a")),
            (
                "s IN ('a', 'Ж', 'нет')".into(),
                count(&|s| ["a", "Ж", "нет"].contains(&s)),
            ),
        ];
        for pattern in patterns {
            let p: Vec<char> = pattern.chars().collect();
            let matches = |s: &str| like(&p, &s.chars().collect::<Vec<_>>());
            cases.push((format!("s LIKE '{pattern}'"), count(&matches)));
            cases.push((format!("s NOT LIKE '{pattern}'"), count(&|s| !matches(s))));
        }
        for (predicate, expected) in cases {
            let sql = format!("SELECT COUNT(*) FROM t WHERE {predicate}");
            assert_eq!(
                finalized(&sql, &input)[0].stats[0].count,
                expected,
                "seed {seed}: {sql}"
            );
        }
    }
}

#[test]
fn string_group_keys_match_brute_force_including_null() {
    for seed in 1..=15u64 {
        let input = batches(seed, 8);
        let rows = rows_of(&input);

        let mut expected: BTreeMap<PartialKeyPart, (u64, i128, u64)> = BTreeMap::new();
        for r in &rows {
            let e = expected.entry(key(&r.1)).or_default();
            e.0 += 1;
            if let Some(a) = r.0 {
                e.1 += i128::from(a);
                e.2 += 1;
            }
        }
        let got = finalized(
            "SELECT s, COUNT(*), SUM(a), COUNT(a) FROM t GROUP BY s",
            &input,
        );
        assert_eq!(got.len(), expected.len(), "seed {seed}");
        for group in &got {
            let (count, sum, non_null) = expected[&group.key[0]];
            assert_eq!(group.stats[0].count, count, "seed {seed} {:?}", group.key);
            assert_eq!(group.stats[2].count, non_null, "seed {seed}");
            if non_null > 0 {
                assert_eq!(group.stats[1].sum_i, sum, "seed {seed}");
            }
        }

        let mut two: BTreeMap<(i64, PartialKeyPart), u64> = BTreeMap::new();
        for r in &rows {
            *two.entry((r.2, key(&r.1))).or_default() += 1;
        }
        let got = finalized("SELECT d, s, COUNT(*) FROM t GROUP BY d, s", &input);
        assert_eq!(got.len(), two.len(), "seed {seed}");
        for group in &got {
            let PartialKeyPart::Int(d) = group.key[0] else {
                panic!("int key")
            };
            assert_eq!(
                group.stats[0].count,
                two[&(d as i64, group.key[1].clone())],
                "seed {seed}"
            );
        }
    }
}

#[test]
fn string_counts_and_distinct_match_brute_force() {
    for seed in 1..=15u64 {
        let input = batches(seed, 6);
        let rows = rows_of(&input);
        let non_null = rows.iter().filter(|r| r.1.is_some()).count() as u64;
        let distinct: BTreeSet<&String> = rows.iter().filter_map(|r| r.1.as_ref()).collect();
        let got = finalized(
            "SELECT COUNT(s), COUNT(DISTINCT s), COUNT(*) FROM t",
            &input,
        );
        assert_eq!(got[0].stats[0].count, non_null, "seed {seed}");
        assert_eq!(got[0].stats[1].count, distinct.len() as u64, "seed {seed}");
        assert_eq!(got[0].stats[2].count, rows.len() as u64, "seed {seed}");
    }
}

#[test]
fn string_partials_merge_across_shards() {
    for seed in 1..=10u64 {
        let input = batches(seed, 8);
        let (left, right) = input.split_at(3);
        for sql in [
            "SELECT s, COUNT(*), SUM(a) FROM t GROUP BY s",
            "SELECT d, COUNT(DISTINCT s) FROM t WHERE s <> '' GROUP BY d",
        ] {
            let merged =
                merge_partial_snapshots(&wire_snapshot(sql, left), &wire_snapshot(sql, right))
                    .unwrap();
            let mut a = finalize_groups(&merged).unwrap();
            let mut b = finalize_groups(&wire_snapshot(sql, &input)).unwrap();
            a.sort_by(|x, y| x.key.cmp(&y.key));
            b.sort_by(|x, y| x.key.cmp(&y.key));
            assert_eq!(
                format!("{a:?}").replace("NaN", "nan"),
                format!("{b:?}").replace("NaN", "nan"),
                "seed {seed}: {sql}"
            );
        }
    }
}
