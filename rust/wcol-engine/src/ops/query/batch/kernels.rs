//! Column kernels: sketches, accumulators and key comparison over selected rows.

use super::*;

/// Add the selected, non-null values of one column to a sketch. A dictionary column inserts
/// each dictionary entry once, however many rows use it.
pub(super) fn fill_sketch(
    col: ColRef<'_>,
    words: &[u32],
    rows: usize,
    full: bool,
    hist: &mut Vec<u64>,
    sketch: &mut Hll,
) {
    let nulls = col.nulls();
    match col {
        ColRef::I64 { values, .. } => {
            if full && mask::all_valid(nulls, rows) {
                for &v in &values[..rows] {
                    sketch.insert_i64(v);
                }
            } else {
                mask::for_each_set(words, |row| {
                    if nulls[row] != 0 {
                        sketch.insert_i64(values[row]);
                    }
                });
            }
        }
        ColRef::Dict { ids, entries, .. } => {
            hist.clear();
            hist.resize(entries.len(), 0);
            mask::for_each_set(words, |row| {
                if nulls[row] != 0 {
                    hist[ids[row] as usize] = 1;
                }
            });
            for (id, &seen) in hist.iter().enumerate() {
                if seen != 0 {
                    sketch.insert_i64(entries[id]);
                }
            }
        }
        ColRef::DictBytes { .. }
        | ColRef::Utf8 { .. }
        | ColRef::F64 { .. }
        | ColRef::DictF64 { .. } => {}
    }
}

pub(super) fn f64_at(col: ColRef<'_>, row: usize) -> f64 {
    match col {
        ColRef::F64 { values, .. } => values[row],
        ColRef::DictF64 { ids, entries, .. } => entries[ids[row] as usize],
        _ => 0.0,
    }
}

pub(super) fn fill_sketch_f64(
    col: ColRef<'_>,
    words: &[u32],
    rows: usize,
    full: bool,
    sketch: &mut Hll,
) {
    let nulls = col.nulls();
    let insert = |sketch: &mut Hll, v: f64| sketch.insert_i64(float_key(v) as i64);
    match col {
        ColRef::F64 { values, .. } => {
            if full && mask::all_valid(nulls, rows) {
                for &v in &values[..rows] {
                    insert(sketch, v);
                }
            } else {
                mask::for_each_set(words, |row| {
                    if nulls[row] != 0 {
                        insert(sketch, values[row]);
                    }
                });
            }
        }
        ColRef::DictF64 { ids, entries, .. } => {
            if full && mask::all_valid(nulls, rows) {
                for &id in &ids[..rows] {
                    insert(sketch, entries[id as usize]);
                }
            } else {
                mask::for_each_set(words, |row| {
                    if nulls[row] != 0 {
                        insert(sketch, entries[ids[row] as usize]);
                    }
                });
            }
        }
        _ => {}
    }
}

pub(super) fn column_facc(col: ColRef<'_>, words: &[u32], rows: usize, full: bool) -> FAcc {
    let nulls = col.nulls();
    let mut acc = FAcc::empty();
    match col {
        ColRef::F64 { values, .. } => {
            if full && mask::all_valid(nulls, rows) {
                for &v in &values[..rows] {
                    acc.add(v);
                }
            } else {
                mask::for_each_set(words, |row| {
                    if nulls[row] != 0 {
                        acc.add(values[row]);
                    }
                });
            }
        }
        ColRef::DictF64 { ids, entries, .. } => {
            if full && mask::all_valid(nulls, rows) {
                for &id in &ids[..rows] {
                    acc.add(entries[id as usize]);
                }
            } else {
                mask::for_each_set(words, |row| {
                    if nulls[row] != 0 {
                        acc.add(entries[ids[row] as usize]);
                    }
                });
            }
        }
        _ => {}
    }
    acc
}

/// Key order of the row kernel's `Vec<KeyPart>`: part by part, `Null` before values; strings compare as bytes.
/// Out of line: every slot sort inlines its comparator into each of its kernels, and a
/// key comparison through chunked keys and interned bytes made those copies 24 KB of Wasm.
#[inline(never)]
pub(super) fn cmp_keys(
    a: &GroupKey,
    b: &GroupKey,
    keys: usize,
    text_mask: u32,
    float_mask: u32,
    interner: &StringInterner,
) -> std::cmp::Ordering {
    for k in 0..keys {
        let a_null = a.nulls & (1 << k) != 0;
        let b_null = b.nulls & (1 << k) != 0;
        let ord = match (a_null, b_null) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) if text_mask & (1u32 << k) != 0 => {
                interner.cmp_ids(a.values[k] as u32, b.values[k] as u32)
            }
            (false, false) if float_mask & (1u32 << k) != 0 => {
                (a.values[k] as u64).cmp(&(b.values[k] as u64))
            }
            (false, false) => a.values[k].cmp(&b.values[k]),
        };
        if ord.is_ne() {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Count / sum / min / max of one column over the selected, non-null rows.
pub(super) fn column_acc(
    col: ColRef<'_>,
    words: &[u32],
    rows: usize,
    full: bool,
    hist: &mut Vec<u64>,
) -> Acc {
    let nulls = col.nulls();
    let all_valid = mask::all_valid(nulls, rows);
    match col {
        ColRef::I64 { values, .. } => {
            let mut acc = Acc::EMPTY;
            if full && all_valid {
                for &v in &values[..rows] {
                    acc.add(v);
                }
            } else {
                mask::for_each_set(words, |row| {
                    if nulls[row] != 0 {
                        acc.add(values[row]);
                    }
                });
            }
            acc
        }
        ColRef::Dict { ids, entries, .. } => {
            // Histogram the ids, then fold each distinct entry once.
            if entries.len() > rows {
                let mut acc = Acc::EMPTY;
                mask::for_each_set(words, |row| {
                    if nulls[row] != 0 {
                        acc.add(entries[ids[row] as usize]);
                    }
                });
                return acc;
            }
            hist.clear();
            hist.resize(entries.len(), 0);
            if full && all_valid {
                for &id in &ids[..rows] {
                    hist[id as usize] += 1;
                }
            } else {
                mask::for_each_set(words, |row| {
                    if nulls[row] != 0 {
                        hist[ids[row] as usize] += 1;
                    }
                });
            }
            let mut acc = Acc::EMPTY;
            for (id, &n) in hist.iter().enumerate() {
                if n > 0 {
                    acc.add_n(entries[id], n);
                }
            }
            acc
        }
        // Only `COUNT` reaches string columns (the planner rejects other string aggregates),
        // and it needs just the selected, non-null rows.
        ColRef::DictBytes { .. }
        | ColRef::Utf8 { .. }
        | ColRef::F64 { .. }
        | ColRef::DictF64 { .. } => {
            let mut acc = Acc::EMPTY;
            if full && all_valid {
                acc.count = rows as u64;
            } else {
                mask::for_each_set(words, |row| acc.count += u64::from(nulls[row] != 0));
            }
            acc
        }
    }
}

/// A partial row's key as this table stores it: null bits and values, with text through
/// `intern` (`None` when it has no id yet) and floats as order-preserving keys. A part of
/// the wrong type means the partial came from a different plan.
pub(super) fn partial_key(
    rows: &PartialRows<'_>,
    row: usize,
    key_len: usize,
    text_mask: u32,
    float_mask: u32,
    mut intern: impl FnMut(&[u8]) -> Option<i64>,
) -> Result<Option<(u8, [i64; MAX_KEYS])>, String> {
    let mut nulls = 0u8;
    let mut values = [0i64; MAX_KEYS];
    for (k, value) in values.iter_mut().enumerate().take(key_len) {
        let (text, float) = (text_mask & (1u32 << k) != 0, float_mask & (1u32 << k) != 0);
        *value = match rows.key(row, k)? {
            KeyPartRef::Null => {
                nulls |= 1 << k;
                0
            }
            KeyPartRef::Bytes(bytes) if text => match intern(bytes) {
                Some(id) => id,
                None => return Ok(None),
            },
            KeyPartRef::Float(bits) if float => float_key(f64::from_bits(bits)) as i64,
            KeyPartRef::Int(v) if !text && !float => v,
            _ => return Err(format!("partial key {k} has the wrong type")),
        };
    }
    Ok(Some((nulls, values)))
}

pub(super) fn pair_key_of(nulls: u8, values: &[i64; MAX_KEYS]) -> PairKey {
    PairKey {
        nulls,
        a: values[0],
        b: values[1],
        c: values[2],
    }
}
