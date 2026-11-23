//! Column-at-a-time filter kernels: each predicate builds a packed mask over the batch and
//! masks are AND-ed. Dictionary columns evaluate the predicate once per dictionary entry
//! (cached while the dictionary is unchanged) and then map ids through that table.

use std::sync::Arc;

use rustc_hash::FxHashSet;

use super::mask;
use super::string_filter::{
    compile_like, compile_text_compare, compile_text_in, dict_entry_slice, test_bytes,
    utf8_row_slice, ByteDictKey, TextPred,
};
use super::{ColRef, Cols};
use crate::ir::{CmpOp, ColumnKind, Predicate, Scalar};
use crate::ops::query::float_key::float_key;
use crate::ops::query::CatalogColumn;

/// A predicate over integer values, compiled to exact `i64` comparisons.
#[derive(Clone, Debug)]
pub(crate) enum Pred {
    /// Inclusive range.
    Range {
        lo: i64,
        hi: i64,
    },
    NotEq(i64),
    /// Matches every non-null row.
    AnyValid,
    /// Matches no row.
    Never,
    InSmall(Vec<i64>),
    InSet(FxHashSet<i64>),
}

/// A predicate over `f64` values, compiled to inclusive ranges on [`float_key`].
#[derive(Clone, Debug)]
pub(crate) enum FPred {
    Range { lo: u64, hi: u64 },
    NotEq(u64),
    Never,
    In(FxHashSet<u64>),
}

impl FPred {
    #[inline]
    pub(crate) fn test(&self, v: f64) -> bool {
        let key = float_key(v);
        match self {
            FPred::Range { lo, hi } => key >= *lo && key <= *hi,
            FPred::NotEq(x) => key != *x,
            FPred::Never => false,
            FPred::In(values) => values.contains(&key),
        }
    }
}

impl Pred {
    #[inline]
    pub(crate) fn test(&self, v: i64) -> bool {
        match self {
            Pred::Range { lo, hi } => v >= *lo && v <= *hi,
            Pred::NotEq(x) => v != *x,
            Pred::AnyValid => true,
            Pred::Never => false,
            Pred::InSmall(values) => values.contains(&v),
            Pred::InSet(values) => values.contains(&v),
        }
    }
}

pub(crate) enum ColumnFilter {
    Int {
        column: u32,
        pred: Pred,
        dict_cache: Option<(Arc<Vec<i64>>, Vec<u8>)>,
        negated: bool,
    },
    Float {
        column: u32,
        pred: FPred,
        dict_cache: Option<(Arc<Vec<f64>>, Vec<u8>)>,
        negated: bool,
    },
    Text {
        column: u32,
        pred: TextPred,
        dict_cache: Option<(ByteDictKey, Vec<u8>)>,
    },
}

/// Compile plan filters. `None` means a filter shape this engine does not run.
pub(crate) fn compile(
    predicates: &[Predicate],
    catalog: &[CatalogColumn],
) -> Option<Vec<ColumnFilter>> {
    let mut filters: Vec<ColumnFilter> = predicates
        .iter()
        .map(|predicate| compile_one(predicate, catalog))
        .collect::<Option<_>>()?;
    // Comparisons before `LIKE` when there is no selectivity estimate. Every predicate
    // compiled here is pure, so the reorder has no observable effect beyond which rows
    // a later predicate still has to test.
    filters.sort_by_key(predicate_rank);
    Some(filters)
}

/// Lower is cheaper. `LIKE` stays after comparisons; equal ranks keep source order.
fn predicate_rank(filter: &ColumnFilter) -> u8 {
    match filter {
        ColumnFilter::Text {
            pred: TextPred::Like { .. },
            ..
        } => 2,
        ColumnFilter::Text { .. } => 1,
        ColumnFilter::Int { .. } | ColumnFilter::Float { .. } => 0,
    }
}

/// AND the predicates onto `mask`. Each one tests only rows that are still selected.
pub(crate) fn evaluate_conjunction(
    filters: &mut [ColumnFilter],
    cols: &Cols<'_>,
    mask: &mut [u32],
) -> Result<(), String> {
    for filter in filters {
        if mask::is_zero(mask) {
            break;
        }
        filter.retain(cols, mask)?;
    }
    Ok(())
}

fn compile_one(filter: &Predicate, catalog: &[CatalogColumn]) -> Option<ColumnFilter> {
    match filter {
        Predicate::Compare { column, op, value } => {
            let kind = catalog.get(*column as usize)?.kind;
            if kind == ColumnKind::Text {
                let pred = match value {
                    Scalar::Text(text) => compile_text_compare(*op, text)?,
                    Scalar::Int(_) | Scalar::Number(_) => match op {
                        CmpOp::Ne => TextPred::AnyValid,
                        _ => TextPred::Never,
                    },
                };
                Some(ColumnFilter::Text {
                    column: *column,
                    pred,
                    dict_cache: None,
                })
            } else if is_float_column(catalog, *column) {
                Some(ColumnFilter::Float {
                    column: *column,
                    pred: compile_float_compare(*op, value)?,
                    dict_cache: None,
                    negated: false,
                })
            } else {
                Some(ColumnFilter::Int {
                    column: *column,
                    pred: compile_compare(*op, value)?,
                    dict_cache: None,
                    negated: false,
                })
            }
        }
        Predicate::In {
            column,
            values,
            negated,
        } => {
            let kind = catalog.get(*column as usize)?.kind;
            if kind == ColumnKind::Text {
                let strings: Vec<&str> = values
                    .iter()
                    .filter_map(|v| match v {
                        Scalar::Text(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .collect();
                if *negated {
                    return None;
                }
                Some(ColumnFilter::Text {
                    column: *column,
                    pred: compile_text_in(&strings),
                    dict_cache: None,
                })
            } else if is_float_column(catalog, *column) {
                Some(ColumnFilter::Float {
                    column: *column,
                    pred: compile_float_in(values),
                    dict_cache: None,
                    negated: *negated,
                })
            } else {
                Some(ColumnFilter::Int {
                    column: *column,
                    pred: compile_in(values),
                    dict_cache: None,
                    negated: *negated,
                })
            }
        }
        Predicate::Like {
            column,
            pattern,
            negated,
        } => {
            if catalog.get(*column as usize)?.kind != ColumnKind::Text {
                return None;
            }
            Some(ColumnFilter::Text {
                column: *column,
                pred: compile_like(pattern, *negated),
                dict_cache: None,
            })
        }
    }
}

fn compile_compare(op: CmpOp, value: &Scalar) -> Option<Pred> {
    match value {
        Scalar::Int(v) => Some(int_pred(op, *v)),
        Scalar::Number(v) => number_pred(op, *v),
        Scalar::Text(_) => Some(Pred::Never),
    }
}

fn int_pred(op: CmpOp, v: i64) -> Pred {
    match op {
        CmpOp::Eq => Pred::Range { lo: v, hi: v },
        CmpOp::Ne => Pred::NotEq(v),
        CmpOp::Lt => match v.checked_sub(1) {
            Some(hi) => Pred::Range { lo: i64::MIN, hi },
            None => Pred::Never,
        },
        CmpOp::Le => Pred::Range {
            lo: i64::MIN,
            hi: v,
        },
        CmpOp::Gt => match v.checked_add(1) {
            Some(lo) => Pred::Range { lo, hi: i64::MAX },
            None => Pred::Never,
        },
        CmpOp::Ge => Pred::Range {
            lo: v,
            hi: i64::MAX,
        },
    }
}

fn number_pred(op: CmpOp, v: f64) -> Option<Pred> {
    if v.is_nan() {
        return None;
    }
    const HI: f64 = 9_223_372_036_854_775_808.0;
    if !(-HI..HI).contains(&v) {
        let above = v >= HI;
        return Some(match op {
            CmpOp::Eq => Pred::Never,
            CmpOp::Ne => Pred::AnyValid,
            CmpOp::Lt | CmpOp::Le => {
                if above {
                    Pred::AnyValid
                } else {
                    Pred::Never
                }
            }
            CmpOp::Gt | CmpOp::Ge => {
                if above {
                    Pred::Never
                } else {
                    Pred::AnyValid
                }
            }
        });
    }
    if v.fract() == 0.0 {
        return Some(int_pred(op, v as i64));
    }
    let floor = v.floor() as i64;
    let ceil = v.ceil() as i64;
    Some(match op {
        CmpOp::Eq => Pred::Never,
        CmpOp::Ne => Pred::AnyValid,
        CmpOp::Lt | CmpOp::Le => Pred::Range {
            lo: i64::MIN,
            hi: floor,
        },
        CmpOp::Gt | CmpOp::Ge => Pred::Range {
            lo: ceil,
            hi: i64::MAX,
        },
    })
}

fn is_float_column(catalog: &[CatalogColumn], column: u32) -> bool {
    catalog
        .get(column as usize)
        .is_some_and(|column| matches!(column.physical, 4 | 5))
}

fn literal_f64(value: &Scalar) -> Option<f64> {
    match value {
        Scalar::Int(v) => Some(*v as f64),
        Scalar::Number(v) => Some(*v),
        Scalar::Text(text) => match text.trim().to_ascii_lowercase().as_str() {
            "nan" => Some(f64::NAN),
            "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
            "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
            _ => None,
        },
    }
}

fn compile_float_compare(op: CmpOp, value: &Scalar) -> Option<FPred> {
    let Some(v) = literal_f64(value) else {
        return Some(FPred::Never);
    };
    let key = float_key(v);
    Some(match op {
        CmpOp::Eq => FPred::Range { lo: key, hi: key },
        CmpOp::Ne => FPred::NotEq(key),
        CmpOp::Lt => {
            if key == 0 {
                FPred::Never
            } else {
                FPred::Range { lo: 0, hi: key - 1 }
            }
        }
        CmpOp::Le => FPred::Range { lo: 0, hi: key },
        CmpOp::Gt => {
            if key == u64::MAX {
                FPred::Never
            } else {
                FPred::Range {
                    lo: key + 1,
                    hi: u64::MAX,
                }
            }
        }
        CmpOp::Ge => FPred::Range {
            lo: key,
            hi: u64::MAX,
        },
    })
}

fn compile_float_in(values: &[Scalar]) -> FPred {
    let keys: FxHashSet<u64> = values
        .iter()
        .filter_map(literal_f64)
        .map(float_key)
        .collect();
    if keys.is_empty() {
        FPred::Never
    } else {
        FPred::In(keys)
    }
}

fn compile_in(values: &[Scalar]) -> Pred {
    let mut ints: Vec<i64> = values
        .iter()
        .filter_map(|value| match value {
            Scalar::Int(v) => Some(*v),
            Scalar::Number(v) if v.fract() == 0.0 && v.abs() < 9.0e18 => Some(*v as i64),
            _ => None,
        })
        .collect();
    ints.sort_unstable();
    ints.dedup();
    match ints.len() {
        0 => Pred::Never,
        1..=8 => Pred::InSmall(ints),
        _ => Pred::InSet(ints.into_iter().collect()),
    }
}

impl ColumnFilter {
    pub(crate) fn retain(&mut self, cols: &Cols<'_>, mask: &mut [u32]) -> Result<(), String> {
        match self {
            ColumnFilter::Int {
                column,
                pred,
                dict_cache,
                negated,
            } => retain_int(*column, pred, dict_cache, *negated, cols, mask),
            ColumnFilter::Float {
                column,
                pred,
                dict_cache,
                negated,
            } => retain_float(*column, pred, dict_cache, *negated, cols, mask),
            ColumnFilter::Text {
                column,
                pred,
                dict_cache,
            } => retain_text(*column, pred, dict_cache, cols, mask),
        }
    }
}

fn keep_row(pass: bool, negated: bool, valid: bool) -> bool {
    valid && if negated { !pass } else { pass }
}

fn retain_int(
    column: u32,
    pred: &Pred,
    dict_cache: &mut Option<(Arc<Vec<i64>>, Vec<u8>)>,
    negated: bool,
    cols: &Cols<'_>,
    mask: &mut [u32],
) -> Result<(), String> {
    let col = cols
        .get(column as usize)
        .copied()
        .flatten()
        .ok_or("filter column was not decoded")?;
    match col {
        ColRef::I64 { values, nulls } => {
            mask::retain(mask, |row| {
                keep_row(pred.test(values[row]), negated, nulls[row] != 0)
            });
        }
        ColRef::Dict {
            ids,
            nulls,
            entries,
        } => {
            if !matches!(dict_cache, Some((cached, _)) if Arc::ptr_eq(cached, entries)) {
                let table = entries.iter().map(|v| u8::from(pred.test(*v))).collect();
                *dict_cache = Some((Arc::clone(entries), table));
            }
            let table = &dict_cache.as_ref().expect("dict pass table").1;
            mask::retain(mask, |row| {
                keep_row(table[ids[row] as usize] != 0, negated, nulls[row] != 0)
            });
        }
        ColRef::F64 { .. } | ColRef::DictF64 { .. } => {
            return Err("integer filter on float column".to_string());
        }
        ColRef::DictBytes { .. } | ColRef::Utf8 { .. } => {
            return Err("integer filter on string column".to_string());
        }
    }
    Ok(())
}

fn retain_float(
    column: u32,
    pred: &FPred,
    dict_cache: &mut Option<(Arc<Vec<f64>>, Vec<u8>)>,
    negated: bool,
    cols: &Cols<'_>,
    mask: &mut [u32],
) -> Result<(), String> {
    let col = cols
        .get(column as usize)
        .copied()
        .flatten()
        .ok_or("filter column was not decoded")?;
    match col {
        ColRef::F64 { values, nulls } => {
            mask::retain(mask, |row| {
                keep_row(pred.test(values[row]), negated, nulls[row] != 0)
            });
        }
        ColRef::DictF64 {
            ids,
            nulls,
            entries,
        } => {
            if !matches!(dict_cache, Some((cached, _)) if Arc::ptr_eq(cached, entries)) {
                let table = entries.iter().map(|v| u8::from(pred.test(*v))).collect();
                *dict_cache = Some((Arc::clone(entries), table));
            }
            let table = &dict_cache.as_ref().expect("float dict pass table").1;
            mask::retain(mask, |row| {
                keep_row(table[ids[row] as usize] != 0, negated, nulls[row] != 0)
            });
        }
        ColRef::I64 { .. }
        | ColRef::Dict { .. }
        | ColRef::DictBytes { .. }
        | ColRef::Utf8 { .. } => {
            return Err("float filter on a non-float column".to_string());
        }
    }
    Ok(())
}

fn retain_text(
    column: u32,
    pred: &TextPred,
    dict_cache: &mut Option<(ByteDictKey, Vec<u8>)>,
    cols: &Cols<'_>,
    mask: &mut [u32],
) -> Result<(), String> {
    let col = cols
        .get(column as usize)
        .copied()
        .flatten()
        .ok_or("filter column was not decoded")?;
    match col {
        ColRef::Utf8 {
            offsets,
            bytes,
            nulls,
        } => {
            mask::retain(mask, |row| {
                nulls[row] != 0 && test_bytes(pred, utf8_row_slice(offsets, bytes, row))
            });
        }
        ColRef::DictBytes {
            ids,
            nulls,
            offsets,
            bytes,
        } => {
            let key = (Arc::clone(offsets), Arc::clone(bytes));
            let same = matches!(
                dict_cache,
                Some((cached, _)) if Arc::ptr_eq(&cached.0, offsets) && Arc::ptr_eq(&cached.1, bytes)
            );
            if !same {
                let entry_count = offsets.len().saturating_sub(1);
                *dict_cache = Some((key, vec![0xFF; entry_count]));
            }
            let table = &mut dict_cache.as_mut().expect("byte dict pass table").1;
            mask::retain(mask, |row| {
                if nulls[row] == 0 {
                    return false;
                }
                let id = ids[row] as usize;
                if table[id] == 0xFF {
                    table[id] = u8::from(test_bytes(pred, dict_entry_slice(offsets, bytes, id)));
                }
                table[id] != 0
            });
        }
        ColRef::I64 { .. } | ColRef::Dict { .. } | ColRef::F64 { .. } | ColRef::DictF64 { .. } => {
            return Err("string filter on numeric column".to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_literals_fold_to_integer_bounds() {
        let lt = number_pred(CmpOp::Lt, 2.5).unwrap();
        assert!(lt.test(2) && !lt.test(3));
        let ge = number_pred(CmpOp::Ge, -1.5).unwrap();
        assert!(ge.test(-1) && !ge.test(-2));
        assert!(!number_pred(CmpOp::Eq, 2.5).unwrap().test(2));
        assert!(number_pred(CmpOp::Lt, 1e30).unwrap().test(i64::MAX));
        assert!(number_pred(CmpOp::Eq, f64::NAN).is_none());
    }
}
