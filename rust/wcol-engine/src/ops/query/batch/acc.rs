//! Integer and float aggregate accumulators.

use super::*;

/// Running integer aggregate for one (group, aggregate) pair.
#[derive(Clone, Copy)]
pub(crate) struct Acc {
    pub(super) count: u64,
    pub(super) sum: i128,
    pub(super) min: i64,
    pub(super) max: i64,
}

impl Acc {
    pub(super) const EMPTY: Acc = Acc {
        count: 0,
        sum: 0,
        min: i64::MAX,
        max: i64::MIN,
    };

    #[inline]
    pub(super) fn add(&mut self, v: i64) {
        self.count += 1;
        self.sum += i128::from(v);
        self.min = self.min.min(v);
        self.max = self.max.max(v);
    }

    #[inline]
    pub(super) fn add_n(&mut self, v: i64, n: u64) {
        self.count += n;
        self.sum += i128::from(v) * i128::from(n);
        self.min = self.min.min(v);
        self.max = self.max.max(v);
    }

    pub(super) fn merge(&mut self, other: &Acc) {
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// The accumulator whose [`Acc::to_stat`] is `stat`, so a reducer can merge another
    /// node's partial as if it had seen the rows. Errors when `stat` could not have come from
    /// this aggregate.
    pub(super) fn from_partial(stat: &PartialStat, kind: u8, offset: i128) -> Result<Acc, String> {
        if stat.count == 0 {
            return Ok(Acc::EMPTY);
        }
        if matches!(
            kind,
            KIND_COUNT_STAR | KIND_COUNT | KIND_COUNT_DISTINCT | KIND_APPROX_DISTINCT
        ) {
            return Ok(Acc {
                count: stat.count,
                ..Acc::EMPTY
            });
        }
        let out_of_range = || "partial aggregate is out of range".to_string();
        let unshift = |v: i128| {
            v.checked_sub(offset)
                .and_then(|v| i64::try_from(v).ok())
                .ok_or_else(out_of_range)
        };
        let sum = offset
            .checked_mul(i128::from(stat.count))
            .and_then(|shift| stat.sum_i.checked_sub(shift))
            .ok_or_else(out_of_range)?;
        Ok(Acc {
            count: stat.count,
            sum,
            min: unshift(stat.min_i)?,
            max: unshift(stat.max_i)?,
        })
    }

    /// The same [`Stat`] the row kernel builds by adding these values (plus `offset`) one at
    /// a time.
    pub(super) fn to_stat(self, kind: u8, offset: i128) -> Stat {
        if self.count == 0 {
            return empty_stat(false);
        }
        if matches!(
            kind,
            KIND_COUNT_STAR | KIND_COUNT | KIND_COUNT_DISTINCT | KIND_APPROX_DISTINCT
        ) {
            return count_stat(self.count);
        }
        let (min, max) = (i128::from(self.min) + offset, i128::from(self.max) + offset);
        let sum = self.sum + offset * i128::from(self.count);
        Stat {
            count: self.count,
            sum_i: sum,
            sum_f: exact_f64(sum),
            min_i: min,
            max_i: max,
            min_f: exact_f64(min),
            max_f: exact_f64(max),
            flags: 1 | 4,
            sketch: None,
        }
    }
}

/// Running float aggregate. Min and max are [`float_key`]s so NaN compares as an integer.
#[derive(Clone, Copy)]
pub(crate) struct FAcc {
    pub(super) count: u64,
    pub(super) sum: f64,
    pub(super) min_key: u64,
    pub(super) max_key: u64,
}

impl FAcc {
    pub(super) const EMPTY: FAcc = FAcc {
        count: 0,
        sum: 0.0,
        min_key: 0,
        max_key: 0,
    };

    pub(super) fn empty() -> Self {
        FAcc {
            min_key: nan_key(),
            ..Self::EMPTY
        }
    }

    #[inline]
    pub(super) fn add(&mut self, v: f64) {
        self.count += 1;
        self.sum += v;
        let key = float_key(v);
        self.min_key = self.min_key.min(key);
        self.max_key = self.max_key.max(key);
    }

    pub(super) fn merge(&mut self, other: &FAcc) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = *other;
            return;
        }
        self.count += other.count;
        self.sum += other.sum;
        self.min_key = self.min_key.min(other.min_key);
        self.max_key = self.max_key.max(other.max_key);
    }

    /// The inverse of [`FAcc::to_stat`]. Exact when `offset` is zero; otherwise the shift is
    /// rounded, as the float sum already is when nodes merge in another order.
    pub(super) fn from_partial(stat: &PartialStat, kind: u8, offset: i128) -> FAcc {
        if stat.count == 0 {
            return FAcc::empty();
        }
        if matches!(
            kind,
            KIND_COUNT_STAR | KIND_COUNT | KIND_COUNT_DISTINCT | KIND_APPROX_DISTINCT
        ) {
            return FAcc {
                count: stat.count,
                ..FAcc::empty()
            };
        }
        let off = offset as f64;
        FAcc {
            count: stat.count,
            sum: stat.sum_f - off * stat.count as f64,
            min_key: float_key(stat.min_f - off),
            max_key: float_key(stat.max_f - off),
        }
    }

    pub(super) fn to_stat(self, kind: u8, offset: i128) -> Stat {
        if self.count == 0 {
            return empty_stat(false);
        }
        if matches!(
            kind,
            KIND_COUNT_STAR | KIND_COUNT | KIND_COUNT_DISTINCT | KIND_APPROX_DISTINCT
        ) {
            return count_stat(self.count);
        }
        let off = offset as f64;
        let n = self.count as f64;
        Stat {
            count: self.count,
            sum_i: 0,
            sum_f: self.sum + off * n,
            min_i: 0,
            max_i: 0,
            min_f: canonical_f64(key_to_f64(self.min_key) + off),
            max_f: canonical_f64(key_to_f64(self.max_key) + off),
            flags: 0,
            sketch: None,
        }
    }
}
