//! Group storage for two integer keys. Reducers hold hundreds of thousands of
//! near-unique groups (eg. `GROUP BY WatchID, ClientIP`) in a 128 MiB isolate, so
//! each group costs only what it needs:
//!
//! - The index holds slot numbers; each key is stored once, in `keys`.
//! - A count aggregate keeps one word; any other keeps count, sum, min and max.
//! - Storage grows in fixed chunks, so growth never copies or leaves a doubled tail.

use std::hash::BuildHasher;

use hashbrown::hash_table::{Entry, HashTable};
use rustc_hash::FxBuildHasher;

use super::chunked::Chunked;
use super::{Acc, AggRef, KIND_APPROX_DISTINCT, KIND_COUNT, KIND_COUNT_DISTINCT, KIND_COUNT_STAR};

/// Up to three `i64` key parts. Not packed into one `u64`: nulls stay a separate byte (bit
/// `i` set means part `i` is NULL). Parts a query does not have stay zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct PairKey {
    pub(super) nulls: u8,
    pub(super) a: i64,
    pub(super) b: i64,
    pub(super) c: i64,
}

impl PairKey {
    pub(super) fn part(&self, part: usize) -> i64 {
        match part {
            0 => self.a,
            1 => self.b,
            _ => self.c,
        }
    }
}

/// `count, sum low, sum high, min, max`.
const FULL_WORDS: usize = 5;

/// Where one aggregate's words sit in a group record.
#[derive(Clone, Copy)]
enum Lane {
    Count { at: usize },
    Full { at: usize },
}

pub(super) struct PairGroups {
    index: HashTable<u32>,
    keys: Chunked<PairKey>,
    words: Chunked<u64>,
    lanes: Vec<Lane>,
    /// A new group's record.
    empty: Vec<u64>,
}

#[inline]
fn hash(key: &PairKey) -> u64 {
    FxBuildHasher.hash_one(key)
}

impl PairGroups {
    pub(super) fn for_keys(
        keys: &[u32],
        text: u32,
        float_keys: u32,
        agg_float: &[bool],
        aggs: &[AggRef],
    ) -> Option<Self> {
        // Text may be a group key, never the counted (last) key, and never alone: a global text
        // distinct hashes its values instead (see `text_distinct`).
        if keys.is_empty() || keys.len() > 3 || float_keys != 0 {
            return None;
        }
        let last = 1u32 << (keys.len() - 1);
        if text != 0 && (keys.len() == 1 || text >= (1 << keys.len()) || text & last != 0) {
            return None;
        }
        if agg_float.iter().any(|float| *float) {
            return None;
        }
        // With COUNT(DISTINCT x) the keys are (group, x): the distinct lane is a presence count
        // and SUM/COUNT/AVG lanes hold per-pair partials. Folding x away (Q9) sums those
        // partials and counts the pairs, so a mixed aggregate fits the same table.
        let distinct = aggs
            .iter()
            .filter(|agg| agg.kind == KIND_COUNT_DISTINCT)
            .count();
        if aggs.iter().any(|agg| agg.kind == KIND_APPROX_DISTINCT) {
            return None;
        }
        if keys.len() == 1 && distinct == 0 {
            return None;
        }
        // Three keys are the grouped distinct of two columns (Q11): only with distinct aggregates.
        if keys.len() == 3 && distinct == 0 {
            return None;
        }
        let mut at = 0;
        let lanes = aggs
            .iter()
            .map(|agg| {
                let lane = if matches!(agg.kind, KIND_COUNT_STAR | KIND_COUNT) {
                    Lane::Count { at }
                } else {
                    Lane::Full { at }
                };
                at += match lane {
                    Lane::Count { .. } => 1,
                    Lane::Full { .. } => FULL_WORDS,
                };
                lane
            })
            .collect();
        Some(Self::new(lanes))
    }

    fn new(lanes: Vec<Lane>) -> Self {
        let mut empty = Vec::new();
        for lane in &lanes {
            match lane {
                Lane::Count { .. } => empty.push(0),
                Lane::Full { .. } => {
                    let Acc {
                        count,
                        sum,
                        min,
                        max,
                    } = Acc::EMPTY;
                    empty.extend([
                        count,
                        sum as u64,
                        (sum >> 64) as u64,
                        min as u64,
                        max as u64,
                    ]);
                }
            }
        }
        Self {
            index: HashTable::new(),
            keys: Chunked::new(1),
            words: Chunked::new(empty.len()),
            lanes,
            empty,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.keys.len
    }

    #[inline]
    pub(super) fn key(&self, slot: u32) -> &PairKey {
        &self.keys.get(slot as usize)[0]
    }

    #[inline]
    pub(super) fn get(&self, key: &PairKey) -> Option<u32> {
        let keys = &self.keys;
        self.index
            .find(hash(key), |&slot| keys.get(slot as usize)[0] == *key)
            .copied()
    }

    pub(super) fn slot(&mut self, key: PairKey) -> u32 {
        let next = self.keys.len as u32;
        let keys = &self.keys;
        match self.index.entry(
            hash(&key),
            |&slot| keys.get(slot as usize)[0] == key,
            |&slot| hash(&keys.get(slot as usize)[0]),
        ) {
            Entry::Occupied(entry) => return *entry.get(),
            Entry::Vacant(entry) => {
                entry.insert(next);
            }
        }
        self.keys.push(&[key]);
        self.words.push(&self.empty);
        next
    }

    /// One more row for a count aggregate, or for any aggregate's non-null count.
    #[inline]
    pub(super) fn bump(&mut self, slot: u32, agg: usize) {
        let at = match self.lanes[agg] {
            Lane::Count { at } | Lane::Full { at } => at,
        };
        self.words.get_mut(slot as usize)[at] += 1;
    }

    /// One non-null value. A count lane only counts it.
    #[inline]
    pub(super) fn add(&mut self, slot: u32, agg: usize, value: i64) {
        let record = self.words.get_mut(slot as usize);
        match self.lanes[agg] {
            Lane::Count { at } => record[at] += 1,
            Lane::Full { at } => {
                let lane = &mut record[at..at + FULL_WORDS];
                let sum = full_sum(lane) + i128::from(value);
                lane[0] += 1;
                lane[1] = sum as u64;
                lane[2] = (sum >> 64) as u64;
                lane[3] = (lane[3] as i64).min(value) as u64;
                lane[4] = (lane[4] as i64).max(value) as u64;
            }
        }
    }

    /// Merge another table's accumulator for this group. A count lane takes only the count.
    pub(super) fn merge(&mut self, slot: u32, agg: usize, acc: &Acc) {
        if acc.count == 0 {
            return;
        }
        let record = self.words.get_mut(slot as usize);
        match self.lanes[agg] {
            Lane::Count { at } => record[at] += acc.count,
            Lane::Full { at } => {
                let lane = &mut record[at..at + FULL_WORDS];
                let sum = full_sum(lane) + acc.sum;
                lane[0] += acc.count;
                lane[1] = sum as u64;
                lane[2] = (sum >> 64) as u64;
                lane[3] = (lane[3] as i64).min(acc.min) as u64;
                lane[4] = (lane[4] as i64).max(acc.max) as u64;
            }
        }
    }

    #[inline]
    pub(super) fn count(&self, slot: u32, agg: usize) -> u64 {
        match self.lanes[agg] {
            Lane::Count { at } | Lane::Full { at } => self.words.get(slot as usize)[at],
        }
    }

    /// The accumulator the full layout would hold. A count lane's sum, min and max stay empty.
    pub(super) fn acc(&self, slot: u32, agg: usize) -> Acc {
        let record = self.words.get(slot as usize);
        match self.lanes[agg] {
            Lane::Count { at } => Acc {
                count: record[at],
                ..Acc::EMPTY
            },
            Lane::Full { at } => {
                let lane = &record[at..at + FULL_WORDS];
                Acc {
                    count: lane[0],
                    sum: full_sum(lane),
                    min: lane[3] as i64,
                    max: lane[4] as i64,
                }
            }
        }
    }

    pub(super) fn clear_retaining_capacity(&mut self) {
        self.index.clear();
        self.keys.clear();
        self.words.clear();
    }

    pub(super) fn release_excess_capacity(&mut self) {
        *self = Self::new(std::mem::take(&mut self.lanes));
    }

    /// Allocated bytes: index buckets (one control byte plus a `u32` slot each) and chunks.
    pub(super) fn resident_bytes(&self) -> usize {
        let cap = self.index.capacity();
        let buckets = if cap == 0 { 0 } else { cap.next_power_of_two() };
        buckets * (1 + std::mem::size_of::<u32>())
            + self.keys.allocated_bytes()
            + self.words.allocated_bytes()
    }
}

#[inline]
fn full_sum(lane: &[u64]) -> i128 {
    (i128::from(lane[2] as i64) << 64) | i128::from(lane[1])
}

#[cfg(test)]
mod tests {
    use super::super::chunked::CHUNK_GROUPS;
    use super::*;

    fn agg(kind: u8) -> AggRef {
        AggRef { kind, column: 0 }
    }

    #[test]
    fn records_match_the_full_accumulator() {
        let aggs = [agg(KIND_COUNT_STAR), agg(crate::ops::query::KIND_SUM)];
        let mut groups = PairGroups::for_keys(&[0, 1], 0, 0, &[false, false], &aggs).unwrap();
        let key = PairKey {
            nulls: 0,
            a: 7,
            b: -3,
            c: 0,
        };
        let slot = groups.slot(key);
        let mut full = Acc::EMPTY;
        for value in [i64::MAX, i64::MAX, -5] {
            groups.bump(slot, 0);
            groups.add(slot, 1, value);
            full.add(value);
        }
        assert_eq!(groups.slot(key), slot);
        assert_eq!(groups.count(slot, 0), 3);
        let acc = groups.acc(slot, 1);
        assert_eq!(
            (acc.count, acc.sum, acc.min, acc.max),
            (full.count, full.sum, full.min, full.max)
        );
        let count = groups.acc(slot, 0);
        assert_eq!(
            (count.count, count.sum, count.min, count.max),
            (3, 0, i64::MAX, i64::MIN)
        );
    }

    #[test]
    fn slots_survive_chunk_boundaries_and_rehashing() {
        let aggs = [agg(KIND_COUNT_STAR)];
        let mut groups = PairGroups::for_keys(&[0, 1], 0, 0, &[false], &aggs).unwrap();
        let n = 3 * CHUNK_GROUPS as i64 + 17;
        for i in 0..n {
            let slot = groups.slot(PairKey {
                nulls: 0,
                a: i,
                b: i * 31,
                c: 0,
            });
            groups.bump(slot, 0);
        }
        assert_eq!(groups.len(), n as usize);
        for i in (0..n).step_by(997) {
            let key = PairKey {
                nulls: 0,
                a: i,
                b: i * 31,
                c: 0,
            };
            let slot = groups.get(&key).unwrap();
            assert_eq!(*groups.key(slot), key);
            assert_eq!(groups.count(slot, 0), 1);
        }
        assert!(groups
            .get(&PairKey {
                nulls: 1,
                a: 0,
                b: 0,
                c: 0
            })
            .is_none());
        groups.clear_retaining_capacity();
        assert_eq!(groups.len(), 0);
        assert!(groups.resident_bytes() > 0);
        let slot = groups.slot(PairKey {
            nulls: 0,
            a: 1,
            b: 31,
            c: 0,
        });
        assert_eq!((slot, groups.count(slot, 0)), (0, 0));
    }
}
