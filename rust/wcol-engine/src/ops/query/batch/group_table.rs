//! The group hash table and top-k slot selection.

use super::*;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct GroupKey {
    pub(super) nulls: u8,
    pub(super) values: [i64; MAX_KEYS],
}

#[inline]
pub(super) fn hash_group_key(key: &GroupKey) -> u64 {
    FxBuildHasher.hash_one(key)
}

/// Groups in first-seen order plus their accumulators, one column per aggregate
/// (`accs[agg][slot]`). Everything but the index grows in fixed chunks.
pub(crate) struct GroupTable {
    /// Slots only: each key is stored once, in `keys`.
    pub(super) index: HashTable<u32>,
    pub(super) keys: Chunked<GroupKey>,
    pub(super) accs: Vec<Chunked<Acc>>,
    pub(super) accs_f: Vec<Chunked<FAcc>>,
    /// `approx_count_distinct` sketches, parallel to `accs` (allocated on first value).
    pub(super) sketches: Vec<Chunked<Option<Hll>>>,
    /// Some aggregate is `approx_count_distinct`; otherwise every sketch stays `None`.
    pub(super) sketched: bool,
    pub(super) aggs: usize,
    /// Single dictionary key: group slot per dictionary id for the current dictionary.
    pub(super) dict_slots: Vec<u32>,
    pub(super) dict: Option<Arc<Vec<i64>>>,
    pub(super) dict_f: Option<Arc<Vec<f64>>>,
    pub(super) dict_bytes: Option<ByteDictKey>,
}

impl GroupTable {
    pub(super) fn new(aggs: usize, sketched: bool) -> Self {
        Self {
            index: HashTable::new(),
            keys: Chunked::new(1),
            accs: (0..aggs).map(|_| Chunked::new(1)).collect(),
            accs_f: (0..aggs).map(|_| Chunked::new(1)).collect(),
            sketches: (0..aggs).map(|_| Chunked::new(1)).collect(),
            sketched,
            aggs,
            dict_slots: Vec::new(),
            dict: None,
            dict_f: None,
            dict_bytes: None,
        }
    }

    pub(crate) fn slot(&mut self, key: GroupKey) -> u32 {
        let next = self.keys.len as u32;
        let keys = &self.keys;
        match self.index.entry(
            hash_group_key(&key),
            |&slot| keys[slot as usize] == key,
            |&slot| hash_group_key(&keys[slot as usize]),
        ) {
            HashEntry::Occupied(entry) => return *entry.get(),
            HashEntry::Vacant(entry) => {
                entry.insert(next);
            }
        }
        self.keys.push(&[key]);
        for agg in 0..self.aggs {
            self.accs[agg].push(&[Acc::EMPTY]);
            self.accs_f[agg].push(&[FAcc::empty()]);
            self.sketches[agg].push(&[None]);
        }
        next
    }

    pub(crate) fn get_slot(&self, key: &GroupKey) -> Option<u32> {
        let keys = &self.keys;
        self.index
            .find(hash_group_key(key), |&slot| keys[slot as usize] == *key)
            .copied()
    }

    pub(crate) fn len(&self) -> usize {
        self.keys.len
    }

    /// Drop groups but keep table allocations so the next run reuses them.
    ///
    /// Dictionary identity stays: a flush can happen mid-batch, and the current
    /// `dict_slots[id] = slot` write still has to be in-bounds. Slot numbers are
    /// stale after reset, so every id becomes `NO_SLOT`.
    pub(crate) fn clear_retaining_capacity(&mut self) {
        self.index.clear();
        self.keys.clear();
        self.accs.iter_mut().for_each(Chunked::clear);
        self.accs_f.iter_mut().for_each(Chunked::clear);
        self.sketches.iter_mut().for_each(Chunked::clear);
        for slot in &mut self.dict_slots {
            *slot = NO_SLOT;
        }
    }

    /// Drop the hash table and group vectors. `dict_slots` stays so a flush
    /// mid-batch can still write `dict_slots[id]`.
    pub(crate) fn release_excess_capacity(&mut self) {
        let aggs = self.aggs;
        let sketched = self.sketched;
        let dict_slots = std::mem::take(&mut self.dict_slots);
        let dict = self.dict.take();
        let dict_f = self.dict_f.take();
        let dict_bytes = self.dict_bytes.take();
        *self = Self::new(aggs, sketched);
        self.dict_slots = dict_slots;
        self.dict = dict;
        self.dict_f = dict_f;
        self.dict_bytes = dict_bytes;
    }

    pub(crate) fn index_capacity(&self) -> usize {
        self.index.capacity()
    }

    /// Allocated bytes, including hash-map capacity. Called for every new group while
    /// spilling is possible, so it must not walk the groups: summing sketch sizes did, and
    /// made `GROUP BY URL` ingest quadratic (96% of reducer CPU). Only queries with
    /// `approx_count_distinct` pay that walk.
    pub(crate) fn resident_bytes(&self) -> usize {
        let sketches = if self.sketched {
            self.sketches
                .iter()
                .flat_map(Chunked::iter)
                .fold(0usize, |sum, sketch| {
                    sum.saturating_add(sketch.as_ref().map_or(0, Hll::resident_bytes))
                })
        } else {
            0
        };
        let columns = |sum: usize, bytes: usize| sum.saturating_add(bytes);
        // A bucket is one control byte and one `u32` slot.
        self.index.capacity() * (1 + std::mem::size_of::<u32>())
            + self.keys.allocated_bytes()
            + self
                .accs
                .iter()
                .map(Chunked::allocated_bytes)
                .fold(0, columns)
            + self
                .accs_f
                .iter()
                .map(Chunked::allocated_bytes)
                .fold(0, columns)
            + self
                .sketches
                .iter()
                .map(Chunked::allocated_bytes)
                .fold(0, columns)
            + sketches
    }

    pub(super) fn null_key(keys: usize) -> GroupKey {
        GroupKey {
            nulls: ((1u16 << keys) - 1) as u8,
            values: [0; MAX_KEYS],
        }
    }

    pub(super) fn value_key(value: i64) -> GroupKey {
        let mut values = [0; MAX_KEYS];
        values[0] = value;
        GroupKey { nulls: 0, values }
    }
}

/// The pair key of `row`. Text parts become ids through `text_id`; a global text distinct
/// (`hash_value`) keys part 0 by the value's hash. `Ok(None)` when `text_id` has no id, which
/// the spill path uses to look up a key without inserting into the interner.
pub(super) fn pair_key(
    key_cols: &[Option<ColRef<'_>>; 3],
    text_parts: u32,
    hash_value: bool,
    row: usize,
    mut text_id: impl FnMut(&[u8]) -> Option<i64>,
) -> Result<Option<PairKey>, String> {
    let mut nulls = 0u8;
    let mut parts = [0i64; 3];
    for (part, col) in key_cols.iter().enumerate() {
        let Some(col) = col else { continue };
        if col.nulls()[row] == 0 {
            nulls |= 1 << part;
        } else if hash_value && part == 0 {
            parts[part] = text_hash(col_string_bytes(*col, row)?);
        } else if text_parts & (1 << part) != 0 {
            match text_id(col_string_bytes(*col, row)?) {
                Some(id) => parts[part] = id,
                None => return Ok(None),
            }
        } else {
            parts[part] = col.value(row);
        }
    }
    Ok(Some(PairKey {
        nulls,
        a: parts[0],
        b: parts[1],
        c: parts[2],
    }))
}

/// One row's aggregates into its pair group. COUNT(DISTINCT) is a presence count.
pub(super) fn bump_pair(
    pairs: &mut PairGroups,
    slot: u32,
    kind: &[u8; 8],
    value_cols: &[Option<ColRef<'_>>; 8],
    n_aggs: usize,
    row: usize,
) {
    for index in 0..n_aggs {
        if kind[index] == KIND_COUNT_STAR {
            pairs.bump(slot, index);
            continue;
        }
        let col = value_cols[index].expect("aggregate column");
        if col.nulls()[row] == 0 {
            continue;
        }
        if kind[index] == KIND_COUNT || kind[index] == KIND_COUNT_DISTINCT {
            pairs.bump(slot, index);
        } else {
            pairs.add(slot, index, col.value(row));
        }
    }
}

/// Deterministic 64-bit hash of a text value (fixed-key SipHash). Two equal strings always get
/// the same key within one build; distinct strings collide with probability about n^2 / 2^65.
pub(super) fn text_hash(bytes: &[u8]) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish() as i64
}

/// One part of a pair key, in numeric order (`NULL` first). Text parts compare by interner id
/// through [`BatchQuery::cmp_pair_key`].
pub(super) fn cmp_key_part(a: &PairKey, b: &PairKey, part: usize) -> std::cmp::Ordering {
    let a_null = a.nulls & (1 << part) != 0;
    let b_null = b.nulls & (1 << part) != 0;
    match (a_null, b_null) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => a.part(part).cmp(&b.part(part)),
    }
}

pub(super) fn cmp_key2(a: &PairKey, b: &PairKey) -> std::cmp::Ordering {
    (0..3)
        .map(|part| cmp_key_part(a, b, part))
        .find(|ord| ord.is_ne())
        .unwrap_or(std::cmp::Ordering::Equal)
}

/// Min-heap of the `keep` slots that `above` ranks highest. The root is the worst of those kept.
pub(super) fn keep_top_slots(
    slot_count: u32,
    keep: usize,
    mut above: impl FnMut(u32, u32) -> bool,
) -> Vec<u32> {
    let mut heap = Vec::with_capacity(keep);
    for slot in 0..slot_count {
        if heap.len() < keep {
            heap.push(slot);
            let last = heap.len() - 1;
            sift_worst_up(&mut heap, last, &mut above);
        } else if above(slot, heap[0]) {
            heap[0] = slot;
            sift_worst_down(&mut heap, &mut above);
        }
    }
    heap.sort_by(|&a, &b| {
        if above(a, b) {
            std::cmp::Ordering::Less
        } else if above(b, a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
    heap
}

pub(super) fn sift_worst_up(
    heap: &mut [u32],
    mut index: usize,
    above: &mut impl FnMut(u32, u32) -> bool,
) {
    while index > 0 {
        let parent = (index - 1) / 2;
        if above(heap[parent], heap[index]) {
            heap.swap(parent, index);
            index = parent;
        } else {
            break;
        }
    }
}

pub(super) fn sift_worst_down(heap: &mut [u32], above: &mut impl FnMut(u32, u32) -> bool) {
    let mut index = 0;
    loop {
        let left = 2 * index + 1;
        if left >= heap.len() {
            break;
        }
        let right = left + 1;
        let mut worst = left;
        if right < heap.len() && above(heap[left], heap[right]) {
            worst = right;
        }
        if above(heap[index], heap[worst]) {
            heap.swap(index, worst);
            index = worst;
        } else {
            break;
        }
    }
}
