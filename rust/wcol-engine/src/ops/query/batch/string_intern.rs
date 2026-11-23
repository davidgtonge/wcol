//! Per-query string interner for `GROUP BY` keys (value identity across batches).

use std::cmp::Ordering;

use rustc_hash::FxHashMap;
use xxhash_rust::xxh3::Xxh3;

use super::ColRef;

/// Bytes per arena chunk. Strings go into fixed chunks, never one doubling buffer: Wasm
/// memory never shrinks, and each doubling of a 12 MiB arena left the old copy as a hole
/// too small for the next, which grew a `GROUP BY Title` reducer by a third.
const CHUNK_BYTES: usize = 256 * 1024;
const CHUNK_SHIFT: u32 = CHUNK_BYTES.trailing_zeros();

pub(crate) struct StringInterner {
    /// Chunks in use come first; a string never spans two. One longer than a chunk gets a
    /// chunk of its own.
    chunks: Vec<Vec<u8>>,
    used_chunks: usize,
    /// Per id: chunk index above `CHUNK_SHIFT` bits, start offset below. A string ends where
    /// the next one in its chunk starts, or at the chunk's end.
    starts: Vec<u32>,
    map: FxHashMap<u128, u32>,
    /// Strings whose 128-bit hash is already taken by different bytes (never expected in
    /// practice, but identity must not depend on it).
    collided: FxHashMap<Vec<u8>, u32>,
}

impl StringInterner {
    pub(crate) fn new() -> Self {
        Self {
            chunks: Vec::new(),
            used_chunks: 0,
            starts: Vec::new(),
            map: FxHashMap::default(),
            collided: FxHashMap::default(),
        }
    }

    /// One id per distinct byte string, stable for the life of the interner.
    pub(crate) fn intern(&mut self, bytes: &[u8]) -> u32 {
        let hash = hash128(bytes);
        let taken = match self.map.get(&hash) {
            Some(&id) if self.bytes(id) == bytes => return id,
            Some(_) => true,
            None => false,
        };
        if taken {
            if let Some(&id) = self.collided.get(bytes) {
                return id;
            }
        }
        let id = u32::try_from(self.starts.len()).expect("more than 2^32 distinct strings");
        let start = self.append(bytes);
        self.starts.push(start);
        if taken {
            self.collided.insert(bytes.to_vec(), id);
        } else {
            self.map.insert(hash, id);
        }
        id
    }

    /// Copy `bytes` into the arena and return its packed start.
    fn append(&mut self, bytes: &[u8]) -> u32 {
        let fits = self.used_chunks > 0 && {
            let chunk = &self.chunks[self.used_chunks - 1];
            // Strictly inside: a start equal to the chunk size would overflow its offset bits.
            chunk.capacity() == CHUNK_BYTES
                && chunk.len() < CHUNK_BYTES
                && chunk.len() + bytes.len() <= CHUNK_BYTES
        };
        if !fits {
            self.open_chunk(bytes.len());
        }
        let index = self.used_chunks - 1;
        let chunk = &mut self.chunks[index];
        let start = chunk.len();
        chunk.extend_from_slice(bytes);
        let index = u32::try_from(index)
            .ok()
            .filter(|&index| index < 1 << (32 - CHUNK_SHIFT))
            .expect("string interner arena past 4 GiB");
        (index << CHUNK_SHIFT) | start as u32
    }

    /// Start the next chunk, reusing a retained one when the string fits it.
    fn open_chunk(&mut self, len: usize) {
        if len > CHUNK_BYTES {
            self.chunks
                .insert(self.used_chunks, Vec::with_capacity(len));
        } else if self.used_chunks == self.chunks.len() {
            self.chunks.push(Vec::with_capacity(CHUNK_BYTES));
        }
        self.used_chunks += 1;
    }

    /// The id of `bytes` if it is already interned. Never inserts.
    pub(crate) fn lookup(&self, bytes: &[u8]) -> Option<u32> {
        match self.map.get(&hash128(bytes)) {
            Some(&id) if self.bytes(id) == bytes => Some(id),
            Some(_) => self.collided.get(bytes).copied(),
            None => None,
        }
    }

    /// Distinct byte strings interned so far. Nulls are never interned.
    pub(crate) fn len(&self) -> usize {
        self.starts.len()
    }

    /// Drop interned strings but keep arena and map allocations for the next run.
    pub(crate) fn clear_retaining_capacity(&mut self) {
        // Keep the standard chunks; a long string's own chunk fits only its size.
        self.chunks.retain(|chunk| chunk.capacity() == CHUNK_BYTES);
        for chunk in &mut self.chunks {
            chunk.clear();
        }
        self.used_chunks = 0;
        self.starts.clear();
        self.map.clear();
        self.collided.clear();
    }

    /// Drop arena and map allocations. Ids from the previous run are no longer valid.
    pub(crate) fn release_excess_capacity(&mut self) {
        *self = Self::new();
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.chunks
            .iter()
            .fold(0usize, |sum, chunk| sum.saturating_add(chunk.capacity()))
            + self.starts.capacity() * std::mem::size_of::<u32>()
            + hashmap_approx_bytes(&self.map)
            + self.collided.iter().fold(0usize, |sum, (bytes, _)| {
                sum.saturating_add(bytes.capacity())
            })
            + hashmap_approx_bytes(&self.collided)
    }

    /// Live interned payload, excluding retained capacity after a flush.
    pub(crate) fn occupied_bytes(&self) -> usize {
        self.chunks[..self.used_chunks]
            .iter()
            .fold(0usize, |sum, chunk| sum.saturating_add(chunk.len()))
            + self.starts.len() * std::mem::size_of::<u32>()
            + self
                .collided
                .iter()
                .fold(0usize, |sum, (bytes, _)| sum.saturating_add(bytes.len()))
    }

    pub(crate) fn bytes(&self, id: u32) -> &[u8] {
        let packed = self.starts[id as usize];
        let chunk = &self.chunks[(packed >> CHUNK_SHIFT) as usize];
        let start = (packed & (CHUNK_BYTES as u32 - 1)) as usize;
        let end = match self.starts.get(id as usize + 1) {
            Some(&next) if next >> CHUNK_SHIFT == packed >> CHUNK_SHIFT => {
                (next & (CHUNK_BYTES as u32 - 1)) as usize
            }
            _ => chunk.len(),
        };
        &chunk[start..end]
    }

    pub(crate) fn cmp_ids(&self, left: u32, right: u32) -> Ordering {
        self.bytes(left).cmp(self.bytes(right))
    }
}

fn hashmap_approx_bytes<K, V>(map: &FxHashMap<K, V>) -> usize {
    let cap = map.capacity();
    if cap == 0 {
        return 0;
    }
    let buckets = cap.next_power_of_two().max(cap);
    buckets.saturating_add(buckets.saturating_mul(std::mem::size_of::<(K, V)>()))
}

fn hash128(bytes: &[u8]) -> u128 {
    let mut hasher = Xxh3::new();
    hasher.update(bytes);
    hasher.digest128()
}

pub(crate) fn col_string_bytes<'a>(col: ColRef<'a>, row: usize) -> Result<&'a [u8], String> {
    match col {
        ColRef::DictBytes {
            ids,
            offsets,
            bytes,
            ..
        } => Ok(super::string_filter::dict_entry_slice(
            offsets,
            bytes,
            ids[row] as usize,
        )),
        ColRef::Utf8 { offsets, bytes, .. } => {
            Ok(super::string_filter::utf8_row_slice(offsets, bytes, row))
        }
        _ => Err("expected a string column".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_bytes_same_id_and_collisions_keep_identity() {
        let mut interner = StringInterner::new();
        let a = interner.intern(b"alpha");
        let b = interner.intern(b"beta");
        assert_eq!(interner.intern(b"alpha"), a);
        assert_ne!(a, b);
        // Force a collision: map "gamma"'s hash to "alpha"'s id.
        let gamma_hash = hash128(b"gamma");
        interner.map.insert(gamma_hash, a);
        let g = interner.intern(b"gamma");
        assert_ne!(g, a);
        assert_eq!(interner.intern(b"gamma"), g);
        assert_eq!(interner.intern(b"alpha"), a);
        assert_eq!(interner.bytes(g), b"gamma");
    }

    #[test]
    fn strings_keep_their_bytes_across_chunks_and_runs() {
        let mut interner = StringInterner::new();
        let long = vec![b'x'; CHUNK_BYTES + 10];
        let strings: Vec<Vec<u8>> = (0..20_000u32)
            .map(|i| format!("title-{i}-{}", "y".repeat((i % 50) as usize)).into_bytes())
            .chain([
                vec![b'z'; CHUNK_BYTES - 7],
                Vec::new(),
                long.clone(),
                Vec::new(),
            ])
            .chain([b"after".to_vec()])
            .collect();
        for _ in 0..2 {
            let ids: Vec<u32> = strings.iter().map(|s| interner.intern(s)).collect();
            for (id, bytes) in ids.iter().zip(&strings) {
                assert_eq!(interner.bytes(*id), bytes.as_slice());
                assert_eq!(interner.lookup(bytes), Some(*id));
            }
            assert!(interner.chunks.len() > 2, "strings did not span chunks");
            interner.clear_retaining_capacity();
        }
        assert!(interner
            .chunks
            .iter()
            .all(|chunk| chunk.capacity() == CHUNK_BYTES));
    }
}
