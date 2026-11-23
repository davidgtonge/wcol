//! Group storage that grows in fixed chunks.
//!
//! Wasm memory never shrinks. A doubling `Vec` frees each old copy as a hole too small for
//! the next doubling, so a reducer's table left about a third of its memory as unusable
//! holes. Equal chunks never copy on growth, and a freed chunk fits the next one exactly.

use std::ops::{Index, IndexMut};

/// Groups per storage chunk.
pub(super) const CHUNK_GROUPS: usize = 4096;
/// Groups the first chunk starts with before doubling up to [`CHUNK_GROUPS`].
const FIRST_GROUPS: usize = 16;

/// Fixed-width records in chunks of [`CHUNK_GROUPS`]. Cleared chunks keep their allocation.
/// A one-wide store indexes by slot.
pub(super) struct Chunked<T> {
    width: usize,
    chunks: Vec<Vec<T>>,
    pub(super) len: usize,
}

impl<T: Clone> Chunked<T> {
    pub(super) fn new(width: usize) -> Self {
        Self {
            width,
            chunks: Vec::new(),
            len: 0,
        }
    }

    pub(super) fn push(&mut self, record: &[T]) {
        debug_assert_eq!(record.len(), self.width);
        let chunk = self.len / CHUNK_GROUPS;
        let full = CHUNK_GROUPS * self.width;
        if chunk == self.chunks.len() {
            // Only the first chunk starts small: most tables hold a few groups, and its
            // doublings stay under one chunk. Every later chunk is full size.
            let first = if chunk == 0 {
                FIRST_GROUPS * self.width
            } else {
                full
            };
            self.chunks.push(Vec::with_capacity(first));
        }
        let records = &mut self.chunks[chunk];
        if records.len() == records.capacity() {
            records.reserve_exact(records.capacity().min(full - records.capacity()));
        }
        records.extend_from_slice(record);
        self.len += 1;
    }

    #[inline]
    pub(super) fn get(&self, slot: usize) -> &[T] {
        let at = (slot % CHUNK_GROUPS) * self.width;
        &self.chunks[slot / CHUNK_GROUPS][at..at + self.width]
    }

    #[inline]
    pub(super) fn get_mut(&mut self, slot: usize) -> &mut [T] {
        let at = (slot % CHUNK_GROUPS) * self.width;
        &mut self.chunks[slot / CHUNK_GROUPS][at..at + self.width]
    }

    /// Records in slot order (one-wide stores).
    pub(super) fn iter(&self) -> impl Iterator<Item = &T> {
        debug_assert_eq!(self.width, 1);
        self.chunks.iter().flat_map(|chunk| chunk.iter())
    }

    pub(super) fn clear(&mut self) {
        for chunk in &mut self.chunks {
            chunk.clear();
        }
        self.len = 0;
    }

    pub(super) fn allocated_bytes(&self) -> usize {
        let records: usize = self.chunks.iter().map(Vec::capacity).sum();
        records * std::mem::size_of::<T>() + self.chunks.capacity() * std::mem::size_of::<Vec<T>>()
    }
}

impl<T: Clone> Index<usize> for Chunked<T> {
    type Output = T;

    #[inline]
    fn index(&self, slot: usize) -> &T {
        debug_assert_eq!(self.width, 1);
        &self.chunks[slot / CHUNK_GROUPS][slot % CHUNK_GROUPS]
    }
}

impl<T: Clone> IndexMut<usize> for Chunked<T> {
    #[inline]
    fn index_mut(&mut self, slot: usize) -> &mut T {
        debug_assert_eq!(self.width, 1);
        &mut self.chunks[slot / CHUNK_GROUPS][slot % CHUNK_GROUPS]
    }
}

#[cfg(test)]
mod tests {
    use super::{Chunked, CHUNK_GROUPS};

    #[test]
    fn slots_cross_chunks_and_cleared_chunks_are_reused() {
        let mut store = Chunked::new(1);
        let n = 2 * CHUNK_GROUPS + 5;
        for slot in 0..n {
            store.push(&[slot as u64]);
        }
        assert_eq!(store.len, n);
        assert!((0..n).all(|slot| store[slot] == slot as u64));
        assert!(store.iter().copied().eq(0..n as u64));
        store[CHUNK_GROUPS] = 7;
        assert_eq!(store.get(CHUNK_GROUPS), &[7]);
        let bytes = store.allocated_bytes();
        store.clear();
        for slot in 0..n {
            store.push(&[slot as u64]);
        }
        assert_eq!(store.allocated_bytes(), bytes, "refill reallocated");
    }
}
