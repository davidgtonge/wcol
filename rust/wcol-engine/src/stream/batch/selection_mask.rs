//! Per-row selection between colocated filter and aggregate (no column compaction).

/// One bit per row (`1` = selected), packed little-endian into `u32` words:
/// row `i` is bit `i & 31` of word `i >> 5`. Bits past `rows` are always zero.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionMask {
    rows: usize,
    all_selected: bool,
    words: Vec<u32>,
}

impl SelectionMask {
    pub fn all(rows: usize) -> Self {
        Self {
            rows,
            all_selected: true,
            words: Vec::new(),
        }
    }

    pub fn with_rows(rows: usize) -> Self {
        Self {
            rows,
            all_selected: false,
            words: vec![0u32; rows.div_ceil(32)],
        }
    }

    /// Wrap mask words built by a kernel. Extra words are dropped and tail bits cleared.
    pub fn from_words(rows: usize, mut words: Vec<u32>) -> Self {
        words.resize(rows.div_ceil(32), 0);
        let tail = rows & 31;
        if tail != 0 {
            if let Some(last) = words.last_mut() {
                *last &= (1u32 << tail) - 1;
            }
        }
        Self {
            rows,
            all_selected: false,
            words,
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn all_selected(&self) -> bool {
        self.all_selected
    }

    /// Packed words, or `None` when every row is selected.
    pub fn words(&self) -> Option<&[u32]> {
        if self.all_selected {
            None
        } else {
            Some(&self.words)
        }
    }

    pub fn set_selected(&mut self, row: usize, selected: bool) {
        assert!(row < self.rows);
        self.all_selected = false;
        let word = row >> 5;
        let bit = row & 31;
        if selected {
            self.words[word] |= 1 << bit;
        } else {
            self.words[word] &= !(1 << bit);
        }
    }

    pub fn is_selected(&self, row: usize) -> bool {
        if row >= self.rows {
            return false;
        }
        if self.all_selected {
            return true;
        }
        (self.words[row >> 5] >> (row & 31)) & 1 != 0
    }

    pub fn selected_count(&self) -> u64 {
        if self.all_selected {
            return self.rows as u64;
        }
        self.words.iter().map(|w| u64::from(w.count_ones())).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_words_clears_tail_bits() {
        let mask = SelectionMask::from_words(33, vec![u32::MAX, u32::MAX, u32::MAX]);
        assert_eq!(mask.words().unwrap().len(), 2);
        assert_eq!(mask.selected_count(), 33);
        assert!(mask.is_selected(32));
        assert!(!mask.is_selected(33));
    }

    #[test]
    fn set_and_count_match_bit_layout() {
        let mut mask = SelectionMask::with_rows(70);
        mask.set_selected(0, true);
        mask.set_selected(37, true);
        mask.set_selected(69, true);
        assert_eq!(mask.words().unwrap(), &[1, 1 << 5, 1 << 5]);
        assert_eq!(mask.selected_count(), 3);
    }
}
