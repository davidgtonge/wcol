//! Packed row masks (`u32` words, bit `i & 31` of word `i >> 5`), ported from the `.wcol`
//! file kernels and generalised from fixed-size chunks to any batch length.

/// Words needed for `rows` rows.
#[inline]
pub(crate) fn words_for(rows: usize) -> usize {
    rows.div_ceil(32)
}

pub(crate) fn full_mask(rows: usize) -> Vec<u32> {
    let mut mask = vec![u32::MAX; words_for(rows)];
    clear_tail(&mut mask, rows);
    mask
}

pub(crate) fn clear_tail(mask: &mut [u32], rows: usize) {
    let tail = rows & 31;
    if tail != 0 {
        if let Some(last) = mask.get_mut(rows >> 5) {
            *last &= (1u32 << tail) - 1;
        }
    }
    for word in mask.iter_mut().skip(words_for(rows)) {
        *word = 0;
    }
}

pub(crate) fn count(mask: &[u32]) -> u64 {
    mask.iter().map(|w| u64::from(w.count_ones())).sum()
}

pub(crate) fn is_zero(mask: &[u32]) -> bool {
    mask.iter().all(|w| *w == 0)
}

/// Clear bits for rows whose validity byte is `0` (batch nulls are one byte per row).
#[cfg(test)]
pub(crate) fn and_valid_bytes(mask: &mut [u32], nulls: &[u8], rows: usize) {
    let full = rows >> 5;
    for (word_idx, word) in mask.iter_mut().enumerate().take(full) {
        *word &= valid_word(&nulls[word_idx << 5..(word_idx << 5) + 32]);
    }
    if rows & 31 != 0 {
        let base = full << 5;
        let mut bits = 0u32;
        for (bit, valid) in nulls[base..rows].iter().enumerate() {
            bits |= u32::from(*valid != 0) << bit;
        }
        mask[full] &= bits;
    }
}

#[inline]
#[cfg(test)]
fn valid_word(nulls: &[u8]) -> u32 {
    let mut bits = 0u32;
    for (bit, valid) in nulls.iter().enumerate().take(32) {
        bits |= u32::from(*valid != 0) << bit;
    }
    bits
}

/// True when every validity byte in `nulls[..rows]` is set.
pub(crate) fn all_valid(nulls: &[u8], rows: usize) -> bool {
    nulls[..rows].iter().all(|v| *v != 0)
}

/// Drop selected rows for which `keep` is false. Unset bits stay unset.
#[inline]
pub(crate) fn retain(mask: &mut [u32], mut keep: impl FnMut(usize) -> bool) {
    for (word_idx, word) in mask.iter_mut().enumerate() {
        let mut bits = *word;
        let base = word_idx << 5;
        while bits != 0 {
            let bit = bits.trailing_zeros() as usize;
            if !keep(base + bit) {
                *word &= !(1u32 << bit);
            }
            bits &= bits - 1;
        }
    }
}

/// Visit each selected row in ascending order.
#[inline]
pub(crate) fn for_each_set<F: FnMut(usize)>(mask: &[u32], mut f: F) {
    for (word_idx, &word) in mask.iter().enumerate() {
        let mut bits = word;
        let base = word_idx << 5;
        while bits != 0 {
            let bit = bits.trailing_zeros() as usize;
            f(base + bit);
            bits &= bits - 1;
        }
    }
}

/// Build a mask 32 rows at a time from a per-row predicate (branch-free inner loop).
#[inline]
#[cfg(test)]
pub(crate) fn build<T: Copy, F: Fn(T) -> bool>(values: &[T], rows: usize, pred: F) -> Vec<u32> {
    let mut mask = vec![0u32; words_for(rows)];
    let full = rows >> 5;
    for (word_idx, word) in mask.iter_mut().enumerate().take(full) {
        let chunk = &values[word_idx << 5..(word_idx << 5) + 32];
        let mut bits = 0u32;
        for (bit, v) in chunk.iter().enumerate() {
            bits |= u32::from(pred(*v)) << bit;
        }
        *word = bits;
    }
    if rows & 31 != 0 {
        let base = full << 5;
        let mut bits = 0u32;
        for (bit, v) in values[base..rows].iter().enumerate() {
            bits |= u32::from(pred(*v)) << bit;
        }
        mask[full] = bits;
    }
    mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_iterate_round_trip() {
        let values: Vec<i64> = (0..70).collect();
        let mask = build(&values, 70, |v| v % 3 == 0);
        let mut seen = Vec::new();
        for_each_set(&mask, |row| seen.push(row));
        let expected: Vec<usize> = (0..70).filter(|v| v % 3 == 0).collect();
        assert_eq!(seen, expected);
        assert_eq!(count(&mask), expected.len() as u64);
    }

    #[test]
    fn valid_bytes_clear_null_rows() {
        let mut mask = full_mask(40);
        let mut nulls = vec![1u8; 40];
        nulls[3] = 0;
        nulls[35] = 0;
        and_valid_bytes(&mut mask, &nulls, 40);
        assert_eq!(count(&mask), 38);
        let mut seen = Vec::new();
        for_each_set(&mask, |row| seen.push(row));
        assert!(!seen.contains(&3) && !seen.contains(&35));
    }

    #[test]
    fn full_mask_has_no_tail_bits() {
        assert_eq!(full_mask(33), vec![u32::MAX, 1]);
        assert_eq!(count(&full_mask(64)), 64);
    }
}
