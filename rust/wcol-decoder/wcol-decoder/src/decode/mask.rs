
use crate::constants::MASK_WORDS;

#[inline]
pub(super) fn set_mask_bit(mask: &mut [u32], idx: usize) {
    let word = idx >> 5;
    let bit = idx & 31;
    mask[word] |= 1 << bit;
}

#[inline]
pub(super) fn clear_mask_bit(mask: &mut [u32], idx: usize) {
    let word = idx >> 5;
    let bit = idx & 31;
    mask[word] &= !(1 << bit);
}

pub(super) fn clear_tail_bits(mask: &mut [u32], rows: usize) {
    let full_words = rows / 32;
    let tail_bits = rows % 32;
    if tail_bits == 0 {
        for word in mask.iter_mut().take(MASK_WORDS).skip(full_words) {
            *word = 0;
        }
        return;
    }
    if full_words < MASK_WORDS {
        let keep = (1u32 << tail_bits) - 1;
        mask[full_words] &= keep;
    }
    for word in mask.iter_mut().take(MASK_WORDS).skip(full_words + 1) {
        *word = 0;
    }
}
