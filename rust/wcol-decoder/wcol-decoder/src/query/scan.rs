use crate::query::mask::{iter_mask, mask_is_full};

#[inline]
pub(crate) fn for_each_row<F>(mask: &[u32], rows: usize, mut f: F)
where
    F: FnMut(usize),
{
    if mask_is_full(mask, rows) {
        for row in 0..rows {
            f(row);
        }
    } else {
        for row in iter_mask(mask, rows) {
            f(row);
        }
    }
}

#[inline]
pub(crate) fn for_each_value<T, F>(values: &[T], mask: &[u32], rows: usize, mut f: F)
where
    T: Copy,
    F: FnMut(T),
{
    if mask_is_full(mask, rows) {
        for &val in values.iter().take(rows) {
            f(val);
        }
    } else {
        for row in iter_mask(mask, rows) {
            f(values[row]);
        }
    }
}
