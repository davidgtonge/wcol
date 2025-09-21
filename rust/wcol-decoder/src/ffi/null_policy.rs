use rustc_hash::FxHashMap;

use crate::runtime::{is_valid, maybe_apply_validity_mask, nullable_row_bitmap};

pub(crate) struct NullBitmapCache<'a> {
    null_pages: &'a FxHashMap<u32, Vec<u8>>,
    rows: usize,
    cache: FxHashMap<u32, Option<&'a [u8]>>,
}

impl<'a> NullBitmapCache<'a> {
    pub(crate) fn new(null_pages: &'a FxHashMap<u32, Vec<u8>>, rows: usize) -> Self {
        Self {
            null_pages,
            rows,
            cache: FxHashMap::default(),
        }
    }

    pub(crate) fn for_col(&mut self, col_id: u32) -> Option<&'a [u8]> {
        if let Some(bitmap) = self.cache.get(&col_id) {
            return *bitmap;
        }
        let bitmap = nullable_row_bitmap(
            self.null_pages.get(&col_id).map(|page| page.as_slice()),
            self.rows,
        );
        self.cache.insert(col_id, bitmap);
        bitmap
    }

    pub(crate) fn apply_to_mask(&mut self, mask: Vec<u32>, col_id: u32) -> Result<Vec<u32>, ()> {
        maybe_apply_validity_mask(mask, self.for_col(col_id), self.rows)
    }
}

pub(crate) fn row_is_valid_for_all(row: usize, bitmaps: &[Option<&[u8]>]) -> bool {
    for bitmap in bitmaps {
        if let Some(page) = bitmap {
            if !is_valid(page, row) {
                return false;
            }
        }
    }
    true
}
