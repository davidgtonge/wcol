use rustc_hash::FxHashMap;

use crate::constants::{PAGE_EXEC_WORDS, PAGE_KIND_DATA, PAGE_KIND_EMPTY, PAGE_KIND_NULL};

pub struct PageMaps {
    pub data_pages: FxHashMap<u32, Vec<u8>>,
    pub null_pages: FxHashMap<u32, Vec<u8>>,
    pub empty_pages: FxHashMap<u32, Vec<u8>>,
}

/// Decompress page payloads from the exec descriptor buffer.
pub fn decompress_pages(descs: &[u32], data: &[u8], rows_in_chunk: usize) -> Result<PageMaps, i32> {
    let mut data_pages = FxHashMap::default();
    let mut null_pages = FxHashMap::default();
    let mut empty_pages = FxHashMap::default();

    for chunk in descs.chunks_exact(PAGE_EXEC_WORDS) {
        let kind = chunk[0];
        let col_id = chunk[1];
        let payload_off = chunk[2] as usize;
        let comp_len = chunk[3] as usize;
        let raw_len = chunk[4] as usize;
        if comp_len == 0 && raw_len != 0 {
            return Err(-16);
        }
        let payload_start = payload_off;
        let payload_end = match payload_start.checked_add(comp_len) {
            Some(end) => end,
            None => return Err(-5),
        };
        if payload_end > data.len() {
            return Err(-5);
        }
        let compressed = &data[payload_start..payload_end];
        let decompressed = if comp_len == raw_len {
            if compressed.len() != raw_len {
                return Err(-6);
            }
            compressed.to_vec()
        } else {
            match lz4_flex::block::decompress(compressed, raw_len) {
                Ok(v) => v,
                Err(_) => return Err(-6),
            }
        };
        if kind == PAGE_KIND_DATA {
            data_pages.insert(col_id, decompressed);
        } else if kind == PAGE_KIND_NULL {
            let needed = (rows_in_chunk + 7) / 8;
            if decompressed.len() < needed {
                return Err(-18);
            }
            null_pages.insert(col_id, decompressed);
        } else if kind == PAGE_KIND_EMPTY {
            let needed = (rows_in_chunk + 7) / 8;
            if decompressed.len() < needed {
                return Err(-18);
            }
            empty_pages.insert(col_id, decompressed);
        }
    }

    Ok(PageMaps {
        data_pages,
        null_pages,
        empty_pages,
    })
}
