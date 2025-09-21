
use crate::decode::simd::find_subslice;

pub(super) fn row_contains_pattern(
    suffix_blob: &[u8],
    suffix_lens: &[usize],
    lcps: &[usize],
    prev: &[u8],
    boundary_buf: &mut Vec<u8>,
    blob_cursor: &mut usize,
    idx: usize,
    pattern_bytes: &[u8],
    min_prefix_len_with_match_prev: Option<usize>,
) -> Result<(bool, Option<usize>), i32> {
    let lcp = lcps[idx];
    if lcp > prev.len() {
        return Err(-118);
    }
    let suffix_len = suffix_lens[idx];
    if *blob_cursor + suffix_len > suffix_blob.len() {
        return Err(-119);
    }
    let suffix = &suffix_blob[*blob_cursor..*blob_cursor + suffix_len];
    *blob_cursor += suffix_len;
    let mut contains = false;
    let mut min_prefix_len_with_match: Option<usize> = None;
    if let Some(prev_len) = min_prefix_len_with_match_prev {
        if lcp >= prev_len {
            contains = true;
            min_prefix_len_with_match = Some(prev_len);
        }
    }
    if !contains {
        let m = pattern_bytes.len();
        if m <= lcp + suffix_len {
            if m > 1 {
                let start_off = lcp.saturating_sub(m - 1);
                let prefix_tail = &prev[start_off..lcp];
                let suffix_head_len = (m - 1).min(suffix_len);
                boundary_buf.clear();
                boundary_buf.extend_from_slice(prefix_tail);
                boundary_buf.extend_from_slice(&suffix[..suffix_head_len]);
                if boundary_buf.len() >= m {
                    if let Some(pos) = find_subslice(boundary_buf, pattern_bytes) {
                        let full_pos = start_off + pos;
                        contains = true;
                        min_prefix_len_with_match = Some(full_pos + m);
                    }
                }
            }
            if !contains {
                if m == 1 {
                    if let Some(pos) = find_subslice(suffix, pattern_bytes) {
                        let full_pos = lcp + pos;
                        contains = true;
                        min_prefix_len_with_match = Some(full_pos + 1);
                    }
                } else if suffix_len >= m {
                    if let Some(pos) = find_subslice(suffix, pattern_bytes) {
                        let full_pos = lcp + pos;
                        contains = true;
                        min_prefix_len_with_match = Some(full_pos + m);
                    }
                }
            }
        }
    }
    Ok((contains, min_prefix_len_with_match))
}
