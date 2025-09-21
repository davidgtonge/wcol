
use crate::constants::MASK_WORDS;
use crate::timing::{self, Tic};
use crate::types::{LikeMaskStats, PlanTiming};

use super::header::validate_option_a_row_count;
use super::pattern::row_contains_pattern;
use super::preamble::decode_string_preamble;

use crate::decode::mask::{clear_mask_bit, clear_tail_bits, set_mask_bit};

pub(crate) fn decode_raw_string_like_mask(
    raw: &[u8],
    rows: usize,
    pattern: &str,
    negated: bool,
    mut timing: Option<&mut PlanTiming>,
    mut stats: Option<&mut LikeMaskStats>,
) -> Result<Vec<u32>, i32> {
    let pattern_bytes = pattern.as_bytes();
    if pattern_bytes.is_empty() {
        validate_option_a_row_count(raw, rows)?;
        let mut mask = if negated {
            vec![0u32; MASK_WORDS]
        } else {
            vec![0xffff_ffff; MASK_WORDS]
        };
        clear_tail_bits(&mut mask, rows);
        return Ok(mask);
    }

    let p = decode_string_preamble(raw, rows, timing.as_deref_mut())?;
    let indices = &p.indices;
    let lcps = &p.lcps;
    let suffix_lens = &p.suffix_lens;
    let suffix_blob = &p.suffix_blob;
    let rows = p.rows;
    let value_count = p.value_count;

    let t_total = Tic::start();
    let t_recon = Tic::start();
    let mut mask = if negated {
        vec![0xffff_ffff; MASK_WORDS]
    } else {
        vec![0u32; MASK_WORDS]
    };
    let mut prev: Vec<u8> = Vec::new();
    let mut boundary_buf: Vec<u8> = Vec::new();
    let mut blob_cursor = 0usize;
    let mut min_prefix_len_with_match_prev: Option<usize> = None;

    let t_verify = Tic::start();
    let mut matches = vec![false; value_count];
    for idx in 0..value_count {
        let (contains, min_prefix_len_with_match) = row_contains_pattern(
            suffix_blob,
            suffix_lens,
            lcps,
            &mut prev,
            &mut boundary_buf,
            &mut blob_cursor,
            idx,
            pattern_bytes,
            min_prefix_len_with_match_prev,
        )?;
        matches[idx] = contains;
        prev.truncate(lcps[idx]);
        prev.extend_from_slice(&suffix_blob[blob_cursor - suffix_lens[idx]..blob_cursor]);
        min_prefix_len_with_match_prev = min_prefix_len_with_match;
    }
    for row in 0..rows {
        let unique_id = *indices.get(row).ok_or(-115)?;
        let contains = *matches.get(unique_id).ok_or(-115)?;
        if contains != negated {
            set_mask_bit(&mut mask, row);
        } else if negated {
            clear_mask_bit(&mut mask, row);
        }
    }
    if let Some(s) = stats.as_deref_mut() {
        s.add_ms_verify(t_verify.elapsed());
        s.set_verify_rows(value_count as u32);
        s.set_blocks_matched(u32::from(matches.iter().any(|v| *v)));
    }

    if blob_cursor != suffix_blob.len() {
        return Err(-121);
    }
    timing::record_elapsed(
        timing.as_deref_mut(),
        |t, ms| t.add_ms_str_reconstruct(ms),
        t_recon,
    );
    if let Some(s) = stats.as_deref_mut() {
        #[cfg(feature = "timing")]
        {
            let total = t_total.elapsed();
            let other = total - s.ms_mask() - s.ms_verify();
            s.add_ms_other(other);
        }
        #[cfg(not(feature = "timing"))]
        let _ = (s, t_total);
    }
    clear_tail_bits(&mut mask, rows);
    Ok(mask)
}
