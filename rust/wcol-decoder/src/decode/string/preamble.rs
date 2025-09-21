#![allow(dead_code)]

use crate::timing::{self, Tic};
use crate::types::PlanTiming;

use super::header::{read_indices, read_lcps, read_suffix_lens, validate_option_a_row_count, validate_string_header_option_a};
use super::token::{decode_full_token_stream, parse_token_dict};

use crate::parse::read_u32;

const STRING_LAYOUT_OPTION_A_FLAG: u8 = 0x80;

pub(super) struct StringColumnPreamble {
    pub indices: Vec<usize>,
    pub lcps: Vec<usize>,
    pub suffix_lens: Vec<usize>,
    pub suffix_blob: Vec<u8>,
    pub data_off: usize,
    pub data_len: usize,
    pub rows: usize,
    pub value_count: usize,
}

pub(super) fn decode_string_preamble(
    raw: &[u8],
    rows: usize,
    mut timing: Option<&mut PlanTiming>,
) -> Result<StringColumnPreamble, i32> {
    validate_option_a_row_count(raw, rows)?;
    let t_perm = Tic::start();
    let perm_width_raw = raw[2];
    if (perm_width_raw & 0x80) != 0 || (perm_width_raw & 0x40) != 0 {
        return Err(-126);
    }
    let perm_width = perm_width_raw & 0x3f;
    let suffix_len_raw = raw[3];
    if (suffix_len_raw & STRING_LAYOUT_OPTION_A_FLAG) == 0 {
        return Err(-128);
    }
    let suffix_len_width = suffix_len_raw & !STRING_LAYOUT_OPTION_A_FLAG;
    if perm_width != 2 && perm_width != 4 {
        return Err(-103);
    }
    if suffix_len_width != 2 && suffix_len_width != 4 {
        return Err(-104);
    }
    let perm_off = read_u32(raw, 4) as usize;
    let lcp_off = read_u32(raw, 8) as usize;
    let len_off = read_u32(raw, 12) as usize;
    let data_off = read_u32(raw, 16) as usize;
    let data_len = read_u32(raw, 20) as usize;
    let dict_off = read_u32(raw, 24) as usize;
    let dict_len = read_u32(raw, 28) as usize;

    validate_string_header_option_a(
        raw.len(),
        perm_off,
        lcp_off,
        len_off,
        dict_off,
        data_off,
        data_len,
        dict_len,
        rows,
        perm_width,
        suffix_len_width,
    )?;
    let value_count = (len_off - lcp_off) / 2;
    if value_count == 0 && rows > 0 {
        return Err(-113);
    }
    let indices = read_indices(raw, perm_off, rows, perm_width, value_count.max(1))?;
    let lcps = read_lcps(raw, lcp_off, value_count);
    let (suffix_lens, _total_suffix) = read_suffix_lens(raw, len_off, value_count, suffix_len_width)?;

    timing::record_elapsed(
        timing.as_deref_mut(),
        |t, ms| t.add_ms_str_perm(ms),
        t_perm,
    );

    let t_token = Tic::start();
    let token_dict = if dict_len > 0 {
        parse_token_dict(&raw[dict_off..dict_off + dict_len])?
    } else {
        Vec::new()
    };

    let total_suffix: usize = suffix_lens.iter().copied().sum();
    if token_dict.is_empty() && total_suffix != data_len {
        return Err(-117);
    }

    let data = &raw[data_off..data_off + data_len];
    let suffix_blob = if token_dict.is_empty() {
        if data.len() != total_suffix {
            return Err(-119);
        }
        data.to_vec()
    } else {
        decode_full_token_stream(data, &token_dict, total_suffix)?
    };
    timing::record_elapsed(
        timing.as_deref_mut(),
        |t, ms| t.add_ms_str_token(ms),
        t_token,
    );

    Ok(StringColumnPreamble {
        indices,
        lcps,
        suffix_lens,
        suffix_blob,
        data_off,
        data_len,
        rows,
        value_count,
    })
}
