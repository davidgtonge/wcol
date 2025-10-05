
use crate::parse::{read_u16, read_usize_advance};

pub(super) fn validate_string_header_option_a(
    raw_len: usize,
    perm_off: usize,
    lcp_off: usize,
    len_off: usize,
    dict_off: usize,
    data_off: usize,
    data_len: usize,
    dict_len: usize,
    rows: usize,
    row_id_width: u8,
    suffix_len_width: u8,
) -> Result<(), i32> {
    if perm_off < 32 || perm_off > raw_len {
        return Err(-105);
    }
    if lcp_off < perm_off || lcp_off > raw_len {
        return Err(-106);
    }
    if len_off < lcp_off || len_off > raw_len {
        return Err(-107);
    }
    if dict_off < len_off || dict_off > raw_len {
        return Err(-108);
    }
    if data_off < dict_off || data_off > raw_len {
        return Err(-109);
    }
    if data_off + data_len > raw_len {
        return Err(-110);
    }
    if dict_off + dict_len > raw_len {
        return Err(-111);
    }
    let row_bytes = row_id_width as usize * rows;
    if perm_off + row_bytes > raw_len {
        return Err(-112);
    }
    if lcp_off != perm_off + row_bytes {
        return Err(-112);
    }
    if !(lcp_off <= len_off && len_off <= dict_off) {
        return Err(-113);
    }
    let lcp_bytes = len_off - lcp_off;
    if lcp_bytes % 2 != 0 {
        return Err(-113);
    }
    let value_count = lcp_bytes / 2;
    let len_bytes = dict_off - len_off;
    if len_bytes % suffix_len_width as usize != 0 {
        return Err(-113);
    }
    if len_bytes / suffix_len_width as usize != value_count {
        return Err(-113);
    }
    Ok(())
}

pub(super) fn validate_option_a_row_count(raw: &[u8], rows: usize) -> Result<(), i32> {
    if raw.len() < 32 {
        return Err(-101);
    }
    let n = read_u16(raw, 0) as usize;
    if n != rows {
        return Err(-102);
    }
    Ok(())
}

pub(super) fn read_indices(
    raw: &[u8],
    perm_off: usize,
    count: usize,
    perm_width: u8,
    max_value: usize,
) -> Result<Vec<usize>, i32> {
    let mut perm: Vec<usize> = Vec::with_capacity(count);
    let mut offset = perm_off;
    for _ in 0..count {
        let value = read_usize_advance(raw, &mut offset, perm_width).map_err(|_| -115)?;
        if value >= max_value {
            return Err(-115);
        }
        perm.push(value);
    }
    Ok(perm)
}

pub(super) fn read_lcps(raw: &[u8], lcp_off: usize, rows: usize) -> Vec<usize> {
    let mut lcps: Vec<usize> = Vec::with_capacity(rows);
    let mut offset = lcp_off;
    for _ in 0..rows {
        lcps.push(read_u16(raw, offset) as usize);
        offset += 2;
    }
    lcps
}

pub(super) fn read_suffix_lens(
    raw: &[u8],
    len_off: usize,
    rows: usize,
    suffix_len_width: u8,
) -> Result<(Vec<usize>, usize), i32> {
    let mut suffix_lens: Vec<usize> = Vec::with_capacity(rows);
    let mut offset = len_off;
    let mut total_suffix = 0usize;
    for _ in 0..rows {
        let len = read_usize_advance(raw, &mut offset, suffix_len_width).map_err(|_| -116)?;
        total_suffix = total_suffix.checked_add(len).ok_or(-116)?;
        suffix_lens.push(len);
    }
    Ok((suffix_lens, total_suffix))
}
