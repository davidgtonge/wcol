use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::ffi;

use super::error::{NativeError, NativeResult};
use super::types::HeaderInfo;

pub(crate) fn call_status(op: &'static str, code: i32) -> NativeResult<()> {
    if code < 0 {
        return Err(NativeError::Status(op, code));
    }
    Ok(())
}

pub(crate) fn checked_count(op: &'static str, value: i32) -> NativeResult<usize> {
    if value < 0 {
        return Err(NativeError::Status(op, value));
    }
    Ok(value as usize)
}

pub(crate) fn parse_header_info(bytes: &[u8]) -> NativeResult<HeaderInfo> {
    if bytes.len() < 60 {
        return Err(NativeError::Invalid("header info too short"));
    }
    let version = read_u32(bytes, 0);
    if version >= 7 && bytes.len() < 92 {
        return Err(NativeError::Invalid("v7 header info too short"));
    }
    let (schema_off, schema_len, index_off, index_len, dict_off, dict_len, data_off, dict_raw_len) =
        if version >= 7 {
            (
                read_u64(bytes, 28),
                read_u64(bytes, 36),
                read_u64(bytes, 44),
                read_u64(bytes, 52),
                read_u64(bytes, 60),
                read_u64(bytes, 68),
                read_u64(bytes, 76),
                read_u64(bytes, 84),
            )
        } else {
            (
                read_u32(bytes, 28) as u64,
                read_u32(bytes, 32) as u64,
                read_u32(bytes, 36) as u64,
                read_u32(bytes, 40) as u64,
                read_u32(bytes, 44) as u64,
                read_u32(bytes, 48) as u64,
                read_u32(bytes, 52) as u64,
                read_u32(bytes, 56) as u64,
            )
        };
    Ok(HeaderInfo {
        version,
        flags: read_u32(bytes, 4),
        ncols: read_u32(bytes, 8),
        nchunks: read_u32(bytes, 12),
        rows_per_chunk: read_u32(bytes, 16),
        total_rows: read_u64(bytes, 20),
        schema_off,
        schema_len,
        index_off,
        index_len,
        dict_off,
        dict_len,
        data_off,
        dict_raw_len,
    })
}

pub(crate) fn decompress_lz4(src: &[u8], raw_len: usize) -> NativeResult<Vec<u8>> {
    let mut out = vec![0u8; raw_len];
    call_status("lz4_decompress", unsafe {
        ffi::lz4_decompress(
            src.as_ptr(),
            src.len(),
            raw_len,
            out.as_mut_ptr(),
            out.len(),
        )
    })?;
    Ok(out)
}

pub(crate) fn read_exact_at_file(file: &File, offset: u64, len: usize) -> NativeResult<Vec<u8>> {
    let mut out = vec![0u8; len];
    let mut read = 0usize;

    while read < len {
        let n = file.read_at(&mut out[read..], offset + read as u64)?;
        if n == 0 {
            return Err(NativeError::Invalid("unexpected EOF"));
        }
        read += n;
    }

    Ok(out)
}

pub(crate) fn read_out_bytes<F>(initial_len: usize, mut func: F) -> NativeResult<Vec<u8>>
where
    F: FnMut(*mut u8, usize) -> isize,
{
    let mut out_len = initial_len.max(64);
    loop {
        let mut out = vec![0u8; out_len];
        let written = func(out.as_mut_ptr(), out_len);
        if written < 0 {
            let needed = (-written) as usize;
            if needed <= out_len {
                return Err(NativeError::Status("out call", written as i32));
            }
            out_len = needed;
            continue;
        }
        out.truncate(written as usize);
        return Ok(out);
    }
}

pub(crate) fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

pub(crate) fn read_f64(bytes: &[u8], offset: usize) -> f64 {
    f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}
