//! Header, schema, dictionary, and chunk-index parsing (wcol v7 only).

use rustc_hash::FxHashMap;

use wcol_format::{INDEX_ENTRY_BYTES, WCOL_VERSION};

use crate::types::{Column, Dictionary, Header, IndexEntry};

pub(crate) fn parse_header(bytes: &[u8]) -> Result<Header, ()> {
    if bytes.len() < wcol_format::HEADER_BYTES {
        return Err(());
    }
    if &bytes[0..4] != b"WCOL" {
        return Err(());
    }
    let version = read_u16(bytes, 4);
    if version != WCOL_VERSION {
        return Err(());
    }
    Ok(Header {
        version,
        flags: read_u16(bytes, 6),
        ncols: read_u32(bytes, 8),
        nchunks: read_u32(bytes, 12),
        rows_per_chunk: read_u32(bytes, 16),
        total_rows: read_u64(bytes, 20),
        schema_off: read_u64(bytes, 28),
        schema_len: read_u64(bytes, 36),
        index_off: read_u64(bytes, 44),
        index_len: read_u64(bytes, 52),
        dict_off: read_u64(bytes, 60),
        dict_len: read_u64(bytes, 68),
        data_off: read_u64(bytes, 76),
        dict_raw_len: read_u64(bytes, 84),
    })
}

pub(crate) fn parse_schema(bytes: &[u8], ncols: usize) -> Result<Vec<Column>, ()> {
    let mut offset = 0usize;
    let mut cols = Vec::with_capacity(ncols);
    for id in 0..ncols {
        if offset + 2 > bytes.len() {
            return Err(());
        }
        let name_len = read_u16(bytes, offset) as usize;
        offset += 2;
        if offset + name_len > bytes.len() {
            return Err(());
        }
        let name = match std::str::from_utf8(&bytes[offset..offset + name_len]) {
            Ok(s) => s.to_string(),
            Err(_) => return Err(()),
        };
        offset += name_len;
        if offset + 14 > bytes.len() {
            return Err(());
        }
        let logical_type = bytes[offset];
        let physical_type = bytes[offset + 1];
        let flags = bytes[offset + 2];
        let encoding = bytes[offset + 3];
        let dict_id = read_u32(bytes, offset + 4);
        let dict_index_width = bytes[offset + 8];
        let scale = read_i32(bytes, offset + 9);
        offset += 14;
        cols.push(Column {
            id: id as u32,
            name,
            logical_type,
            physical_type,
            flags,
            encoding,
            dict_id,
            dict_index_width,
            scale,
        });
    }
    Ok(cols)
}

pub(crate) fn parse_dicts(bytes: &[u8]) -> Result<FxHashMap<u32, Dictionary>, ()> {
    if bytes.is_empty() {
        return Ok(FxHashMap::default());
    }
    let mut offset = 0usize;
    if bytes.len() < 4 {
        return Err(());
    }
    let dict_count = read_u32(bytes, offset) as usize;
    offset += 4;
    let mut dicts = FxHashMap::default();

    for _ in 0..dict_count {
        if offset + 12 > bytes.len() {
            return Err(());
        }
        let dict_id = read_u32(bytes, offset);
        offset += 4;
        let value_count = read_u32(bytes, offset) as usize;
        offset += 4;
        let len_width = bytes[offset];
        offset += 4;
        if len_width != 2 && len_width != 4 {
            return Err(());
        }

        let mut lengths: Vec<u32> = Vec::with_capacity(value_count);
        let mut total_len: u32 = 0;
        for _ in 0..value_count {
            let len = read_u32_advance(bytes, &mut offset, len_width)?;
            total_len = total_len.checked_add(len).ok_or(())?;
            lengths.push(len);
        }
        let blob_len = total_len as usize;
        if offset + blob_len > bytes.len() {
            return Err(());
        }
        let blob = bytes[offset..offset + blob_len].to_vec();
        offset += blob_len;

        let mut offsets = Vec::with_capacity(value_count + 1);
        offsets.push(0u32);
        let mut running = 0u32;
        for len in lengths {
            running = running.checked_add(len).ok_or(())?;
            offsets.push(running);
        }

        dicts.insert(
            dict_id,
            Dictionary {
                offsets,
                blob,
                values: Vec::new(),
                lookup: FxHashMap::default(),
                hash_cache: Vec::new(),
            },
        );
    }

    Ok(dicts)
}

pub(crate) fn parse_chunk_index(bytes: &[u8], ncols: usize) -> Result<Vec<IndexEntry>, ()> {
    if bytes.len() < ncols * INDEX_ENTRY_BYTES {
        return Err(());
    }
    let mut entries = Vec::with_capacity(ncols);
    let mut offset = 0usize;
    for _ in 0..ncols {
        let data_off = read_u64(bytes, offset);
        offset += 8;
        let data_comp_len = read_u32(bytes, offset);
        offset += 4;
        let data_raw_len = read_u32(bytes, offset);
        offset += 4;
        let null_off = read_u64(bytes, offset);
        offset += 8;
        let null_comp_len = read_u32(bytes, offset);
        offset += 4;
        let null_raw_len = read_u32(bytes, offset);
        offset += 4;
        let empty_mode = read_u32(bytes, offset) as u8;
        offset += 4;
        let empty_count = read_u32(bytes, offset);
        offset += 4;
        let empty_off = read_u64(bytes, offset);
        offset += 8;
        let empty_comp_len = read_u32(bytes, offset);
        offset += 4;
        let empty_raw_len = read_u32(bytes, offset);
        offset += 4;
        let min = read_f64(bytes, offset);
        offset += 8;
        let max = read_f64(bytes, offset);
        offset += 8;
        let presence = read_u64(bytes, offset);
        offset += 8;
        entries.push(IndexEntry {
            data_off,
            data_comp_len,
            data_raw_len,
            null_off,
            null_comp_len,
            null_raw_len,
            empty_mode,
            empty_count,
            empty_off,
            empty_comp_len,
            empty_raw_len,
            min,
            max,
            presence,
        });
    }
    Ok(entries)
}

pub(crate) fn read_u32_advance(bytes: &[u8], offset: &mut usize, width: u8) -> Result<u32, ()> {
    match width {
        2 => {
            if *offset + 2 > bytes.len() {
                return Err(());
            }
            let v = read_u16(bytes, *offset) as u32;
            *offset += 2;
            Ok(v)
        }
        4 => {
            if *offset + 4 > bytes.len() {
                return Err(());
            }
            let v = read_u32(bytes, *offset);
            *offset += 4;
            Ok(v)
        }
        _ => Err(()),
    }
}

pub(crate) fn read_usize_advance(bytes: &[u8], offset: &mut usize, width: u8) -> Result<usize, ()> {
    read_u32_advance(bytes, offset, width).map(|v| v as usize)
}

pub(crate) fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

pub(crate) fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

pub(crate) fn read_i32(bytes: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

pub(crate) fn read_f64(bytes: &[u8], offset: usize) -> f64 {
    f64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}
