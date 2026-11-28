//! WCS1 dictionary payloads: a dictionary sent as bounded chunks, each appending entries
//! `[start_index, start_index + count)` to the receiver's table for `(scope, column)`.

use std::sync::Arc;

use crate::error::{WcolError, WcolResult};
use crate::stream::{DictEntries, DictionaryValues};

/// Type tag at byte 12. Tags 0 and 1 were whole-dictionary payloads nothing sends any more.
const DICT_TAG_AT: usize = 12;
const DICT_PAYLOAD_STRING_V2: u8 = 2;
const DICT_PAYLOAD_NUMERIC_V2: u8 = 3;
/// String body starts here: scope, column, start_index, tag, count.
const DICT_HEADER_V2: usize = 17;
const DICT_V2_NUMERIC_BODY: usize = 19;

/// Encode dictionary entries `[from_entry..)` into one or more bounded v2 payloads.
pub fn encode_dictionary_frames(
    dict: &DictionaryValues,
    from_entry: usize,
    max_payload: usize,
) -> WcolResult<Vec<Vec<u8>>> {
    let mut frames = Vec::new();
    let mut at = from_entry;
    while let Some((frame, next)) = encode_one_dictionary_frame(dict, at, max_payload)? {
        if next <= at {
            return Err(WcolError::from("wcs: dictionary frame did not advance"));
        }
        frames.push(frame);
        at = next;
    }
    Ok(frames)
}

/// Next dictionary payload and the entry index that follows it.
/// `None` means `from_entry` is already at the end of the dictionary.
pub fn encode_one_dictionary_frame(
    dict: &DictionaryValues,
    from_entry: usize,
    max_payload: usize,
) -> WcolResult<Option<(Vec<u8>, usize)>> {
    if from_entry > dict.entry_count() {
        return Err(WcolError::from("dictionary from_entry out of range"));
    }
    if from_entry == dict.entry_count() {
        return Ok(None);
    }
    if max_payload < DICT_HEADER_V2 + 4 {
        return Err(WcolError::from(
            "wcs: dictionary_chunk_too_large (max_payload too small)",
        ));
    }
    match &dict.entries {
        DictEntries::Bytes { offsets, bytes } => encode_one_byte_frame(
            dict.scope,
            dict.column,
            offsets,
            bytes,
            from_entry,
            max_payload,
        ),
        DictEntries::Int(entries) => encode_one_numeric_frame(
            dict.scope,
            dict.column,
            2,
            from_entry,
            entries.len(),
            max_payload,
            |index, out| out.copy_from_slice(&entries[index].to_le_bytes()),
        ),
        DictEntries::Float(entries) => encode_one_numeric_frame(
            dict.scope,
            dict.column,
            5,
            from_entry,
            entries.len(),
            max_payload,
            |index, out| out.copy_from_slice(&entries[index].to_le_bytes()),
        ),
    }
}

/// Apply one dictionary frame payload into `dicts` (supports v1 full install and v2 chunks).
pub fn apply_dictionary_payload(
    dicts: &mut std::collections::HashMap<(u32, u32), DictionaryValues>,
    payload: &[u8],
) -> WcolResult<()> {
    let chunk = decode_dictionary_chunk(payload)?;
    let key = (chunk.scope, chunk.column);
    match dicts.get_mut(&key) {
        None => {
            if chunk.start_index != 0 {
                return Err(WcolError::from("wcs: dictionary chunk gap at stream start"));
            }
            dicts.insert(key, chunk.into_values()?);
        }
        Some(existing) => merge_dictionary_chunk(existing, &chunk)?,
    }
    Ok(())
}

/// Parsed dictionary payload before merge into a stream-scoped table.
pub struct DictionaryChunk {
    pub scope: u32,
    pub column: u32,
    pub start_index: u32,
    pub entries: DictEntries,
}

impl DictionaryChunk {
    pub(crate) fn into_values(self) -> WcolResult<DictionaryValues> {
        Ok(DictionaryValues {
            scope: self.scope,
            column: self.column,
            entries: self.entries,
        })
    }
}

fn encode_one_byte_frame(
    dict_scope: u32,
    dict_id: u32,
    offsets: &[u32],
    bytes: &[u8],
    start: usize,
    max_payload: usize,
) -> WcolResult<Option<(Vec<u8>, usize)>> {
    let total = offsets.len().saturating_sub(1);
    if start > total {
        return Err(WcolError::from("dictionary from_entry out of range"));
    }
    if start == total {
        return Ok(None);
    }
    let mut chunk_count = 0usize;
    let mut size = DICT_HEADER_V2;
    while start + chunk_count < total {
        let entry_start = offsets[start + chunk_count] as usize;
        let entry_end = offsets[start + chunk_count + 1] as usize;
        let entry_size = 4 + entry_end.saturating_sub(entry_start);
        if chunk_count > 0 && size + entry_size > max_payload {
            break;
        }
        if chunk_count == 0 && size + entry_size > max_payload {
            return Err(WcolError::fatal(format!(
                "wcs: dictionary_entry_too_large (scope={dict_scope} column={dict_id} index={start} encoded={entry_size} max={max_payload})"
            )));
        }
        size += entry_size;
        chunk_count += 1;
    }
    let mut out = vec![0u8; size];
    write_v2_header(
        &mut out,
        dict_scope,
        dict_id,
        start,
        chunk_count,
        DICT_PAYLOAD_STRING_V2,
    );
    let mut at = DICT_HEADER_V2;
    for index in start..start + chunk_count {
        let entry_start = offsets[index] as usize;
        let entry_end = offsets[index + 1] as usize;
        let value = &bytes[entry_start..entry_end];
        out[at..at + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
        at += 4;
        out[at..at + value.len()].copy_from_slice(value);
        at += value.len();
    }
    Ok(Some((out, start + chunk_count)))
}

fn encode_one_numeric_frame<F>(
    dict_scope: u32,
    dict_id: u32,
    physical: u8,
    start: usize,
    total_entries: usize,
    max_payload: usize,
    write_value: F,
) -> WcolResult<Option<(Vec<u8>, usize)>>
where
    F: Fn(usize, &mut [u8]),
{
    if start > total_entries {
        return Err(WcolError::from("dictionary from_entry out of range"));
    }
    if start == total_entries {
        return Ok(None);
    }
    const VALUE_WIDTH: usize = 8;
    let mut chunk_count = 0usize;
    let mut size = DICT_V2_NUMERIC_BODY;
    while start + chunk_count < total_entries {
        let next = size + VALUE_WIDTH;
        if chunk_count > 0 && next > max_payload {
            break;
        }
        if chunk_count == 0 && next > max_payload {
            return Err(WcolError::fatal(format!(
                "wcs: dictionary_entry_too_large (scope={dict_scope} column={dict_id} index={start})"
            )));
        }
        size = next;
        chunk_count += 1;
    }
    let mut out = vec![0u8; size];
    write_v2_header(
        &mut out,
        dict_scope,
        dict_id,
        start,
        chunk_count,
        DICT_PAYLOAD_NUMERIC_V2,
    );
    out[17] = physical;
    out[18] = VALUE_WIDTH as u8;
    let mut at = DICT_V2_NUMERIC_BODY;
    for index in start..start + chunk_count {
        write_value(index, &mut out[at..at + VALUE_WIDTH]);
        at += VALUE_WIDTH;
    }
    Ok(Some((out, start + chunk_count)))
}

/// Shift an encoded v2 dictionary chunk's `start_index` by `base`. A partition stream sends
/// a subset of the scan's dictionary as chunks of its own, partition-local, numbering.
pub fn rebase_dictionary_frame(payload: &mut [u8], base: usize) -> WcolResult<()> {
    let field = payload
        .get_mut(8..12)
        .ok_or_else(|| WcolError::from("truncated dictionary frame"))?;
    let start = u32::from_le_bytes(field.try_into().unwrap()) as usize;
    let rebased = u32::try_from(start + base)
        .map_err(|_| WcolError::from("dictionary start index overflow"))?;
    field.copy_from_slice(&rebased.to_le_bytes());
    Ok(())
}

fn write_v2_header(out: &mut [u8], scope: u32, column: u32, start: usize, count: usize, tag: u8) {
    out[0..4].copy_from_slice(&scope.to_le_bytes());
    out[4..8].copy_from_slice(&column.to_le_bytes());
    out[8..12].copy_from_slice(&(start as u32).to_le_bytes());
    out[12] = tag;
    out[13..17].copy_from_slice(&(count as u32).to_le_bytes());
}

pub fn merge_dictionary_chunk(
    existing: &mut DictionaryValues,
    chunk: &DictionaryChunk,
) -> WcolResult<()> {
    if existing.scope != chunk.scope || existing.column != chunk.column {
        return Err(WcolError::from("dictionary chunk scope mismatch"));
    }
    let start = chunk.start_index as usize;
    let current = existing.entry_count();
    if start != current {
        return Err(WcolError::from("wcs: dictionary chunk gap"));
    }
    match (&mut existing.entries, &chunk.entries) {
        (DictEntries::Int(existing_entries), DictEntries::Int(chunk_entries)) => {
            Arc::make_mut(existing_entries).extend_from_slice(chunk_entries);
        }
        (DictEntries::Float(existing_entries), DictEntries::Float(chunk_entries)) => {
            Arc::make_mut(existing_entries).extend_from_slice(chunk_entries);
        }
        (
            DictEntries::Bytes {
                offsets: existing_offsets,
                bytes: existing_bytes,
            },
            DictEntries::Bytes {
                offsets: chunk_offsets,
                bytes: chunk_bytes,
            },
        ) => {
            let chunk_len = chunk_offsets.len().saturating_sub(1);
            if chunk_len == 0 {
                return Ok(());
            }
            let offsets = Arc::make_mut(existing_offsets);
            let bytes = Arc::make_mut(existing_bytes);
            for index in 0..chunk_len {
                let entry_start = chunk_offsets[index] as usize;
                let entry_end = chunk_offsets[index + 1] as usize;
                bytes.extend_from_slice(&chunk_bytes[entry_start..entry_end]);
                offsets.push(u32::try_from(bytes.len()).map_err(|_| "dictionary too large")?);
            }
        }
        _ => return Err(WcolError::from("dictionary chunk type mismatch")),
    }
    Ok(())
}

pub fn decode_dictionary_chunk(payload: &[u8]) -> WcolResult<DictionaryChunk> {
    match payload.get(DICT_TAG_AT) {
        None => Err(WcolError::from("truncated dictionary payload")),
        Some(&DICT_PAYLOAD_STRING_V2) => decode_string_dictionary_v2(payload),
        Some(&DICT_PAYLOAD_NUMERIC_V2) => decode_numeric_dictionary_v2(payload),
        Some(_) => Err(WcolError::from("unsupported dictionary payload tag")),
    }
}

fn decode_string_dictionary_v2(payload: &[u8]) -> WcolResult<DictionaryChunk> {
    if payload.len() < DICT_HEADER_V2 {
        return Err(WcolError::from("truncated dictionary payload"));
    }
    let scope = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let column = u32::from_le_bytes(payload[4..8].try_into().unwrap());
    let start_index = u32::from_le_bytes(payload[8..12].try_into().unwrap());
    let count = u32::from_le_bytes(payload[13..17].try_into().unwrap()) as usize;
    let entries = decode_byte_entries(&payload[DICT_HEADER_V2..], count)?;
    Ok(DictionaryChunk {
        scope,
        column,
        start_index,
        entries,
    })
}

fn decode_numeric_dictionary_v2(payload: &[u8]) -> WcolResult<DictionaryChunk> {
    if payload.len() < DICT_HEADER_V2 + 2 {
        return Err(WcolError::from("truncated numeric dictionary"));
    }
    let scope = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let column = u32::from_le_bytes(payload[4..8].try_into().unwrap());
    let start_index = u32::from_le_bytes(payload[8..12].try_into().unwrap());
    let count = u32::from_le_bytes(payload[13..17].try_into().unwrap()) as usize;
    let entries = decode_numeric_entries(&payload[17..], count)?;
    Ok(DictionaryChunk {
        scope,
        column,
        start_index,
        entries,
    })
}

fn decode_byte_entries(payload: &[u8], count: usize) -> WcolResult<DictEntries> {
    // Every entry has a four-byte length, so the payload bounds the count before allocating.
    if count > payload.len() / 4 {
        return Err(WcolError::from("truncated dictionary string"));
    }
    let mut offsets = Vec::with_capacity(count + 1);
    offsets.push(0u32);
    let mut bytes = Vec::new();
    let mut at = 0usize;
    for _ in 0..count {
        if payload.len() - at < 4 {
            return Err(WcolError::from("truncated dictionary string"));
        }
        let len = u32::from_le_bytes(payload[at..at + 4].try_into().unwrap()) as usize;
        at += 4;
        if payload.len() - at < len {
            return Err(WcolError::from("truncated dictionary string"));
        }
        bytes.extend_from_slice(&payload[at..at + len]);
        at += len;
        offsets.push(u32::try_from(bytes.len()).map_err(|_| "dictionary too large")?);
    }
    if at != payload.len() {
        return Err(WcolError::from("dictionary payload has trailing bytes"));
    }
    Ok(DictEntries::Bytes {
        offsets: Arc::new(offsets),
        bytes: Arc::new(bytes),
    })
}

fn decode_numeric_entries(payload: &[u8], count: usize) -> WcolResult<DictEntries> {
    if payload.len() < 2 {
        return Err(WcolError::from("truncated numeric dictionary"));
    }
    let physical = payload[0];
    let width = payload[1] as usize;
    let raw = &payload[2..];
    if width == 0
        || raw.len()
            != count
                .checked_mul(width)
                .ok_or("numeric dictionary overflow")?
    {
        return Err(WcolError::from("numeric dictionary length mismatch"));
    }
    match (physical, width) {
        (2, 8) | (1, 4) => {
            let mut values = Vec::with_capacity(count);
            for row in 0..count {
                let at = row * width;
                let value = if width == 8 {
                    i64::from_le_bytes(raw[at..at + 8].try_into().unwrap())
                } else {
                    i32::from_le_bytes(raw[at..at + 4].try_into().unwrap()) as i64
                };
                values.push(value);
            }
            Ok(DictEntries::Int(Arc::new(values)))
        }
        (5, 8) | (4, 4) => {
            let mut values = Vec::with_capacity(count);
            for row in 0..count {
                let at = row * width;
                let value = if width == 8 {
                    f64::from_le_bytes(raw[at..at + 8].try_into().unwrap())
                } else {
                    f32::from_le_bytes(raw[at..at + 4].try_into().unwrap()) as f64
                };
                values.push(value);
            }
            Ok(DictEntries::Float(Arc::new(values)))
        }
        _ => Err(WcolError::from("unsupported numeric dictionary type")),
    }
}

#[cfg(test)]
mod dictionary_frame_tests {
    use super::*;

    fn byte_dict(values: &[&str]) -> DictionaryValues {
        let mut offsets = vec![0u32];
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(value.as_bytes());
            offsets.push(bytes.len() as u32);
        }
        DictionaryValues {
            scope: 1,
            column: 7,
            entries: DictEntries::Bytes {
                offsets: Arc::new(offsets),
                bytes: Arc::new(bytes),
            },
        }
    }

    #[test]
    fn chunked_dictionary_round_trips_many_short_strings() {
        let strings: Vec<String> = (0..100_000).map(|index| format!("k{index:06}")).collect();
        let refs: Vec<&str> = strings.iter().map(String::as_str).collect();
        let dict = byte_dict(&refs);
        let frames =
            encode_dictionary_frames(&dict, 0, crate::stream::wcs::MAX_WCS_PAYLOAD).unwrap();
        assert!(
            frames.len() > 1,
            "expected multiple frames, got {}",
            frames.len()
        );
        for payload in &frames {
            assert!(payload.len() <= crate::stream::wcs::MAX_WCS_PAYLOAD);
        }
        let mut dicts = std::collections::HashMap::new();
        for payload in frames {
            apply_dictionary_payload(&mut dicts, &payload).unwrap();
        }
        let installed = dicts.get(&(1, 7)).unwrap();
        assert_eq!(installed.entry_count(), 100_000);
    }

    #[test]
    fn dictionary_extension_chunks_append() {
        let dict = byte_dict(&["a", "b", "c", "d"]);
        let first = encode_dictionary_frames(&dict, 0, 24).unwrap();
        assert!(first.len() >= 2);
        let mut dicts = std::collections::HashMap::new();
        apply_dictionary_payload(&mut dicts, &first[0]).unwrap();
        assert!(dicts.get(&(1, 7)).unwrap().entry_count() < 4);
        for payload in &first[1..] {
            apply_dictionary_payload(&mut dicts, payload).unwrap();
        }
        assert_eq!(dicts.get(&(1, 7)).unwrap().entry_count(), 4);
    }

    #[test]
    fn tag_is_byte_12_and_whole_dictionary_tags_are_rejected() {
        let dict = byte_dict(&["ab", "cd"]);
        let mut frame = encode_dictionary_frames(&dict, 0, 64).unwrap().remove(0);
        assert_eq!(frame[DICT_TAG_AT], DICT_PAYLOAD_STRING_V2);
        for retired in [0, 1] {
            frame[DICT_TAG_AT] = retired;
            assert!(decode_dictionary_chunk(&frame).is_err());
        }
    }

    #[test]
    fn a_count_beyond_the_payload_is_an_error_not_an_allocation() {
        for tag in [DICT_PAYLOAD_STRING_V2, DICT_PAYLOAD_NUMERIC_V2] {
            let mut frame = vec![0u8; DICT_V2_NUMERIC_BODY];
            frame[DICT_TAG_AT] = tag;
            frame[13..17].copy_from_slice(&u32::MAX.to_le_bytes());
            frame[17] = 2;
            frame[18] = 8;
            assert!(decode_dictionary_chunk(&frame).is_err(), "tag {tag}");
        }
    }

    #[test]
    fn oversized_dictionary_entry_is_explicit() {
        let huge = vec![b'x'; crate::stream::wcs::MAX_WCS_PAYLOAD];
        let dict = byte_dict(&[std::str::from_utf8(&huge).unwrap()]);
        let err =
            encode_dictionary_frames(&dict, 0, crate::stream::wcs::MAX_WCS_PAYLOAD).unwrap_err();
        assert!(
            err.as_str().contains("dictionary_entry_too_large"),
            "{}",
            err
        );
    }
}
