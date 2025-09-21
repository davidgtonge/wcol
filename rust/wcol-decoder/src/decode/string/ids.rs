use crate::timing::{self, Tic};
use crate::types::{Dictionary, PlanTiming};
use xxhash_rust::xxh3::{xxh3_128_with_seed, xxh3_64, xxh3_64_with_seed};

use super::preamble::{decode_string_preamble, StringColumnPreamble};
use super::reconstruct::reconstruct_unique_values;

const RAW_STRING_HASH_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RawStringOptionAHashBits {
    Bits64,
    Bits128,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RawStringOptionAHash {
    pub(crate) hash_hi: u64,
    pub(crate) hash_lo: u64,
    pub(crate) sink_hash: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct RawStringOptionAFastPath {
    pub(crate) row_to_local_id: Vec<u32>,
    pub(crate) unique_hashes: Vec<RawStringOptionAHash>,
    pub(crate) value_count: usize,
}

/// When set (`WCOL_STRING_HASH_IDS=1`), raw string decode deduplicates by hash only (no UTF-8 dict values).
pub(crate) fn raw_string_hash_ids_enabled() -> bool {
    std::env::var("WCOL_STRING_HASH_IDS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn hash_option_a_unique(bytes: &[u8], bits: RawStringOptionAHashBits) -> RawStringOptionAHash {
    match bits {
        RawStringOptionAHashBits::Bits64 => {
            let h = xxh3_64_with_seed(bytes, RAW_STRING_HASH_SEED);
            RawStringOptionAHash {
                hash_hi: h,
                hash_lo: 0,
                sink_hash: h,
            }
        }
        RawStringOptionAHashBits::Bits128 => {
            let h = xxh3_128_with_seed(bytes, RAW_STRING_HASH_SEED);
            let lo = h as u64;
            let hi = (h >> 64) as u64;
            RawStringOptionAHash {
                hash_hi: hi,
                hash_lo: lo,
                sink_hash: lo ^ hi.rotate_left(13),
            }
        }
    }
}

pub(crate) fn decode_raw_string_option_a_fast_path(
    raw: &[u8],
    rows: usize,
    hash_bits: RawStringOptionAHashBits,
) -> Result<Option<RawStringOptionAFastPath>, i32> {
    let p = decode_string_preamble(raw, rows, None)?;
    decode_raw_string_option_a_from_preamble(&p, hash_bits).map(Some)
}

pub(crate) fn decode_raw_string_option_a_from_preamble(
    p: &StringColumnPreamble,
    hash_bits: RawStringOptionAHashBits,
) -> Result<RawStringOptionAFastPath, i32> {
    let unique_hashes = reconstruct_unique_values(
        &p.lcps,
        &p.suffix_lens,
        &p.suffix_blob,
        p.value_count,
        |bytes| Ok(hash_option_a_unique(bytes, hash_bits)),
    )?;

    let mut row_to_local_id = Vec::with_capacity(p.rows);
    for row in 0..p.rows {
        let local_id = *p.indices.get(row).ok_or(-115)?;
        row_to_local_id.push(local_id as u32);
    }

    Ok(RawStringOptionAFastPath {
        row_to_local_id,
        unique_hashes,
        value_count: p.value_count,
    })
}

fn dict_id_for_sink_hash(dict: &mut Dictionary, sink_hash: u64) -> u32 {
    for (idx, &h) in dict.hash_cache.iter().enumerate() {
        if h == sink_hash {
            return idx as u32;
        }
    }
    let id = dict.hash_cache.len() as u32;
    dict.hash_cache.push(sink_hash);
    id
}

fn decode_raw_string_ids_hash_only(
    p: &StringColumnPreamble,
    dict: &mut Dictionary,
) -> Result<Vec<u32>, i32> {
    let unique_ids = reconstruct_unique_values(
        &p.lcps,
        &p.suffix_lens,
        &p.suffix_blob,
        p.value_count,
        |bytes| Ok(dict_id_for_sink_hash(dict, xxh3_64_with_seed(bytes, RAW_STRING_HASH_SEED))),
    )?;

    let mut ids = vec![0u32; p.rows];
    for row in 0..p.rows {
        let unique_id = p.indices[row];
        ids[row] = *unique_ids.get(unique_id).ok_or(-115)?;
    }
    Ok(ids)
}

pub(crate) fn decode_raw_string_ids(
    raw: &[u8],
    rows: usize,
    dict: &mut Dictionary,
    mut timing: Option<&mut PlanTiming>,
) -> Result<Vec<u32>, i32> {
    let p = decode_string_preamble(raw, rows, timing.as_deref_mut())?;

    if raw_string_hash_ids_enabled() {
        return decode_raw_string_ids_hash_only(&p, dict);
    }

    let indices = &p.indices;
    let rows = p.rows;
    let value_count = p.value_count;

    let t_recon = Tic::start();
    let t_dict = Tic::start();

    let unique_ids = reconstruct_unique_values(
        &p.lcps,
        &p.suffix_lens,
        &p.suffix_blob,
        value_count,
        |bytes| {
            let value = match std::str::from_utf8(bytes) {
                Ok(s) => s,
                Err(_) => return Err(-123),
            };
            let id = if let Some(id) = dict.lookup.get(value) {
                *id
            } else {
                let id = dict.values.len() as u32;
                let owned = value.to_string();
                dict.values.push(owned.clone());
                dict.lookup.insert(owned, id);
                dict.hash_cache.push(xxh3_64(value.as_bytes()));
                id
            };
            Ok(id)
        },
    )?;

    let mut ids = vec![0u32; rows];
    for row in 0..rows {
        let unique_id = indices[row];
        let id = *unique_ids.get(unique_id).ok_or(-115)?;
        ids[row] = id;
    }

    timing::record_elapsed(
        timing.as_deref_mut(),
        |t, ms| t.add_ms_str_reconstruct(ms),
        t_recon,
    );
    timing::record_elapsed(timing.as_deref_mut(), |t, ms| t.add_ms_str_dict(ms), t_dict);
    Ok(ids)
}
