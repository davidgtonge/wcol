//! Versioned typed hashing for exchange keys on [`WcolBatch`] views.
//!
//! Hashes logical values (resolved dictionary entries, not local dict ids). Null keys share one
//! deterministic tag so `GROUP BY`-style shuffles route all nulls to the same partition.

use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::stream::batch::{WcolBatch, WcolColumn};

/// Supported exchange hash version (mixed into the XXH3 seed).
pub const WCOL_EXCHANGE_HASH_VERSION_DEFAULT: u32 = 1;

const HASH_SEED_BASE: u64 = 0x5743_4f4c_5f4558; // "WCOL_EX"

const TAG_NULL: u8 = 0;
const TAG_I64: u8 = 1;
const TAG_BYTES: u8 = 2;
const TAG_COMPOSITE_END: u8 = 3;

/// Row-group-scoped hashes of byte-dictionary entries.
///
/// A single string group key looks up the hash of the dictionary value. The dictionary id
/// is never hashed: the same UTF-8 bytes in another row group route to the same partition.
/// Composite keys still hash the length-prefixed tuple, so this cache stays empty for them.
#[derive(Clone, Debug, Default)]
pub struct ExchangeHashCache {
    single_dict: Option<SingleByteDict>,
}

#[derive(Clone, Debug)]
struct SingleByteDict {
    column: u32,
    hashes: Vec<u64>,
    null_hash: u64,
}

impl ExchangeHashCache {
    pub fn prepare(batch: &WcolBatch<'_>, keys: &[u32], hash_version: u32) -> Result<Self, String> {
        if keys.len() != 1 {
            return Ok(Self::default());
        }
        let column = keys[0];
        let Some(WcolColumn::DictId { dictionary, .. }) = find_column(batch, column) else {
            return Ok(Self::default());
        };
        let Some((offsets, bytes)) = dictionary.byte_entries() else {
            return Ok(Self::default());
        };
        let entries = offsets.len().saturating_sub(1);
        let mut hashes = Vec::with_capacity(entries);
        for id in 0..entries {
            hashes.push(hash_row_keys_from_fingerprint(
                &fingerprint_single_bytes(column, Some(byte_span(offsets, bytes, id)?))?,
                hash_version,
            ));
        }
        Ok(Self {
            single_dict: Some(SingleByteDict {
                column,
                hashes,
                null_hash: hash_row_keys_from_fingerprint(
                    &fingerprint_single_bytes(column, None)?,
                    hash_version,
                ),
            }),
        })
    }

    fn lookup(
        &self,
        batch: &WcolBatch<'_>,
        keys: &[u32],
        row: usize,
    ) -> Result<Option<u64>, String> {
        let Some(cache) = &self.single_dict else {
            return Ok(None);
        };
        if keys.len() != 1 || keys[0] != cache.column {
            return Ok(None);
        }
        let Some(WcolColumn::DictId { ids, nulls, .. }) = find_column(batch, cache.column) else {
            return Ok(None);
        };
        if row >= ids.len() || row >= nulls.len() {
            return Err("hash row out of range".to_string());
        }
        if nulls[row] == 0 {
            return Ok(Some(cache.null_hash));
        }
        let id = ids[row] as usize;
        cache
            .hashes
            .get(id)
            .copied()
            .map(Some)
            .ok_or_else(|| "dictionary index out of range".to_string())
    }
}

pub fn exchange_hash_seed(hash_version: u32) -> u64 {
    HASH_SEED_BASE ^ ((hash_version as u64) << 32)
}

pub fn partition_for_row(
    batch: &WcolBatch<'_>,
    row: usize,
    keys: &[u32],
    partitions: u32,
    hash_version: u32,
    cache: &ExchangeHashCache,
) -> Result<u32, String> {
    let hash = match cache.lookup(batch, keys, row)? {
        Some(hash) => hash,
        None => hash_row_keys_at(batch, keys, row, hash_version)?,
    };
    Ok(partition_index(hash, partitions))
}

pub fn partition_index(hash: u64, partitions: u32) -> u32 {
    let partitions = partitions.max(1);
    // Power-of-two counts mask the low bits. That matches modulo and stays a single instruction.
    if partitions.is_power_of_two() {
        return (hash as u32) & (partitions - 1);
    }
    (hash % u64::from(partitions)) as u32
}

pub fn hash_row_keys_at(
    batch: &WcolBatch<'_>,
    keys: &[u32],
    row: usize,
    hash_version: u32,
) -> Result<u64, String> {
    let buf = join_key_fingerprint_at(batch, keys, row)?;
    Ok(hash_row_keys_from_fingerprint(&buf, hash_version))
}

fn hash_row_keys_from_fingerprint(buf: &[u8], hash_version: u32) -> u64 {
    xxh3_64_with_seed(buf, exchange_hash_seed(hash_version))
}

/// Where a scan-side combiner sends each group: the exchange keys as `(position in the
/// partial key, source column)`, the partition count and the hash version. The source
/// column is hashed with the value, so a group routes where its raw rows would.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CombinerRoute {
    pub keys: Vec<(usize, u32)>,
    pub partitions: u32,
    pub hash_version: u32,
}

impl CombinerRoute {
    /// The partition of one group, given each exchange key's part. The fingerprint is the one
    /// [`hash_row_keys_at`] builds from the same values: ints as themselves, floats through
    /// `float_key`, text as length-prefixed bytes. A global text `COUNT(DISTINCT)` keys its
    /// groups by 64-bit hashes of the text, so those route by the hash: consistently among
    /// combiners, though not with raw rows, so a plan sends one or the other.
    pub(crate) fn partition<'a>(
        &self,
        parts: impl IntoIterator<Item = (u32, crate::stream::partials::KeyPartRef<'a>)>,
        buf: &mut Vec<u8>,
    ) -> Result<u32, String> {
        use crate::stream::partials::KeyPartRef;
        buf.clear();
        for (column, part) in parts {
            buf.extend_from_slice(&column.to_le_bytes());
            match part {
                KeyPartRef::Null => buf.push(TAG_NULL),
                KeyPartRef::Int(value) => {
                    buf.push(TAG_I64);
                    buf.extend_from_slice(&value.to_le_bytes());
                }
                KeyPartRef::Float(bits) => {
                    let key = crate::ops::query::float_key::float_key(f64::from_bits(bits)) as i64;
                    buf.push(TAG_I64);
                    buf.extend_from_slice(&key.to_le_bytes());
                }
                KeyPartRef::Bytes(bytes) => push_bytes(buf, bytes)?,
            }
        }
        buf.push(TAG_COMPOSITE_END);
        Ok(partition_index(
            hash_row_keys_from_fingerprint(buf, self.hash_version),
            self.partitions,
        ))
    }
}

/// Canonical key bytes for equality checks (same encoding as [`hash_row_keys_at`]).
pub fn join_key_fingerprint_at(
    batch: &WcolBatch<'_>,
    keys: &[u32],
    row: usize,
) -> Result<Vec<u8>, String> {
    if row >= batch.row_count {
        return Err("hash row out of range".to_string());
    }
    let mut buf = Vec::with_capacity(keys.len() * 16);
    for &key in keys {
        feed_key_component_row(batch, key, row, &mut buf)?;
    }
    buf.push(TAG_COMPOSITE_END);
    Ok(buf)
}

/// Inner-join semantics: any null key column → not matchable.
pub fn join_key_is_null(batch: &WcolBatch<'_>, keys: &[u32], row: usize) -> Result<bool, String> {
    if row >= batch.row_count {
        return Err("join key row out of range".to_string());
    }
    for &key in keys {
        let Some(col) = find_column(batch, key) else {
            return Err(format!("join key column {key} missing"));
        };
        let null = match col {
            WcolColumn::I64 { nulls, .. }
            | WcolColumn::F64 { nulls, .. }
            | WcolColumn::DictId { nulls, .. }
            | WcolColumn::Utf8 { nulls, .. } => nulls[row],
        };
        if null == 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn join_keys_equal(
    build: &WcolBatch<'_>,
    build_keys: &[u32],
    build_row: usize,
    probe: &WcolBatch<'_>,
    probe_keys: &[u32],
    probe_row: usize,
) -> Result<bool, String> {
    if build_keys.len() != probe_keys.len() {
        return Err("join key arity mismatch".to_string());
    }
    Ok(join_key_fingerprint_at(build, build_keys, build_row)?
        == join_key_fingerprint_at(probe, probe_keys, probe_row)?)
}

fn feed_key_component_row(
    batch: &WcolBatch<'_>,
    column: u32,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), String> {
    buf.extend_from_slice(&column.to_le_bytes());
    let Some(col) = find_column(batch, column) else {
        return Err(format!(
            "hash partition key column {column} missing from batch"
        ));
    };
    match col {
        WcolColumn::I64 { values, nulls, .. } => {
            if values.len() != batch.row_count || nulls.len() != batch.row_count {
                return Err("misaligned i64 key column".to_string());
            }
            if nulls[row] == 0 {
                buf.push(TAG_NULL);
            } else {
                buf.push(TAG_I64);
                buf.extend_from_slice(&values[row].to_le_bytes());
            }
        }
        WcolColumn::F64 { values, nulls, .. } => {
            if values.len() != batch.row_count || nulls.len() != batch.row_count {
                return Err("misaligned f64 key column".to_string());
            }
            if nulls[row] == 0 {
                buf.push(TAG_NULL);
            } else {
                let key = crate::ops::query::float_key::float_key(values[row]) as i64;
                buf.push(TAG_I64);
                buf.extend_from_slice(&key.to_le_bytes());
            }
        }
        WcolColumn::DictId {
            ids,
            nulls,
            dictionary,
            ..
        } => {
            if ids.len() != batch.row_count || nulls.len() != batch.row_count {
                return Err("misaligned dict key column".to_string());
            }
            if nulls[row] == 0 {
                buf.push(TAG_NULL);
            } else {
                let id = ids[row] as usize;
                let logical = if let Some(entries) = dictionary.int_entries() {
                    *entries.get(id).ok_or("dictionary index out of range")?
                } else if let Some(entries) = dictionary.float_entries() {
                    crate::ops::query::float_key::float_key(
                        *entries.get(id).ok_or("dictionary index out of range")?,
                    ) as i64
                } else if let Some((offsets, bytes)) = dictionary.byte_entries() {
                    push_bytes(buf, byte_span(offsets, bytes, id)?)?;
                    return Ok(());
                } else {
                    return Err("dictionary key has no entries".to_string());
                };
                buf.push(TAG_I64);
                buf.extend_from_slice(&logical.to_le_bytes());
            }
        }
        WcolColumn::Utf8 {
            offsets,
            bytes,
            nulls,
            ..
        } => {
            if nulls.len() != batch.row_count || offsets.len() != batch.row_count.saturating_add(1)
            {
                return Err("misaligned utf8 key column".to_string());
            }
            if nulls[row] == 0 {
                buf.push(TAG_NULL);
            } else {
                push_bytes(buf, byte_span(offsets, bytes, row)?)?;
            }
        }
    }
    Ok(())
}

fn push_bytes(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<(), String> {
    let len = u32::try_from(bytes.len()).map_err(|_| "string key longer than 4 GiB".to_string())?;
    buf.push(TAG_BYTES);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
}

fn byte_span<'a>(offsets: &'a [u32], bytes: &'a [u8], index: usize) -> Result<&'a [u8], String> {
    let start = *offsets.get(index).ok_or("string key index out of range")? as usize;
    let end = *offsets
        .get(index + 1)
        .ok_or("string key index out of range")? as usize;
    if end < start || end > bytes.len() {
        return Err("string key byte range out of range".to_string());
    }
    Ok(&bytes[start..end])
}

/// Canonical encoding of one string key, including the composite terminator.
fn fingerprint_single_bytes(column: u32, bytes: Option<&[u8]>) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(16 + bytes.map(|b| b.len()).unwrap_or(0));
    buf.extend_from_slice(&column.to_le_bytes());
    match bytes {
        None => buf.push(TAG_NULL),
        Some(bytes) => push_bytes(&mut buf, bytes)?,
    }
    buf.push(TAG_COMPOSITE_END);
    Ok(buf)
}

fn find_column<'a>(batch: &'a WcolBatch<'_>, column: u32) -> Option<&'a WcolColumn<'a>> {
    batch.columns.iter().find(|col| match col {
        WcolColumn::I64 { column: id, .. }
        | WcolColumn::F64 { column: id, .. }
        | WcolColumn::DictId { column: id, .. }
        | WcolColumn::Utf8 { column: id, .. } => *id == column,
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod partial_split_tests;
