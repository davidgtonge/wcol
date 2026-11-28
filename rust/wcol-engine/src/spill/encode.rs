//! Binary codec for one spilled partial group (stats only; key lives in `key_blob`).

use crate::ops::query::hll::Hll;
use crate::ops::query::{PartialAggSpec, PartialGroup, PartialStat};

const PAYLOAD_VERSION: u8 = 2;

pub fn encode_partial_group_payload(group: &PartialGroup) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    out.push(PAYLOAD_VERSION);
    let n = u32::try_from(group.stats.len()).map_err(|_| "too many aggs")?;
    out.extend_from_slice(&n.to_le_bytes());
    for stat in &group.stats {
        encode_stat(stat, &mut out)?;
    }
    Ok(out)
}

pub fn decode_partial_group_payload(
    bytes: &[u8],
    key: Vec<crate::ops::query::PartialKeyPart>,
    aggs: &[PartialAggSpec],
) -> Result<PartialGroup, String> {
    if bytes.first() != Some(&PAYLOAD_VERSION) {
        return Err("unsupported spill partial version".to_string());
    }
    let n = bytes
        .get(1..5)
        .map(|count| u32::from_le_bytes(count.try_into().expect("4 bytes")) as usize)
        .ok_or("spill partial truncated")?;
    if n != aggs.len() {
        return Err("spill partial agg count mismatch".to_string());
    }
    let mut cursor = 5usize;
    let mut stats = Vec::with_capacity(n);
    for _ in 0..n {
        let (stat, next) = decode_stat(bytes, cursor)?;
        cursor = next;
        stats.push(stat);
    }
    if cursor != bytes.len() {
        return Err("trailing spill partial bytes".to_string());
    }
    Ok(PartialGroup { key, stats })
}

/// `flags` bit 1: an integer stat. Its float fields are normally `exact_f64` of the
/// integer ones (see `merge_stat`); the codec then omits them.
const FLAG_INTEGER: u8 = 1;
const FLOATS_DERIVED: u8 = 1;

/// One stat as varints. Spilled runs are mostly COUNT/SUM partials of small integers, and
/// a fixed 82-byte stat made a run body twice the group table it was flushed from.
fn encode_stat(stat: &PartialStat, out: &mut Vec<u8>) -> Result<(), String> {
    let derived = stat.flags & FLAG_INTEGER != 0
        && [
            (stat.sum_f, stat.sum_i),
            (stat.min_f, stat.min_i),
            (stat.max_f, stat.max_i),
        ]
        .iter()
        .all(|(float, int)| float.to_bits() == crate::ops::query::exact_f64(*int).to_bits());
    out.push(stat.flags);
    out.push(if derived { FLOATS_DERIVED } else { 0 });
    push_varint(out, u128::from(stat.count));
    for value in [stat.sum_i, stat.min_i, stat.max_i] {
        push_varint(out, zigzag(value));
    }
    if !derived {
        for value in [stat.sum_f, stat.min_f, stat.max_f] {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    match &stat.sketch {
        None => out.push(0),
        Some(sketch) => {
            out.push(1);
            let mut tmp = Vec::new();
            sketch.encode(&mut tmp);
            let len = u32::try_from(tmp.len()).map_err(|_| "sketch too large")?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&tmp);
        }
    }
    Ok(())
}

fn decode_stat(bytes: &[u8], cursor: usize) -> Result<(PartialStat, usize), String> {
    let mut off = cursor;
    let flags = take_u8(bytes, &mut off)?;
    let derived = take_u8(bytes, &mut off)? & FLOATS_DERIVED != 0;
    let count = u64::try_from(take_varint(bytes, &mut off)?).map_err(|_| "count overflow")?;
    let sum_i = unzigzag(take_varint(bytes, &mut off)?);
    let min_i = unzigzag(take_varint(bytes, &mut off)?);
    let max_i = unzigzag(take_varint(bytes, &mut off)?);
    let (sum_f, min_f, max_f) = if derived {
        use crate::ops::query::exact_f64;
        (exact_f64(sum_i), exact_f64(min_i), exact_f64(max_i))
    } else {
        (
            take_f64(bytes, &mut off)?,
            take_f64(bytes, &mut off)?,
            take_f64(bytes, &mut off)?,
        )
    };
    let sketch = match take_u8(bytes, &mut off)? {
        0 => None,
        _ => {
            let len_bytes = bytes.get(off..off + 4).ok_or("truncated sketch len")?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            off += 4;
            let (sketch, used) = Hll::decode(bytes.get(off..off + len).ok_or("truncated sketch")?)?;
            if used != len {
                return Err("sketch length mismatch".to_string());
            }
            off += used;
            Some(sketch)
        }
    };
    Ok((
        PartialStat {
            count,
            flags,
            sum_i,
            sum_f,
            min_i,
            max_i,
            min_f,
            max_f,
            sketch,
        },
        off,
    ))
}

fn zigzag(value: i128) -> u128 {
    ((value << 1) ^ (value >> 127)) as u128
}

fn unzigzag(value: u128) -> i128 {
    ((value >> 1) as i128) ^ -((value & 1) as i128)
}

fn push_varint(out: &mut Vec<u8>, mut value: u128) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn take_varint(bytes: &[u8], off: &mut usize) -> Result<u128, String> {
    let mut value = 0u128;
    for shift in (0..128).step_by(7) {
        let byte = take_u8(bytes, off)?;
        value |= u128::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("varint too long".to_string())
}

fn take_u8(bytes: &[u8], off: &mut usize) -> Result<u8, String> {
    let byte = *bytes.get(*off).ok_or("truncated partial stat")?;
    *off += 1;
    Ok(byte)
}

fn take_f64(bytes: &[u8], off: &mut usize) -> Result<f64, String> {
    let chunk = bytes.get(*off..*off + 8).ok_or("truncated f64")?;
    *off += 8;
    Ok(f64::from_le_bytes(chunk.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::query::PartialKeyPart;

    #[test]
    fn a_short_payload_is_an_error_not_a_panic() {
        for bytes in [&[PAYLOAD_VERSION][..], &[PAYLOAD_VERSION, 1, 0]] {
            assert!(decode_partial_group_payload(bytes, Vec::new(), &[]).is_err());
        }
    }

    fn stat(flags: u8, count: u64, sum_i: i128, min_i: i128, max_i: i128) -> PartialStat {
        use crate::ops::query::exact_f64;
        PartialStat {
            count,
            flags,
            sum_i,
            sum_f: exact_f64(sum_i),
            min_i,
            max_i,
            min_f: exact_f64(min_i),
            max_f: exact_f64(max_i),
            sketch: None,
        }
    }

    fn round_trip(stats: Vec<PartialStat>) -> usize {
        let group = PartialGroup {
            key: vec![PartialKeyPart::Int(1)],
            stats,
        };
        let aggs: Vec<PartialAggSpec> = group
            .stats
            .iter()
            .map(|_| PartialAggSpec { kind: 2, column: 0 })
            .collect();
        let bytes = encode_partial_group_payload(&group).unwrap();
        let back = decode_partial_group_payload(&bytes, group.key.clone(), &aggs).unwrap();
        // Bit-compare floats: NaN min/max of an empty stat must survive too.
        for (a, b) in group.stats.iter().zip(&back.stats) {
            assert_eq!(
                (a.count, a.flags, a.sum_i, a.min_i, a.max_i),
                (b.count, b.flags, b.sum_i, b.min_i, b.max_i)
            );
            for (x, y) in [(a.sum_f, b.sum_f), (a.min_f, b.min_f), (a.max_f, b.max_f)] {
                assert_eq!(x.to_bits(), y.to_bits());
            }
            assert_eq!(a.sketch, b.sketch);
        }
        bytes.len()
    }

    #[test]
    fn small_integer_stats_are_compact() {
        // Q9's four lanes for one (RegionID, UserID) pair.
        let len = round_trip(vec![
            stat(1 | 4, 3, 3, 3, 3),
            stat(1 | 4, 3, 0, 0, 0),
            stat(1 | 4, 3, 4_032, 1_344, 1_344),
            stat(1 | 4, 3, 3, 3, 3),
        ]);
        assert!(len <= 5 + 4 * 12, "payload {len} bytes");
    }

    #[test]
    fn extreme_and_non_derived_values_round_trip() {
        let mut empty = stat(1 | 4, 0, 0, 0, 0);
        empty.min_f = f64::NAN;
        empty.max_f = f64::NAN;
        let mut float = stat(0, 2, 0, 0, 0);
        float.sum_f = 1.5;
        float.min_f = -0.25;
        float.max_f = 1.75;
        let mut sketched = stat(1 | 4, 1, 7, 7, 7);
        let mut hll = Hll::new();
        hll.insert_i64(42);
        sketched.sketch = Some(hll);
        round_trip(vec![
            empty,
            float,
            sketched,
            stat(1, u64::MAX, i128::MAX, i128::MIN, -1),
        ]);
    }
}
