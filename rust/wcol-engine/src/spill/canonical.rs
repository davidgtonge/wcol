//! Canonical group key bytes for spill (stable across batches and dictionary scopes).
//!
//! Lexicographic order on [`encode_canonical_key`] output matches [`cmp_spill_key_parts`],
//! the same order as live aggregation (`cmp_keys` in `ops/query/batch`). Merge scans and sorted
//! runs rely on `key_blob` byte order (`ORDER BY key_blob`, `encode_run`).

use std::cmp::Ordering;

use crate::ops::query::PartialKeyPart;

const VERSION: u8 = 1;

#[derive(Debug)]
pub enum CanonicalKeyError {
    Empty,
    TooLarge,
}

impl std::fmt::Display for CanonicalKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "canonical key is empty"),
            Self::TooLarge => write!(f, "canonical key exceeds limit"),
        }
    }
}

/// Encode group key parts for spill storage and ordering.
pub fn encode_canonical_key(parts: &[PartialKeyPart]) -> Result<Vec<u8>, CanonicalKeyError> {
    if parts.is_empty() {
        return Err(CanonicalKeyError::Empty);
    }
    let mut out = Vec::with_capacity(64);
    out.push(VERSION);
    out.push(u8::try_from(parts.len()).map_err(|_| CanonicalKeyError::TooLarge)?);
    for part in parts {
        encode_part(part, &mut out)?;
    }
    if out.len() > 512 * 1024 {
        return Err(CanonicalKeyError::TooLarge);
    }
    Ok(out)
}

/// Spill / batch group key order: part-by-part, `Null` before any value.
pub fn cmp_spill_key_parts(a: &[PartialKeyPart], b: &[PartialKeyPart]) -> Ordering {
    for (left, right) in a.iter().zip(b.iter()) {
        let ord = cmp_spill_key_part(left, right);
        if ord.is_ne() {
            return ord;
        }
    }
    a.len().cmp(&b.len())
}

fn cmp_spill_key_part(left: &PartialKeyPart, right: &PartialKeyPart) -> Ordering {
    use PartialKeyPart::*;
    match (left, right) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Int(x), Int(y)) => x.cmp(y),
        (Float(x), Float(y)) => x.cmp(y),
        (Bytes(x), Bytes(y)) => x.as_slice().cmp(y.as_slice()),
        (left, right) => spill_key_part_tag(left).cmp(&spill_key_part_tag(right)),
    }
}

fn spill_key_part_tag(part: &PartialKeyPart) -> u8 {
    match part {
        PartialKeyPart::Null => 0,
        PartialKeyPart::Int(_) => 1,
        PartialKeyPart::Float(_) => 2,
        PartialKeyPart::Bytes(_) => 3,
    }
}

pub fn decode_canonical_key(bytes: &[u8]) -> Result<Vec<PartialKeyPart>, String> {
    if bytes.first() != Some(&VERSION) {
        return Err("unsupported canonical key version".to_string());
    }
    let key_len = *bytes.get(1).ok_or("truncated canonical key")? as usize;
    let mut parts = Vec::with_capacity(key_len);
    let mut cursor = 2usize;
    for _ in 0..key_len {
        let (part, next) = decode_part(bytes, cursor)?;
        cursor = next;
        parts.push(part);
    }
    if cursor != bytes.len() {
        return Err("trailing canonical key bytes".to_string());
    }
    Ok(parts)
}

fn encode_part(part: &PartialKeyPart, out: &mut Vec<u8>) -> Result<(), CanonicalKeyError> {
    match part {
        PartialKeyPart::Null => out.push(0),
        PartialKeyPart::Int(v) => {
            out.push(1);
            out.extend_from_slice(&order_preserving_i128(*v));
        }
        PartialKeyPart::Float(bits) => {
            out.push(2);
            out.extend_from_slice(&bits.to_be_bytes());
        }
        PartialKeyPart::Bytes(b) => {
            out.push(3);
            encode_bytes_body(b, out)?;
        }
    }
    Ok(())
}

/// Order-preserving byte string: incremented payload bytes, `0` terminator (`255` → `0xFF 0xFF`).
fn encode_bytes_body(bytes: &[u8], out: &mut Vec<u8>) -> Result<(), CanonicalKeyError> {
    for &b in bytes {
        if b == 255 {
            out.push(0xFF);
            out.push(0xFF);
        } else {
            out.push(b.wrapping_add(1));
        }
    }
    out.push(0);
    Ok(())
}

fn decode_bytes_body(bytes: &[u8], cursor: usize) -> Result<(Vec<u8>, usize), String> {
    let mut out = Vec::new();
    let mut index = cursor;
    loop {
        let b = *bytes.get(index).ok_or("truncated bytes key body")?;
        index += 1;
        if b == 0 {
            break;
        }
        if b == 0xFF {
            let next = *bytes.get(index).ok_or("truncated bytes key escape")?;
            index += 1;
            if next != 0xFF {
                return Err("invalid bytes key escape".into());
            }
            out.push(255);
        } else {
            out.push(b - 1);
        }
    }
    Ok((out, index))
}

fn decode_part(bytes: &[u8], cursor: usize) -> Result<(PartialKeyPart, usize), String> {
    let tag = *bytes.get(cursor).ok_or("truncated key part tag")?;
    match tag {
        0 => Ok((PartialKeyPart::Null, cursor + 1)),
        1 => {
            let end = cursor + 1 + 16;
            let chunk: [u8; 16] = bytes
                .get(cursor + 1..end)
                .ok_or("truncated int key")?
                .try_into()
                .unwrap();
            Ok((
                PartialKeyPart::Int(decode_order_preserving_i128(chunk)),
                end,
            ))
        }
        2 => {
            let end = cursor + 1 + 8;
            let chunk: [u8; 8] = bytes
                .get(cursor + 1..end)
                .ok_or("truncated float key")?
                .try_into()
                .unwrap();
            Ok((PartialKeyPart::Float(u64::from_be_bytes(chunk)), end))
        }
        3 => decode_bytes_body(bytes, cursor + 1).map(|(b, next)| (PartialKeyPart::Bytes(b), next)),
        _ => Err(format!("unknown canonical key tag {tag}")),
    }
}

fn order_preserving_i128(v: i128) -> [u8; 16] {
    ((v as u128) ^ (1u128 << 127)).to_be_bytes()
}

fn decode_order_preserving_i128(bytes: [u8; 16]) -> i128 {
    (u128::from_be_bytes(bytes) ^ (1u128 << 127)) as i128
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_wire_order(left: &[PartialKeyPart], right: &[PartialKeyPart]) {
        let enc_left = encode_canonical_key(left).unwrap();
        let enc_right = encode_canonical_key(right).unwrap();
        assert_eq!(
            enc_left.cmp(&enc_right),
            cmp_spill_key_parts(left, right),
            "left={left:?} right={right:?}"
        );
    }

    #[test]
    fn wire_order_matches_spill_key_cmp() {
        assert_wire_order(&[PartialKeyPart::Null], &[PartialKeyPart::Int(0)]);
        assert_wire_order(&[PartialKeyPart::Int(-1)], &[PartialKeyPart::Int(0)]);
        assert_wire_order(&[PartialKeyPart::Int(0)], &[PartialKeyPart::Int(1)]);
        assert_wire_order(
            &[PartialKeyPart::Int(i128::MIN)],
            &[PartialKeyPart::Int(i128::MAX)],
        );
        assert_wire_order(
            &[PartialKeyPart::Float(1.0f64.to_bits())],
            &[PartialKeyPart::Float(2.0f64.to_bits())],
        );
        assert_wire_order(
            &[PartialKeyPart::Bytes(b"a".to_vec())],
            &[PartialKeyPart::Bytes(b"b".to_vec())],
        );
        assert_wire_order(
            &[PartialKeyPart::Bytes(b"alpha".to_vec())],
            &[PartialKeyPart::Bytes(b"beta".to_vec())],
        );
        assert_wire_order(
            &[PartialKeyPart::Bytes(vec![255])],
            &[PartialKeyPart::Bytes(vec![255, 0])],
        );
        assert_wire_order(
            &[PartialKeyPart::Int(1), PartialKeyPart::Null],
            &[PartialKeyPart::Int(1), PartialKeyPart::Int(0)],
        );
        assert_wire_order(
            &[PartialKeyPart::Null, PartialKeyPart::Bytes(b"x".to_vec())],
            &[PartialKeyPart::Int(0), PartialKeyPart::Bytes(b"x".to_vec())],
        );
    }

    #[test]
    fn wire_order_exhaustive_samples() {
        let samples: Vec<PartialKeyPart> = vec![
            PartialKeyPart::Null,
            PartialKeyPart::Int(-42),
            PartialKeyPart::Int(0),
            PartialKeyPart::Int(99),
            PartialKeyPart::Float((-0.0f64).to_bits()),
            PartialKeyPart::Float(f64::NAN.to_bits()),
            PartialKeyPart::Bytes(Vec::new()),
            PartialKeyPart::Bytes(b"alpha".to_vec()),
            PartialKeyPart::Bytes(b"beta".to_vec()),
            PartialKeyPart::Bytes(vec![255]),
        ];
        for left in &samples {
            for right in &samples {
                assert_wire_order(std::slice::from_ref(left), std::slice::from_ref(right));
            }
        }
    }

    #[test]
    fn roundtrip_composite_key() {
        let parts = vec![
            PartialKeyPart::Int(42),
            PartialKeyPart::Null,
            PartialKeyPart::Bytes(b"hello".to_vec()),
        ];
        let enc = encode_canonical_key(&parts).unwrap();
        assert_eq!(decode_canonical_key(&enc).unwrap(), parts);
        assert_eq!(enc.first(), Some(&VERSION));
    }
}
