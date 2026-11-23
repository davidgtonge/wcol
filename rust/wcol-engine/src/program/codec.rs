//! Binary codecs for predicates, aggregates, sort keys, and the byte helpers they share.
//!
//! Logical-plan framing stays in the planner. Fragment codecs call these functions so a
//! predicate has one encoding.

use super::types::{AggExpr, AggKind, CmpOp, Compare, Literal, Predicate, SortKey, SortTarget};
use crate::ir::Scalar;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalWireError {
    BadVersion,
    TrailingBytes,
    UnexpectedEnd,
    UnknownTag,
}

pub fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub fn read_u32(cur: &mut &[u8]) -> Result<u32, LogicalWireError> {
    if cur.len() < 4 {
        return Err(LogicalWireError::UnexpectedEnd);
    }
    let value = u32::from_le_bytes(cur[..4].try_into().unwrap());
    *cur = &cur[4..];
    Ok(value)
}

pub fn read_u64(cur: &mut &[u8]) -> Result<u64, LogicalWireError> {
    if cur.len() < 8 {
        return Err(LogicalWireError::UnexpectedEnd);
    }
    let value = u64::from_le_bytes(cur[..8].try_into().unwrap());
    *cur = &cur[8..];
    Ok(value)
}

/// LEB128: small values (ids, counts, lengths) take one or two bytes.
pub fn push_var(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub fn read_var(cur: &mut &[u8]) -> Result<u64, LogicalWireError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = cur.split_first().ok_or(LogicalWireError::UnexpectedEnd)?;
        *cur = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
    }
    Err(LogicalWireError::UnknownTag)
}

pub fn read_var_u32(cur: &mut &[u8]) -> Result<u32, LogicalWireError> {
    u32::try_from(read_var(cur)?).map_err(|_| LogicalWireError::UnknownTag)
}

/// Signed values as varints: small magnitudes either side of zero stay short.
pub fn push_var_signed(out: &mut Vec<u8>, value: i64) {
    push_var(out, ((value << 1) ^ (value >> 63)) as u64);
}

pub fn read_var_signed(cur: &mut &[u8]) -> Result<i64, LogicalWireError> {
    let raw = read_var(cur)?;
    Ok((raw >> 1) as i64 ^ -((raw & 1) as i64))
}

pub fn push_str(out: &mut Vec<u8>, text: &str) {
    push_u32(out, text.len() as u32);
    out.extend_from_slice(text.as_bytes());
}

pub fn read_str(cur: &mut &[u8]) -> Result<String, LogicalWireError> {
    let len = read_u32(cur)? as usize;
    if cur.len() < len {
        return Err(LogicalWireError::UnexpectedEnd);
    }
    let text = std::str::from_utf8(&cur[..len])
        .map_err(|_| LogicalWireError::UnexpectedEnd)?
        .to_string();
    *cur = &cur[len..];
    Ok(text)
}

pub fn encode_literal(literal: &Literal, out: &mut Vec<u8>) {
    match literal {
        Literal::Int(v) => {
            out.push(1);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Literal::Float(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Literal::Bool(v) => {
            out.push(3);
            out.push(u8::from(*v));
        }
        Literal::Text(v) => {
            out.push(4);
            push_str(out, v);
        }
    }
}

pub fn decode_literal(cur: &mut &[u8]) -> Result<Literal, LogicalWireError> {
    let tag = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)?;
    *cur = &cur[1..];
    Ok(match tag {
        1 => {
            if cur.len() < 8 {
                return Err(LogicalWireError::UnexpectedEnd);
            }
            let value = i64::from_le_bytes(cur[..8].try_into().unwrap());
            *cur = &cur[8..];
            Literal::Int(value)
        }
        2 => {
            if cur.len() < 8 {
                return Err(LogicalWireError::UnexpectedEnd);
            }
            let value = u64::from_le_bytes(cur[..8].try_into().unwrap());
            *cur = &cur[8..];
            Literal::Float(value)
        }
        3 => {
            let value = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)?;
            *cur = &cur[1..];
            Literal::Bool(value != 0)
        }
        4 => Literal::Text(read_str(cur)?),
        _ => return Err(LogicalWireError::UnknownTag),
    })
}

pub fn encode_cmp(op: CmpOp, out: &mut Vec<u8>) {
    out.push(match op {
        CmpOp::Eq => 1,
        CmpOp::NotEq => 2,
        CmpOp::Lt => 3,
        CmpOp::LtEq => 4,
        CmpOp::Gt => 5,
        CmpOp::GtEq => 6,
    });
}

pub fn decode_cmp(cur: &mut &[u8]) -> Result<CmpOp, LogicalWireError> {
    let tag = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)?;
    *cur = &cur[1..];
    Ok(match tag {
        1 => CmpOp::Eq,
        2 => CmpOp::NotEq,
        3 => CmpOp::Lt,
        4 => CmpOp::LtEq,
        5 => CmpOp::Gt,
        6 => CmpOp::GtEq,
        _ => return Err(LogicalWireError::UnknownTag),
    })
}

pub fn encode_predicate(predicate: &Predicate, out: &mut Vec<u8>) {
    match predicate {
        Predicate::Compare(compare) => {
            out.push(1);
            push_u32(out, compare.column);
            encode_cmp(compare.op, out);
            encode_literal(&compare.value, out);
        }
        Predicate::IsNull { column } => {
            out.push(2);
            push_u32(out, *column);
        }
        Predicate::IsNotNull { column } => {
            out.push(3);
            push_u32(out, *column);
        }
        Predicate::Like {
            column,
            pattern,
            negated,
        } => {
            out.push(4);
            push_u32(out, *column);
            push_str(out, pattern);
            out.push(u8::from(*negated));
        }
        Predicate::In {
            column,
            values,
            negated,
        } => {
            out.push(5);
            push_u32(out, *column);
            out.push(u8::from(*negated));
            push_u32(out, values.len() as u32);
            for value in values {
                encode_literal(value, out);
            }
        }
    }
}

pub fn decode_predicate(cur: &mut &[u8]) -> Result<Predicate, LogicalWireError> {
    let tag = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)?;
    *cur = &cur[1..];
    Ok(match tag {
        1 => Predicate::Compare(Compare {
            column: read_u32(cur)?,
            op: decode_cmp(cur)?,
            value: decode_literal(cur)?,
        }),
        2 => Predicate::IsNull {
            column: read_u32(cur)?,
        },
        3 => Predicate::IsNotNull {
            column: read_u32(cur)?,
        },
        4 => {
            let column = read_u32(cur)?;
            let pattern = read_str(cur)?;
            let negated = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)? != 0;
            *cur = &cur[1..];
            Predicate::Like {
                column,
                pattern,
                negated,
            }
        }
        5 => {
            let column = read_u32(cur)?;
            let negated = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)? != 0;
            *cur = &cur[1..];
            let count = read_u32(cur)? as usize;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_literal(cur)?);
            }
            Predicate::In {
                column,
                values,
                negated,
            }
        }
        _ => return Err(LogicalWireError::UnknownTag),
    })
}

pub fn encode_agg_kind(kind: AggKind, out: &mut Vec<u8>) {
    out.push(match kind {
        AggKind::CountStar => 1,
        AggKind::Count => 2,
        AggKind::Sum => 3,
        AggKind::Avg => 4,
        AggKind::Min => 5,
        AggKind::Max => 6,
        AggKind::CountDistinct => 7,
        AggKind::ApproxDistinct => 8,
    });
}

pub fn decode_agg_kind(cur: &mut &[u8]) -> Result<AggKind, LogicalWireError> {
    let tag = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)?;
    *cur = &cur[1..];
    Ok(match tag {
        1 => AggKind::CountStar,
        2 => AggKind::Count,
        3 => AggKind::Sum,
        4 => AggKind::Avg,
        5 => AggKind::Min,
        6 => AggKind::Max,
        7 => AggKind::CountDistinct,
        8 => AggKind::ApproxDistinct,
        _ => return Err(LogicalWireError::UnknownTag),
    })
}

pub fn encode_agg(agg: &AggExpr, out: &mut Vec<u8>) {
    encode_agg_kind(agg.kind, out);
    out.push(agg.column.is_some() as u8);
    if let Some(column) = agg.column {
        push_u32(out, column);
    }
    out.push(agg.offset as u8);
}

pub fn decode_agg(cur: &mut &[u8]) -> Result<AggExpr, LogicalWireError> {
    let kind = decode_agg_kind(cur)?;
    let has_column = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)? != 0;
    *cur = &cur[1..];
    let column = if has_column {
        Some(read_u32(cur)?)
    } else {
        None
    };
    let offset = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)? as i8;
    *cur = &cur[1..];
    Ok(AggExpr {
        kind,
        column,
        offset,
    })
}

pub fn encode_sort_key(key: &SortKey, out: &mut Vec<u8>) {
    match key.target {
        SortTarget::Column(column) => {
            out.push(1);
            push_u32(out, column);
        }
        SortTarget::Aggregate(index) => {
            out.push(2);
            push_u32(out, index);
        }
    }
    out.push(u8::from(key.descending));
    out.push(u8::from(key.nulls_first));
}

pub fn decode_sort_key(cur: &mut &[u8]) -> Result<SortKey, LogicalWireError> {
    let tag = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)?;
    *cur = &cur[1..];
    let target = match tag {
        1 => SortTarget::Column(read_u32(cur)?),
        2 => SortTarget::Aggregate(read_u32(cur)?),
        _ => return Err(LogicalWireError::UnknownTag),
    };
    let descending = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)? != 0;
    *cur = &cur[1..];
    let nulls_first = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)? != 0;
    *cur = &cur[1..];
    Ok(SortKey {
        target,
        descending,
        nulls_first,
    })
}

pub fn encode_scalar(scalar: &Scalar, out: &mut Vec<u8>) {
    match scalar {
        Scalar::Int(value) => {
            out.push(1);
            push_u64(out, *value as u64);
        }
        Scalar::Number(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Scalar::Text(text) => {
            out.push(3);
            push_str(out, text);
        }
    }
}

pub fn decode_scalar(cur: &mut &[u8]) -> Result<Scalar, LogicalWireError> {
    let tag = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)?;
    *cur = &cur[1..];
    Ok(match tag {
        1 => Scalar::Int(read_u64(cur)? as i64),
        2 => {
            let bytes = cur.get(..8).ok_or(LogicalWireError::UnexpectedEnd)?;
            *cur = &cur[8..];
            Scalar::Number(f64::from_le_bytes(bytes.try_into().unwrap()))
        }
        3 => Scalar::Text(read_str(cur)?),
        _ => return Err(LogicalWireError::UnknownTag),
    })
}

pub fn encode_scalar_list(values: &[Scalar], out: &mut Vec<u8>) {
    push_u32(out, values.len() as u32);
    for value in values {
        encode_scalar(value, out);
    }
}

pub fn decode_scalar_list(cur: &mut &[u8]) -> Result<Vec<Scalar>, LogicalWireError> {
    let count = read_u32(cur)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(decode_scalar(cur)?);
    }
    Ok(out)
}

#[cfg(test)]
mod varint_tests {
    use super::*;

    #[test]
    fn varints_round_trip_at_the_edges() {
        let mut out = Vec::new();
        let unsigned = [
            0,
            1,
            127,
            128,
            16_383,
            16_384,
            u64::from(u32::MAX),
            u64::MAX,
        ];
        let signed = [0, -1, 1, -64, 64, i64::MIN, i64::MAX];
        unsigned.iter().for_each(|v| push_var(&mut out, *v));
        signed.iter().for_each(|v| push_var_signed(&mut out, *v));
        let mut cur = out.as_slice();
        assert!(unsigned.iter().all(|v| read_var(&mut cur) == Ok(*v)));
        assert!(signed.iter().all(|v| read_var_signed(&mut cur) == Ok(*v)));
        assert!(cur.is_empty());
        assert_eq!(
            read_var(&mut [0x80u8].as_slice()),
            Err(LogicalWireError::UnexpectedEnd)
        );
    }
}
