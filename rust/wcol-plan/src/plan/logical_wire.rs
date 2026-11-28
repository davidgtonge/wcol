//! Deterministic binary serialisation of [`WcolLogicalPlan`] for hashing and transport.

use crate::plan::physical_plan::{LogicalOp, WcolLogicalPlan};
use crate::program::codec::{
    decode_agg, decode_predicate, decode_sort_key, encode_agg, encode_predicate, encode_sort_key,
    push_str, push_u32, push_u64, read_str, read_u32, read_u64,
};

pub use crate::program::codec::LogicalWireError;

const WIRE_VERSION: u8 = 2;

pub fn encode_logical_plan(plan: &WcolLogicalPlan) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(WIRE_VERSION);
    encode_op(&plan.root, &mut out);
    out
}

pub fn decode_logical_plan(bytes: &[u8]) -> Result<WcolLogicalPlan, LogicalWireError> {
    if bytes.first().copied() != Some(WIRE_VERSION) {
        return Err(LogicalWireError::BadVersion);
    }
    let mut cur = &bytes[1..];
    let root = decode_op(&mut cur)?;
    if !cur.is_empty() {
        return Err(LogicalWireError::TrailingBytes);
    }
    Ok(WcolLogicalPlan { root })
}

impl WcolLogicalPlan {
    /// Canonical bytes for task-id hashing and coordinator transport.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        encode_logical_plan(self)
    }
}

fn encode_op(op: &LogicalOp, out: &mut Vec<u8>) {
    match op {
        LogicalOp::Scan { table, columns } => {
            out.push(1);
            push_str(out, table);
            push_u32(out, columns.len() as u32);
            for column in columns {
                push_u32(out, *column);
            }
        }
        LogicalOp::Filter { input, predicates } => {
            out.push(2);
            encode_op(input, out);
            push_u32(out, predicates.len() as u32);
            for predicate in predicates {
                encode_predicate(predicate, out);
            }
        }
        LogicalOp::Project { input, columns } => {
            out.push(3);
            encode_op(input, out);
            push_u32(out, columns.len() as u32);
            for column in columns {
                push_u32(out, *column);
            }
        }
        LogicalOp::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            out.push(4);
            encode_op(input, out);
            push_u32(out, group_by.len() as u32);
            for column in group_by {
                push_u32(out, *column);
            }
            push_u32(out, aggregates.len() as u32);
            for agg in aggregates {
                encode_agg(agg, out);
            }
        }
        LogicalOp::Sort { input, keys } => {
            out.push(5);
            encode_op(input, out);
            push_u32(out, keys.len() as u32);
            for key in keys {
                encode_sort_key(key, out);
            }
        }
        LogicalOp::Limit { input, skip, fetch } => {
            out.push(6);
            encode_op(input, out);
            push_u64(out, *skip);
            out.push(fetch.is_some() as u8);
            if let Some(fetch) = fetch {
                push_u64(out, *fetch);
            }
        }
    }
}

fn decode_op(cur: &mut &[u8]) -> Result<LogicalOp, LogicalWireError> {
    let tag = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)?;
    *cur = &cur[1..];
    Ok(match tag {
        1 => {
            let table = read_str(cur)?;
            let count = read_u32(cur)? as usize;
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                columns.push(read_u32(cur)?);
            }
            LogicalOp::Scan { table, columns }
        }
        2 => {
            let input = Box::new(decode_op(cur)?);
            let count = read_u32(cur)? as usize;
            let mut predicates = Vec::with_capacity(count);
            for _ in 0..count {
                predicates.push(decode_predicate(cur)?);
            }
            LogicalOp::Filter { input, predicates }
        }
        3 => {
            let input = Box::new(decode_op(cur)?);
            let count = read_u32(cur)? as usize;
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                columns.push(read_u32(cur)?);
            }
            LogicalOp::Project { input, columns }
        }
        4 => {
            let input = Box::new(decode_op(cur)?);
            let group_count = read_u32(cur)? as usize;
            let mut group_by = Vec::with_capacity(group_count);
            for _ in 0..group_count {
                group_by.push(read_u32(cur)?);
            }
            let agg_count = read_u32(cur)? as usize;
            let mut aggregates = Vec::with_capacity(agg_count);
            for _ in 0..agg_count {
                aggregates.push(decode_agg(cur)?);
            }
            LogicalOp::Aggregate {
                input,
                group_by,
                aggregates,
            }
        }
        5 => {
            let input = Box::new(decode_op(cur)?);
            let count = read_u32(cur)? as usize;
            let mut keys = Vec::with_capacity(count);
            for _ in 0..count {
                keys.push(decode_sort_key(cur)?);
            }
            LogicalOp::Sort { input, keys }
        }
        6 => {
            let input = Box::new(decode_op(cur)?);
            let skip = read_u64(cur)?;
            let has_fetch = *cur.first().ok_or(LogicalWireError::UnexpectedEnd)?;
            *cur = &cur[1..];
            let fetch = if has_fetch != 0 {
                Some(read_u64(cur)?)
            } else {
                None
            };
            LogicalOp::Limit { input, skip, fetch }
        }
        _ => return Err(LogicalWireError::UnknownTag),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::logical_from_parser::lower_sql_v0_to_logical;
    use crate::plan::sql_parser::{Column, ColumnKind};

    #[test]
    fn round_trip_logical_bytes() {
        let columns = vec![Column {
            name: "AdvEngineID",
            kind: ColumnKind::Number,
        }];
        let plan = lower_sql_v0_to_logical(
            "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
            &columns,
            "hits",
        )
        .unwrap();
        let bytes = plan.canonical_bytes();
        let again = decode_logical_plan(&bytes).unwrap();
        assert_eq!(plan, again);
    }
}
