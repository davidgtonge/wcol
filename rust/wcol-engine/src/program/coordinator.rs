//! Coordinator fragment wire format. The planner chooses the steps; this module encodes them.

//! Coordinator operators for a row query: sort, limit, and project after the exchange.
//!
//! The program carries no SQL. Node row frames are merged in node order on one ingress.

use crate::program::codec::{
    decode_sort_key, encode_sort_key, push_str, push_u32, push_u64, read_list, read_str, read_u32,
    read_u64, read_u8, LogicalWireError,
};
use crate::program::stream_program::{
    OP_COORDINATOR_FRAGMENT, OP_FINALIZE_FRAGMENT, OP_SHARD_EXPORT, PLAN_V2,
};
use crate::program::types::SortKey;

const COORDINATOR_WIRE_VERSION: u8 = 1;

/// One column of a coordinator Parquet sink, in output order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportColumn {
    pub name: String,
    pub optional: bool,
    pub physical: i32,
    pub type_length: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoordinatorStep {
    Project(Vec<u32>),
    Sort(Vec<SortKey>),
    Limit {
        skip: u64,
        fetch: Option<u64>,
    },
    Parquet {
        target_row_group_rows: u32,
        columns: Vec<ExportColumn>,
    },
}

pub fn encode_coordinator_program(node_count: u32, steps: &[CoordinatorStep]) -> Vec<u8> {
    let body = encode_coordinator_steps(steps);
    let mut out = Vec::with_capacity(body.len() + 32);
    out.push(PLAN_V2);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.push(OP_COORDINATOR_FRAGMENT);
    out.extend_from_slice(&node_count.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Finalizer program: `producer_count` partial streams in. The steps see one row per group,
/// keys at columns `0..k` and aggregate values at `k + i`.
pub fn encode_finalize_program(producer_count: u32, steps: &[CoordinatorStep]) -> Vec<u8> {
    let body = encode_coordinator_steps(steps);
    let mut out = Vec::with_capacity(body.len() + 32);
    out.push(PLAN_V2);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.push(OP_FINALIZE_FRAGMENT);
    out.extend_from_slice(&producer_count.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Shard export program ([`OP_SHARD_EXPORT`]): one reducer's final partial in, `steps` out.
pub fn encode_shard_export_program(steps: &[CoordinatorStep]) -> Vec<u8> {
    let body = encode_coordinator_steps(steps);
    let mut out = Vec::with_capacity(body.len() + 32);
    out.push(PLAN_V2);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.push(OP_SHARD_EXPORT);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

pub fn encode_coordinator_steps(steps: &[CoordinatorStep]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(COORDINATOR_WIRE_VERSION);
    push_u32(&mut out, steps.len() as u32);
    for step in steps {
        match step {
            CoordinatorStep::Project(columns) => {
                out.push(1);
                push_u32(&mut out, columns.len() as u32);
                for column in columns {
                    push_u32(&mut out, *column);
                }
            }
            CoordinatorStep::Sort(keys) => {
                out.push(2);
                push_u32(&mut out, keys.len() as u32);
                for key in keys {
                    encode_sort_key(key, &mut out);
                }
            }
            CoordinatorStep::Limit { skip, fetch } => {
                out.push(3);
                push_u64(&mut out, *skip);
                match fetch {
                    Some(fetch) => {
                        out.push(1);
                        push_u64(&mut out, *fetch);
                    }
                    None => out.push(0),
                }
            }
            CoordinatorStep::Parquet {
                target_row_group_rows,
                columns,
            } => {
                out.push(4);
                push_u32(&mut out, *target_row_group_rows);
                push_u32(&mut out, columns.len() as u32);
                for column in columns {
                    push_str(&mut out, &column.name);
                    out.push(u8::from(column.optional));
                    out.extend_from_slice(&column.physical.to_le_bytes());
                    out.extend_from_slice(&column.type_length.to_le_bytes());
                }
            }
        }
    }
    out
}

pub fn decode_coordinator_steps(
    mut bytes: &[u8],
) -> Result<Vec<CoordinatorStep>, LogicalWireError> {
    let version = *bytes.first().ok_or(LogicalWireError::UnexpectedEnd)?;
    bytes = &bytes[1..];
    if version != COORDINATOR_WIRE_VERSION {
        return Err(LogicalWireError::UnknownTag);
    }
    let steps = read_list(&mut bytes, decode_coordinator_step)?;
    if !bytes.is_empty() {
        return Err(LogicalWireError::UnknownTag);
    }
    Ok(steps)
}

fn read_i32(bytes: &mut &[u8]) -> Result<i32, LogicalWireError> {
    Ok(read_u32(bytes)? as i32)
}

fn decode_coordinator_step(bytes: &mut &[u8]) -> Result<CoordinatorStep, LogicalWireError> {
    Ok(match read_u8(bytes)? {
        1 => CoordinatorStep::Project(read_list(bytes, read_u32)?),
        2 => CoordinatorStep::Sort(read_list(bytes, decode_sort_key)?),
        3 => {
            let skip = read_u64(bytes)?;
            let fetch = match read_u8(bytes)? {
                0 => None,
                _ => Some(read_u64(bytes)?),
            };
            CoordinatorStep::Limit { skip, fetch }
        }
        4 => {
            let target_row_group_rows = read_u32(bytes)?;
            let columns = read_list(bytes, |bytes| {
                Ok(ExportColumn {
                    name: read_str(bytes)?,
                    optional: read_u8(bytes)? != 0,
                    physical: read_i32(bytes)?,
                    type_length: read_i32(bytes)?,
                })
            })?;
            CoordinatorStep::Parquet {
                target_row_group_rows,
                columns,
            }
        }
        _ => return Err(LogicalWireError::UnknownTag),
    })
}
