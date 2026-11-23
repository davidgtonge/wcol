//! V2 byte-stream operator program: opcodes, cursor, catalog encoding.

use crate::error::{WcolError, WcolResult};
use crate::ir::CatalogColumn;

pub const PLAN_V2: u8 = 0xFF;
/// Physical page-copy compaction into one Parquet object (ReadAt + WritePort).
pub const OP_PAGE_COMPACT: u8 = 11;
/// One node's [`crate::program::NodeFragment`] from the physical planner: plan fetches, decode,
/// operators built from plan structures. No SQL. WCS1 partial on the host edge.
pub const OP_NODE_FRAGMENT: u8 = 12;
/// Coordinator row fragment: concatenated node row frames in, sort/limit/project, typed frames out.
pub const OP_COORDINATOR_FRAGMENT: u8 = 13;
/// WCS1 `FRAME_PARTIAL` streams from N producers → merged PWCL on the output edge.
pub const OP_WCS_PARTIAL_REDUCE: u8 = 14;
/// Finalizer: merge node partials, then finalized groups as rows through the coordinator steps
/// (sort, limit, project), out as row frames or one Parquet file.
pub const OP_FINALIZE_FRAGMENT: u8 = 15;
/// One hash-partition reducer: row batches in, a final partial for that key range out.
pub const OP_HASH_REDUCER: u8 = 16;
/// One reducer's Parquet shard: its final partial in (every group complete and once), rows
/// through the coordinator steps (no sort or limit) and out as Parquet as each frame arrives.
pub const OP_SHARD_EXPORT: u8 = 17;

pub struct PlanCursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> PlanCursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    pub fn u8(&mut self) -> WcolResult<u8> {
        let b = *self
            .bytes
            .get(self.at)
            .ok_or_else(|| WcolError::from("truncated plan"))?;
        self.at += 1;
        Ok(b)
    }

    pub fn u32(&mut self) -> WcolResult<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub fn u64(&mut self) -> WcolResult<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub fn i64(&mut self) -> WcolResult<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    pub fn u16(&mut self) -> WcolResult<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub fn array<const N: usize>(&mut self) -> WcolResult<[u8; N]> {
        let end = self
            .at
            .checked_add(N)
            .ok_or_else(|| WcolError::from("truncated plan"))?;
        let slice = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| WcolError::from("truncated plan"))?;
        let mut out = [0u8; N];
        out.copy_from_slice(slice);
        self.at = end;
        Ok(out)
    }

    pub fn bytes(&mut self, len: usize) -> WcolResult<&'a [u8]> {
        let end = self
            .at
            .checked_add(len)
            .ok_or_else(|| WcolError::from("truncated plan"))?;
        let slice = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| WcolError::from("truncated plan"))?;
        self.at = end;
        Ok(slice)
    }

    pub fn str(&mut self) -> WcolResult<&'a str> {
        let len = self.u32()? as usize;
        std::str::from_utf8(self.bytes(len)?).map_err(|_| WcolError::from("sql is not utf-8"))
    }

    pub fn optional_u8(&mut self) -> WcolResult<Option<u8>> {
        if self.at >= self.bytes.len() {
            return Ok(None);
        }
        Ok(Some(self.u8()?))
    }

    pub fn exhausted(&self) -> bool {
        self.at >= self.bytes.len()
    }

    pub fn catalog(&mut self) -> WcolResult<Vec<CatalogColumn>> {
        use crate::ir::ColumnKind;
        let count = self.u32()? as usize;
        let mut catalog = Vec::with_capacity(count);
        for _ in 0..count {
            let kind = match self.u8()? {
                0 => ColumnKind::Number,
                1 => ColumnKind::Text,
                _ => return Err(WcolError::from("column kind is invalid")),
            };
            let physical = i32::from_le_bytes(self.array()?);
            let type_length = i32::from_le_bytes(self.array()?);
            let max_def = self.u8()?;
            let max_rep = self.u8()?;
            let name_len = u16::from_le_bytes(self.array()?) as usize;
            let name_bytes = self.bytes(name_len)?;
            let name = std::str::from_utf8(name_bytes)
                .map_err(|_| WcolError::from("column name is not utf-8"))?
                .to_string();
            catalog.push(CatalogColumn {
                name,
                kind,
                physical,
                type_length,
                max_def,
                max_rep,
            });
        }
        Ok(catalog)
    }
}

pub fn encode_catalog(out: &mut Vec<u8>, catalog: &[CatalogColumn]) {
    out.extend_from_slice(&(catalog.len() as u32).to_le_bytes());
    for column in catalog {
        let kind = match column.kind {
            crate::ir::ColumnKind::Number => 0u8,
            crate::ir::ColumnKind::Text => 1u8,
        };
        out.push(kind);
        out.extend_from_slice(&column.physical.to_le_bytes());
        out.extend_from_slice(&column.type_length.to_le_bytes());
        out.push(column.max_def);
        out.push(column.max_rep);
        let name = column.name.as_bytes();
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name);
    }
}

/// Single-operator v2 program: merge worker PWCL partials from WCS ingress.
pub fn encode_wcs_partial_reduce_program(producer_count: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.push(PLAN_V2);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.push(OP_WCS_PARTIAL_REDUCE);
    out.extend_from_slice(&producer_count.to_le_bytes());
    out
}
