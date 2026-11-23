//! Minimal WCS1 frame helpers (no LZ4). Canonical home for wire framing (I-07).

use crate::stream::core::{WCS_HEADER_BYTES, WCS_MAX_PAYLOAD};

pub const MAX_WCS_PAYLOAD: usize = WCS_MAX_PAYLOAD;

pub const FRAME_DATA_BATCH: u8 = 3;
pub const FRAME_PARTIAL: u8 = 4;
pub const FRAME_END: u8 = 5;
/// Dictionary installed once per `(dict_scope, column)` before the batches that use it.
pub const FRAME_DICTIONARY: u8 = 6;

pub fn encode_frame(
    frame_type: u8,
    sequence: u32,
    payload: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    if payload.len() > MAX_WCS_PAYLOAD {
        return None;
    }
    let total = WCS_HEADER_BYTES + payload.len();
    if out.len() < total {
        return None;
    }
    out[..4].copy_from_slice(&crate::stream::core::WCS_MAGIC.to_le_bytes());
    out[4] = 1;
    out[5] = frame_type;
    out[6..8].copy_from_slice(&0u16.to_le_bytes());
    out[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    out[12..32].fill(0);
    out[24..28].copy_from_slice(&sequence.to_le_bytes());
    out[WCS_HEADER_BYTES..total].copy_from_slice(payload);
    Some(total)
}

pub fn encode_data_batch_projection(
    sequence: u32,
    dict_scope: u32,
    chunk_id: u32,
    body: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let payload_len = 13 + body.len();
    if payload_len > MAX_WCS_PAYLOAD {
        return None;
    }
    let total = WCS_HEADER_BYTES + payload_len;
    if out.len() < total {
        return None;
    }
    out[..4].copy_from_slice(&crate::stream::core::WCS_MAGIC.to_le_bytes());
    out[4] = 1;
    out[5] = FRAME_DATA_BATCH;
    out[6..8].copy_from_slice(&0u16.to_le_bytes());
    out[8..12].copy_from_slice(&(payload_len as u32).to_le_bytes());
    out[12..32].fill(0);
    out[24..28].copy_from_slice(&sequence.to_le_bytes());
    let payload = &mut out[WCS_HEADER_BYTES..total];
    payload[0] = 2; // BatchKind::PROJECTION
    payload[1..5].copy_from_slice(&dict_scope.to_le_bytes());
    payload[5..9].copy_from_slice(&chunk_id.to_le_bytes());
    payload[9..13].copy_from_slice(&(body.len() as u32).to_le_bytes());
    payload[13..].copy_from_slice(body);
    Some(total)
}
