//! Run-open payloads for R2 put and read.

/// Smallest run read the merge issues.
pub const RUN_READ_CHUNK: u32 = 256 * 1024;
/// Largest single run read.
pub const MAX_RUN_READ_CHUNK: u32 = 8 * 1024 * 1024;
/// Raw bytes the merge may request across all open runs at once.
pub const MERGE_READ_BUDGET: u64 = 8 * 1024 * 1024;

/// Bytes per run read: the merge budget shared by the open runs. Each R2 read is one
/// round trip, so few runs read large chunks; many runs fall back to the 256 KiB floor.
pub fn run_read_chunk(open_runs: usize, budget: u64) -> u32 {
    let share = budget / open_runs.max(1) as u64;
    share.clamp(u64::from(RUN_READ_CHUNK), u64::from(MAX_RUN_READ_CHUNK)) as u32
}

/// Bytes in every part of a run but its last. R2 needs at least 5 MiB per non-final part and
/// equal sizes for all of them; the smallest legal part keeps the least run in Wasm memory.
pub const RUN_PART_BYTES: usize = 5 * 1024 * 1024;

/// One part of a run upload. A run that fits in one part is a single put; a longer one is a
/// multipart upload the host opens at part 1 and completes at the last part.
#[derive(Clone, Debug)]
pub struct RunPutPlan {
    pub object_key: String,
    /// 1-based, as R2 numbers multipart parts.
    pub part: u32,
    pub last: bool,
    /// Bytes in this part.
    pub byte_length: u64,
    /// Records in the whole run.
    pub record_count: u64,
}

const RUN_PUT_TRAILER: usize = 4 + 1 + 8 + 8;

#[derive(Clone, Debug)]
pub struct RunReadPlan {
    pub object_key: String,
    pub byte_length: u64,
    pub offset: u64,
    pub length: u32,
}

pub fn decode_run_put_open(payload: &[u8]) -> Result<RunPutPlan, String> {
    if payload.len() < 4 + RUN_PUT_TRAILER {
        return Err("run put open payload truncated".into());
    }
    let key_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let end = 4usize.saturating_add(key_len);
    let key = std::str::from_utf8(payload.get(4..end).ok_or("run put key truncated")?)
        .map_err(|_| "run put key not utf-8")?
        .to_string();
    let trailer = payload
        .get(end..end.saturating_add(RUN_PUT_TRAILER))
        .ok_or("run put trailer truncated")?;
    Ok(RunPutPlan {
        object_key: key,
        part: u32::from_le_bytes(trailer[0..4].try_into().unwrap()),
        last: trailer[4] != 0,
        byte_length: u64::from_le_bytes(trailer[5..13].try_into().unwrap()),
        record_count: u64::from_le_bytes(trailer[13..21].try_into().unwrap()),
    })
}

pub fn encode_run_put_open(plan: &RunPutPlan) -> Result<Vec<u8>, String> {
    let key = plan.object_key.as_bytes();
    if key.len() > u32::MAX as usize {
        return Err("run put key too long".into());
    }
    let mut out = Vec::with_capacity(4 + key.len() + RUN_PUT_TRAILER);
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&plan.part.to_le_bytes());
    out.push(u8::from(plan.last));
    out.extend_from_slice(&plan.byte_length.to_le_bytes());
    out.extend_from_slice(&plan.record_count.to_le_bytes());
    Ok(out)
}

pub fn decode_run_read_open(payload: &[u8]) -> Result<RunReadPlan, String> {
    if payload.len() < 20 {
        return Err("run read open payload truncated".into());
    }
    let key_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let end = 4usize.saturating_add(key_len);
    let key = std::str::from_utf8(payload.get(4..end).ok_or("run read key truncated")?)
        .map_err(|_| "run read key not utf-8")?
        .to_string();
    let trailer = payload
        .get(end..end.saturating_add(20))
        .ok_or("run read trailer truncated")?;
    let byte_length = u64::from_le_bytes(trailer[0..8].try_into().unwrap());
    let offset = u64::from_le_bytes(trailer[8..16].try_into().unwrap());
    let length = u32::from_le_bytes(trailer[16..20].try_into().unwrap());
    Ok(RunReadPlan {
        object_key: key,
        byte_length,
        offset,
        length,
    })
}

pub fn encode_run_read_open(plan: &RunReadPlan) -> Result<Vec<u8>, String> {
    let key = plan.object_key.as_bytes();
    if key.len() > u32::MAX as usize {
        return Err("run read key too long".into());
    }
    let mut out = Vec::with_capacity(4 + key.len() + 20);
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&plan.byte_length.to_le_bytes());
    out.extend_from_slice(&plan.offset.to_le_bytes());
    out.extend_from_slice(&plan.length.to_le_bytes());
    Ok(out)
}
