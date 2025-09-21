use std::collections::BinaryHeap;

use crate::ffi::{write_f64, write_u32, write_u64};
use crate::types::{Plan, RowCandidate, RowKey};

const ROW_CANDIDATE_HEADER_SIZE: usize = 36;

pub(crate) fn row_take_count(plan: &Plan) -> usize {
    if plan.limit == 0 {
        0
    } else {
        (plan.limit as usize).saturating_add(plan.offset as usize)
    }
}

pub(crate) fn finalize_rows_from_heap(plan: &mut Plan) {
    if plan.row_heap.is_empty() {
        return;
    }
    let mut items: Vec<RowCandidate> = plan.row_heap.drain().collect();
    items.sort();
    plan.rows.clear();
    let offset = plan.offset as usize;
    for candidate in items.into_iter().skip(offset).take(plan.limit as usize) {
        plan.rows.push(candidate.row_id);
    }
}

pub(crate) fn merge_row_candidates_from_bytes(plan: &mut Plan, bytes: &[u8]) -> Result<(), i32> {
    let take = row_take_count(plan);
    if take == 0 || plan.row_order_by.is_empty() {
        return Ok(());
    }
    let mut offset = 0usize;
    while offset < bytes.len() {
        if offset + ROW_CANDIDATE_HEADER_SIZE > bytes.len() {
            return Err(-2);
        }
        let row_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let k1_type = bytes[offset];
        let k2_type = bytes[offset + 1];
        offset += 4;
        let k1_num = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let k2_num = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let k1_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let k2_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let k1 = match k1_type {
            0 => RowKey::Num(k1_num),
            1 => {
                if offset + k1_len > bytes.len() {
                    return Err(-2);
                }
                let data = bytes[offset..offset + k1_len].to_vec();
                offset += k1_len;
                RowKey::Bytes(data)
            }
            2 => RowKey::Null,
            _ => return Err(-2),
        };
        let k2 = match k2_type {
            0 => None,
            1 => Some(RowKey::Num(k2_num)),
            2 => {
                if offset + k2_len > bytes.len() {
                    return Err(-2);
                }
                let data = bytes[offset..offset + k2_len].to_vec();
                offset += k2_len;
                Some(RowKey::Bytes(data))
            }
            3 => Some(RowKey::Null),
            _ => return Err(-2),
        };

        let candidate = RowCandidate { k1, k2, row_id };
        push_candidate(&mut plan.row_heap, candidate, take);
    }
    Ok(())
}

pub(crate) fn copy_row_candidates(plan: &Plan, out: &mut [u8]) -> Result<usize, usize> {
    if plan.row_order_by.is_empty() || row_take_count(plan) == 0 {
        return Ok(0);
    }
    let needed = encoded_row_candidates_size(&plan.row_heap);
    if out.len() < needed {
        return Err(needed);
    }
    if needed == 0 {
        return Ok(0);
    }
    let mut offset = 0usize;
    for candidate in plan.row_heap.iter() {
        write_u64(out, offset, candidate.row_id);
        offset += 8;

        let (k1_type, k1_num, k1_bytes) = match &candidate.k1 {
            RowKey::Num(v) => (0u8, *v, None),
            RowKey::Bytes(bytes) => (1u8, 0.0f64, Some(bytes.as_slice())),
            RowKey::Null => (2u8, 0.0f64, None),
        };
        let (k2_type, k2_num, k2_bytes) = match &candidate.k2 {
            None => (0u8, 0.0f64, None),
            Some(RowKey::Num(v)) => (1u8, *v, None),
            Some(RowKey::Bytes(bytes)) => (2u8, 0.0f64, Some(bytes.as_slice())),
            Some(RowKey::Null) => (3u8, 0.0f64, None),
        };

        out[offset] = k1_type;
        out[offset + 1] = k2_type;
        out[offset + 2] = 0;
        out[offset + 3] = 0;
        offset += 4;

        write_f64(out, offset, k1_num);
        offset += 8;
        write_f64(out, offset, k2_num);
        offset += 8;

        let k1_len = k1_bytes.map(|bytes| bytes.len()).unwrap_or(0) as u32;
        let k2_len = k2_bytes.map(|bytes| bytes.len()).unwrap_or(0) as u32;
        write_u32(out, offset, k1_len);
        offset += 4;
        write_u32(out, offset, k2_len);
        offset += 4;

        if let Some(bytes) = k1_bytes {
            out[offset..offset + bytes.len()].copy_from_slice(bytes);
            offset += bytes.len();
        }
        if let Some(bytes) = k2_bytes {
            out[offset..offset + bytes.len()].copy_from_slice(bytes);
            offset += bytes.len();
        }
    }
    Ok(needed)
}

pub(crate) fn push_candidate(
    heap: &mut BinaryHeap<RowCandidate>,
    candidate: RowCandidate,
    take: usize,
) {
    if heap.len() < take {
        heap.push(candidate);
        return;
    }
    if let Some(worst) = heap.peek() {
        if candidate < *worst {
            heap.pop();
            heap.push(candidate);
        }
    }
}

fn encoded_row_candidates_size(heap: &BinaryHeap<RowCandidate>) -> usize {
    let mut needed = 0usize;
    for candidate in heap.iter() {
        let k1_len = match &candidate.k1 {
            RowKey::Bytes(bytes) => bytes.len(),
            _ => 0,
        };
        let k2_len = match &candidate.k2 {
            Some(RowKey::Bytes(bytes)) => bytes.len(),
            _ => 0,
        };
        needed = needed.saturating_add(ROW_CANDIDATE_HEADER_SIZE + k1_len + k2_len);
    }
    needed
}
