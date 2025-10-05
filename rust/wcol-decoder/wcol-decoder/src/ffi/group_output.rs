use crate::constants::{AGG_KIND_APPROX_DISTINCT, AGG_KIND_COUNT, AGG_KIND_COUNT_STAR};
use crate::ffi::{write_f64, write_u32, write_u64};
use crate::query::group_dict_hist::{
    encode_group_hist_partial, group_hist_to_records, plan_uses_group_dict_histogram,
};
use crate::types::{GroupAggState, GroupKey, GroupState, Plan};

pub(crate) fn group_output_count(plan: &Plan, total: usize) -> usize {
    let offset = (plan.offset as usize).min(total);
    let remaining = total.saturating_sub(offset);
    if plan.limit > 0 {
        remaining.min(plan.limit as usize)
    } else {
        remaining
    }
}

pub(crate) fn copy_group_hist_partial(plan: &Plan, out: &mut [u8]) -> Result<usize, usize> {
    let counts = match plan.group_dict_hist_counts.as_ref() {
        Some(c) => c,
        None => return Ok(0),
    };
    let dict_len = plan.group_dict_hist_dict_len as usize;
    let sums = plan.group_dict_hist_sums.as_deref();
    let bytes = encode_group_hist_partial(
        plan.group_dict_hist_dict_len,
        &counts[..dict_len],
        sums,
    );
    if out.len() < bytes.len() {
        return Err(bytes.len());
    }
    out[..bytes.len()].copy_from_slice(&bytes);
    Ok(bytes.len())
}

pub(crate) fn copy_groups(plan: &Plan, out: &mut [u8]) -> Result<usize, usize> {
    if plan_uses_group_dict_histogram(plan) {
        let counts = match plan.group_dict_hist_counts.as_ref() {
            Some(c) => c,
            None => return Ok(0),
        };
        let records = group_hist_to_records(
            counts,
            plan.group_dict_hist_sums.as_deref(),
            plan.group_dict_hist_dict_len,
            &plan.group_aggs,
            plan.group_order_by_count,
            plan.limit,
            plan.offset,
        );
        if out.len() < records.len() {
            return Err(records.len());
        }
        out[..records.len()].copy_from_slice(&records);
        return Ok(records.len());
    }

    if plan.group_emit_raw {
        let agg_count = plan.group_aggs.len();
        let mut needed = 0usize;
        let mut offset = 0usize;
        while offset < plan.group_rows_raw_with_keys.len() {
            if offset + 24 > plan.group_rows_raw_with_keys.len() {
                return Err(plan.group_rows_raw_with_keys.len().saturating_add(1));
            }
            let key_len = u32::from_le_bytes(
                plan.group_rows_raw_with_keys[offset + 16..offset + 20]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let payload_len = 24usize
                .saturating_add(key_len)
                .saturating_add(agg_count.saturating_mul(8 + 8 + 8 + 4 + 4));
            if offset + payload_len > plan.group_rows_raw_with_keys.len() {
                return Err(plan.group_rows_raw_with_keys.len().saturating_add(1));
            }
            needed = needed.saturating_add(16 + agg_count * (8 + 8 + 8 + 4 + 4));
            offset += payload_len;
        }
        if out.len() < needed {
            return Err(needed);
        }
        let mut in_off = 0usize;
        let mut out_off = 0usize;
        while in_off < plan.group_rows_raw_with_keys.len() {
            let key_len = u32::from_le_bytes(
                plan.group_rows_raw_with_keys[in_off + 16..in_off + 20]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let aggs_off = in_off + 24 + key_len;
            let agg_bytes = agg_count * (8 + 8 + 8 + 4 + 4);
            out[out_off..out_off + 16]
                .copy_from_slice(&plan.group_rows_raw_with_keys[in_off..in_off + 16]);
            out_off += 16;
            out[out_off..out_off + agg_bytes]
                .copy_from_slice(&plan.group_rows_raw_with_keys[aggs_off..aggs_off + agg_bytes]);
            out_off += agg_bytes;
            in_off = aggs_off + agg_bytes;
        }
        return Ok(needed);
    }

    let agg_count = plan.group_aggs.len();
    let agg_record_size = 8 + 8 + 8 + 4 + 4;
    let record_size = 16 + agg_count * agg_record_size;
    let total = plan.group_state.len();
    let take = group_output_count(plan, total);
    let skip = (plan.offset as usize).min(total);
    let needed = take * record_size;
    if out.len() < needed {
        return Err(needed);
    }

    let mut offset = 0usize;
    if plan.group_order_by_count && plan.limit > 0 {
        let count_kind = plan
            .group_by
            .as_ref()
            .map(|g| g.count_kind)
            .unwrap_or(AGG_KIND_COUNT_STAR);
        let mut pairs: Vec<(usize, &GroupKey, &GroupState)> = plan
            .group_keys
            .iter()
            .enumerate()
            .filter_map(|(idx, key)| plan.group_state.get(key).map(|state| (idx, key, state)))
            .collect();
        pairs.sort_by(|a, b| {
            group_order_value(plan, count_kind, b.2)
                .cmp(&group_order_value(plan, count_kind, a.2))
                .then_with(|| a.0.cmp(&b.0))
        });
        for (_idx, key, state) in pairs.into_iter().skip(skip).take(take) {
            offset = write_group_record(plan, out, offset, key, state);
        }
    } else {
        for key in plan.group_keys.iter().skip(skip).take(take) {
            let state = match plan.group_state.get(key) {
                Some(s) => s,
                None => continue,
            };
            offset = write_group_record(plan, out, offset, key, state);
        }
    }
    Ok(needed)
}

pub(crate) fn copy_groups_with_keys(plan: &Plan, out: &mut [u8]) -> Result<usize, usize> {
    if plan.group_emit_raw {
        if std::env::var("WCOL_DEBUG_V2")
            .map(|v| v != "0")
            .unwrap_or(false)
        {
            eprintln!(
                "WCOL_V2_COPY group_emit_raw=1 raw_len={}",
                plan.group_rows_raw_with_keys.len()
            );
        }
        let needed = plan.group_rows_raw_with_keys.len();
        if out.len() < needed {
            return Err(needed);
        }
        out[..needed].copy_from_slice(&plan.group_rows_raw_with_keys);
        return Ok(needed);
    }
    if std::env::var("WCOL_DEBUG_V2")
        .map(|v| v != "0")
        .unwrap_or(false)
    {
        eprintln!("WCOL_V2_COPY group_emit_raw=0");
    }

    let agg_count = plan.group_aggs.len();
    let agg_record_size = 8 + 8 + 8 + 4 + 4;
    let total = plan.group_state.len();
    let take = group_output_count(plan, total);
    let skip = (plan.offset as usize).min(total);

    let keys_iter = if plan.group_order_by_count && plan.limit > 0 {
        let count_kind = plan
            .group_by
            .as_ref()
            .map(|g| g.count_kind)
            .unwrap_or(AGG_KIND_COUNT_STAR);
        let mut pairs: Vec<(usize, &GroupKey, &GroupState)> = plan
            .group_keys
            .iter()
            .enumerate()
            .filter_map(|(idx, key)| plan.group_state.get(key).map(|state| (idx, key, state)))
            .collect();
        pairs.sort_by(|a, b| {
            group_order_value(plan, count_kind, b.2)
                .cmp(&group_order_value(plan, count_kind, a.2))
                .then_with(|| a.0.cmp(&b.0))
        });
        pairs
            .into_iter()
            .skip(skip)
            .take(take)
            .map(|(_, k, s)| (*k, s))
            .collect::<Vec<_>>()
    } else {
        plan.group_keys
            .iter()
            .skip(skip)
            .take(take)
            .filter_map(|key| plan.group_state.get(key).map(|state| (*key, state)))
            .collect::<Vec<_>>()
    };

    let needed = keys_iter
        .iter()
        .map(|(key, _)| {
            let key_len = plan.group_key_repr.get(key).map(|v| v.len()).unwrap_or(0);
            24 + key_len + agg_count * agg_record_size
        })
        .sum::<usize>();
    if out.len() < needed {
        return Err(needed);
    }

    let mut offset = 0usize;
    for (key, state) in keys_iter {
        let key_bytes = plan
            .group_key_repr
            .get(&key)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        offset = write_group_record_with_key_bytes(plan, out, offset, &key, key_bytes, state);
    }
    Ok(needed)
}

fn group_order_value(plan: &Plan, count_kind: u8, state: &GroupState) -> u64 {
    for (idx, agg) in plan.group_aggs.iter().enumerate() {
        if agg.kind == count_kind {
            return match state.aggs.get(idx) {
                Some(GroupAggState::Numeric(s)) => s.count as u64,
                Some(GroupAggState::Distinct(set)) => set.len() as u64,
                None => 0,
            };
        }
    }
    0
}

fn write_group_record(
    plan: &Plan,
    out: &mut [u8],
    mut offset: usize,
    key: &GroupKey,
    state: &GroupState,
) -> usize {
    write_u64(out, offset, key.a);
    offset += 8;
    write_u64(out, offset, key.b);
    offset += 8;
    for (idx, agg) in plan.group_aggs.iter().enumerate() {
        let (sum, min, max, count) = agg_stats(agg.kind, state.aggs.get(idx));
        write_f64(out, offset, sum);
        offset += 8;
        write_f64(out, offset, min);
        offset += 8;
        write_f64(out, offset, max);
        offset += 8;
        write_u32(out, offset, count);
        offset += 4;
        out[offset..offset + 4].fill(0);
        offset += 4;
    }
    offset
}

fn write_group_record_with_key_bytes(
    plan: &Plan,
    out: &mut [u8],
    mut offset: usize,
    key: &GroupKey,
    key_bytes: &[u8],
    state: &GroupState,
) -> usize {
    write_u64(out, offset, key.a);
    offset += 8;
    write_u64(out, offset, key.b);
    offset += 8;
    write_u32(out, offset, key_bytes.len() as u32);
    offset += 4;
    out[offset..offset + 4].fill(0);
    offset += 4;
    out[offset..offset + key_bytes.len()].copy_from_slice(key_bytes);
    offset += key_bytes.len();
    for (idx, agg) in plan.group_aggs.iter().enumerate() {
        let (sum, min, max, count) = agg_stats(agg.kind, state.aggs.get(idx));
        write_f64(out, offset, sum);
        offset += 8;
        write_f64(out, offset, min);
        offset += 8;
        write_f64(out, offset, max);
        offset += 8;
        write_u32(out, offset, count);
        offset += 4;
        out[offset..offset + 4].fill(0);
        offset += 4;
    }
    offset
}

fn agg_stats(kind: u8, state: Option<&GroupAggState>) -> (f64, f64, f64, u32) {
    match (kind, state) {
        (AGG_KIND_APPROX_DISTINCT, Some(GroupAggState::Distinct(set))) => {
            let count = set.len() as u32;
            (count as f64, 0.0, 0.0, count)
        }
        (_, Some(GroupAggState::Numeric(s))) => {
            if kind == AGG_KIND_COUNT_STAR || kind == AGG_KIND_COUNT {
                let c = s.count as f64;
                (c, c, c, s.count)
            } else {
                let min = if s.count > 0 { s.min } else { 0.0 };
                let max = if s.count > 0 { s.max } else { 0.0 };
                (s.sum, min, max, s.count)
            }
        }
        _ => (0.0, 0.0, 0.0, 0),
    }
}
