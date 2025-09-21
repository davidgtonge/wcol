#![allow(dead_code)]

use crate::constants::{FLAG_DICT, MASK_WORDS, OP_BETWEEN, OP_LIKE, OP_NOT_LIKE};
use crate::decode::find_subslice;
use crate::query::compare::{cmp_f64, cmp_i32, cmp_i64, cmp_u32};
use crate::query::mask::{is_valid, mask_is_zero, mask_or, set_bit};
use crate::query::scale::scaled_rhs_pair;
use crate::runtime::dict_value_bytes;
use crate::types::{Column, ColumnData, Filter, IndexEntry, Runtime};

#[derive(Clone, Copy)]
pub(crate) struct Possible {
    pub(crate) maybe_true: bool,
    pub(crate) maybe_false: bool,
}

impl Possible {
    const fn unknown() -> Self {
        Self {
            maybe_true: true,
            maybe_false: true,
        }
    }

    const fn of(maybe_true: bool, maybe_false: bool) -> Self {
        Self {
            maybe_true,
            maybe_false,
        }
    }
}

#[inline]
fn build_cmp_mask<T, F, C>(
    values: &[T],
    rows: usize,
    op: u8,
    rhs: C,
    rhs2: C,
    cast: F,
    cmp: fn(C, u8, C, C) -> bool,
) -> Vec<u32>
where
    T: Copy,
    F: Fn(T) -> C,
    C: Copy,
{
    let mut mask = vec![0u32; MASK_WORDS];
    for i in 0..rows {
        if cmp(cast(values[i]), op, rhs, rhs2) {
            set_bit(&mut mask, i);
        }
    }
    mask
}

#[inline]
fn scaled_i64_rhs(value: f64, value2: f64, scale: i32) -> Option<(i64, i64)> {
    scaled_rhs_pair(value, value2, scale)
}

#[inline]
fn empty_mask() -> Vec<u32> {
    vec![0u32; MASK_WORDS]
}

pub(crate) fn filter_possible(
    filter: &Filter,
    col: &Column,
    entry: &IndexEntry,
    runtime: &Runtime,
) -> Possible {
    if col.physical_type == crate::constants::TYPE_STRING {
        return Possible::unknown();
    }
    if filter.op == OP_LIKE || filter.op == OP_NOT_LIKE {
        return Possible::unknown();
    }
    if filter.value_str.is_some() || filter.in_list_str.is_some() {
        return Possible::unknown();
    }
    if (col.flags & FLAG_DICT) != 0 {
        return dict_filter_possible(filter, col, entry, runtime);
    }
    numeric_filter_possible(filter, entry)
}

fn numeric_filter_possible(filter: &Filter, entry: &IndexEntry) -> Possible {
    let min = entry.min;
    let max = entry.max;
    let has_nulls = entry.null_raw_len > 0;
    if !min.is_finite() || !max.is_finite() {
        return Possible::unknown();
    }
    if let Some(values) = &filter.in_list {
        if values.is_empty() {
            return Possible {
                maybe_true: false,
                maybe_false: true,
            };
        }
        let mut any = false;
        for value in values {
            if !value.is_finite() {
                continue;
            }
            if *value >= min && *value <= max {
                any = true;
                break;
            }
        }
        let mut always_true = false;
        if !has_nulls && min == max {
            for value in values {
                if *value == min {
                    always_true = true;
                    break;
                }
            }
        }
        return Possible {
            maybe_true: any,
            maybe_false: !always_true,
        };
    }
    if !filter.value.is_finite() || !filter.value2.is_finite() {
        return Possible::unknown();
    }
    match filter.op {
        crate::constants::OP_EQ => {
            let maybe_true = filter.value >= min && filter.value <= max;
            let always_true = !has_nulls && min == max && filter.value == min;
            Possible {
                maybe_true,
                maybe_false: !always_true,
            }
        }
        crate::constants::OP_NEQ => {
            let always_false = !has_nulls && min == max && filter.value == min;
            let always_true = !has_nulls && (filter.value < min || filter.value > max);
            Possible {
                maybe_true: !always_false,
                maybe_false: !always_true,
            }
        }
        crate::constants::OP_LT => {
            let maybe_true = min < filter.value;
            let always_true = !has_nulls && max < filter.value;
            Possible {
                maybe_true,
                maybe_false: !always_true,
            }
        }
        crate::constants::OP_LTE => {
            let maybe_true = min <= filter.value;
            let always_true = !has_nulls && max <= filter.value;
            Possible {
                maybe_true,
                maybe_false: !always_true,
            }
        }
        crate::constants::OP_GT => {
            let maybe_true = max > filter.value;
            let always_true = !has_nulls && min > filter.value;
            Possible {
                maybe_true,
                maybe_false: !always_true,
            }
        }
        crate::constants::OP_GTE => {
            let maybe_true = max >= filter.value;
            let always_true = !has_nulls && min >= filter.value;
            Possible {
                maybe_true,
                maybe_false: !always_true,
            }
        }
        OP_BETWEEN => {
            let lo = filter.value.min(filter.value2);
            let hi = filter.value.max(filter.value2);
            let maybe_true = !(max < lo || min > hi);
            let always_true = !has_nulls && min >= lo && max <= hi;
            Possible {
                maybe_true,
                maybe_false: !always_true,
            }
        }
        _ => Possible {
            maybe_true: true,
            maybe_false: true,
        },
    }
}

fn dict_filter_possible(
    filter: &Filter,
    col: &Column,
    entry: &IndexEntry,
    runtime: &Runtime,
) -> Possible {
    let dict = match runtime.dicts.get(&col.dict_id) {
        Some(d) => d,
        None => {
            return Possible {
                maybe_true: true,
                maybe_false: true,
            }
        }
    };
    let dict_len = dict.values.len();
    let has_nulls = entry.null_raw_len > 0;
    if dict_len == 0 {
        return Possible::unknown();
    }
    if dict_len > 64 {
        return Possible::unknown();
    }
    let presence = entry.presence;
    if presence == 0 {
        return Possible {
            maybe_true: false,
            maybe_false: true,
        };
    }
    if let Some(values) = &filter.in_list {
        let mut list_mask = 0u64;
        for value in values {
            if *value < 0.0 || value.fract() != 0.0 {
                continue;
            }
            let id = *value as u64;
            if id < dict_len as u64 {
                list_mask |= 1u64 << id;
            }
        }
        let maybe_true = (presence & list_mask) != 0;
        let always_true = !has_nulls && presence != 0 && (presence & !list_mask) == 0;
        return Possible {
            maybe_true,
            maybe_false: !always_true,
        };
    }
    if filter.value < 0.0 || filter.value.fract() != 0.0 {
        return Possible {
            maybe_true: false,
            maybe_false: true,
        };
    }
    let id = filter.value as u64;
    if id >= dict_len as u64 {
        let always_true = !has_nulls && presence != 0 && filter.op == crate::constants::OP_NEQ;
        return Possible {
            maybe_true: filter.op == crate::constants::OP_NEQ && presence != 0,
            maybe_false: !always_true,
        };
    }
    let id_mask = 1u64 << id;
    match filter.op {
        crate::constants::OP_EQ => {
            let maybe_true = (presence & id_mask) != 0;
            let always_true = !has_nulls && presence == id_mask;
            Possible {
                maybe_true,
                maybe_false: !always_true,
            }
        }
        crate::constants::OP_NEQ => {
            let maybe_true = (presence & !id_mask) != 0;
            let always_true = !has_nulls && (presence & id_mask) == 0;
            Possible {
                maybe_true,
                maybe_false: !always_true,
            }
        }
        _ => Possible {
            maybe_true: true,
            maybe_false: true,
        },
    }
}

pub(crate) fn eval_possible(tokens: &[i32], statuses: &[Possible]) -> Option<Possible> {
    let mut stack: Vec<Possible> = Vec::new();
    for token in tokens {
        if *token >= 0 {
            let idx = *token as usize;
            if idx >= statuses.len() {
                return None;
            }
            stack.push(statuses[idx]);
            continue;
        }
        match *token {
            crate::constants::COMB_NOT => {
                let a = stack.pop()?;
                stack.push(Possible {
                    maybe_true: a.maybe_false,
                    maybe_false: a.maybe_true,
                });
            }
            crate::constants::COMB_AND => {
                let b = stack.pop()?;
                let a = stack.pop()?;
                stack.push(Possible {
                    maybe_true: a.maybe_true && b.maybe_true,
                    maybe_false: a.maybe_false || b.maybe_false,
                });
            }
            crate::constants::COMB_OR => {
                let b = stack.pop()?;
                let a = stack.pop()?;
                stack.push(Possible {
                    maybe_true: a.maybe_true || b.maybe_true,
                    maybe_false: a.maybe_false && b.maybe_false,
                });
            }
            _ => return None,
        }
    }
    if stack.len() != 1 {
        return None;
    }
    stack.pop()
}

pub(crate) fn build_filter_mask(
    col: &Column,
    data: &ColumnData,
    filter: &Filter,
    rows: usize,
    runtime: Option<&Runtime>,
) -> Vec<u32> {
    let mut mask = vec![0u32; MASK_WORDS];

    if filter.op == OP_LIKE || filter.op == OP_NOT_LIKE {
        if let Some(like_ids) = filter.like_ids.as_deref() {
            return build_like_mask_ids(data, like_ids, filter.op == OP_NOT_LIKE, rows);
        }
        if let (Some(pattern), Some(rt)) = (filter.value_str.as_deref(), runtime) {
            return build_like_mask(col, data, pattern, filter.op == OP_NOT_LIKE, rows, rt);
        }
        return mask;
    }

    if let Some(values) = &filter.in_list {
        for value in values {
            let one = build_single_mask(col, data, filter.op, *value, *value, rows);
            if mask_is_zero(&mask) {
                mask = one;
            } else {
                mask = mask_or(&mask, &one);
            }
        }
        return mask;
    }

    build_single_mask(col, data, filter.op, filter.value, filter.value2, rows)
}

pub(crate) fn build_like_id_set(col: &Column, pattern: &str, runtime: &Runtime) -> Vec<u8> {
    let dict = match runtime.dicts.get(&col.dict_id) {
        Some(d) => d,
        None => return Vec::new(),
    };
    let pattern_bytes = pattern.as_bytes();
    let size = if !dict.offsets.is_empty() {
        dict.offsets.len().saturating_sub(1)
    } else {
        dict.values.len()
    };
    if size == 0 {
        return Vec::new();
    }
    let mut out = vec![0u8; size];
    if pattern_bytes.is_empty() {
        out.fill(1);
        return out;
    }
    for id in 0..size {
        let s = match dict_value_bytes(dict, id) {
            Some(b) => b,
            None => continue,
        };
        let contains = find_subslice(s, pattern_bytes).is_some();
        if contains {
            out[id] = 1;
        }
    }
    out
}

fn build_like_mask_ids(data: &ColumnData, like_ids: &[u8], negated: bool, rows: usize) -> Vec<u32> {
    let mut mask = vec![0u32; MASK_WORDS];
    let ColumnData::U32(ids) = data else {
        return mask;
    };
    let n = rows.min(ids.len());
    for i in 0..n {
        let id = ids[i] as usize;
        let contains = like_ids.get(id).copied().unwrap_or(0) != 0;
        if contains != negated {
            set_bit(&mut mask, i);
        }
    }
    mask
}

fn build_like_mask(
    col: &Column,
    data: &ColumnData,
    pattern: &str,
    negated: bool,
    rows: usize,
    runtime: &Runtime,
) -> Vec<u32> {
    let mut mask = vec![0u32; MASK_WORDS];
    let dict = match runtime.dicts.get(&col.dict_id) {
        Some(d) => d,
        None => return mask,
    };
    let pattern_bytes = pattern.as_bytes();
    if pattern_bytes.is_empty() {
        for i in 0..rows {
            set_bit(&mut mask, i);
        }
        return mask;
    }
    let ColumnData::U32(ids) = data else {
        return mask;
    };
    for i in 0..rows.min(ids.len()) {
        let id = ids[i] as usize;
        let s = match dict_value_bytes(dict, id) {
            Some(b) => b,
            None => {
                if negated {
                    set_bit(&mut mask, i);
                }
                continue;
            }
        };
        let contains = find_subslice(s, pattern_bytes).is_some();
        if contains != negated {
            set_bit(&mut mask, i);
        }
    }
    mask
}

pub(crate) fn build_single_mask(
    col: &Column,
    data: &ColumnData,
    op: u8,
    value: f64,
    value2: f64,
    rows: usize,
) -> Vec<u32> {
    macro_rules! scaled_i64 {
        ($values:expr, $cast:expr) => {{
            let (rhs, rhs2) = match scaled_i64_rhs(value, value2, col.scale) {
                Some(pair) => pair,
                None => return empty_mask(),
            };
            build_cmp_mask($values, rows, op, rhs, rhs2, $cast, cmp_i64)
        }};
    }

    match data {
        ColumnData::U8(values) => {
            if col.scale != 0 {
                scaled_i64!(values, |v: u8| v as i64)
            } else {
                build_cmp_mask(values, rows, op, value as u32, value2 as u32, |v: u8| v as u32, cmp_u32)
            }
        }
        ColumnData::U16(values) => {
            if col.scale != 0 {
                scaled_i64!(values, |v: u16| v as i64)
            } else {
                build_cmp_mask(values, rows, op, value as u32, value2 as u32, |v: u16| v as u32, cmp_u32)
            }
        }
        ColumnData::I8(values) => {
            if col.scale != 0 {
                scaled_i64!(values, |v: i8| v as i64)
            } else {
                build_cmp_mask(values, rows, op, value as i32, value2 as i32, |v: i8| v as i32, cmp_i32)
            }
        }
        ColumnData::I16(values) => {
            if col.scale != 0 {
                scaled_i64!(values, |v: i16| v as i64)
            } else {
                build_cmp_mask(values, rows, op, value as i32, value2 as i32, |v: i16| v as i32, cmp_i32)
            }
        }
        ColumnData::I32(values) => {
            if col.scale != 0 {
                scaled_i64!(values, |v: i32| v as i64)
            } else {
                let rhs = value as i32;
                let rhs2 = value2 as i32;
                #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
                {
                    return build_mask_i32_simd(values, op, rhs, rhs2, rows);
                }
                #[cfg(not(all(target_arch = "wasm32", target_feature = "simd128")))]
                {
                    build_cmp_mask(values, rows, op, rhs, rhs2, |v: i32| v, cmp_i32)
                }
            }
        }
        ColumnData::I64(values) => {
            if col.scale != 0 {
                scaled_i64!(values, |v: i64| v)
            } else {
                build_cmp_mask(values, rows, op, value as i64, value2 as i64, |v: i64| v, cmp_i64)
            }
        }
        ColumnData::U32(values) => {
            if col.scale != 0 {
                scaled_i64!(values, |v: u32| v as i64)
            } else {
                let rhs = value as u32;
                let rhs2 = value2 as u32;
                #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
                {
                    return build_mask_u32_simd(values, op, rhs, rhs2, rows);
                }
                #[cfg(not(all(target_arch = "wasm32", target_feature = "simd128")))]
                {
                    build_cmp_mask(values, rows, op, rhs, rhs2, |v: u32| v, cmp_u32)
                }
            }
        }
        ColumnData::F64(values) => {
            let rhs = value;
            let rhs2 = value2;
            #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
            {
                return build_mask_f64_simd(values, op, rhs, rhs2, rows);
            }
            #[cfg(not(all(target_arch = "wasm32", target_feature = "simd128")))]
            {
                build_cmp_mask(values, rows, op, rhs, rhs2, |v: f64| v, cmp_f64)
            }
        }
        ColumnData::Bool(values) => {
            let mut mask = empty_mask();
            let want = value != 0.0;
            for i in 0..rows {
                let bit = is_valid(values, i);
                if (want && bit) || (!want && !bit) {
                    set_bit(&mut mask, i);
                }
            }
            mask
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
#[inline(always)]
unsafe fn simd_cmp_u32x4(
    op: u8,
    v: core::arch::wasm32::v128,
    rhs_v: core::arch::wasm32::v128,
    rhs2_v: core::arch::wasm32::v128,
    all: core::arch::wasm32::v128,
    sign: core::arch::wasm32::v128,
    rhs_signed: core::arch::wasm32::v128,
    rhs2_signed: core::arch::wasm32::v128,
) -> core::arch::wasm32::v128 {
    use core::arch::wasm32::*;
    match op {
        crate::constants::OP_EQ => i32x4_eq(v, rhs_v),
        crate::constants::OP_NEQ => v128_xor(i32x4_eq(v, rhs_v), all),
        crate::constants::OP_LT => i32x4_lt(v128_xor(v, sign), rhs_signed),
        crate::constants::OP_LTE => i32x4_le(v128_xor(v, sign), rhs_signed),
        crate::constants::OP_GT => i32x4_gt(v128_xor(v, sign), rhs_signed),
        crate::constants::OP_GTE => i32x4_ge(v128_xor(v, sign), rhs_signed),
        OP_BETWEEN => {
            let vs = v128_xor(v, sign);
            let ge = i32x4_ge(vs, rhs_signed);
            let le = i32x4_le(vs, rhs2_signed);
            v128_and(ge, le)
        }
        _ => u32x4_splat(0),
    }
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
#[inline(always)]
unsafe fn simd_cmp_i32x4(
    op: u8,
    v: core::arch::wasm32::v128,
    rhs_v: core::arch::wasm32::v128,
    rhs2_v: core::arch::wasm32::v128,
    all: core::arch::wasm32::v128,
) -> core::arch::wasm32::v128 {
    use core::arch::wasm32::*;
    match op {
        crate::constants::OP_EQ => i32x4_eq(v, rhs_v),
        crate::constants::OP_NEQ => v128_xor(i32x4_eq(v, rhs_v), all),
        crate::constants::OP_LT => i32x4_lt(v, rhs_v),
        crate::constants::OP_LTE => i32x4_le(v, rhs_v),
        crate::constants::OP_GT => i32x4_gt(v, rhs_v),
        crate::constants::OP_GTE => i32x4_ge(v, rhs_v),
        OP_BETWEEN => {
            let ge = i32x4_ge(v, rhs_v);
            let le = i32x4_le(v, rhs2_v);
            v128_and(ge, le)
        }
        _ => u32x4_splat(0),
    }
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
#[inline(always)]
unsafe fn simd_cmp_f64x2(
    op: u8,
    v: core::arch::wasm32::v128,
    rhs_v: core::arch::wasm32::v128,
    rhs2_v: core::arch::wasm32::v128,
) -> core::arch::wasm32::v128 {
    use core::arch::wasm32::*;
    match op {
        crate::constants::OP_EQ => f64x2_eq(v, rhs_v),
        crate::constants::OP_NEQ => v128_or(f64x2_lt(v, rhs_v), f64x2_gt(v, rhs_v)),
        crate::constants::OP_LT => f64x2_lt(v, rhs_v),
        crate::constants::OP_LTE => f64x2_le(v, rhs_v),
        crate::constants::OP_GT => f64x2_gt(v, rhs_v),
        crate::constants::OP_GTE => f64x2_ge(v, rhs_v),
        OP_BETWEEN => v128_and(f64x2_ge(v, rhs_v), f64x2_le(v, rhs2_v)),
        _ => u64x2_splat(0),
    }
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
fn build_mask_u32_simd(values: &[u32], op: u8, rhs: u32, rhs2: u32, rows: usize) -> Vec<u32> {
    use core::arch::wasm32::*;
    let mut mask = vec![0u32; MASK_WORDS];
    unsafe {
        let mut i = 0;
        let ptr = values.as_ptr();
        let rhs_v = u32x4_splat(rhs);
        let rhs2_v = u32x4_splat(rhs2);
        let all = u32x4_splat(u32::MAX);
        let sign = u32x4_splat(0x8000_0000);
        let rhs_signed = v128_xor(rhs_v, sign);
        let rhs2_signed = v128_xor(rhs2_v, sign);
        while i + 32 <= rows {
            let mut word = 0u32;
            for lane_chunk in 0..8usize {
                let base = i + (lane_chunk << 2);
                let v = v128_load(ptr.add(base) as *const v128);
                let cmp = simd_cmp_u32x4(op, v, rhs_v, rhs2_v, all, sign, rhs_signed, rhs2_signed);
                word |= (i32x4_bitmask(cmp) as u32) << (lane_chunk << 2);
            }
            mask[i >> 5] = word;
            i += 32;
        }
        while i + 4 <= rows {
            let v = v128_load(ptr.add(i) as *const v128);
            let cmp = simd_cmp_u32x4(op, v, rhs_v, rhs2_v, all, sign, rhs_signed, rhs2_signed);
            mask[i >> 5] |= (i32x4_bitmask(cmp) as u32) << (i & 31);
            i += 4;
        }
        for j in i..rows {
            if cmp_u32(values[j], op, rhs, rhs2) {
                set_bit(&mut mask, j);
            }
        }
    }
    mask
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
fn build_mask_i32_simd(values: &[i32], op: u8, rhs: i32, rhs2: i32, rows: usize) -> Vec<u32> {
    use core::arch::wasm32::*;
    let mut mask = vec![0u32; MASK_WORDS];
    unsafe {
        let mut i = 0;
        let ptr = values.as_ptr();
        let rhs_v = i32x4_splat(rhs);
        let rhs2_v = i32x4_splat(rhs2);
        let all = u32x4_splat(u32::MAX);
        while i + 32 <= rows {
            let mut word = 0u32;
            for lane_chunk in 0..8usize {
                let base = i + (lane_chunk << 2);
                let v = v128_load(ptr.add(base) as *const v128);
                let cmp = simd_cmp_i32x4(op, v, rhs_v, rhs2_v, all);
                word |= (i32x4_bitmask(cmp) as u32) << (lane_chunk << 2);
            }
            mask[i >> 5] = word;
            i += 32;
        }
        while i + 4 <= rows {
            let v = v128_load(ptr.add(i) as *const v128);
            let cmp = simd_cmp_i32x4(op, v, rhs_v, rhs2_v, all);
            mask[i >> 5] |= (i32x4_bitmask(cmp) as u32) << (i & 31);
            i += 4;
        }
        for j in i..rows {
            if cmp_i32(values[j], op, rhs, rhs2) {
                set_bit(&mut mask, j);
            }
        }
    }
    mask
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
fn build_mask_f64_simd(values: &[f64], op: u8, rhs: f64, rhs2: f64, rows: usize) -> Vec<u32> {
    use core::arch::wasm32::*;
    let mut mask = vec![0u32; MASK_WORDS];
    unsafe {
        let mut i = 0usize;
        let ptr = values.as_ptr();
        let rhs_v = f64x2_splat(rhs);
        let rhs2_v = f64x2_splat(rhs2);
        while i + 32 <= rows {
            let mut word = 0u32;
            for lane_chunk in 0..16usize {
                let base = i + (lane_chunk << 1);
                let v = v128_load(ptr.add(base) as *const v128);
                let cmp = simd_cmp_f64x2(op, v, rhs_v, rhs2_v);
                word |= (i64x2_bitmask(cmp) as u32) << (lane_chunk << 1);
            }
            mask[i >> 5] = word;
            i += 32;
        }
        while i + 2 <= rows {
            let v = v128_load(ptr.add(i) as *const v128);
            let cmp = simd_cmp_f64x2(op, v, rhs_v, rhs2_v);
            mask[i >> 5] |= (i64x2_bitmask(cmp) as u32) << (i & 31);
            i += 2;
        }
        for j in i..rows {
            if cmp_f64(values[j], op, rhs, rhs2) {
                set_bit(&mut mask, j);
            }
        }
    }
    mask
}
