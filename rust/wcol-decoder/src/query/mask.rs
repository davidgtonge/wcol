use crate::constants::MASK_WORDS;

pub(crate) fn mask_from_bitmap(bitmap: &[u8], rows: usize) -> Option<Vec<u32>> {
    let needed = rows.div_ceil(8);
    if bitmap.len() < needed {
        return None;
    }
    let mut mask = vec![0u32; MASK_WORDS];
    let needed_words = rows.div_ceil(32);
    if needed_words == 0 {
        return Some(mask);
    }
    fill_mask_from_bitmap_words(&mut mask[..needed_words], &bitmap[..needed]);
    let tail_bits = rows & 31;
    if tail_bits != 0 {
        mask[needed_words - 1] &= (1u32 << tail_bits) - 1;
    }
    Some(mask)
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
pub(crate) fn bitmap_is_all_valid(bitmap: &[u8], rows: usize) -> bool {
    use core::arch::wasm32::*;
    let needed = rows.div_ceil(8);
    if needed == 0 {
        return true;
    }
    if bitmap.len() < needed {
        return false;
    }
    let tail_bits = rows & 7;
    let full_bytes = if tail_bits == 0 { needed } else { needed - 1 };
    unsafe {
        let mut i = 0usize;
        let ptr = bitmap.as_ptr();
        let all = u8x16_splat(0xffu8);
        while i + 16 <= full_bytes {
            let v = v128_load(ptr.add(i) as *const v128);
            let eq = u8x16_eq(v, all);
            if u8x16_bitmask(eq) != 0xffff {
                return false;
            }
            i += 16;
        }
        for j in i..full_bytes {
            if bitmap[j] != 0xff {
                return false;
            }
        }
    }
    if tail_bits == 0 {
        return true;
    }
    let keep = (1u8 << tail_bits) - 1;
    (bitmap[full_bytes] & keep) == keep
}

#[cfg(all(target_arch = "wasm32", not(target_feature = "simd128")))]
pub(crate) fn bitmap_is_all_valid(_bitmap: &[u8], _rows: usize) -> bool {
    panic!("bitmap_is_all_valid requires wasm32 simd128");
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn bitmap_is_all_valid(bitmap: &[u8], rows: usize) -> bool {
    let needed = rows.div_ceil(8);
    if needed == 0 {
        return true;
    }
    if bitmap.len() < needed {
        return false;
    }
    let tail_bits = rows & 7;
    let full_bytes = if tail_bits == 0 { needed } else { needed - 1 };
    for byte in bitmap.iter().take(full_bytes) {
        if *byte != 0xff {
            return false;
        }
    }
    if tail_bits == 0 {
        return true;
    }
    let keep = (1u8 << tail_bits) - 1;
    (bitmap[full_bytes] & keep) == keep
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
fn fill_mask_from_bitmap_words(dst_words: &mut [u32], src_bytes: &[u8]) {
    use core::arch::wasm32::*;
    let dst_len = dst_words.len() * 4;
    let dst_bytes =
        unsafe { core::slice::from_raw_parts_mut(dst_words.as_mut_ptr() as *mut u8, dst_len) };
    unsafe {
        let mut i = 0usize;
        while i + 16 <= src_bytes.len() {
            let v = v128_load(src_bytes.as_ptr().add(i) as *const v128);
            v128_store(dst_bytes.as_mut_ptr().add(i) as *mut v128, v);
            i += 16;
        }
        if i < src_bytes.len() {
            dst_bytes[i..src_bytes.len()].copy_from_slice(&src_bytes[i..]);
        }
    }
}

#[cfg(not(all(target_arch = "wasm32", target_feature = "simd128")))]
fn fill_mask_from_bitmap_words(dst_words: &mut [u32], src_bytes: &[u8]) {
    let mut word = 0usize;
    let mut idx = 0usize;
    while idx < src_bytes.len() {
        let remain = src_bytes.len() - idx;
        let take = remain.min(4);
        let mut bytes = [0u8; 4];
        bytes[..take].copy_from_slice(&src_bytes[idx..idx + take]);
        dst_words[word] = u32::from_le_bytes(bytes);
        idx += take;
        word += 1;
    }
}

pub(crate) fn set_bit(mask: &mut [u32], idx: usize) {
    let word = idx >> 5;
    let bit = idx & 31;
    mask[word] |= 1 << bit;
}

pub(crate) fn get_bit(mask: &[u32], idx: usize) -> bool {
    let word = idx >> 5;
    let bit = idx & 31;
    (mask[word] & (1 << bit)) != 0
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
pub(crate) fn mask_and(a: &[u32], b: &[u32]) -> Vec<u32> {
    mask_and_simd_wasm(a, b)
}

#[cfg(all(
    not(target_arch = "wasm32"),
    target_arch = "x86_64",
    target_feature = "sse2"
))]
pub(crate) fn mask_and(a: &[u32], b: &[u32]) -> Vec<u32> {
    mask_and_simd(a, b)
}

#[cfg(not(any(
    all(target_arch = "wasm32", target_feature = "simd128"),
    all(
        not(target_arch = "wasm32"),
        target_arch = "x86_64",
        target_feature = "sse2"
    )
)))]
pub(crate) fn mask_and(a: &[u32], b: &[u32]) -> Vec<u32> {
    a.iter().zip(b.iter()).map(|(x, y)| x & y).collect()
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
pub(crate) fn mask_or(a: &[u32], b: &[u32]) -> Vec<u32> {
    mask_or_simd_wasm(a, b)
}

#[cfg(all(
    not(target_arch = "wasm32"),
    target_arch = "x86_64",
    target_feature = "sse2"
))]
pub(crate) fn mask_or(a: &[u32], b: &[u32]) -> Vec<u32> {
    mask_or_simd(a, b)
}

#[cfg(not(any(
    all(target_arch = "wasm32", target_feature = "simd128"),
    all(
        not(target_arch = "wasm32"),
        target_arch = "x86_64",
        target_feature = "sse2"
    )
)))]
pub(crate) fn mask_or(a: &[u32], b: &[u32]) -> Vec<u32> {
    a.iter().zip(b.iter()).map(|(x, y)| x | y).collect()
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
pub(crate) fn mask_not(mask: &[u32]) -> Vec<u32> {
    mask_not_simd_wasm(mask)
}

#[cfg(all(
    not(target_arch = "wasm32"),
    target_arch = "x86_64",
    target_feature = "sse2"
))]
pub(crate) fn mask_not(mask: &[u32]) -> Vec<u32> {
    mask_not_simd(mask)
}

#[cfg(not(any(
    all(target_arch = "wasm32", target_feature = "simd128"),
    all(
        not(target_arch = "wasm32"),
        target_arch = "x86_64",
        target_feature = "sse2"
    )
)))]
pub(crate) fn mask_not(mask: &[u32]) -> Vec<u32> {
    mask.iter().map(|x| !x).collect()
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
fn mask_and_simd(a: &[u32], b: &[u32]) -> Vec<u32> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(a.len());
    let mut idx = 0;
    while idx + 4 <= a.len() {
        unsafe {
            let av = _mm_loadu_si128(a[idx..].as_ptr() as *const __m128i);
            let bv = _mm_loadu_si128(b[idx..].as_ptr() as *const __m128i);
            let cv = _mm_and_si128(av, bv);
            let mut chunk = [0u32; 4];
            _mm_storeu_si128(chunk.as_mut_ptr() as *mut __m128i, cv);
            out.extend_from_slice(&chunk);
        }
        idx += 4;
    }
    while idx < a.len() {
        out.push(a[idx] & b[idx]);
        idx += 1;
    }
    out
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
fn mask_or_simd(a: &[u32], b: &[u32]) -> Vec<u32> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(a.len());
    let mut idx = 0;
    while idx + 4 <= a.len() {
        unsafe {
            let av = _mm_loadu_si128(a[idx..].as_ptr() as *const __m128i);
            let bv = _mm_loadu_si128(b[idx..].as_ptr() as *const __m128i);
            let cv = _mm_or_si128(av, bv);
            let mut chunk = [0u32; 4];
            _mm_storeu_si128(chunk.as_mut_ptr() as *mut __m128i, cv);
            out.extend_from_slice(&chunk);
        }
        idx += 4;
    }
    while idx < a.len() {
        out.push(a[idx] | b[idx]);
        idx += 1;
    }
    out
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
fn mask_not_simd(mask: &[u32]) -> Vec<u32> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(mask.len());
    let mut idx = 0;
    while idx + 4 <= mask.len() {
        unsafe {
            let mv = _mm_loadu_si128(mask[idx..].as_ptr() as *const __m128i);
            let nv = _mm_xor_si128(mv, _mm_set1_epi32(-1));
            let mut chunk = [0u32; 4];
            _mm_storeu_si128(chunk.as_mut_ptr() as *mut __m128i, nv);
            out.extend_from_slice(&chunk);
        }
        idx += 4;
    }
    while idx < mask.len() {
        out.push(!mask[idx]);
        idx += 1;
    }
    out
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
fn mask_and_simd_wasm(a: &[u32], b: &[u32]) -> Vec<u32> {
    use core::arch::wasm32::*;
    let mut out = vec![0u32; MASK_WORDS];
    unsafe {
        let mut i = 0;
        let ap = a.as_ptr();
        let bp = b.as_ptr();
        let op = out.as_mut_ptr();
        while i + 4 <= MASK_WORDS {
            let va = v128_load(ap.add(i) as *const v128);
            let vb = v128_load(bp.add(i) as *const v128);
            v128_store(op.add(i) as *mut v128, v128_and(va, vb));
            i += 4;
        }
        for j in i..MASK_WORDS {
            out[j] = a[j] & b[j];
        }
    }
    out
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
fn mask_or_simd_wasm(a: &[u32], b: &[u32]) -> Vec<u32> {
    use core::arch::wasm32::*;
    let mut out = vec![0u32; MASK_WORDS];
    unsafe {
        let mut i = 0;
        let ap = a.as_ptr();
        let bp = b.as_ptr();
        let op = out.as_mut_ptr();
        while i + 4 <= MASK_WORDS {
            let va = v128_load(ap.add(i) as *const v128);
            let vb = v128_load(bp.add(i) as *const v128);
            v128_store(op.add(i) as *mut v128, v128_or(va, vb));
            i += 4;
        }
        for j in i..MASK_WORDS {
            out[j] = a[j] | b[j];
        }
    }
    out
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
fn mask_not_simd_wasm(mask: &[u32]) -> Vec<u32> {
    use core::arch::wasm32::*;
    let mut out = vec![0u32; MASK_WORDS];
    unsafe {
        let all = u32x4_splat(u32::MAX);
        let mut i = 0;
        let mp = mask.as_ptr();
        let op = out.as_mut_ptr();
        while i + 4 <= MASK_WORDS {
            let va = v128_load(mp.add(i) as *const v128);
            v128_store(op.add(i) as *mut v128, v128_xor(va, all));
            i += 4;
        }
        for j in i..MASK_WORDS {
            out[j] = !mask[j];
        }
    }
    out
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
pub(crate) fn mask_is_zero(mask: &[u32]) -> bool {
    use core::arch::wasm32::*;
    unsafe {
        let mut i = 0;
        let mp = mask.as_ptr();
        while i + 4 <= MASK_WORDS {
            let v = v128_load(mp.add(i) as *const v128);
            if v128_any_true(v) {
                return false;
            }
            i += 4;
        }
        for j in i..MASK_WORDS {
            if mask[j] != 0 {
                return false;
            }
        }
    }
    true
}

#[cfg(not(all(target_arch = "wasm32", target_feature = "simd128")))]
pub(crate) fn mask_is_zero(mask: &[u32]) -> bool {
    mask.iter().all(|x| *x == 0)
}

pub(crate) fn is_valid(bitmap: &[u8], row: usize) -> bool {
    let byte = bitmap[row >> 3];
    let bit = row & 7;
    (byte & (1 << bit)) != 0
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
pub(crate) fn mask_count(mask: &[u32]) -> u32 {
    use core::arch::wasm32::*;
    let mut total: u32 = 0;
    unsafe {
        let mut i = 0;
        let mp = mask.as_ptr();
        while i + 4 <= MASK_WORDS {
            let v = v128_load(mp.add(i) as *const v128);
            let pc = i8x16_popcnt(v);
            let sum16 = i16x8_extadd_pairwise_i8x16(pc);
            let sum32 = i32x4_extadd_pairwise_i16x8(sum16);
            total += i32x4_extract_lane::<0>(sum32) as u32;
            total += i32x4_extract_lane::<1>(sum32) as u32;
            total += i32x4_extract_lane::<2>(sum32) as u32;
            total += i32x4_extract_lane::<3>(sum32) as u32;
            i += 4;
        }
        for j in i..MASK_WORDS {
            total += mask[j].count_ones();
        }
    }
    total
}

#[cfg(not(all(target_arch = "wasm32", target_feature = "simd128")))]
pub(crate) fn mask_count(mask: &[u32]) -> u32 {
    mask.iter().map(|x| x.count_ones()).sum()
}

fn mask_is_full_tail(mask: &[u32], full_words: usize, tail_bits: usize) -> bool {
    if tail_bits == 0 {
        return mask.iter().skip(full_words).take(MASK_WORDS - full_words).all(|word| *word == 0);
    }
    let keep = (1u32 << tail_bits) - 1;
    if mask[full_words] != keep {
        return false;
    }
    mask.iter()
        .skip(full_words + 1)
        .take(MASK_WORDS - full_words - 1)
        .all(|word| *word == 0)
}

#[cfg(all(target_arch = "wasm32", not(target_feature = "simd128")))]
pub(crate) fn mask_is_full(mask: &[u32], rows: usize) -> bool {
    mask_is_full_scalar(mask, rows)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn mask_is_full(mask: &[u32], rows: usize) -> bool {
    mask_is_full_scalar(mask, rows)
}

fn mask_is_full_scalar(mask: &[u32], rows: usize) -> bool {
    let full_words = rows / 32;
    let tail_bits = rows % 32;
    for word in mask.iter().take(full_words) {
        if *word != 0xffff_ffff {
            return false;
        }
    }
    mask_is_full_tail(mask, full_words, tail_bits)
}

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
pub(crate) fn mask_is_full(mask: &[u32], rows: usize) -> bool {
    use core::arch::wasm32::*;
    let full_words = rows / 32;
    let tail_bits = rows % 32;
    unsafe {
        let mut i = 0usize;
        let mp = mask.as_ptr();
        let all = u32x4_splat(u32::MAX);
        while i + 4 <= full_words {
            let v = v128_load(mp.add(i) as *const v128);
            let neq = v128_xor(v, all);
            if v128_any_true(neq) {
                return false;
            }
            i += 4;
        }
        for j in i..full_words {
            if mask[j] != 0xffff_ffff {
                return false;
            }
        }
    }
    if tail_bits == 0 {
        return mask_is_full_tail(mask, full_words, 0);
    }
    mask_is_full_tail(mask, full_words, tail_bits)
}

pub(crate) fn combine_masks(tokens: &[i32], masks: &[Vec<u32>]) -> Result<Vec<u32>, ()> {
    let mut stack: Vec<Vec<u32>> = Vec::new();
    for token in tokens {
        if *token >= 0 {
            let idx = *token as usize;
            if idx >= masks.len() {
                return Err(());
            }
            stack.push(masks[idx].clone());
            continue;
        }
        match *token {
            crate::constants::COMB_NOT => {
                let a = stack.pop().ok_or(())?;
                stack.push(mask_not(&a));
            }
            crate::constants::COMB_AND => {
                let b = stack.pop().ok_or(())?;
                let a = stack.pop().ok_or(())?;
                stack.push(mask_and(&a, &b));
            }
            crate::constants::COMB_OR => {
                let b = stack.pop().ok_or(())?;
                let a = stack.pop().ok_or(())?;
                stack.push(mask_or(&a, &b));
            }
            _ => return Err(()),
        }
    }
    if stack.len() != 1 {
        return Err(());
    }
    stack.pop().ok_or(())
}

pub(crate) fn iter_mask(mask: &[u32], rows: usize) -> impl Iterator<Item = usize> + '_ {
    mask.iter().enumerate().flat_map(move |(word_idx, word)| {
        let base = word_idx * 32;
        (0..32).filter_map(move |bit| {
            let row = base + bit;
            if row >= rows {
                return None;
            }
            if (word & (1 << bit)) != 0 {
                Some(row)
            } else {
                None
            }
        })
    })
}
