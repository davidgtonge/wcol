
#[cfg(all(
    target_arch = "wasm32",
    target_feature = "simd128",
    any(feature = "simd_like_raw", feature = "simd_like_dict")
))]
#[allow(unused_imports)]
use core::arch::wasm32::{
    u8x16_bitmask, u8x16_eq, u8x16_splat, v128, v128_load,
};

#[inline]
pub(crate) fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    #[cfg(all(
        target_arch = "wasm32",
        target_feature = "simd128",
        any(feature = "simd_like_raw", feature = "simd_like_dict")
    ))]
    {
        return find_subslice_simd(haystack, needle);
    }
    #[cfg(not(all(
        target_arch = "wasm32",
        target_feature = "simd128",
        any(feature = "simd_like_raw", feature = "simd_like_dict")
    )))]
    {
        if needle.is_empty() {
            return Some(0);
        }
        return haystack.windows(needle.len()).position(|w| w == needle);
    }
}

#[cfg(all(
    target_arch = "wasm32",
    target_feature = "simd128",
    any(feature = "simd_like_raw", feature = "simd_like_dict")
))]
fn find_subslice_simd(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let n = needle.len();
    if n == 0 {
        return Some(0);
    }
    if n == 1 {
        return memchr_simd(haystack, needle[0]);
    }
    if haystack.len() < n {
        return None;
    }
    let first = needle[0];
    let last_start = haystack.len() - n;
    let mut i = 0usize;
    let chunk_end = haystack.len().saturating_sub(16);
    let first_vec = u8x16_splat(first);
    while i <= chunk_end {
        let ptr = unsafe { haystack.as_ptr().add(i) as *const v128 };
        let block = unsafe { v128_load(ptr) };
        let eq = u8x16_eq(block, first_vec);
        let mut mask = u8x16_bitmask(eq) as u32;
        while mask != 0 {
            let bit = mask.trailing_zeros() as usize;
            let pos = i + bit;
            if pos > last_start {
                return None;
            }
            if &haystack[pos..pos + n] == needle {
                return Some(pos);
            }
            mask &= mask - 1;
        }
        i += 16;
    }
    while i <= last_start {
        if haystack[i] == first && &haystack[i..i + n] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(all(
    target_arch = "wasm32",
    target_feature = "simd128",
    any(feature = "simd_like_raw", feature = "simd_like_dict")
))]
fn memchr_simd(haystack: &[u8], needle: u8) -> Option<usize> {
    let mut i = 0usize;
    let chunk_end = haystack.len().saturating_sub(16);
    let needle_vec = u8x16_splat(needle);
    while i <= chunk_end {
        let ptr = unsafe { haystack.as_ptr().add(i) as *const v128 };
        let block = unsafe { v128_load(ptr) };
        let eq = u8x16_eq(block, needle_vec);
        let mask = u8x16_bitmask(eq) as u32;
        if mask != 0 {
            return Some(i + (mask.trailing_zeros() as usize));
        }
        i += 16;
    }
    while i < haystack.len() {
        if haystack[i] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}
