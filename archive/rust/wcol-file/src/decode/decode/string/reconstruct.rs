
pub(super) fn reconstruct_unique_values<T, F>(
    lcps: &[usize],
    suffix_lens: &[usize],
    suffix_blob: &[u8],
    value_count: usize,
    mut build: F,
) -> Result<Vec<T>, i32>
where
    F: FnMut(&[u8]) -> Result<T, i32>,
{
    let mut blob_cursor = 0usize;
    let mut prev: Vec<u8> = Vec::new();
    let mut out = Vec::with_capacity(value_count);
    for idx in 0..value_count {
        let lcp = lcps[idx];
        if lcp > prev.len() {
            return Err(-118);
        }
        let suffix_len = suffix_lens[idx];
        if blob_cursor + suffix_len > suffix_blob.len() {
            return Err(-119);
        }
        let suffix = &suffix_blob[blob_cursor..blob_cursor + suffix_len];
        blob_cursor += suffix_len;
        prev.truncate(lcp);
        prev.extend_from_slice(suffix);
        out.push(build(prev.as_slice())?);
    }
    if blob_cursor != suffix_blob.len() {
        return Err(-121);
    }
    Ok(out)
}
