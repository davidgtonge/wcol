//! Stream-layer helpers for dictionary-encoded column chunks.

/// True if the column chunk contains a Parquet dictionary page (type 2).
pub fn chunk_has_dictionary_page(bytes: &[u8]) -> bool {
    use compact_parquet_thrift::parquet::page_header;
    let mut at = 0usize;
    while at < bytes.len() {
        let tail = &bytes[at..];
        let (header, header_len) = match page_header(tail) {
            Ok(v) => v,
            Err(_) => break,
        };
        if header.page_type == Some(2) {
            return true;
        }
        let compressed = header
            .compressed_size
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0);
        let body_end = at.saturating_add(header_len).saturating_add(compressed);
        if body_end <= at {
            break;
        }
        at = body_end;
    }
    false
}
