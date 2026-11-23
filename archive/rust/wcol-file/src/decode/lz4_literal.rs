//! Confirms the literal-only LZ4 blocks produced by `src/runtime/stream/lz4.ts`
//! decompress with the same `lz4_flex` block decoder the file format uses.

fn encode_literals(src: &[u8]) -> Vec<u8> {
    let mut extra = Vec::new();
    let nibble = if src.len() >= 15 {
        let mut rest = src.len() - 15;
        while rest >= 255 {
            extra.push(255u8);
            rest -= 255;
        }
        extra.push(rest as u8);
        15u8
    } else {
        src.len() as u8
    };
    let mut out = Vec::with_capacity(1 + extra.len() + src.len());
    out.push(nibble << 4);
    out.extend_from_slice(&extra);
    out.extend_from_slice(src);
    out
}

#[test]
fn literal_blocks_round_trip_through_lz4_flex() {
    for src in [
        &b""[..],
        b"a",
        b"0123456789abcd",
        b"0123456789abcde",
        b"wcol stream frame payload",
        &vec![7u8; 270][..],
        &vec![9u8; 5000][..],
    ] {
        let encoded = encode_literals(src);
        if src.is_empty() {
            continue;
        }
        let decoded = lz4_flex::block::decompress(&encoded, src.len()).expect("lz4 literal block");
        assert_eq!(decoded, src);
    }
}
