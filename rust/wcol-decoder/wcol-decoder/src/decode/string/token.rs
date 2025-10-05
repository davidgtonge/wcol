
use crate::parse::read_u16;

pub(super) fn parse_token_dict(bytes: &[u8]) -> Result<Vec<Vec<u8>>, i32> {
    let offset_bytes = 2 * (128 + 1);
    if bytes.len() < offset_bytes {
        return Err(-130);
    }
    let mut offsets: Vec<u16> = Vec::with_capacity(129);
    for i in 0..129 {
        let off = read_u16(bytes, i * 2);
        offsets.push(off);
    }
    let blob = &bytes[offset_bytes..];
    let mut tokens: Vec<Vec<u8>> = Vec::with_capacity(128);
    for idx in 0..128 {
        let start = offsets[idx] as usize;
        let end = offsets[idx + 1] as usize;
        if start > end || end > blob.len() {
            return Err(-131);
        }
        tokens.push(blob[start..end].to_vec());
    }
    Ok(tokens)
}

pub(super) fn decode_full_token_stream(
    data: &[u8],
    tokens: &[Vec<u8>],
    expected_total_len: usize,
) -> Result<Vec<u8>, i32> {
    let mut out: Vec<u8> = Vec::with_capacity(expected_total_len);
    let mut cursor = 0usize;
    while out.len() < expected_total_len {
        if cursor >= data.len() {
            return Err(-140);
        }
        let b = data[cursor];
        cursor += 1;
        if b < 128 {
            let token = tokens.get(b as usize).ok_or(-141)?;
            if out.len() + token.len() > expected_total_len {
                return Err(-142);
            }
            out.extend_from_slice(token);
        } else {
            let run = (b - 128) as usize;
            if cursor + run > data.len() {
                return Err(-143);
            }
            if out.len() + run > expected_total_len {
                return Err(-143);
            }
            out.extend_from_slice(&data[cursor..cursor + run]);
            cursor += run;
        }
    }
    if cursor < data.len() {
        return Err(-122);
    }
    Ok(out)
}
