//! Versioned immutable sorted spill run (one object per hash-table flush).
//!
//! Records are `(canonical_key, partial_payload)` sorted by key bytes.

pub const RUN_MAGIC: &[u8; 8] = b"WCOLrun1";
pub const RUN_VERSION: u8 = 1;
pub const RUN_HEADER_LEN: usize = 8 + 1 + 1 + 8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpillRunDescriptor {
    pub object_key: String,
    pub run_sequence: u32,
    pub byte_length: u64,
    pub record_count: u64,
    pub format_version: u8,
}

pub fn record_wire_bytes(key: &[u8], payload: &[u8]) -> usize {
    8usize
        .saturating_add(key.len())
        .saturating_add(payload.len())
}

pub fn planned_run_bytes(rows: &[(Vec<u8>, Vec<u8>)]) -> u64 {
    let body = rows
        .iter()
        .map(|(key, payload)| record_wire_bytes(key, payload) as u64)
        .sum::<u64>();
    RUN_HEADER_LEN as u64 + body
}

pub fn encode_header(out: &mut Vec<u8>, record_count: u64) {
    out.extend_from_slice(RUN_MAGIC);
    out.push(RUN_VERSION);
    out.push(0);
    out.extend_from_slice(&record_count.to_le_bytes());
}

pub fn decode_header(bytes: &[u8]) -> Result<(u8, u64), String> {
    if bytes.len() < RUN_HEADER_LEN {
        return Err("sorted run header truncated".into());
    }
    if &bytes[..8] != RUN_MAGIC {
        return Err("sorted run bad magic".into());
    }
    let version = bytes[8];
    if version != RUN_VERSION {
        return Err("unsupported sorted run version".into());
    }
    let record_count = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
    Ok((version, record_count))
}

pub fn encode_record(out: &mut Vec<u8>, key: &[u8], payload: &[u8]) -> Result<(), String> {
    if key.len() > u32::MAX as usize || payload.len() > u32::MAX as usize {
        return Err("sorted run record too large".into());
    }
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

pub fn encode_run(out: &mut Vec<u8>, rows: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    if !rows.windows(2).all(|pair| pair[0].0 <= pair[1].0) {
        return Err("sorted run records must be ordered by key".into());
    }
    encode_header(out, rows.len() as u64);
    for (key, payload) in rows {
        encode_record(out, key, payload)?;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunRecord {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

pub fn decode_run(bytes: &[u8]) -> Result<Vec<RunRecord>, String> {
    let (_version, count) = decode_header(bytes)?;
    let mut off = RUN_HEADER_LEN;
    let mut rows = Vec::with_capacity(count.min(1024) as usize);
    for _ in 0..count {
        let key_len = read_u32(bytes, &mut off)? as usize;
        let key = read_blob(bytes, &mut off, key_len)?;
        let payload_len = read_u32(bytes, &mut off)? as usize;
        let payload = read_blob(bytes, &mut off, payload_len)?;
        rows.push(RunRecord { key, payload });
    }
    if off != bytes.len() {
        return Err("trailing bytes in sorted run".into());
    }
    if !rows.windows(2).all(|w| w[0].key <= w[1].key) {
        return Err("sorted run keys out of order".into());
    }
    Ok(rows)
}

fn read_u32(bytes: &[u8], off: &mut usize) -> Result<u32, String> {
    let end = off.saturating_add(4);
    let chunk = bytes.get(*off..end).ok_or("truncated u32")?;
    *off = end;
    Ok(u32::from_le_bytes(chunk.try_into().unwrap()))
}

fn read_blob(bytes: &[u8], off: &mut usize, len: usize) -> Result<Vec<u8>, String> {
    let end = off.saturating_add(len);
    let chunk = bytes.get(*off..end).ok_or("truncated blob")?;
    *off = end;
    Ok(chunk.to_vec())
}

/// Incremental sorted-run parser for bounded R2 read chunks.
///
/// Records are decoded one at a time from the buffered chunk. A merge then holds each run's
/// raw bytes once instead of the chunk plus a decoded copy of every record in it.
#[derive(Clone, Debug, Default)]
pub struct RunStreamDecoder {
    header_done: bool,
    records_remaining: u64,
    record_buf: Vec<u8>,
    /// Bytes of `record_buf` already decoded.
    at: usize,
}

impl RunStreamDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retained_bytes(&self) -> usize {
        self.record_buf.capacity()
    }

    /// Append the next chunk after the undecoded tail (at most a partial record), reusing the
    /// buffer: one allocation per run for the whole merge instead of one per R2 read.
    pub fn feed(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        let tail = self.record_buf.len() - self.at;
        self.record_buf.copy_within(self.at.., 0);
        self.record_buf.truncate(tail);
        self.at = 0;
        let needed = tail + chunk.len();
        if needed > self.record_buf.capacity() {
            // A sixteenth of slack keeps a later, slightly longer tail from reallocating.
            self.record_buf.reserve_exact(needed + needed / 16 - tail);
        }
        self.record_buf.extend_from_slice(chunk);
    }

    /// The next whole record, or `None` until more bytes are fed or once the run ends.
    pub fn next_record(&mut self) -> Result<Option<RunRecord>, String> {
        if !self.header_done {
            let rest = &self.record_buf[self.at..];
            if rest.len() < RUN_HEADER_LEN {
                return Ok(None);
            }
            let (_version, count) = decode_header(rest)?;
            self.records_remaining = count;
            self.header_done = true;
            self.at += RUN_HEADER_LEN;
        }
        if self.records_remaining == 0 {
            return Ok(None);
        }
        let Some(record) = try_decode_one_record(&self.record_buf[self.at..])? else {
            return Ok(None);
        };
        self.at += 8 + record.key.len() + record.payload.len();
        self.records_remaining -= 1;
        Ok(Some(record))
    }

    pub fn finish(&self) -> Result<(), String> {
        if !self.header_done {
            return Err("sorted run stream ended before header".into());
        }
        if self.records_remaining > 0 || self.at != self.record_buf.len() {
            return Err("sorted run stream truncated".into());
        }
        Ok(())
    }
}

fn try_decode_one_record(buf: &[u8]) -> Result<Option<RunRecord>, String> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let key_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if buf.len() < 4 + key_len + 4 {
        return Ok(None);
    }
    let payload_len =
        u32::from_le_bytes(buf[4 + key_len..8 + key_len].try_into().unwrap()) as usize;
    let total = 4 + key_len + 4 + payload_len;
    if buf.len() < total {
        return Ok(None);
    }
    Ok(Some(RunRecord {
        key: buf[4..4 + key_len].to_vec(),
        payload: buf[8 + key_len..total].to_vec(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_sorted_run() {
        let rows = vec![
            (b"a".to_vec(), b"p1".to_vec()),
            (b"m".to_vec(), b"p2".to_vec()),
            (b"z".to_vec(), b"p3".to_vec()),
        ];
        let mut blob = Vec::new();
        encode_run(&mut blob, &rows).unwrap();
        assert_eq!(planned_run_bytes(&rows) as usize, blob.len());
        let decoded = decode_run(&blob).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].key, b"a");
        assert_eq!(decoded[2].payload, b"p3");
    }

    #[test]
    fn rejects_unsorted_keys() {
        let rows = vec![(b"z".to_vec(), b"".to_vec()), (b"a".to_vec(), b"".to_vec())];
        let mut blob = Vec::new();
        assert!(encode_run(&mut blob, &rows).is_err());
    }

    #[test]
    fn stream_decoder_across_chunk_boundaries() {
        let rows = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"22".to_vec()),
        ];
        let mut blob = Vec::new();
        encode_run(&mut blob, &rows).unwrap();
        let mut decoder = RunStreamDecoder::new();
        let mut got = Vec::new();
        for chunk in blob.chunks(7) {
            decoder.feed(chunk);
            while let Some(record) = decoder.next_record().unwrap() {
                got.push(record);
            }
        }
        decoder.finish().unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].key, b"a");
        assert_eq!(got[1].payload, b"22");
    }

    #[test]
    fn stream_decoder_matches_whole_run_for_any_chunking() {
        let rows: Vec<_> = (0..500u32)
            .map(|i| {
                (
                    i.to_be_bytes().to_vec(),
                    vec![(i % 251) as u8; (i % 37) as usize],
                )
            })
            .collect();
        let mut blob = Vec::new();
        encode_run(&mut blob, &rows).unwrap();
        for size in [1usize, 3, 64, 1000, blob.len()] {
            let mut decoder = RunStreamDecoder::new();
            let mut got = Vec::new();
            for chunk in blob.chunks(size) {
                decoder.feed(chunk);
                while let Some(record) = decoder.next_record().unwrap() {
                    got.push(record);
                }
            }
            decoder.finish().unwrap();
            let pairs: Vec<_> = got.into_iter().map(|r| (r.key, r.payload)).collect();
            assert_eq!(pairs, rows, "chunk size {size}");
        }
    }
}
