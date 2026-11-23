//! Incremental WCS1 frame parse + encode helpers for query stage operators.
use crate::error::{WcolError, WcolResult};

use crate::stream::core::{Progress, WCS_HEADER_BYTES, WCS_MAGIC, WCS_MAX_PAYLOAD};

pub struct CompleteFrame {
    pub frame_type: u8,
    pub payload: Vec<u8>,
}

pub struct WcsIngress {
    phase: Phase,
    header: [u8; WCS_HEADER_BYTES],
    header_have: usize,
    payload_left: usize,
    frame_type: u8,
    payload: Vec<u8>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Header,
    Payload,
    Between,
}

impl Default for WcsIngress {
    fn default() -> Self {
        Self::new()
    }
}

impl WcsIngress {
    pub fn new() -> Self {
        Self {
            phase: Phase::Header,
            header: [0; WCS_HEADER_BYTES],
            header_have: 0,
            payload_left: 0,
            frame_type: 0,
            payload: Vec::new(),
        }
    }

    pub fn feed(&mut self, input: &[u8]) -> WcolResult<(usize, Vec<CompleteFrame>)> {
        self.feed_limited(input, usize::MAX, usize::MAX)
    }

    /// Decode until `frame_limit` frames are complete or the next frame cannot start.
    /// A new frame starts only when `budget_left` can hold [`WCS_MAX_PAYLOAD`].
    /// An in-progress frame is always finished. Bytes after the stop point are not consumed.
    pub fn feed_limited(
        &mut self,
        input: &[u8],
        frame_limit: usize,
        budget_left: usize,
    ) -> WcolResult<(usize, Vec<CompleteFrame>)> {
        let mut consumed = 0usize;
        let mut out = Vec::new();
        while consumed < input.len() {
            match self.phase {
                Phase::Header => {
                    if self.header_have == 0
                        && (out.len() >= frame_limit || budget_left < WCS_MAX_PAYLOAD)
                    {
                        break;
                    }
                    let take = (WCS_HEADER_BYTES - self.header_have).min(input.len() - consumed);
                    if take == 0 {
                        break;
                    }
                    self.header[self.header_have..self.header_have + take]
                        .copy_from_slice(&input[consumed..consumed + take]);
                    self.header_have += take;
                    consumed += take;
                    if self.header_have == WCS_HEADER_BYTES {
                        self.validate_header()?;
                        self.phase = Phase::Payload;
                        if self.payload_left == 0 {
                            out.push(CompleteFrame {
                                frame_type: self.frame_type,
                                payload: Vec::new(),
                            });
                            self.phase = Phase::Between;
                        }
                    }
                }
                Phase::Payload => {
                    let take = self.payload_left.min(input.len() - consumed);
                    if take == 0 {
                        break;
                    }
                    self.payload
                        .extend_from_slice(&input[consumed..consumed + take]);
                    self.payload_left -= take;
                    consumed += take;
                    if self.payload_left == 0 {
                        out.push(CompleteFrame {
                            frame_type: self.frame_type,
                            payload: std::mem::take(&mut self.payload),
                        });
                        self.phase = Phase::Between;
                    }
                }
                Phase::Between => {
                    if out.len() >= frame_limit || budget_left < WCS_MAX_PAYLOAD {
                        break;
                    }
                    self.header_have = 0;
                    self.phase = Phase::Header;
                }
            }
        }
        Ok((consumed, out))
    }

    /// Bytes retained in the decoder. Unarrived payload is not included.
    pub fn buffered_bytes(&self) -> usize {
        match self.phase {
            Phase::Between => 0,
            Phase::Header => self.header_have,
            Phase::Payload => self.payload.len(),
        }
    }

    /// True when a header or payload is only partly buffered.
    pub fn incomplete(&self) -> bool {
        match self.phase {
            Phase::Payload => true,
            Phase::Header => self.header_have > 0,
            Phase::Between => false,
        }
    }

    fn validate_header(&mut self) -> WcolResult<()> {
        if u32::from_le_bytes(self.header[..4].try_into().unwrap()) != WCS_MAGIC {
            return Err(WcolError::from("invalid WCS1 magic"));
        }
        if self.header[4] != 1 {
            return Err(WcolError::from("unsupported WCS1 version"));
        }
        let payload = u32::from_le_bytes(self.header[8..12].try_into().unwrap()) as usize;
        if payload > WCS_MAX_PAYLOAD {
            return Err(WcolError::from("WCS frame exceeds maximum size"));
        }
        self.frame_type = self.header[5];
        self.payload_left = payload;
        self.payload.clear();
        Ok(())
    }
}

pub fn progress_after_io(consumed: usize, produced: usize, finished: bool) -> Progress {
    Progress {
        consumed,
        produced,
        finished,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::core::{WCS_HEADER_BYTES, WCS_MAGIC, WCS_MAX_PAYLOAD};
    use crate::stream::wcs::{encode_frame, FRAME_DATA_BATCH, FRAME_END};

    #[test]
    fn incomplete_is_false_on_a_complete_frame_and_true_mid_header() {
        let mut ingress = WcsIngress::new();
        assert!(!ingress.incomplete());
        let mut frame = vec![0u8; 64];
        let n = encode_frame(FRAME_END, 0, &[], &mut frame).unwrap();
        ingress.feed(&frame[..n]).unwrap();
        assert!(!ingress.incomplete());
        let mut mid = WcsIngress::new();
        mid.feed(&frame[..4]).unwrap();
        assert!(mid.incomplete());
    }

    #[test]
    fn partial_payload_counts_retained_bytes_not_the_unarrived_tail() {
        let payload = vec![7u8; 100];
        let mut frame = vec![0u8; WCS_HEADER_BYTES + payload.len()];
        let n = encode_frame(FRAME_DATA_BATCH, 1, &payload, &mut frame).unwrap();
        assert_eq!(n, frame.len());
        let mut ingress = WcsIngress::new();
        let chunk = 64;
        let (consumed, frames) = ingress.feed(&frame[..chunk]).unwrap();
        assert_eq!(consumed, chunk);
        assert!(frames.is_empty());
        assert!(ingress.incomplete());
        assert!(
            ingress.buffered_bytes() <= chunk,
            "retained {}, chunk {chunk}",
            ingress.buffered_bytes()
        );
        assert!(
            ingress.payload.capacity() < 1024,
            "decoder reserved {} bytes",
            ingress.payload.capacity()
        );
    }

    #[test]
    fn header_over_the_maximum_payload_is_rejected() {
        let mut header = [0u8; WCS_HEADER_BYTES];
        header[..4].copy_from_slice(&WCS_MAGIC.to_le_bytes());
        header[4] = 1;
        header[5] = FRAME_DATA_BATCH;
        header[8..12].copy_from_slice(&((WCS_MAX_PAYLOAD as u32) + 1).to_le_bytes());
        match WcsIngress::new().feed(&header) {
            Err(err) => assert!(err.as_str().contains("maximum size"), "{}", err.as_str()),
            Ok(_) => panic!("oversized frame was accepted"),
        }
    }

    #[test]
    fn feed_limited_stops_after_one_frame_and_leaves_the_suffix() {
        let mut first = vec![0u8; 64];
        let n1 = encode_frame(FRAME_DATA_BATCH, 0, b"one", &mut first).unwrap();
        let mut second = vec![0u8; 64];
        let n2 = encode_frame(FRAME_END, 1, &[], &mut second).unwrap();
        let mut chunk = first[..n1].to_vec();
        chunk.extend_from_slice(&second[..n2]);
        let mut ingress = WcsIngress::new();
        let (consumed, frames) = ingress.feed_limited(&chunk, 1, usize::MAX).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload, b"one");
        assert_eq!(consumed, n1);
        assert_eq!(&chunk[consumed..], &second[..n2]);
    }
}
