//! WCS1 partial emission for typed ingest+query operators.
use crate::error::WcolResult;

use crate::stream::core::WCS_HEADER_BYTES;
use crate::stream::wcs;

pub(crate) struct EmitPartial {
    out_sequence: u32,
    pub(crate) partial_emitted: bool,
    end_emitted: bool,
    pending_frame: Option<Vec<u8>>,
    pending_is_end: bool,
    pending_is_last: bool,
}

impl EmitPartial {
    pub(crate) fn new() -> Self {
        Self {
            out_sequence: 0,
            partial_emitted: false,
            end_emitted: false,
            pending_frame: None,
            pending_is_end: false,
            pending_is_last: false,
        }
    }

    pub(crate) fn wire_complete(&self) -> bool {
        self.end_emitted && self.pending_frame.is_none()
    }

    fn queue_end_frame(&mut self) -> WcolResult<()> {
        if self.end_emitted || self.pending_frame.is_some() {
            return Ok(());
        }
        if !self.partial_emitted {
            return Ok(());
        }
        let mut scratch = vec![0u8; WCS_HEADER_BYTES];
        let n = wcs::encode_frame(wcs::FRAME_END, self.out_sequence, &[], &mut scratch)
            .ok_or("END frame too large for scratch")?;
        self.out_sequence = self.out_sequence.wrapping_add(1);
        self.pending_frame = Some(scratch[..n].to_vec());
        self.pending_is_end = true;
        self.pending_is_last = true;
        Ok(())
    }

    fn note_pending_fully_flushed(&mut self) -> WcolResult<()> {
        if self.pending_is_end {
            self.end_emitted = true;
            self.pending_is_end = false;
            return Ok(());
        }
        self.partial_emitted = true;
        if self.pending_is_last {
            self.queue_end_frame()?;
        }
        Ok(())
    }

    pub(crate) fn flush(&mut self, output: &mut [u8]) -> WcolResult<usize> {
        let Some(frame) = self.pending_frame.as_ref() else {
            return Ok(0);
        };
        if output.is_empty() {
            return Ok(0);
        }
        let n = frame.len().min(output.len());
        output[..n].copy_from_slice(&frame[..n]);
        if n == frame.len() {
            self.pending_frame = None;
            self.note_pending_fully_flushed()?;
        } else {
            self.pending_frame = Some(frame[n..].to_vec());
        }
        Ok(n)
    }

    /// One partial then END (hash reducers without chunked spill).
    pub(crate) fn emit(&mut self, partial: &[u8], output: &mut [u8]) -> WcolResult<usize> {
        self.emit_frame(partial, output, true)
    }

    /// Another partial; END is queued later via [`Self::finish`].
    pub(crate) fn emit_more(&mut self, partial: &[u8], output: &mut [u8]) -> WcolResult<usize> {
        self.emit_frame(partial, output, false)
    }

    /// Queue FRAME_END after the last spill chunk (no more partials).
    pub(crate) fn finish(&mut self) -> WcolResult<()> {
        if self.end_emitted || self.pending_frame.is_some() {
            return Ok(());
        }
        self.partial_emitted = true;
        self.queue_end_frame()
    }

    fn emit_frame(&mut self, partial: &[u8], output: &mut [u8], last: bool) -> WcolResult<usize> {
        if self.wire_complete() {
            return Ok(0);
        }
        if self.pending_frame.is_some() {
            return self.flush(output);
        }
        if partial.len() > wcs::MAX_WCS_PAYLOAD {
            return Err(crate::error::WcolError::from(
                "WCS frame exceeds maximum size",
            ));
        }
        let mut scratch = vec![0u8; WCS_HEADER_BYTES + partial.len()];
        let n = wcs::encode_frame(4, self.out_sequence, partial, &mut scratch)
            .ok_or("partial frame too large for scratch")?;
        self.out_sequence = self.out_sequence.wrapping_add(1);
        if n > output.len() {
            self.pending_frame = Some(scratch[..n].to_vec());
            self.pending_is_end = false;
            self.pending_is_last = last;
            return Ok(0);
        }
        output[..n].copy_from_slice(&scratch[..n]);
        self.partial_emitted = true;
        if !last {
            return Ok(n);
        }
        self.queue_end_frame()?;
        if let Some(end_frame) = self.pending_frame.as_ref() {
            let end_len = end_frame.len();
            if n + end_len <= output.len() {
                output[n..n + end_len].copy_from_slice(end_frame);
                self.pending_frame = None;
                self.pending_is_end = false;
                self.end_emitted = true;
                return Ok(n + end_len);
            }
        }
        Ok(n)
    }
}

#[cfg(test)]
mod emit_partial_tests {
    use super::EmitPartial;
    #[test]
    fn flush_after_oversized_buffer_marks_partial_emitted() {
        let partial = b"PWCL\x01\x00\x00\x00\x00";
        let mut emit = EmitPartial::new();
        let mut tiny = [0u8; 8];
        assert_eq!(emit.emit(partial, &mut tiny).unwrap(), 0);
        assert!(emit.pending_frame.is_some());
        assert!(!emit.partial_emitted);

        let mut big = vec![0u8; 256];
        while !emit.wire_complete() {
            let n = emit.flush(&mut big).unwrap();
            assert!(n > 0);
        }
        assert!(emit.end_emitted);

        assert_eq!(emit.emit(partial, &mut big).unwrap(), 0);
    }

    #[test]
    fn emit_fits_single_buffer_without_pending() {
        let partial = b"x";
        let mut emit = EmitPartial::new();
        let mut out = vec![0u8; 128];
        let n = emit.emit(partial, &mut out).unwrap();
        assert!(n > 0);
        assert!(emit.wire_complete());
        assert!(emit.end_emitted);
        assert!(emit.pending_frame.is_none());
        let header = &out[..crate::stream::core::WCS_HEADER_BYTES];
        assert_eq!(
            u32::from_le_bytes(header[..4].try_into().unwrap()),
            crate::stream::core::WCS_MAGIC
        );
        let payload_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        assert_eq!(
            &out[crate::stream::core::WCS_HEADER_BYTES
                ..crate::stream::core::WCS_HEADER_BYTES + partial.len()],
            partial
        );
        assert_eq!(payload_len, partial.len());
    }

    #[test]
    fn end_frame_follows_partial_on_wire() {
        use crate::stream::wcs::FRAME_END;
        let partial = b"abc";
        let mut emit = EmitPartial::new();
        let mut out = vec![0u8; 512];
        let n = emit.emit(partial, &mut out).unwrap();
        assert!(n > 0);
        assert!(emit.wire_complete());
        assert_eq!(out[5], crate::stream::wcs::FRAME_PARTIAL);
        let end_header = &out[n - (crate::stream::core::WCS_HEADER_BYTES)..n];
        assert_eq!(end_header[5], FRAME_END);
    }

    #[test]
    fn emit_more_keeps_stream_open_until_finish() {
        use crate::stream::wcs::{FRAME_END, FRAME_PARTIAL};
        let mut emit = EmitPartial::new();
        let mut out = vec![0u8; 512];
        let n1 = emit.emit_more(b"one", &mut out).unwrap();
        assert!(n1 > 0);
        assert!(!emit.wire_complete());
        assert_eq!(out[5], FRAME_PARTIAL);
        let n2 = emit.emit_more(b"two", &mut out).unwrap();
        assert!(n2 > 0);
        assert!(!emit.wire_complete());
        emit.finish().unwrap();
        let n3 = emit.flush(&mut out).unwrap();
        assert!(n3 > 0);
        assert!(emit.wire_complete());
        assert_eq!(out[5], FRAME_END);
    }
}
