//! Codec budget for one scan. Owned pages decompress into their own buffer.

use crate::error::{WcolError, WcolResult};

use super::decode::ScanLimits;

pub struct PageScratch {
    /// Largest uncompressed page `begin_page_decode` accepts.
    pub codec_cap: usize,
}

const MAX_CODEC_SCRATCH: usize = 32 * 1024 * 1024;

impl PageScratch {
    /// Placeholder when decode uses a shared group [`PageScratch`] via [`NumericScan::poll_with_scratch`].
    pub fn minimal() -> Self {
        Self { codec_cap: 0 }
    }

    pub fn for_limits(limits: &ScanLimits) -> Self {
        let codec_total = limits
            .budget
            .codec_scratch_bytes
            .clamp(64 * 1024, MAX_CODEC_SCRATCH);
        // Keep the same cap the streaming Snappy scratch used, so a page that used to
        // exceed that budget still fails here.
        let ring = 64 * 1024;
        let snappy_out_cap = codec_total
            .saturating_sub(ring * 2)
            .clamp(256 * 1024, MAX_CODEC_SCRATCH);
        Self {
            codec_cap: snappy_out_cap,
        }
    }

    /// Reject a Snappy page whose uncompressed size exceeds the codec budget.
    pub fn begin_page_decode(&self, codec: i32, uncompressed_size: usize) -> WcolResult<()> {
        if codec == 1 && uncompressed_size > self.codec_cap {
            return Err(WcolError::from("snappy scratch exceeds codec budget"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::parquet::decode::ScanLimits;

    #[test]
    fn snappy_scratch_cannot_grow_past_allocation() {
        let limits = ScanLimits::for_execution_group(1024);
        let scratch = PageScratch::for_limits(&limits);
        let cap = scratch.codec_cap;
        assert!(scratch.begin_page_decode(1, cap + 1).is_err());
        assert!(scratch.begin_page_decode(1, cap).is_ok());
        assert!(scratch.begin_page_decode(0, cap + 1).is_ok());
    }
}
