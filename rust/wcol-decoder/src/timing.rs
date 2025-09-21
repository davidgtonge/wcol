//! Plan/decode timing — enabled with the `timing` Cargo feature.

use crate::types::Filter;

#[cfg(all(feature = "timing", target_arch = "wasm32"))]
#[link(wasm_import_module = "env")]
extern "C" {
    fn wcol_now_ms() -> f64;
}

#[inline]
pub fn now_ms() -> f64 {
    #[cfg(all(feature = "timing", target_arch = "wasm32"))]
    unsafe {
        return wcol_now_ms();
    }
    #[cfg(all(feature = "timing", not(target_arch = "wasm32")))]
    {
        use std::time::Instant;
        static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        let start = START.get_or_init(Instant::now);
        return start.elapsed().as_secs_f64() * 1000.0;
    }
    #[cfg(not(feature = "timing"))]
    {
        0.0
    }
}

#[derive(Clone, Copy)]
pub struct Tic(f64);

impl Tic {
    #[inline]
    pub fn start() -> Self {
        #[cfg(feature = "timing")]
        {
            Self(now_ms())
        }
        #[cfg(not(feature = "timing"))]
        {
            Self(0.0)
        }
    }

    #[inline]
    pub fn elapsed(self) -> f64 {
        #[cfg(feature = "timing")]
        {
            now_ms() - self.0
        }
        #[cfg(not(feature = "timing"))]
        {
            0.0
        }
    }
}

#[inline]
pub fn record_elapsed<F>(timing: Option<&mut crate::types::PlanTiming>, f: F, tic: Tic)
where
    F: FnOnce(&mut crate::types::PlanTiming, f64),
{
    #[cfg(feature = "timing")]
    if let Some(timing) = timing {
        f(timing, tic.elapsed());
    }
    #[cfg(not(feature = "timing"))]
    let _ = (timing, f, tic);
}

macro_rules! plan_timing_add {
    ($method:ident, $field:ident) => {
        pub fn $method(&mut self, ms: f64) {
            #[cfg(feature = "timing")]
            {
                self.$field += ms;
            }
            #[cfg(not(feature = "timing"))]
            {
                let _ = ms;
            }
        }
    };
}

impl crate::types::PlanTiming {
    plan_timing_add!(add_ms_decode, ms_decode);
    plan_timing_add!(add_ms_filters, ms_filters);
    plan_timing_add!(add_ms_filters_decode, ms_filters_decode);
    plan_timing_add!(add_ms_filters_build, ms_filters_build);
    plan_timing_add!(add_ms_filters_nulls, ms_filters_nulls);
    plan_timing_add!(add_ms_filters_combine, ms_filters_combine);
    plan_timing_add!(add_ms_aggs, ms_aggs);
    plan_timing_add!(add_ms_group, ms_group);
    plan_timing_add!(add_ms_rows, ms_rows);
    plan_timing_add!(add_ms_str_perm, ms_str_perm);
    plan_timing_add!(add_ms_str_token, ms_str_token);
    plan_timing_add!(add_ms_str_reconstruct, ms_str_reconstruct);
    plan_timing_add!(add_ms_str_dict, ms_str_dict);

    pub fn inc_chunks(&mut self) {
        #[cfg(feature = "timing")]
        {
            self.chunks = self.chunks.saturating_add(1);
        }
    }

    pub fn write_copy_buffer(
        &self,
        out: &mut [u8],
        write_u32: fn(&mut [u8], usize, u32),
        write_f64: fn(&mut [u8], usize, f64),
    ) {
        #[cfg(feature = "timing")]
        {
            write_u32(out, 0, self.chunks);
            write_f64(out, 4, self.ms_decode);
            write_f64(out, 12, self.ms_filters);
            write_f64(out, 20, self.ms_filters_decode);
            write_f64(out, 28, self.ms_filters_build);
            write_f64(out, 36, self.ms_filters_nulls);
            write_f64(out, 44, self.ms_filters_combine);
            write_f64(out, 52, self.ms_aggs);
            write_f64(out, 60, self.ms_group);
            write_f64(out, 68, self.ms_rows);
            write_f64(out, 76, self.ms_str_perm);
            write_f64(out, 84, self.ms_str_token);
            write_f64(out, 92, self.ms_str_reconstruct);
            write_f64(out, 100, self.ms_str_dict);
        }
        #[cfg(not(feature = "timing"))]
        {
            let _ = (self, write_u32, write_f64);
            out.fill(0);
        }
    }
}

impl crate::types::FilterTiming {
    pub fn sync_filters(&mut self, filters: &[Filter]) {
        #[cfg(feature = "timing")]
        {
            if self.cols.len() != filters.len() {
                self.cols.clear();
                self.ops.clear();
                self.ms_decode.clear();
                self.ms_build.clear();
                self.ms_nulls.clear();
                self.like_blocks_total.clear();
                self.like_blocks_skipped.clear();
                self.like_blocks_passed.clear();
                self.like_blocks_matched.clear();
                self.like_rows_verified.clear();
                self.like_ms_mask.clear();
                self.like_ms_verify.clear();
                self.like_ms_other.clear();
                for f in filters {
                    self.cols.push(f.col_id);
                    self.ops.push(f.op);
                    self.ms_decode.push(0.0);
                    self.ms_build.push(0.0);
                    self.ms_nulls.push(0.0);
                    self.like_blocks_total.push(0);
                    self.like_blocks_skipped.push(0);
                    self.like_blocks_passed.push(0);
                    self.like_blocks_matched.push(0);
                    self.like_rows_verified.push(0);
                    self.like_ms_mask.push(0.0);
                    self.like_ms_verify.push(0.0);
                    self.like_ms_other.push(0.0);
                }
            }
        }
        #[cfg(not(feature = "timing"))]
        let _ = (self, filters);
    }

    pub fn filter_count(&self) -> usize {
        #[cfg(feature = "timing")]
        {
            self.cols.len()
        }
        #[cfg(not(feature = "timing"))]
        {
            0
        }
    }

    pub fn add_ms_decode(&mut self, idx: usize, ms: f64) {
        #[cfg(feature = "timing")]
        if idx < self.ms_decode.len() {
            self.ms_decode[idx] += ms;
        }
        #[cfg(not(feature = "timing"))]
        let _ = (self, idx, ms);
    }

    pub fn add_ms_build(&mut self, idx: usize, ms: f64) {
        #[cfg(feature = "timing")]
        if idx < self.ms_build.len() {
            self.ms_build[idx] += ms;
        }
        #[cfg(not(feature = "timing"))]
        let _ = (self, idx, ms);
    }

    pub fn add_ms_nulls(&mut self, idx: usize, ms: f64) {
        #[cfg(feature = "timing")]
        if idx < self.ms_nulls.len() {
            self.ms_nulls[idx] += ms;
        }
        #[cfg(not(feature = "timing"))]
        let _ = (self, idx, ms);
    }

    pub fn merge_like_stats(&mut self, idx: usize, stats: &crate::types::LikeMaskStats) {
        #[cfg(feature = "timing")]
        if idx < self.like_blocks_total.len() {
            self.like_blocks_total[idx] += stats.blocks_total;
            self.like_blocks_skipped[idx] += stats.blocks_skipped;
            self.like_blocks_passed[idx] += stats.blocks_passed;
            self.like_blocks_matched[idx] += stats.blocks_matched;
            self.like_rows_verified[idx] += stats.rows_verified;
            self.like_ms_mask[idx] += stats.ms_mask;
            self.like_ms_verify[idx] += stats.ms_verify;
            self.like_ms_other[idx] += stats.ms_other;
        }
        #[cfg(not(feature = "timing"))]
        let _ = (self, idx, stats);
    }

    pub fn reset_counters(&mut self) {
        #[cfg(feature = "timing")]
        {
            for v in self.ms_decode.iter_mut() {
                *v = 0.0;
            }
            for v in self.ms_build.iter_mut() {
                *v = 0.0;
            }
            for v in self.ms_nulls.iter_mut() {
                *v = 0.0;
            }
        }
    }

    pub fn shrink_buffers(&mut self) {
        #[cfg(feature = "timing")]
        {
            self.cols.shrink_to_fit();
            self.ops.shrink_to_fit();
            self.ms_decode.shrink_to_fit();
            self.ms_build.shrink_to_fit();
            self.ms_nulls.shrink_to_fit();
            self.like_blocks_total.shrink_to_fit();
            self.like_blocks_skipped.shrink_to_fit();
            self.like_blocks_passed.shrink_to_fit();
            self.like_blocks_matched.shrink_to_fit();
            self.like_rows_verified.shrink_to_fit();
            self.like_ms_mask.shrink_to_fit();
            self.like_ms_verify.shrink_to_fit();
            self.like_ms_other.shrink_to_fit();
        }
    }

    pub fn write_copy_buffer(
        &self,
        out: &mut [u8],
        write_u32: fn(&mut [u8], usize, u32),
        write_f64: fn(&mut [u8], usize, f64),
    ) -> usize {
        #[cfg(feature = "timing")]
        {
            const REC_SIZE: usize = 4 + 1 + 3 + 8 * 3 + 4 * 5 + 8 * 3;
            let count = self.cols.len();
            let mut offset = 0;
            for i in 0..count {
                write_u32(out, offset, self.cols[i]);
                offset += 4;
                out[offset] = self.ops[i];
                offset += 1;
                out[offset..offset + 3].fill(0);
                offset += 3;
                write_f64(out, offset, self.ms_decode[i]);
                offset += 8;
                write_f64(out, offset, self.ms_build[i]);
                offset += 8;
                write_f64(out, offset, self.ms_nulls[i]);
                offset += 8;
                write_u32(out, offset, self.like_blocks_total[i]);
                offset += 4;
                write_u32(out, offset, self.like_blocks_skipped[i]);
                offset += 4;
                write_u32(out, offset, self.like_blocks_passed[i]);
                offset += 4;
                write_u32(out, offset, self.like_blocks_matched[i]);
                offset += 4;
                write_u32(out, offset, self.like_rows_verified[i]);
                offset += 4;
                write_f64(out, offset, self.like_ms_mask[i]);
                offset += 8;
                write_f64(out, offset, self.like_ms_verify[i]);
                offset += 8;
                write_f64(out, offset, self.like_ms_other[i]);
                offset += 8;
            }
            count * REC_SIZE
        }
        #[cfg(not(feature = "timing"))]
        {
            let _ = (self, out, write_u32, write_f64);
            0
        }
    }
}

impl crate::types::LikeMaskStats {
    pub fn add_ms_verify(&mut self, ms: f64) {
        #[cfg(feature = "timing")]
        {
            self.ms_verify += ms;
        }
        #[cfg(not(feature = "timing"))]
        let _ = ms;
    }

    pub fn set_verify_rows(&mut self, rows: u32) {
        #[cfg(feature = "timing")]
        {
            self.rows_verified = rows;
        }
        #[cfg(not(feature = "timing"))]
        let _ = rows;
    }

    pub fn set_blocks_matched(&mut self, matched: u32) {
        #[cfg(feature = "timing")]
        {
            self.blocks_matched = if matched > 0 { 1 } else { 0 };
        }
        #[cfg(not(feature = "timing"))]
        let _ = matched;
    }

    pub fn add_ms_other(&mut self, ms: f64) {
        #[cfg(feature = "timing")]
        if ms >= 0.0 {
            self.ms_other += ms;
        }
        #[cfg(not(feature = "timing"))]
        let _ = ms;
    }

    #[cfg(feature = "timing")]
    pub fn ms_mask(&self) -> f64 {
        self.ms_mask
    }

    #[cfg(feature = "timing")]
    pub fn ms_verify(&self) -> f64 {
        self.ms_verify
    }
}
