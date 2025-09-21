use super::config::CacheCounterMode;
use super::error::{NativeError, NativeResult};

#[derive(Clone, Copy, Debug, Default)]
pub struct CachePerfSnapshot {
    pub cycles: u64,
    pub instructions: u64,
    pub cache_refs: u64,
    pub cache_misses: u64,
    pub llc_refs: u64,
    pub llc_misses: u64,
    pub l1d_misses: u64,
    pub l2_misses: u64,
}

#[derive(Debug)]
pub struct CachePerfCollector {
    available: bool,
}

impl CachePerfCollector {
    pub fn new(mode: CacheCounterMode) -> NativeResult<Self> {
        if mode == CacheCounterMode::Strict {
            return Err(NativeError::Invalid(
                "cache counters strict mode is unavailable in this build",
            ));
        }
        Ok(Self { available: false })
    }

    pub fn available(&self) -> bool {
        self.available
    }

    pub fn snapshot(&self) -> CachePerfSnapshot {
        CachePerfSnapshot::default()
    }
}
