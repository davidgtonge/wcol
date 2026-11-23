use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::error::{NativeError, NativeResult};
use super::helpers::read_exact_at_file;

const DEFAULT_PAGE_CACHE_MIB: usize = 2048;

type CacheKey = (u64, u32);

#[derive(Clone, Copy, Debug, Default)]
pub struct ReadIoStats {
    pub total_requests: u64,
    pub unique_requests: u64,
    pub overlap_unique_requests: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub bytes_requested: u64,
    pub bytes_from_disk: u64,
    pub cache_entries: u64,
    pub cache_used_bytes: u64,
    pub cache_capacity_bytes: u64,
}

#[derive(Default)]
struct CacheInner {
    max_bytes: usize,
    used_bytes: usize,
    seen: HashSet<CacheKey>,
    unique_ranges: Vec<(u64, u64)>,
    map: HashMap<CacheKey, Arc<[u8]>>,
    order: VecDeque<CacheKey>,
}

pub(crate) struct ReadCache {
    inner: Mutex<CacheInner>,
    total_requests: AtomicU64,
    unique_requests: AtomicU64,
    overlap_unique_requests: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    bytes_requested: AtomicU64,
    bytes_from_disk: AtomicU64,
}

impl ReadCache {
    pub(crate) fn from_env() -> Self {
        let mib = std::env::var("WCOL_PAGE_CACHE_MB")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(DEFAULT_PAGE_CACHE_MIB);
        let max_bytes = mib.saturating_mul(1024 * 1024);
        Self {
            inner: Mutex::new(CacheInner {
                max_bytes,
                ..CacheInner::default()
            }),
            total_requests: AtomicU64::new(0),
            unique_requests: AtomicU64::new(0),
            overlap_unique_requests: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            bytes_requested: AtomicU64::new(0),
            bytes_from_disk: AtomicU64::new(0),
        }
    }

    pub(crate) fn stats(&self) -> ReadIoStats {
        let (entries, used, cap) = match self.inner.lock() {
            Ok(inner) => (
                inner.map.len() as u64,
                inner.used_bytes as u64,
                inner.max_bytes as u64,
            ),
            Err(_) => (0, 0, 0),
        };
        ReadIoStats {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            unique_requests: self.unique_requests.load(Ordering::Relaxed),
            overlap_unique_requests: self.overlap_unique_requests.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            bytes_requested: self.bytes_requested.load(Ordering::Relaxed),
            bytes_from_disk: self.bytes_from_disk.load(Ordering::Relaxed),
            cache_entries: entries,
            cache_used_bytes: used,
            cache_capacity_bytes: cap,
        }
    }

    pub(crate) fn read_exact(
        &self,
        file: &File,
        offset: u64,
        len: usize,
    ) -> NativeResult<Arc<[u8]>> {
        if len == 0 {
            return Ok(Arc::from([]));
        }
        if len > u32::MAX as usize {
            return Err(NativeError::Invalid("requested read length exceeds u32"));
        }

        let key = (offset, len as u32);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.bytes_requested
            .fetch_add(len as u64, Ordering::Relaxed);

        if let Ok(mut inner) = self.inner.lock() {
            if inner.seen.insert(key) {
                self.unique_requests.fetch_add(1, Ordering::Relaxed);
                let end = offset.saturating_add(len as u64);
                let has_overlap = inner
                    .unique_ranges
                    .iter()
                    .any(|(s, e)| offset < *e && *s < end);
                if has_overlap {
                    self.overlap_unique_requests.fetch_add(1, Ordering::Relaxed);
                }
                inner.unique_ranges.push((offset, end));
            }
            if let Some(hit) = inner.map.get(&key) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(hit.clone());
            }
        }

        let bytes = read_exact_at_file(file, offset, len)?;
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        self.bytes_from_disk
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let value: Arc<[u8]> = Arc::from(bytes.into_boxed_slice());

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| NativeError::Invalid("read cache mutex poisoned"))?;
        if let Some(hit) = inner.map.get(&key) {
            return Ok(hit.clone());
        }

        if inner.max_bytes == 0 {
            return Ok(value);
        }

        let entry_len = value.len();
        if entry_len > inner.max_bytes {
            inner.map.clear();
            inner.order.clear();
            inner.used_bytes = 0;
            return Ok(value);
        }

        while inner.used_bytes.saturating_add(entry_len) > inner.max_bytes {
            let Some(evicted) = inner.order.pop_front() else {
                break;
            };
            if let Some(old) = inner.map.remove(&evicted) {
                inner.used_bytes = inner.used_bytes.saturating_sub(old.len());
            }
        }

        inner.order.push_back(key);
        inner.used_bytes = inner.used_bytes.saturating_add(entry_len);
        inner.map.insert(key, value.clone());
        Ok(value)
    }
}
