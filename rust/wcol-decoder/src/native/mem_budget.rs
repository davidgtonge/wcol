use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::config::ArenaReleasePolicy;
use super::error::{NativeError, NativeResult};

#[derive(Clone, Copy, Debug)]
pub struct MemoryBasis {
    pub total_bytes: u64,
    pub source: &'static str,
}

#[derive(Debug)]
pub struct QueryMemoryBudget {
    cap_bytes: u64,
    query_id: u64,
    used_bytes: AtomicU64,
}

impl QueryMemoryBudget {
    pub fn new(cap_bytes: u64, query_id: u64, initial_used_bytes: u64) -> Self {
        let initial = initial_used_bytes.min(cap_bytes);
        Self {
            cap_bytes,
            query_id,
            used_bytes: AtomicU64::new(initial),
        }
    }

    pub fn query_id(&self) -> u64 {
        self.query_id
    }

    pub fn cap_bytes(&self) -> u64 {
        self.cap_bytes
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }

    pub fn try_reserve(
        &self,
        add_bytes: u64,
        worker_id: usize,
        stage: &'static str,
    ) -> NativeResult<()> {
        if add_bytes == 0 {
            return Ok(());
        }
        loop {
            let current = self.used_bytes.load(Ordering::Relaxed);
            let next = current.saturating_add(add_bytes);
            if next > self.cap_bytes {
                return Err(NativeError::ArenaCap {
                    worker_id,
                    requested_bytes: add_bytes,
                    thread_used_bytes: 0,
                    thread_cap_bytes: 0,
                    global_used_bytes: current,
                    global_cap_bytes: self.cap_bytes,
                    query_id: self.query_id,
                    stage,
                });
            }
            if self
                .used_bytes
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(());
            }
        }
    }
}

#[derive(Debug)]
pub struct ThreadArenaState {
    worker_id: usize,
    base_bytes: u64,
    grow_bytes: u64,
    max_bytes: u64,
    reserved_bytes: u64,
    used_bytes: u64,
    idle_queries: u32,
    last_query_reserved_peak: u64,
}

impl ThreadArenaState {
    pub fn new(worker_id: usize, base_bytes: u64, grow_bytes: u64, max_bytes: u64) -> Self {
        let base = base_bytes.max(1);
        Self {
            worker_id,
            base_bytes: base,
            grow_bytes: grow_bytes.max(1),
            max_bytes: max_bytes.max(base),
            reserved_bytes: base,
            used_bytes: 0,
            idle_queries: 0,
            last_query_reserved_peak: base,
        }
    }

    pub fn begin_query(&mut self) {
        self.used_bytes = 0;
        self.last_query_reserved_peak = self.reserved_bytes;
    }

    pub fn reserve_for(
        &mut self,
        need_bytes: u64,
        budget: &QueryMemoryBudget,
        retained_cap: &RetainedMemoryCap,
        stage: &'static str,
    ) -> NativeResult<()> {
        let next_used = self.used_bytes.saturating_add(need_bytes);
        self.set_live_bytes(next_used, budget, retained_cap, stage)
    }

    pub fn set_live_bytes(
        &mut self,
        live_bytes: u64,
        budget: &QueryMemoryBudget,
        retained_cap: &RetainedMemoryCap,
        stage: &'static str,
    ) -> NativeResult<()> {
        let requested = live_bytes.saturating_sub(self.used_bytes);
        let next_used = live_bytes;
        if next_used <= self.reserved_bytes {
            self.used_bytes = next_used;
            self.last_query_reserved_peak = self.last_query_reserved_peak.max(self.reserved_bytes);
            return Ok(());
        }

        let mut target = self.reserved_bytes;
        while target < next_used {
            target = target.saturating_add(self.grow_bytes);
            if target > self.max_bytes {
                return Err(NativeError::ArenaCap {
                    worker_id: self.worker_id,
                    requested_bytes: requested,
                    thread_used_bytes: self.used_bytes,
                    thread_cap_bytes: self.max_bytes,
                    global_used_bytes: budget.used_bytes(),
                    global_cap_bytes: budget.cap_bytes(),
                    query_id: budget.query_id(),
                    stage,
                });
            }
        }
        let grow = target.saturating_sub(self.reserved_bytes);
        if let Err(err) = budget.try_reserve(grow, self.worker_id, stage) {
            return match err {
                NativeError::ArenaCap {
                    requested_bytes,
                    global_used_bytes,
                    global_cap_bytes,
                    query_id,
                    ..
                } => Err(NativeError::ArenaCap {
                    worker_id: self.worker_id,
                    requested_bytes,
                    thread_used_bytes: self.used_bytes,
                    thread_cap_bytes: self.max_bytes,
                    global_used_bytes,
                    global_cap_bytes,
                    query_id,
                    stage,
                }),
                other => Err(other),
            };
        }
        retained_cap.add_live(grow);
        self.reserved_bytes = target;
        self.used_bytes = next_used;
        self.last_query_reserved_peak = self.last_query_reserved_peak.max(self.reserved_bytes);
        Ok(())
    }

    pub fn reset_live_bytes(&mut self) {
        self.used_bytes = 0;
    }

    pub fn end_query(
        &mut self,
        release_policy: ArenaReleasePolicy,
        keep_up_to_bytes: u64,
        retained_cap: &RetainedMemoryCap,
        decay_after_queries: u32,
    ) {
        if self.used_bytes <= self.base_bytes {
            self.idle_queries = self.idle_queries.saturating_add(1);
        } else {
            self.idle_queries = 0;
        }

        let mut desired = match release_policy {
            ArenaReleasePolicy::ReleaseAll => self.base_bytes,
            ArenaReleasePolicy::KeepAll => self.reserved_bytes,
            ArenaReleasePolicy::KeepUpToMb => self
                .reserved_bytes
                .min(keep_up_to_bytes.max(self.base_bytes)),
        };
        if self.idle_queries >= decay_after_queries {
            desired = desired.min(self.base_bytes);
        }

        let current = self.reserved_bytes;
        let accepted = retained_cap.adjust_retained(current, desired);
        self.reserved_bytes = accepted.max(self.base_bytes);
        self.used_bytes = 0;
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    pub fn last_query_reserved_peak(&self) -> u64 {
        self.last_query_reserved_peak
    }
}

#[derive(Debug)]
pub struct RetainedMemoryCap {
    total_retained: AtomicU64,
    cap_bytes: AtomicU64,
}

impl RetainedMemoryCap {
    pub fn with_initial(cap_bytes: u64, initial_retained: u64) -> Arc<Self> {
        Arc::new(Self {
            total_retained: AtomicU64::new(initial_retained.min(cap_bytes)),
            cap_bytes: AtomicU64::new(cap_bytes),
        })
    }

    pub fn set_cap_bytes(&self, cap_bytes: u64) {
        self.cap_bytes.store(cap_bytes, Ordering::Relaxed);
    }

    pub fn current_retained(&self) -> u64 {
        self.total_retained.load(Ordering::Relaxed)
    }

    pub fn add_live(&self, add_bytes: u64) {
        if add_bytes == 0 {
            return;
        }
        self.total_retained.fetch_add(add_bytes, Ordering::SeqCst);
    }

    pub fn adjust_retained(&self, current_worker_reserved: u64, desired_reserved: u64) -> u64 {
        loop {
            let total = self.total_retained.load(Ordering::Relaxed);
            let cap = self.cap_bytes.load(Ordering::Relaxed);
            let without_current = total.saturating_sub(current_worker_reserved);
            let mut accepted = desired_reserved;
            let next_total = without_current.saturating_add(accepted);
            if next_total > cap {
                accepted = cap.saturating_sub(without_current);
            }
            let target_total = without_current.saturating_add(accepted);
            if self
                .total_retained
                .compare_exchange(total, target_total, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return accepted;
            }
        }
    }
}

pub fn detect_memory_basis() -> MemoryBasis {
    if let Some(bytes) = read_cgroup_limit() {
        return MemoryBasis {
            total_bytes: bytes,
            source: "cgroup",
        };
    }
    if let Some(bytes) = read_meminfo_total() {
        return MemoryBasis {
            total_bytes: bytes,
            source: "meminfo",
        };
    }
    if let Some(bytes) = read_sys_total() {
        return MemoryBasis {
            total_bytes: bytes,
            source: "sysconf",
        };
    }
    MemoryBasis {
        total_bytes: 8 * 1024 * 1024 * 1024,
        source: "fallback",
    }
}

fn read_cgroup_limit() -> Option<u64> {
    let v2 = fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let trimmed = v2.trim();
    if trimmed != "max" {
        if let Ok(value) = trimmed.parse::<u64>() {
            if value > 0 {
                return Some(value);
            }
        }
    }
    let v1 = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes").ok()?;
    let value = v1.trim().parse::<u64>().ok()?;
    if value > 0 {
        return Some(value);
    }
    None
}

fn read_meminfo_total() -> Option<u64> {
    #[cfg(not(target_os = "linux"))]
    {
        return None;
    }
    #[cfg(target_os = "linux")]
    {
        let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
        for line in meminfo.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
                return Some(kb.saturating_mul(1024));
            }
        }
        None
    }
}

fn read_sys_total() -> Option<u64> {
    #[cfg(unix)]
    unsafe {
        let pages = libc::sysconf(libc::_SC_PHYS_PAGES);
        let page_size = libc::sysconf(libc::_SC_PAGESIZE);
        if pages <= 0 || page_size <= 0 {
            return None;
        }
        let total = (pages as u128).saturating_mul(page_size as u128);
        return Some(total.min(u64::MAX as u128) as u64);
    }
    #[cfg(not(unix))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn alternating_tiny_huge_queries_do_not_keep_growing() {
        let retained = RetainedMemoryCap::with_initial(u64::MAX, 0);
        let budget = QueryMemoryBudget::new(8 * 1024 * 1024 * 1024, 1, 100 * 1024 * 1024);
        let mut arena =
            ThreadArenaState::new(0, 100 * 1024 * 1024, 50 * 1024 * 1024, 1024 * 1024 * 1024);

        for i in 0..8u32 {
            arena.begin_query();
            let need = if i % 2 == 0 {
                8 * 1024 * 1024
            } else {
                280 * 1024 * 1024
            };
            arena
                .reserve_for(need, &budget, &retained, "test")
                .expect("reserve");
            arena.end_query(
                ArenaReleasePolicy::KeepUpToMb,
                256 * 1024 * 1024,
                &retained,
                2,
            );
            assert!(arena.reserved_bytes() <= 256 * 1024 * 1024);
        }

        arena.begin_query();
        arena
            .reserve_for(4 * 1024 * 1024, &budget, &retained, "test")
            .expect("reserve tiny");
        arena.end_query(
            ArenaReleasePolicy::KeepUpToMb,
            256 * 1024 * 1024,
            &retained,
            2,
        );
        arena.begin_query();
        arena
            .reserve_for(4 * 1024 * 1024, &budget, &retained, "test")
            .expect("reserve tiny");
        arena.end_query(
            ArenaReleasePolicy::KeepUpToMb,
            256 * 1024 * 1024,
            &retained,
            2,
        );
        assert_eq!(arena.reserved_bytes(), 100 * 1024 * 1024);
    }

    #[test]
    fn concurrent_growth_caps_and_abort_cleanup() {
        let budget = Arc::new(QueryMemoryBudget::new(100, 99, 0));
        let mut joins = Vec::new();
        for _ in 0..8 {
            let budget = budget.clone();
            joins.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let _ = budget.try_reserve(1, 0, "test");
                }
            }));
        }
        for join in joins {
            join.join().expect("join");
        }
        assert_eq!(budget.used_bytes(), 100);

        let retained = RetainedMemoryCap::with_initial(100 * 1024 * 1024, 100 * 1024 * 1024);
        let budget = QueryMemoryBudget::new(
            260 * 1024 * 1024,
            2,
            100 * 1024 * 1024, // two workers' base retained bytes
        );
        let mut w1 =
            ThreadArenaState::new(0, 50 * 1024 * 1024, 50 * 1024 * 1024, 1024 * 1024 * 1024);
        let mut w2 =
            ThreadArenaState::new(1, 50 * 1024 * 1024, 50 * 1024 * 1024, 1024 * 1024 * 1024);

        w1.begin_query();
        w2.begin_query();
        w1.reserve_for(120 * 1024 * 1024, &budget, &retained, "scan")
            .expect("w1 reserve");
        let err = w2
            .reserve_for(120 * 1024 * 1024, &budget, &retained, "scan")
            .expect_err("w2 should hit global cap");
        match err {
            NativeError::ArenaCap { .. } => {}
            other => panic!("expected ArenaCap, got {other}"),
        }

        w1.end_query(ArenaReleasePolicy::ReleaseAll, 0, &retained, 1);
        w2.end_query(ArenaReleasePolicy::ReleaseAll, 0, &retained, 1);
        assert_eq!(w1.reserved_bytes(), 50 * 1024 * 1024);
        assert_eq!(w2.reserved_bytes(), 50 * 1024 * 1024);
        assert_eq!(retained.current_retained(), 100 * 1024 * 1024);
    }
}
