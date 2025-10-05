#[cfg(feature = "bench_api")]
mod bench;
mod deferred_filter;
mod dict;
mod exec;
mod group_output;
mod materialize;
mod mem;
mod null_policy;
mod plan;
mod required_pages;
mod row_candidates;
mod row_exec;
mod row_order;
mod runtime;

#[cfg(feature = "bench_api")]
#[allow(unused_imports)]
pub use bench::*;
#[allow(unused_imports)]
pub use deferred_filter::*;
#[allow(unused_imports)]
pub use dict::*;
#[allow(unused_imports)]
pub use exec::*;
#[allow(unused_imports)]
pub use group_output::*;
#[allow(unused_imports)]
pub use materialize::*;
#[allow(unused_imports)]
pub use mem::*;
#[allow(unused_imports)]
pub use null_policy::*;
#[allow(unused_imports)]
pub use plan::*;
#[allow(unused_imports)]
pub use required_pages::*;
#[allow(unused_imports)]
pub use row_candidates::*;
#[allow(unused_imports)]
pub use row_exec::*;
#[allow(unused_imports)]
pub use row_order::*;
#[allow(unused_imports)]
pub use runtime::*;

use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};
#[cfg(all(feature = "timing", not(target_arch = "wasm32")))]
use std::time::Instant;

use crate::timing;
use crate::types::{Plan, Runtime};

pub(crate) static RUNTIMES: LazyLock<Mutex<FxHashMap<u32, Box<Runtime>>>> =
    LazyLock::new(|| Mutex::new(FxHashMap::default()));
pub(crate) static PLANS: LazyLock<Mutex<FxHashMap<u32, Box<Plan>>>> =
    LazyLock::new(|| Mutex::new(FxHashMap::default()));
pub(crate) static HANDLE_COUNTER: AtomicU32 = AtomicU32::new(1);
pub(crate) static PLAN_LOCK_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static PLAN_LOCK_WAIT_NS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static RUNTIME_LOCK_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static RUNTIME_LOCK_WAIT_NS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub(crate) fn next_handle() -> u32 {
    HANDLE_COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[inline]
pub(crate) fn now_ms() -> f64 {
    timing::now_ms()
}

macro_rules! lock_map_timed {
    ($mutex:expr, $lock_count:expr, $wait_ns:expr) => {{
        #[cfg(feature = "timing")]
        let started = {
            #[cfg(target_arch = "wasm32")]
            {
                now_ms()
            }
            #[cfg(not(target_arch = "wasm32"))]
            {
                Instant::now()
            }
        };
        let guard = $mutex.lock().unwrap();
        $lock_count.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "timing")]
        {
            #[cfg(target_arch = "wasm32")]
            {
                let elapsed_ns = ((now_ms() - started).max(0.0) * 1_000_000.0) as u64;
                $wait_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
            }
            #[cfg(not(target_arch = "wasm32"))]
            {
                $wait_ns.fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        }
        guard
    }};
}

#[inline]
pub(crate) fn lock_plans_timed() -> MutexGuard<'static, FxHashMap<u32, Box<Plan>>> {
    lock_map_timed!(PLANS, PLAN_LOCK_COUNT, PLAN_LOCK_WAIT_NS)
}

#[inline]
pub(crate) fn lock_runtimes_timed() -> MutexGuard<'static, FxHashMap<u32, Box<Runtime>>> {
    lock_map_timed!(RUNTIMES, RUNTIME_LOCK_COUNT, RUNTIME_LOCK_WAIT_NS)
}

pub(crate) fn write_u32(out: &mut [u8], offset: usize, value: u32) {
    out[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_i32(out: &mut [u8], offset: usize, value: i32) {
    out[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u64(out: &mut [u8], offset: usize, value: u64) {
    out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_f64(out: &mut [u8], offset: usize, value: f64) {
    out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u8(out: &mut [u8], offset: usize, value: u8) {
    out[offset] = value;
}
