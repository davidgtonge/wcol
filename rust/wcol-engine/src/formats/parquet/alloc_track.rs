//! Temporary allocator probe. Live bytes are what Rust still holds. Peak is the
//! highest live total since the module started. `memory` is the Wasm buffer,
//! which stays at its high-water mark after those allocations are freed.

#[cfg(target_arch = "wasm32")]
mod tracking {
    use std::alloc::{GlobalAlloc, Layout};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);
    static INNER: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

    pub fn live() -> usize {
        LIVE.load(Ordering::Relaxed)
    }

    pub fn peak() -> usize {
        PEAK.load(Ordering::Relaxed)
    }

    pub fn memory_bytes() -> usize {
        core::arch::wasm32::memory_size(0) * 65536
    }

    fn add(size: usize) {
        let now = LIVE.fetch_add(size, Ordering::Relaxed) + size;
        let mut seen = PEAK.load(Ordering::Relaxed);
        while now > seen {
            match PEAK.compare_exchange_weak(seen, now, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(current) => seen = current,
            }
        }
    }

    fn sub(size: usize) {
        let now = LIVE.load(Ordering::Relaxed);
        LIVE.store(now.saturating_sub(size), Ordering::Relaxed);
    }

    struct Track;

    unsafe impl GlobalAlloc for Track {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = INNER.alloc(layout);
            if !ptr.is_null() {
                add(layout.size());
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            INNER.dealloc(ptr, layout);
            sub(layout.size());
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = INNER.alloc_zeroed(layout);
            if !ptr.is_null() {
                add(layout.size());
            }
            ptr
        }

        // Forwarded so dlmalloc can grow a block in place: the default realloc copies into a
        // new block, keeping old and new alive together and raising peak memory. It costs
        // about 96 KB of Wasm (dlmalloc's realloc), which the node budget allows for.
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let grown = INNER.realloc(ptr, layout, new_size);
            if !grown.is_null() {
                sub(layout.size());
                add(new_size);
            }
            grown
        }
    }

    #[global_allocator]
    static ALLOC: Track = Track;
}

#[cfg(all(not(target_arch = "wasm32"), not(feature = "alloc-track-native")))]
mod tracking {
    pub fn live() -> usize {
        0
    }

    pub fn peak() -> usize {
        0
    }

    pub fn memory_bytes() -> usize {
        0
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "alloc-track-native"))]
mod tracking {
    use std::alloc::{GlobalAlloc, Layout};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);

    pub fn live() -> usize {
        LIVE.load(Ordering::Relaxed)
    }

    pub fn peak() -> usize {
        PEAK.load(Ordering::Relaxed)
    }

    pub fn memory_bytes() -> usize {
        0
    }

    fn add(size: usize) {
        let now = LIVE.fetch_add(size, Ordering::Relaxed) + size;
        let mut seen = PEAK.load(Ordering::Relaxed);
        while now > seen {
            match PEAK.compare_exchange_weak(seen, now, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(current) => seen = current,
            }
        }
    }

    fn sub(size: usize) {
        let now = LIVE.load(Ordering::Relaxed);
        LIVE.store(now.saturating_sub(size), Ordering::Relaxed);
    }

    struct Track;

    unsafe impl GlobalAlloc for Track {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = std::alloc::System.alloc(layout);
            if !ptr.is_null() {
                add(layout.size());
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            std::alloc::System.dealloc(ptr, layout);
            sub(layout.size());
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = std::alloc::System.alloc_zeroed(layout);
            if !ptr.is_null() {
                add(layout.size());
            }
            ptr
        }
    }

    #[global_allocator]
    static ALLOC: Track = Track;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemorySnapshot {
    pub live_bytes: usize,
    pub peak_bytes: usize,
    pub wasm_linear_bytes: usize,
}

pub fn snapshot() -> MemorySnapshot {
    MemorySnapshot {
        live_bytes: tracking::live(),
        peak_bytes: tracking::peak(),
        wasm_linear_bytes: tracking::memory_bytes(),
    }
}
