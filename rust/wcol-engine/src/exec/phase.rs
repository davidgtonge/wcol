//! One-row phase breadcrumb. The host import runs before the Rust call returns.
//! Repeating consume/aggregate pairs are capped so a long step does not write
//! once per frame. Flush and step exit always record.

use std::cell::Cell;

pub const POLL_ENTER: u32 = 1;
pub const GRAPH_STEP_ENTER: u32 = 2;
pub const WCS_CONSUME_ENTER: u32 = 3;
pub const AGGREGATE_ENTER: u32 = 4;
pub const FLUSH_ENTER: u32 = 5;
pub const GRAPH_STEP_EXIT: u32 = 6;
pub const POLL_EXIT: u32 = 7;
pub const HOST_WAIT: u32 = 8;

const HOT_MARKS_PER_STEP: u32 = 8;

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static LAST: Cell<u32> = const { Cell::new(u32::MAX) };
    static HOT_LEFT: Cell<u32> = const { Cell::new(HOT_MARKS_PER_STEP) };
}

pub fn set_enabled(on: bool) {
    ENABLED.set(on);
    LAST.set(u32::MAX);
    HOT_LEFT.set(HOT_MARKS_PER_STEP);
}

pub fn mark(phase: u32, port: u32, rows: u32, frames: u32, group_bytes: u32) {
    if !ENABLED.get() {
        return;
    }
    if phase == GRAPH_STEP_ENTER {
        HOT_LEFT.set(HOT_MARKS_PER_STEP);
    }
    if LAST.get() == phase {
        return;
    }
    let reserved = phase == GRAPH_STEP_ENTER || phase == GRAPH_STEP_EXIT || phase == FLUSH_ENTER;
    if !reserved {
        let left = HOT_LEFT.get();
        if left == 0 {
            return;
        }
        HOT_LEFT.set(left - 1);
    }
    LAST.set(phase);
    #[cfg(target_arch = "wasm32")]
    unsafe {
        wcol_phase(phase, port, rows, frames, group_bytes);
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (port, rows, frames, group_bytes);
    }
}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "wcol")]
extern "C" {
    fn wcol_phase(phase: u32, port: u32, rows: u32, frames: u32, group_bytes: u32);
}
