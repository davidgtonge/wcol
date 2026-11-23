//! Synchronous reducer trace. The host import runs before the Rust call returns,
//! so a step that never comes back still leaves its entry line.

use std::cell::Cell;

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
}

pub fn set_enabled(on: bool) {
    ENABLED.set(on);
}

pub fn enabled() -> bool {
    ENABLED.get()
}

pub struct Guard {
    kind: &'static str,
    detail: u64,
    armed: bool,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.armed {
            emit(&format!(
                "reducer-trace {} exit n={}",
                self.kind, self.detail
            ));
        }
    }
}

/// Log `{kind} enter` and `{kind} exit` when the guard drops. `n` is rows or live groups.
pub fn guard(kind: &'static str, n: u64) -> Guard {
    if !enabled() {
        return Guard {
            kind,
            detail: n,
            armed: false,
        };
    }
    emit(&format!("reducer-trace {kind} enter n={n}"));
    Guard {
        kind,
        detail: n,
        armed: true,
    }
}

pub fn note(message: &str) {
    if enabled() {
        emit(message);
    }
}

fn emit(message: &str) {
    #[cfg(target_arch = "wasm32")]
    unsafe {
        wcol_trace(message.as_ptr(), message.len());
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = message;
    }
}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "wcol")]
extern "C" {
    fn wcol_trace(ptr: *const u8, len: usize);
}
