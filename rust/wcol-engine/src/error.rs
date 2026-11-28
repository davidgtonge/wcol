//! Unified errors for the Wasm stream executor and C FFI surface.

use std::sync::Mutex;

pub type WcolResult<T> = Result<T, WcolError>;

const MAX_ERROR_LEN: usize = 4096;

/// Leads the message of a fatal [`WcolError`]. The TS host matches it
/// (`src/runtime/coordinator/task-errors.ts`) to abort a task instead of retrying it.
pub const FATAL_MARK: &str = "wcol-fatal:";

/// Executor / pipeline / FFI session errors. Exposed to the host as UTF-8 bytes.
#[derive(Debug, thiserror::Error)]
pub enum WcolError {
    #[error("{0}")]
    Static(&'static str),
    /// A [`Self::fatal`] error is a `Dynamic` led by [`FATAL_MARK`]: the mark is what tells
    /// it apart on both sides of the FFI, so there is one test for it, [`Self::is_fatal`].
    #[error("{0}")]
    Dynamic(String),
}

impl WcolError {
    pub fn static_msg(message: &'static str) -> Self {
        Self::Static(message)
    }

    pub fn msg(message: impl Into<String>) -> Self {
        Self::Dynamic(message.into())
    }

    /// The same input will fail again. Hosts must abort the task and must not retry it.
    pub fn fatal(detail: impl AsRef<str>) -> Self {
        let detail = detail.as_ref();
        let message = if detail.starts_with(FATAL_MARK) {
            detail.to_string()
        } else {
            format!("{FATAL_MARK} {detail}")
        };
        Self::Dynamic(message)
    }

    /// Whether retrying the same input is pointless (see [`Self::fatal`]).
    pub fn is_fatal(&self) -> bool {
        self.as_str().starts_with(FATAL_MARK)
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Static(s) => s,
            Self::Dynamic(s) => s.as_str(),
        }
    }
}

impl From<&'static str> for WcolError {
    fn from(value: &'static str) -> Self {
        Self::Static(value)
    }
}

impl From<String> for WcolError {
    fn from(value: String) -> Self {
        Self::Dynamic(value)
    }
}

impl From<crate::formats::parquet::Error> for WcolError {
    fn from(value: crate::formats::parquet::Error) -> Self {
        Self::Dynamic(value.to_string())
    }
}

impl From<WcolError> for String {
    fn from(value: WcolError) -> Self {
        value.to_string()
    }
}

static LAST_ERROR: Mutex<Option<String>> = Mutex::new(None);

fn truncate_message(mut msg: String) -> String {
    if msg.len() > MAX_ERROR_LEN {
        let mut end = MAX_ERROR_LEN;
        while !msg.is_char_boundary(end) {
            end -= 1;
        }
        msg.truncate(end);
        msg.push('…');
    }
    msg
}

/// Record the most recent error from a stateless FFI call or a failed open.
pub fn set_last_error(err: WcolError) {
    let msg = truncate_message(err.to_string());
    if let Ok(mut guard) = LAST_ERROR.lock() {
        *guard = Some(msg);
    }
}

pub fn clear_last_error() {
    if let Ok(mut guard) = LAST_ERROR.lock() {
        *guard = None;
    }
}

/// Pointer and length into the last-error buffer (valid until the next `set_last_error` / successful open).
pub fn last_error_bytes() -> Option<(*const u8, u32)> {
    let guard = LAST_ERROR.lock().ok()?;
    guard.as_ref().map(|s| (s.as_ptr(), s.len() as u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_stops_on_a_char_boundary() {
        let msg = format!("a{}", "é".repeat(MAX_ERROR_LEN));
        let out = truncate_message(msg);
        assert!(out.len() <= MAX_ERROR_LEN + '…'.len_utf8());
        assert!(out.ends_with('…'));
    }

    #[test]
    fn fatal_errors_carry_the_host_mark_once() {
        let err = WcolError::fatal(WcolError::fatal("row too large").as_str());
        assert!(err.is_fatal());
        assert_eq!(err.as_str(), "wcol-fatal: row too large");
        assert!(!WcolError::msg("retry me").is_fatal());
    }
}
