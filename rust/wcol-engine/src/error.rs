//! Unified errors for the Wasm stream executor and C FFI surface.

use std::sync::Mutex;

pub type WcolResult<T> = Result<T, WcolError>;

const MAX_ERROR_LEN: usize = 4096;

/// Executor / pipeline / FFI session errors. Exposed to the host as UTF-8 bytes.
#[derive(Debug, thiserror::Error)]
pub enum WcolError {
    #[error("{0}")]
    Static(&'static str),
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
        let message = if detail.starts_with("wcol-fatal:") {
            detail.to_string()
        } else {
            format!("wcol-fatal: {detail}")
        };
        Self::Dynamic(message)
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
        msg.truncate(MAX_ERROR_LEN);
        msg.push('…');
    }
    msg
}

/// Record the most recent error from a stateless FFI call or failed `wcs_open`.
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
