use std::io;

#[derive(Debug)]
pub enum NativeError {
    Io(io::Error),
    Status(&'static str, i32),
    Invalid(&'static str),
    Utf8(std::string::FromUtf8Error),
    ArenaCap {
        worker_id: usize,
        requested_bytes: u64,
        thread_used_bytes: u64,
        thread_cap_bytes: u64,
        global_used_bytes: u64,
        global_cap_bytes: u64,
        query_id: u64,
        stage: &'static str,
    },
}

impl std::fmt::Display for NativeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Status(op, code) => write!(f, "{op} failed with status {code}"),
            Self::Invalid(msg) => write!(f, "{msg}"),
            Self::Utf8(err) => write!(f, "{err}"),
            Self::ArenaCap {
                worker_id,
                requested_bytes,
                thread_used_bytes,
                thread_cap_bytes,
                global_used_bytes,
                global_cap_bytes,
                query_id,
                stage,
            } => write!(
                f,
                "arena cap exceeded: worker={worker_id} stage={stage} query_id={query_id} requested_bytes={requested_bytes} thread_used_bytes={thread_used_bytes} thread_cap_bytes={thread_cap_bytes} global_used_bytes={global_used_bytes} global_cap_bytes={global_cap_bytes}"
            ),
        }
    }
}

impl std::error::Error for NativeError {}

impl From<io::Error> for NativeError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<std::string::FromUtf8Error> for NativeError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::Utf8(value)
    }
}

pub type NativeResult<T> = Result<T, NativeError>;
