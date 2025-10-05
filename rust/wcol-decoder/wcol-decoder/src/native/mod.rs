mod cache;
mod config;
mod error;
mod exec;
mod helpers;
mod mem_budget;
mod perf_counters;
mod pool;
mod result;
mod runtime;
mod types;

pub use cache::ReadIoStats;
pub use config::{
    ArenaReleasePolicy, CacheCounterMode, GroupEngineMode, MergeKeysMode, NativeRuntimeConfig,
    QueryExecutionConfig,
};
pub use error::{NativeError, NativeResult};
pub use runtime::NativeRuntime;
pub use types::{AggregateStats, GroupAggInfo, GroupKeyInfo, GroupResult, HeaderInfo, QueryResult};

const HEADER_FLAG_DICT_COMPRESSED: u32 = 1;
const AGG_RECORD_SIZE: usize = 4 + 1 + 3 + 8 + 8 + 8 + 4;
const GROUP_AGG_RECORD_SIZE: usize = 8 + 8 + 8 + 4 + 4;
