use std::path::PathBuf;

use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "wcol")]
#[command(about = "wcol converter + query runner", long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Commands>,

    #[arg(value_name = "INPUT")]
    pub input: Option<PathBuf>,

    #[arg(short, long)]
    pub out: Option<PathBuf>,
    #[arg(long)]
    pub show_schema: bool,
    #[arg(long)]
    pub show_stats: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    Convert {
        input: PathBuf,
        #[arg(short, long)]
        out: Option<PathBuf>,
        #[arg(long)]
        show_schema: bool,
        #[arg(long)]
        show_stats: bool,
        #[arg(long)]
        split_row_groups: Option<usize>,
    },
    Query {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        sql: Option<String>,
        #[arg(long)]
        sql_file: Option<PathBuf>,
        #[arg(long, default_value_t = 1)]
        workers: usize,
        #[arg(long, value_enum, default_value_t = QueryFormat::Json)]
        format: QueryFormat,
        #[command(flatten)]
        native: NativeExecOpts,
    },
    Bench {
        #[arg(long)]
        file: PathBuf,
        #[arg(long, default_value = "rust/wcol-sql-parser/readme.md")]
        sql_file: PathBuf,
        #[arg(long, default_value_t = 15)]
        runs: usize,
        #[arg(long, default_value_t = 5)]
        warmup: usize,
        #[arg(long, default_value_t = 1)]
        workers: usize,
        #[arg(long)]
        only: Option<String>,
        #[command(flatten)]
        native: NativeExecOpts,
    },
    #[command(hide = true)]
    BenchWorker {
        #[arg(long)]
        file: PathBuf,
    },
    Parity {
        #[arg(long)]
        wcol_file: PathBuf,
        #[arg(long)]
        parquet_file: PathBuf,
        #[arg(long, default_value = "rust/wcol-sql-parser/readme.md")]
        sql_file: PathBuf,
        #[arg(long, default_value_t = 1)]
        workers: usize,
        #[arg(long)]
        only: Option<String>,
        #[command(flatten)]
        native: NativeExecOpts,
    },
}

#[derive(Clone, Debug, Default, ClapArgs)]
pub struct NativeExecOpts {
    #[arg(long)]
    pub arena_base_mb: Option<u64>,
    #[arg(long)]
    pub arena_grow_mb: Option<u64>,
    #[arg(long)]
    pub arena_max_mb: Option<u64>,
    #[arg(long)]
    pub arena_global_cap_mb: Option<String>,
    #[arg(long, value_enum)]
    pub arena_release: Option<ArenaReleaseValue>,
    #[arg(long)]
    pub arena_keep_up_to_mb: Option<u64>,
    #[arg(long)]
    pub arena_retained_global_cap_mb: Option<String>,
    #[arg(long)]
    pub arena_retained_idle_decay_queries: Option<u32>,
    #[arg(long)]
    pub string_window_mb: Option<u64>,
    #[arg(long)]
    pub group_partitions: Option<usize>,
    #[arg(long)]
    pub partition_count: Option<usize>,
    #[arg(long)]
    pub merge_workers: Option<usize>,
    #[arg(long)]
    pub reduce_workers: Option<usize>,
    #[arg(long, value_enum)]
    pub group_engine: Option<GroupEngineValue>,
    #[arg(long)]
    pub scan_partition_queue_mb: Option<String>,
    #[arg(long)]
    pub partition_sort_chunk_mb: Option<u64>,
    #[arg(long, value_enum)]
    pub merge_keys: Option<MergeKeysValue>,
    #[arg(long, value_enum)]
    pub cache_counters: Option<CacheCountersValue>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum ArenaReleaseValue {
    ReleaseAll,
    KeepAll,
    KeepUpToMb,
}

impl ArenaReleaseValue {
    pub fn as_env(self) -> &'static str {
        match self {
            Self::ReleaseAll => "release-all",
            Self::KeepAll => "keep-all",
            Self::KeepUpToMb => "keep-up-to-mb",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum CacheCountersValue {
    Off,
    On,
    Strict,
}

impl CacheCountersValue {
    pub fn as_env(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::On => "on",
            Self::Strict => "strict",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum MergeKeysValue {
    Hash,
    Bytes,
}

impl MergeKeysValue {
    pub fn as_env(self) -> &'static str {
        match self {
            Self::Hash => "hash",
            Self::Bytes => "bytes",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum GroupEngineValue {
    Legacy,
    PartitionSort,
    PartitionSortV2,
    PartitionDirect,
}

impl GroupEngineValue {
    pub fn as_env(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::PartitionSort => "partition-sort",
            Self::PartitionSortV2 => "partition-sort-v2",
            Self::PartitionDirect => "partition-direct",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum QueryFormat {
    Json,
    Summary,
}

#[derive(Clone, Debug)]
pub struct QuerySpec {
    pub index: usize,
    pub sql: String,
}
