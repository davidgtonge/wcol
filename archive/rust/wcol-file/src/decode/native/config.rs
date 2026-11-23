use super::mem_budget::MemoryBasis;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArenaReleasePolicy {
    ReleaseAll,
    KeepAll,
    KeepUpToMb,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheCounterMode {
    Off,
    On,
    Strict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergeKeysMode {
    Hash,
    Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupEngineMode {
    Legacy,
    PartitionSort,
    PartitionSortV2,
    PartitionDirect,
}

#[derive(Clone, Copy, Debug)]
pub struct NativeRuntimeConfig {
    pub arena_base_bytes: u64,
    pub arena_grow_bytes: u64,
    pub arena_max_bytes: u64,
    pub global_cap_override_bytes: Option<u64>,
    pub retained_global_cap_override_bytes: Option<u64>,
    pub arena_release_policy: ArenaReleasePolicy,
    pub arena_keep_up_to_bytes: u64,
    pub retained_idle_decay_queries: u32,
    pub string_window_bytes: u64,
    pub group_partitions_override: Option<usize>,
    pub merge_workers_override: Option<usize>,
    pub reduce_workers_override: Option<usize>,
    pub partition_count_override: Option<usize>,
    pub cache_counter_mode: CacheCounterMode,
    pub merge_keys_mode: MergeKeysMode,
    pub group_engine_mode: GroupEngineMode,
    pub scan_partition_queue_cap_bytes: Option<u64>,
    pub partition_sort_chunk_bytes: u64,
    pub scan_chunk_batch_size_override: Option<usize>,
    pub hot_partition_threshold_records: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct QueryExecutionConfig {
    pub arena_base_bytes: u64,
    pub arena_grow_bytes: u64,
    pub arena_max_bytes: u64,
    pub global_cap_bytes: u64,
    pub retained_global_cap_bytes: u64,
    pub arena_release_policy: ArenaReleasePolicy,
    pub arena_keep_up_to_bytes: u64,
    pub retained_idle_decay_queries: u32,
    pub string_window_bytes: u64,
    pub group_partitions: usize,
    pub merge_workers: usize,
    pub reduce_workers: usize,
    pub partition_count: usize,
    pub cache_counter_mode: CacheCounterMode,
    pub merge_keys_mode: MergeKeysMode,
    pub group_engine_mode: GroupEngineMode,
    pub scan_partition_queue_cap_bytes: Option<u64>,
    pub partition_sort_chunk_bytes: u64,
    pub scan_chunk_batch_size: usize,
    pub hot_partition_threshold_records: usize,
    pub memory_basis: MemoryBasis,
}

impl NativeRuntimeConfig {
    pub fn from_env() -> Self {
        let arena_base_mb = parse_u64_env("WCOL_THREAD_ARENA_BASE_MB").unwrap_or(100);
        let arena_grow_mb = parse_u64_env("WCOL_THREAD_ARENA_GROW_MB").unwrap_or(50);
        let arena_max_mb = parse_u64_env("WCOL_THREAD_ARENA_MAX_MB").unwrap_or(1024);
        let keep_up_to_mb = parse_u64_env("WCOL_THREAD_ARENA_KEEP_UP_TO_MB").unwrap_or(256);
        let retained_idle_decay_queries =
            parse_u32_env("WCOL_QUERY_RETAINED_IDLE_DECAY_QUERIES").unwrap_or(3);
        let string_window_mb = parse_u64_env("WCOL_STRING_WINDOW_MB").unwrap_or(4);
        let hot_partition_threshold_records =
            parse_usize_env("WCOL_HOT_PARTITION_THRESHOLD_RECORDS").unwrap_or(1_000_000);
        let arena_release_policy = match std::env::var("WCOL_THREAD_ARENA_RELEASE")
            .unwrap_or_default()
            .as_str()
        {
            "release-all" => ArenaReleasePolicy::ReleaseAll,
            "keep-all" => ArenaReleasePolicy::KeepAll,
            _ => ArenaReleasePolicy::KeepUpToMb,
        };
        let cache_counter_mode = match std::env::var("WCOL_CACHE_COUNTERS")
            .unwrap_or_else(|_| "on".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "off" => CacheCounterMode::Off,
            "strict" => CacheCounterMode::Strict,
            _ => CacheCounterMode::On,
        };
        let merge_keys_mode = match std::env::var("WCOL_MERGE_KEYS")
            .unwrap_or_else(|_| "bytes".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "hash" => MergeKeysMode::Hash,
            _ => MergeKeysMode::Bytes,
        };
        let group_engine_mode = match std::env::var("WCOL_GROUP_ENGINE")
            .unwrap_or_else(|_| "partition-sort-v2".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "legacy" => GroupEngineMode::Legacy,
            "partition-sort" => GroupEngineMode::PartitionSort,
            "partition-direct" => GroupEngineMode::PartitionDirect,
            _ => GroupEngineMode::PartitionSortV2,
        };
        let partition_sort_chunk_mb = parse_u64_env("WCOL_PARTITION_SORT_CHUNK_MB").unwrap_or(8);
        Self {
            arena_base_bytes: arena_base_mb.saturating_mul(1024 * 1024),
            arena_grow_bytes: arena_grow_mb.saturating_mul(1024 * 1024),
            arena_max_bytes: arena_max_mb.saturating_mul(1024 * 1024),
            global_cap_override_bytes: parse_mem_mb_or_auto("WCOL_QUERY_GLOBAL_CAP_MB"),
            retained_global_cap_override_bytes: parse_mem_mb_or_auto(
                "WCOL_QUERY_RETAINED_GLOBAL_CAP_MB",
            ),
            arena_release_policy,
            arena_keep_up_to_bytes: keep_up_to_mb.saturating_mul(1024 * 1024),
            retained_idle_decay_queries,
            string_window_bytes: string_window_mb.saturating_mul(1024 * 1024),
            group_partitions_override: parse_usize_env("WCOL_GROUP_PARTITIONS"),
            merge_workers_override: parse_usize_env("WCOL_MERGE_WORKERS"),
            reduce_workers_override: parse_usize_env("WCOL_REDUCE_WORKERS"),
            partition_count_override: parse_usize_env("WCOL_PARTITION_COUNT"),
            cache_counter_mode,
            merge_keys_mode,
            group_engine_mode,
            scan_partition_queue_cap_bytes: parse_mem_mb_or_auto("WCOL_SCAN_PARTITION_QUEUE_MB"),
            partition_sort_chunk_bytes: partition_sort_chunk_mb.saturating_mul(1024 * 1024),
            scan_chunk_batch_size_override: parse_usize_env("WCOL_SCAN_CHUNK_BATCH"),
            hot_partition_threshold_records,
        }
    }

    pub fn for_workers(&self, workers: usize, memory_basis: MemoryBasis) -> QueryExecutionConfig {
        let worker_count = workers.max(1);
        let global_cap_bytes = self.global_cap_override_bytes.unwrap_or_else(|| {
            memory_basis
                .total_bytes
                .saturating_mul(60)
                .saturating_div(100)
        });
        let retained_global_cap_bytes =
            self.retained_global_cap_override_bytes.unwrap_or_else(|| {
                global_cap_bytes
                    .saturating_div(4)
                    .min(8 * 1024 * 1024 * 1024)
            });
        let auto_partitions = {
            let target = worker_count.saturating_mul(8).max(64);
            let pow2 = target.next_power_of_two();
            pow2.min(2048)
        };
        let group_partitions = self
            .partition_count_override
            .or(self.group_partitions_override)
            .map(|v| v.max(1))
            .unwrap_or(auto_partitions);
        let merge_workers = self.merge_workers_override.unwrap_or(worker_count).max(1);
        let reduce_workers = self
            .reduce_workers_override
            .or(self.merge_workers_override)
            .unwrap_or(worker_count)
            .max(1);
        QueryExecutionConfig {
            arena_base_bytes: self.arena_base_bytes.max(1),
            arena_grow_bytes: self.arena_grow_bytes.max(1),
            arena_max_bytes: self.arena_max_bytes.max(self.arena_base_bytes.max(1)),
            global_cap_bytes: global_cap_bytes.max(1),
            retained_global_cap_bytes,
            arena_release_policy: self.arena_release_policy,
            arena_keep_up_to_bytes: self.arena_keep_up_to_bytes,
            retained_idle_decay_queries: self.retained_idle_decay_queries.max(1),
            string_window_bytes: self.string_window_bytes.max(1),
            group_partitions,
            partition_count: group_partitions,
            merge_workers,
            reduce_workers,
            cache_counter_mode: self.cache_counter_mode,
            merge_keys_mode: self.merge_keys_mode,
            group_engine_mode: self.group_engine_mode,
            scan_partition_queue_cap_bytes: self.scan_partition_queue_cap_bytes,
            partition_sort_chunk_bytes: self.partition_sort_chunk_bytes.max(1024),
            scan_chunk_batch_size: self
                .scan_chunk_batch_size_override
                .unwrap_or(match self.group_engine_mode {
                    GroupEngineMode::PartitionSort
                    | GroupEngineMode::PartitionSortV2
                    | GroupEngineMode::PartitionDirect => 8,
                    GroupEngineMode::Legacy => 1,
                })
                .max(1),
            hot_partition_threshold_records: self.hot_partition_threshold_records.max(1),
            memory_basis,
        }
    }
}

fn parse_u64_env(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse::<u64>().ok()
}

fn parse_u32_env(name: &str) -> Option<u32> {
    std::env::var(name).ok()?.parse::<u32>().ok()
}

fn parse_usize_env(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse::<usize>().ok()
}

fn parse_mem_mb_or_auto(name: &str) -> Option<u64> {
    let raw = std::env::var(name).ok()?;
    if raw.eq_ignore_ascii_case("auto") {
        return None;
    }
    raw.parse::<u64>()
        .ok()
        .map(|mb| mb.saturating_mul(1024 * 1024))
}
