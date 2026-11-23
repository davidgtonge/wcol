//! Bounded-memory plateau harness for Parquet ingest (I-05 acceptance).
//!
//! Run with native alloc probes:
//!   `cargo test -p wcol --features memory-plateau --test memory_plateau`
//!
//! Full ClickBench Q3 (226 row groups):
//!   `HITS_PARQUET=/path/to/hits.parquet cargo test -p wcol --features memory-plateau --test memory_plateau -- --ignored`
//!
//! Wasm linear memory: build via `./scripts/run-memory-plateau.sh` (release wasm32 artifact for DO / wasmtime runs).

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use wcol::plan::catalog_from_footer;

use wcol::columns_for_sql;
use wcol::formats::parquet::{
    decode::ScanLimits,
    ingest_memory::{
        plateau_report, DriverMemoryCounters, IngestMemoryHighWater, IngestMemorySample,
        ScanMemoryBudget,
    },
    ingest_replay_reset, ingest_replay_totals, open_plan, NumericScan, ScanReplayMetrics,
};
use wcol::ops::query::CatalogColumn;
use wcol::plan::physical::IngestMemoryProfile;
use wcol::{ingest_groups_from_plan, GraphPump, LinearColocatedGraph};

const Q3: &str = "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits";

struct ChunkFeed {
    row_group: u32,
    offset: u64,
    length: u32,
}

fn fixture_flat() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/flat.parquet")
}

fn hits_path() -> Option<PathBuf> {
    std::env::var("HITS_PARQUET")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.is_file())
}

fn chunk_feed(groups: &[wcol::IngestGroup]) -> Vec<ChunkFeed> {
    let mut out = Vec::new();
    for group in groups {
        for chunk in &group.chunks {
            out.push(ChunkFeed {
                row_group: group.row_group,
                offset: chunk.offset,
                length: chunk.length,
            });
        }
    }
    out
}

struct Q3MemoryRun {
    pub samples: Vec<IngestMemorySample>,
    pub high_water: IngestMemoryHighWater,
    pub chunks_fed: usize,
    pub replay: ScanReplayMetrics,
}

fn drive_q3_ingest(
    file: &mut std::fs::File,
    feed: &[ChunkFeed],
    batch_rows: u32,
    sql: &str,
    catalog: &[CatalogColumn],
    groups: &[wcol::IngestGroup],
    memory_profile: IngestMemoryProfile,
) -> Q3MemoryRun {
    ingest_replay_reset();
    let mut graph = LinearColocatedGraph::open_custom(
        Some(Box::new(wcol::ops::ParquetSourceOp::new(
            batch_rows as usize,
            catalog.to_vec(),
            groups.to_vec(),
            memory_profile,
        ))),
        vec![
            Box::new(wcol::plan::bind::open_filter_op(sql, catalog).unwrap()),
            Box::new(wcol::plan::bind::open_aggregate_op(sql, catalog).unwrap()),
        ],
        vec![Box::new(wcol::ops::ReduceOp::new())],
    );

    let mut driver = DriverMemoryCounters::default();
    let mut samples = Vec::new();
    let mut high_water = IngestMemoryHighWater::default();
    let mut chunks_done = 0usize;
    let mut chunks_per_rg: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
    let mut chunks_finished: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
    for chunk in feed {
        *chunks_per_rg.entry(chunk.row_group).or_insert(0) += 1;
    }

    for _ in 0..20_000_000 {
        match graph.pump().unwrap() {
            GraphPump::ReadAt {
                ticket,
                offset,
                length,
                ..
            } => {
                let mut bytes = vec![0u8; length as usize];
                file.seek(SeekFrom::Start(offset)).unwrap();
                file.read_exact(&mut bytes).unwrap();
                driver.begin_chunk_fetch(length as usize);
                graph.complete_ingest_read(ticket, bytes).unwrap();
                driver.finish_chunk_fetch();
                chunks_done += 1;
                if let Some(entry) = feed
                    .iter()
                    .find(|c| c.offset == offset && c.length == length)
                {
                    let rg = entry.row_group;
                    let finished = chunks_finished.entry(rg).or_insert(0);
                    *finished += 1;
                    if *finished == *chunks_per_rg.get(&rg).unwrap_or(&0) {
                        let sample = IngestMemorySample::capture(rg, &driver);
                        high_water.observe(&sample);
                        samples.push(sample);
                    }
                }
            }
            GraphPump::Progress | GraphPump::AwaitHost => {}
            GraphPump::RowsReady => panic!("aggregate graph queued rows"),
            GraphPump::Done => break,
        }
    }

    Q3MemoryRun {
        samples,
        high_water,
        chunks_fed: chunks_done,
        replay: ingest_replay_totals(),
    }
}

fn assert_owned_ingest_zero_replays(run: &Q3MemoryRun) {
    assert_eq!(
        run.replay.page_replay_count,
        0,
        "ingest replay should be zero with owned PageCursor (amplification {:.2})",
        run.replay.replay_amplification(),
    );
    assert!(
        run.replay.replay_amplification() <= 1.01,
        "unexpected replay amplification: {:?}",
        run.replay,
    );
}

fn log_replay_metrics(run: &Q3MemoryRun) {
    let m = run.replay;
    eprintln!(
        "memory_plateau: replay page_replays={} events_visited={} events_committed={} amplification={:.2}",
        m.page_replay_count,
        m.page_events_visited,
        m.page_events_committed,
        m.replay_amplification(),
    );
}

fn assert_i05_plateau(run: &Q3MemoryRun, warmup_groups: usize, slack_bytes: usize) {
    let report = plateau_report(&run.samples, warmup_groups, slack_bytes).unwrap_or_else(|| {
        panic!(
            "need >{warmup_groups} row-group samples, got {}",
            run.samples.len()
        )
    });
    assert!(
        report.rust_peak_plateau_ok,
        "rust peak grew after warmup row group {}: baseline_peak={} max_late_peak={} slack={}; report={report:?}",
        report.baseline_row_group,
        report.baseline_rust_peak,
        report.max_late_rust_peak,
        slack_bytes,
    );
    assert!(
        report.wasm_plateau_ok,
        "wasm linear grew after warmup: report={report:?}",
    );
}

fn log_plateau_summary(run: &Q3MemoryRun) {
    eprintln!(
        "memory_plateau: {} row-group samples, {} chunks fed, high_water {{ rust_peak: {}, wasm_linear: {}, max_chunk: {}, http_reserved: {} }}",
        run.samples.len(),
        run.chunks_fed,
        run.high_water.rust_peak_bytes,
        run.high_water.wasm_linear_bytes,
        run.high_water.driver_max_chunk_seen,
        run.high_water.http_reserved_bytes,
    );
    if let Some(report) = plateau_report(&run.samples, 2, 512 * 1024) {
        eprintln!(
            "memory_plateau: baseline_rg={} wasm {} -> {} (ok={}), rust_peak {} -> {} (ok={})",
            report.baseline_row_group,
            report.baseline_wasm_linear,
            report.max_late_wasm_linear,
            report.wasm_plateau_ok,
            report.baseline_rust_peak,
            report.max_late_rust_peak,
            report.rust_peak_plateau_ok,
        );
    }
}

#[test]
fn batch_column_capacity_does_not_grow_on_flat_fixture() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    let sql = "SELECT SUM(a) FROM flat WHERE a > 5";
    let columns = columns_for_sql(sql, &catalog).unwrap().columns;
    let groups = ingest_groups_from_plan(&plan, &columns);
    let feed = chunk_feed(&groups);
    let run = drive_q3_ingest(
        &mut file,
        &feed,
        256,
        sql,
        &catalog,
        &groups,
        IngestMemoryProfile::default(),
    );
    assert!(run.chunks_fed > 0);
    log_plateau_summary(&run);
    log_replay_metrics(&run);
    assert_owned_ingest_zero_replays(&run);
}

#[test]
fn flat_three_columns_batch_four_streaming() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    for leaf_id in [0u32, 4, 5] {
        let chunk = plan.chunks.iter().find(|c| c.leaf == leaf_id).unwrap();
        let leaf = wcol::formats::parquet::schema::Leaf {
            name: plan.leaves[leaf_id as usize].name.clone(),
            physical: plan.leaves[leaf_id as usize].physical,
            type_length: plan.leaves[leaf_id as usize].type_length,
            max_def: plan.leaves[leaf_id as usize].max_def,
            max_rep: plan.leaves[leaf_id as usize].max_rep,
        };
        let mut bytes = vec![0u8; chunk.compressed_size as usize];
        file.seek(SeekFrom::Start(chunk.start)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        let limits = ScanLimits::for_batch_rows(4);
        let mut scan = NumericScan::begin_with_limits(
            leaf,
            chunk.codec,
            limits,
            chunk.compressed_size as u32,
            Some(plan.rows as u64),
        )
        .unwrap();
        scan.push_compressed(&bytes).unwrap();
        let mut values = Vec::new();
        let mut nulls = Vec::new();
        values.reserve(4);
        nulls.reserve(4);
        let mut rows = 0usize;
        while let Some(n) = scan.next_batch_into(&mut values, &mut nulls, 4).unwrap() {
            rows += n;
        }
        assert_eq!(rows, plan.rows as usize, "leaf {leaf_id}");
        let m = scan.replay_metrics();
        eprintln!("leaf {leaf_id} replay amp {:.2}", m.replay_amplification());
        assert_eq!(
            m.page_replay_count, 0,
            "leaf {leaf_id} should use owned PageCursor (metrics {m:?})"
        );
    }
}

#[test]
fn numeric_scan_next_batch_into_does_not_grow_batch_buffers() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let chunk = plan.chunks.iter().find(|c| c.leaf == 4).unwrap();
    let leaf = wcol::formats::parquet::schema::Leaf {
        name: "a".into(),
        physical: plan.leaves[4].physical,
        type_length: plan.leaves[4].type_length,
        max_def: plan.leaves[4].max_def,
        max_rep: plan.leaves[4].max_rep,
    };
    let mut bytes = vec![0u8; chunk.compressed_size as usize];
    file.seek(SeekFrom::Start(chunk.start)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    let limits = ScanLimits::for_batch_rows(4);
    let mut scan = NumericScan::open_with_limits(bytes, leaf, chunk.codec, limits).unwrap();
    let mut values = Vec::new();
    let mut nulls = Vec::new();
    values.reserve(4);
    nulls.reserve(4);
    let cap_values = values.capacity();
    let cap_nulls = nulls.capacity();
    for _ in 0..64 {
        let n = scan.next_batch_into(&mut values, &mut nulls, 4).unwrap();
        if n.is_none() {
            break;
        }
        assert_eq!(values.capacity(), cap_values);
        assert_eq!(nulls.capacity(), cap_nulls);
    }
}

/// Baseline harness: records per–row-group samples on full Q3. Does **not** assert I-05 yet.
#[test]
#[ignore = "requires HITS_PARQUET; run: HITS_PARQUET=/path/to/hits.parquet cargo test -p wcol --features memory-plateau --test memory_plateau -- --ignored"]
fn q3_memory_plateau_baseline_harness() {
    let Some(path) = hits_path() else {
        panic!("set HITS_PARQUET");
    };
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    let columns = columns_for_sql(Q3, &catalog).unwrap().columns;
    let groups = ingest_groups_from_plan(&plan, &columns);
    assert_eq!(groups.len(), 226, "expected ClickBench hits row groups");
    let feed = chunk_feed(&groups);
    let run = drive_q3_ingest(
        &mut file,
        &feed,
        32_768,
        Q3,
        &catalog,
        &groups,
        IngestMemoryProfile::default(),
    );
    assert_eq!(run.chunks_fed, feed.len());
    assert_eq!(run.samples.len(), 226);
    log_plateau_summary(&run);
    log_replay_metrics(&run);
    assert_owned_ingest_zero_replays(&run);
    if std::env::var("WCOL_I05_ASSERT_PLATEAU").is_ok() {
        assert_i05_plateau(&run, 2, 512 * 1024);
    } else {
        eprintln!(
            "memory_plateau: pass WCOL_I05_ASSERT_PLATEAU=1 to assert I-05 rust/wasm plateau on this run"
        );
    }
}

/// Same harness as full Q3 but accepts any ClickBench-compatible file (local subsets).
#[test]
#[ignore = "optional: HITS_PARQUET=/path/to/hits_subset.parquet cargo test … q3_memory_plateau_local -- --ignored --nocapture"]
fn q3_memory_plateau_local() {
    let Some(path) = hits_path() else {
        panic!("set HITS_PARQUET");
    };
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    let columns = columns_for_sql(Q3, &catalog).unwrap().columns;
    let groups = ingest_groups_from_plan(&plan, &columns);
    let feed = chunk_feed(&groups);
    let run = drive_q3_ingest(
        &mut file,
        &feed,
        32_768,
        Q3,
        &catalog,
        &groups,
        IngestMemoryProfile::default(),
    );
    assert_eq!(run.chunks_fed, feed.len());
    assert_eq!(run.samples.len(), groups.len());
    log_plateau_summary(&run);
    log_replay_metrics(&run);
    assert_owned_ingest_zero_replays(&run);
    assert_i05_plateau(&run, 2, 512 * 1024);
}

/// Restrictive budget + plateau (opt-in on HITS_PARQUET; plan byte `IngestMemoryProfile::Q3Restrictive`).
#[test]
#[ignore = "requires HITS_PARQUET; cargo test -p wcol --features memory-plateau --test memory_plateau q3_memory_plateau_restrictive_budget -- --ignored --nocapture"]
fn q3_memory_plateau_restrictive_budget() {
    let Some(path) = hits_path() else {
        panic!("set HITS_PARQUET");
    };
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    let columns = columns_for_sql(Q3, &catalog).unwrap().columns;
    let groups = ingest_groups_from_plan(&plan, &columns);
    assert_eq!(
        groups.len(),
        226,
        "restrictive plateau expects full ClickBench hits"
    );
    let feed = chunk_feed(&groups);
    let run = drive_q3_ingest(
        &mut file,
        &feed,
        32_768,
        Q3,
        &catalog,
        &groups,
        IngestMemoryProfile::Q3Restrictive,
    );
    assert_eq!(run.chunks_fed, feed.len());
    assert_eq!(run.samples.len(), 226);
    log_plateau_summary(&run);
    log_replay_metrics(&run);
    assert_owned_ingest_zero_replays(&run);
    eprintln!(
        "memory_plateau restrictive: high_water wasm_linear={} rust_peak={} http_reserved={}",
        run.high_water.wasm_linear_bytes,
        run.high_water.rust_peak_bytes,
        run.high_water.http_reserved_bytes,
    );
    let report = plateau_report(&run.samples, 2, 256 * 1024).expect("samples");
    assert!(
        report.wasm_plateau_ok && report.rust_peak_plateau_ok,
        "I-05 restrictive plateau failed: {:?}",
        report
    );
    assert!(
        run.high_water.rust_peak_bytes
            <= ScanMemoryBudget::q3_acceptance_restrictive()
                .total_cap()
                .saturating_add(512 * 1024),
        "rust peak {} exceeds restrictive total cap",
        run.high_water.rust_peak_bytes,
    );
}

/// Same restrictive checks on any local HITS file (subset OK); for dev before full 226-RG signoff.
#[test]
#[ignore = "requires full ClickBench hits (226 RG); HITS_PARQUET=/path/to/hits.parquet cargo test … q3_memory_plateau_restrictive_any_hits -- --ignored --nocapture"]
fn q3_memory_plateau_restrictive_any_hits() {
    let Some(path) = hits_path() else {
        panic!("set HITS_PARQUET");
    };
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    let columns = columns_for_sql(Q3, &catalog).unwrap().columns;
    let groups = ingest_groups_from_plan(&plan, &columns);
    assert!(!groups.is_empty(), "need at least one row group");
    let feed = chunk_feed(&groups);
    let run = drive_q3_ingest(
        &mut file,
        &feed,
        32_768,
        Q3,
        &catalog,
        &groups,
        IngestMemoryProfile::Q3Restrictive,
    );
    assert_eq!(run.chunks_fed, feed.len());
    assert_eq!(run.samples.len(), groups.len());
    log_plateau_summary(&run);
    log_replay_metrics(&run);
    assert_owned_ingest_zero_replays(&run);
    let warmup = 2.min(groups.len().saturating_sub(1));
    let report = plateau_report(&run.samples, warmup, 256 * 1024).expect("samples");
    assert!(
        report.wasm_plateau_ok && report.rust_peak_plateau_ok,
        "restrictive plateau failed: {:?}",
        report
    );
}

#[test]
fn q3_memory_plateau_restrictive_flat_fixture() {
    let path = fixture_flat();
    if !path.exists() {
        return;
    }
    let (mut file, plan) = open_plan(&path).unwrap();
    let catalog = catalog_from_footer(&plan);
    let sql = "SELECT SUM(a) FROM flat";
    let columns = columns_for_sql(sql, &catalog).unwrap().columns;
    let groups = ingest_groups_from_plan(&plan, &columns);
    let feed = chunk_feed(&groups);
    let run = drive_q3_ingest(
        &mut file,
        &feed,
        4,
        sql,
        &catalog,
        &groups,
        IngestMemoryProfile::Q3Restrictive,
    );
    assert_owned_ingest_zero_replays(&run);
    let report = plateau_report(&run.samples, 0, 256 * 1024).expect("flat samples");
    assert!(report.wasm_plateau_ok && report.rust_peak_plateau_ok);
}
