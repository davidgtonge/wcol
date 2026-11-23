//! Build a page-copy compaction plan from an existing row-export manifest + local Parquet files.

use std::path::{Path, PathBuf};

use base64::Engine;
use serde::Deserialize;
use wcol::formats::parquet::open_plan;
use wcol::plan::encode_page_compact_wire;
use wcol::{plan_page_compaction, CompactionInputFile, PageCompactionPolicy, SourceVersion};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportPart {
    object_key: String,
    bytes: u64,
}

#[derive(Deserialize)]
struct ExportManifest {
    parts: Vec<ExportPart>,
}

#[derive(Deserialize)]
struct ExportManifestFile {
    parts: Option<Vec<ExportPart>>,
    manifest: Option<ExportManifest>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PlanTaskOut {
    part_index: usize,
    object_key: String,
    wire_program: String,
    rows: u64,
    output_row_groups: u32,
    encoded_bytes: u64,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PlanOut {
    job_key: String,
    source_manifest_key: String,
    output_prefix: String,
    manifest_key: String,
    input_keys: Vec<String>,
    input_bytes: u64,
    input_row_groups: u32,
    output_parts: usize,
    tasks: Vec<PlanTaskOut>,
}

fn usage() -> ! {
    eprintln!(
        "usage: page_compact_plan --export-manifest PATH --file-dir DIR \\
  --source-manifest-key KEY --output-prefix PREFIX --job-key KEY \\
  [--max-inputs N] [--output-parts N] [--target-output-mib N] [--target-row-group-mib N]"
    );
    std::process::exit(2);
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn source_version(keys: &[String]) -> SourceVersion {
    let mut joined = String::new();
    for key in keys {
        joined.push_str(key);
        joined.push('\0');
    }
    SourceVersion {
        object_key_hash: xxh3_64(joined.as_bytes()),
    }
}

fn row_group_count(plan: &wcol::formats::parquet::schema::Plan) -> u32 {
    plan.chunks
        .iter()
        .map(|c| c.row_group + 1)
        .max()
        .unwrap_or(0)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let export_manifest = arg_value(&args, "--export-manifest").unwrap_or_else(|| usage());
    let file_dir = arg_value(&args, "--file-dir").unwrap_or_else(|| usage());
    let source_manifest_key = arg_value(&args, "--source-manifest-key").unwrap_or_else(|| usage());
    let output_prefix = arg_value(&args, "--output-prefix").unwrap_or_else(|| usage());
    let job_key = arg_value(&args, "--job-key").unwrap_or_else(|| usage());
    let max_inputs: usize = arg_value(&args, "--max-inputs")
        .and_then(|v| v.parse().ok())
        .unwrap_or(usize::MAX);
    let output_parts: usize = arg_value(&args, "--output-parts")
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let target_output_mib: u64 = arg_value(&args, "--target-output-mib")
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);
    let target_row_group_mib: u64 = arg_value(&args, "--target-row-group-mib")
        .and_then(|v| v.parse().ok())
        .unwrap_or(128);

    let manifest_text =
        std::fs::read_to_string(&export_manifest).unwrap_or_else(|e| panic!("read manifest: {e}"));
    let file: ExportManifestFile =
        serde_json::from_str(&manifest_text).unwrap_or_else(|e| panic!("parse manifest: {e}"));
    let export = if let Some(parts) = file.parts {
        ExportManifest { parts }
    } else if let Some(m) = file.manifest {
        m
    } else {
        eprintln!("manifest JSON missing parts");
        std::process::exit(1);
    };
    let parts: Vec<&ExportPart> = export.parts.iter().take(max_inputs).collect();
    if parts.is_empty() {
        eprintln!("no input parts");
        std::process::exit(1);
    }

    let dir = PathBuf::from(&file_dir);
    let mut input_keys = Vec::new();
    let mut input_bytes = 0u64;
    let mut input_row_groups = 0u32;
    let mut compaction_inputs = Vec::new();

    for part in &parts {
        let file_name = Path::new(&part.object_key)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(&part.object_key);
        let path = dir.join(file_name);
        let (_file, plan) = open_plan(&path).unwrap_or_else(|e| {
            panic!("open {} ({:?}): {e}", part.object_key, path);
        });
        input_row_groups += row_group_count(&plan);
        input_bytes = input_bytes.saturating_add(part.bytes);
        input_keys.push(part.object_key.clone());
        compaction_inputs.push(CompactionInputFile {
            object_key: part.object_key.clone(),
            file_size: part.bytes,
            plan,
        });
    }

    let source = source_version(&input_keys);
    let policy = PageCompactionPolicy {
        target_output_bytes: target_output_mib * 1024 * 1024,
        target_row_group_bytes: target_row_group_mib * 1024 * 1024,
        output_parts,
    };
    let plan = plan_page_compaction(source, &job_key, compaction_inputs, policy)
        .unwrap_or_else(|e| panic!("plan: {e}"));
    let leaves = &plan.inputs[0].plan.leaves;

    let mut tasks = Vec::new();
    let manifest_key = format!("{output_prefix}-manifest.json");
    for part in &plan.parts {
        let wire = encode_page_compact_wire(part, leaves).unwrap_or_else(|e| panic!("wire: {e}"));
        let object_key = format!("{output_prefix}-part-{:02}.parquet", part.part_index + 1);
        tasks.push(PlanTaskOut {
            part_index: part.part_index,
            object_key,
            wire_program: base64::engine::general_purpose::STANDARD.encode(&wire),
            rows: part.rows,
            output_row_groups: part.merged_row_groups.len() as u32,
            encoded_bytes: part.encoded_bytes,
        });
    }

    let out = PlanOut {
        job_key,
        source_manifest_key,
        output_prefix,
        manifest_key,
        input_keys: input_keys.clone(),
        input_bytes,
        input_row_groups,
        output_parts: tasks.len(),
        tasks,
    };
    println!("{}", serde_json::to_string(&out).unwrap());
}
