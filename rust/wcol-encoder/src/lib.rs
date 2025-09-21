mod constants;
mod dict_limits;
mod encode;
mod format;
mod scan;
mod types;
mod utils;

use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use arrow2::io::parquet::read::infer_schema;
use parquet2::read::read_metadata;
#[cfg(feature = "sav")]
use sav_to_cbor::parser::streaming_nom::NomStreamingSavParser;

#[cfg(feature = "sav")]
use crate::encode::encode_sav_chunks;
use crate::encode::{encode_chunks_streamed, encode_chunks_streamed_row_groups};
use crate::format::write_wcol;
use crate::scan::{finalize_columns, init_columns, scan_columns};
#[cfg(feature = "sav")]
use crate::scan::{init_sav_columns, sav_row_count, scan_sav_columns};
use crate::utils::{print_schema, print_stats};

pub fn convert_to_wcol(
    input: &Path,
    output: &Path,
    show_schema: bool,
    show_stats: bool,
    split_row_groups: Option<usize>,
) -> Result<Vec<PathBuf>> {
    let ext = input
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase());
    match ext.as_deref() {
        Some("sav") => {
            #[cfg(feature = "sav")]
            {
                let path = convert_sav_to_wcol(input, output, show_schema, show_stats)?;
                Ok(vec![path])
            }
            #[cfg(not(feature = "sav"))]
            {
                anyhow::bail!("SAV conversion requires the wcol-encoder 'sav' feature");
            }
        }
        _ => convert_parquet_to_wcol(input, output, show_schema, show_stats, split_row_groups),
    }
}

pub fn convert_parquet_to_wcol(
    input: &Path,
    output: &Path,
    show_schema: bool,
    show_stats: bool,
    split_row_groups: Option<usize>,
) -> Result<Vec<PathBuf>> {
    let mut file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let metadata = read_metadata(&mut file).context("read parquet metadata")?;
    let schema = infer_schema(&metadata).context("infer parquet schema")?;

    eprintln!("Parquet row groups: {}", metadata.row_groups.len());
    let mut columns = init_columns(&schema)?;
    scan_columns(input, &metadata, &schema, &mut columns)?;
    finalize_columns(&mut columns)?;
    if show_schema {
        print_schema(&columns);
    }

    let mut outputs = Vec::new();
    if let Some(part_size) = split_row_groups {
        if part_size == 0 {
            bail!("--split-row-groups must be > 0");
        }
        let total_groups = metadata.row_groups.len();
        let mut part_idx = 0usize;
        let mut start = 0usize;
        while start < total_groups {
            let end = (start + part_size).min(total_groups);
            let groups = metadata.row_groups[start..end].to_vec();
            let part_rows = groups.iter().map(|rg| rg.num_rows() as u64).sum::<u64>();
            let mut chunks = encode_chunks_streamed_row_groups(input, groups, &schema, &columns)?;
            if show_stats {
                print_stats(&columns, &chunks);
            }
            let part_output = part_output_path(output, part_idx);
            write_wcol(&part_output, &columns, &mut chunks, part_rows)?;
            outputs.push(part_output);
            part_idx += 1;
            start = end;
        }
    } else {
        let total_rows = metadata.num_rows as u64;
        let mut chunks = encode_chunks_streamed(input, &metadata, &schema, &columns)?;
        if show_stats {
            print_stats(&columns, &chunks);
        }
        write_wcol(output, &columns, &mut chunks, total_rows).with_context(|| {
            "single-file conversion failed; try --split-row-groups 16 for large parquet inputs"
        })?;
        outputs.push(output.to_path_buf());
    }

    let unsafe_cols: Vec<_> = columns
        .iter()
        .filter(|col| col.unsafe_int)
        .map(|col| col.name.as_str())
        .collect();
    if !unsafe_cols.is_empty() {
        eprintln!(
            "Warning: columns stored as f64 due to large integers: {}",
            unsafe_cols.join(", ")
        );
    }

    Ok(outputs)
}

#[cfg(feature = "sav")]
pub fn convert_sav_to_wcol(
    input: &Path,
    output: &Path,
    show_schema: bool,
    show_stats: bool,
) -> Result<PathBuf> {
    let data = std::fs::read(input).with_context(|| format!("read {}", input.display()))?;
    let mut parser = NomStreamingSavParser::new();
    parser.push_chunk(&data);
    let columns = parser
        .collect_columns()
        .map_err(|e| anyhow::anyhow!(e.to_string()))
        .context("parse SAV")?;
    let total_rows = sav_row_count(&columns)?;

    let mut specs = init_sav_columns(&columns)?;
    scan_sav_columns(&columns, &mut specs)?;
    finalize_columns(&mut specs)?;
    if show_schema {
        print_schema(&specs);
    }

    let mut chunks = encode_sav_chunks(&specs, &columns, total_rows)?;
    if show_stats {
        print_stats(&specs, &chunks);
    }
    write_wcol(output, &specs, &mut chunks, total_rows as u64)?;

    Ok(output.to_path_buf())
}

fn part_output_path(output: &Path, idx: usize) -> PathBuf {
    let stem = output
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let ext = output
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("wcol");
    let file_name = format!("{stem}.part{:04}.{ext}", idx + 1);
    output.with_file_name(file_name)
}
