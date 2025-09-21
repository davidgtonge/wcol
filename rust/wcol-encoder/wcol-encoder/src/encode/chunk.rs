use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::mpsc::sync_channel;

use anyhow::{Context, Result};
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::io::parquet::read::FileReader;
use rayon::prelude::*;

use crate::constants::ROWS_PER_CHUNK;
use crate::types::{ChunkPages, ColumnBuffer, ColumnSpec};

use super::append::append_array_slice;
use super::page::finalize_chunk;

#[allow(dead_code)]
pub(crate) fn encode_chunks(
    input: &Path,
    metadata: &parquet2::metadata::FileMetaData,
    schema: &arrow2::datatypes::Schema,
    columns: &[ColumnSpec],
) -> Result<Vec<ChunkPages>> {
    eprintln!("Rayon threads: {}", rayon::current_num_threads());
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let row_groups = metadata.row_groups.clone();
    let mut reader = FileReader::new(file, row_groups, schema.clone(), None, None, None);

    let mut buffers: Vec<ColumnBuffer> = columns.iter().map(ColumnBuffer::new).collect();
    let mut chunks = Vec::new();
    let mut rows_in_chunk = 0usize;

    for maybe_chunk in &mut reader {
        let chunk = maybe_chunk.context("read parquet row group")?;
        let rg_rows = chunk.len();
        let mut offset = 0usize;

        while offset < rg_rows {
            let remaining = ROWS_PER_CHUNK - rows_in_chunk;
            let take = remaining.min(rg_rows - offset);
            let arrays: Vec<_> = chunk.arrays().iter().map(|a| a.as_ref()).collect();
            let append_results: Vec<Result<()>> = columns
                .par_iter()
                .zip(buffers.par_iter_mut())
                .zip(arrays.par_iter())
                .map(|((col, buffer), array)| append_array_slice(col, buffer, *array, offset, take))
                .collect();
            for r in append_results {
                r?;
            }
            rows_in_chunk += take;
            offset += take;

            if rows_in_chunk == ROWS_PER_CHUNK {
                chunks.push(finalize_chunk(rows_in_chunk, columns, &mut buffers)?);
                rows_in_chunk = 0;
            }
        }
    }

    if rows_in_chunk > 0 {
        chunks.push(finalize_chunk(rows_in_chunk, columns, &mut buffers)?);
    }

    Ok(chunks)
}

fn append_chunk(
    chunk: &Chunk<Box<dyn Array>>,
    columns: &[ColumnSpec],
    buffers: &mut [ColumnBuffer],
    out: &mut Vec<ChunkPages>,
    rows_in_chunk: &mut usize,
) -> Result<()> {
    let rg_rows = chunk.len();
    let mut offset = 0usize;

    while offset < rg_rows {
        let remaining = ROWS_PER_CHUNK - *rows_in_chunk;
        let take = remaining.min(rg_rows - offset);
        let arrays: Vec<_> = chunk.arrays().iter().map(|a| a.as_ref()).collect();
        let append_results: Vec<Result<()>> = columns
            .par_iter()
            .zip(buffers.par_iter_mut())
            .zip(arrays.par_iter())
            .map(|((col, buffer), array)| append_array_slice(col, buffer, *array, offset, take))
            .collect();
        for r in append_results {
            r?;
        }
        *rows_in_chunk += take;
        offset += take;

        if *rows_in_chunk == ROWS_PER_CHUNK {
            out.push(finalize_chunk(*rows_in_chunk, columns, buffers)?);
            *rows_in_chunk = 0;
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn encode_chunks_from_chunks(
    chunks: &[Chunk<Box<dyn Array>>],
    columns: &[ColumnSpec],
) -> Result<Vec<ChunkPages>> {
    eprintln!("Rayon threads: {}", rayon::current_num_threads());
    let mut buffers: Vec<ColumnBuffer> = columns.iter().map(ColumnBuffer::new).collect();
    let mut out = Vec::new();
    let mut rows_in_chunk = 0usize;

    for chunk in chunks {
        append_chunk(chunk, columns, &mut buffers, &mut out, &mut rows_in_chunk)?;
    }

    if rows_in_chunk > 0 {
        out.push(finalize_chunk(rows_in_chunk, columns, &mut buffers)?);
    }

    Ok(out)
}

pub(crate) fn encode_chunks_streamed(
    input: &Path,
    metadata: &parquet2::metadata::FileMetaData,
    schema: &arrow2::datatypes::Schema,
    columns: &[ColumnSpec],
) -> Result<Vec<ChunkPages>> {
    encode_chunks_streamed_row_groups(input, metadata.row_groups.clone(), schema, columns)
}

pub(crate) fn encode_chunks_streamed_row_groups(
    input: &Path,
    row_groups: Vec<parquet2::metadata::RowGroupMetaData>,
    schema: &arrow2::datatypes::Schema,
    columns: &[ColumnSpec],
) -> Result<Vec<ChunkPages>> {
    eprintln!("Rayon threads: {}", rayon::current_num_threads());
    let total = row_groups.len();
    let max_inflight = (rayon::current_num_threads().max(1)) * 2;
    let (tx, rx) = sync_channel::<Result<(usize, Chunk<Box<dyn Array>>)>>(max_inflight);

    let out_result = rayon::scope(move |s| {
        for (idx, row_group) in row_groups.into_iter().enumerate() {
            let tx = tx.clone();
            let schema = schema.clone();
            let input = input.to_path_buf();
            s.spawn(move |_| {
                let result = (|| {
                    let file =
                        File::open(&input).with_context(|| format!("open {}", input.display()))?;
                    let mut reader =
                        FileReader::new(file, vec![row_group], schema, None, None, None);
                    let chunk = reader
                        .next()
                        .transpose()
                        .context("read parquet row group")?
                        .unwrap_or_else(|| Chunk::new(vec![]));
                    Ok((idx, chunk))
                })();
                let _ = tx.send(result);
            });
        }
        drop(tx);

        let mut buffers: Vec<ColumnBuffer> = columns.iter().map(ColumnBuffer::new).collect();
        let mut out = Vec::new();
        let mut rows_in_chunk = 0usize;
        let mut pending: BTreeMap<usize, Chunk<Box<dyn Array>>> = BTreeMap::new();
        let mut next_idx = 0usize;
        let mut stream_err: Option<anyhow::Error> = None;

        for _ in 0..total {
            let msg = match rx.recv() {
                Ok(msg) => msg,
                Err(err) => {
                    stream_err = Some(anyhow::anyhow!("receive parquet row group: {}", err));
                    break;
                }
            };
            let (idx, chunk) = match msg {
                Ok(value) => value,
                Err(err) => {
                    stream_err = Some(err);
                    continue;
                }
            };
            pending.insert(idx, chunk);
            while let Some(chunk) = pending.remove(&next_idx) {
                if stream_err.is_some() {
                    break;
                }
                if let Err(err) =
                    append_chunk(&chunk, columns, &mut buffers, &mut out, &mut rows_in_chunk)
                {
                    stream_err = Some(err);
                    break;
                }
                next_idx += 1;
            }
        }

        if stream_err.is_none() && rows_in_chunk > 0 {
            match finalize_chunk(rows_in_chunk, columns, &mut buffers) {
                Ok(chunk) => out.push(chunk),
                Err(err) => stream_err = Some(err),
            }
        }

        match stream_err {
            Some(err) => Err(err),
            None => Ok(out),
        }
    });

    out_result
}
