//! Test-only: pump staged Parquet bytes through decode + colocated query → result JSON.
//! Production paths use node-fragment Wasm plans instead.

use wcol::formats::parquet::ParquetDecode;
use wcol::plan::result::{partial_json, ResultColumn};
use wcol::stream::OwnedWcolBatch;
use wcol::LinearColocatedGraph;

pub fn decode_query_to_json(
    decode: &mut ParquetDecode,
    query: &mut LinearColocatedGraph,
    mut read_at: impl FnMut(u64, u32) -> Result<Vec<u8>, String>,
) -> Result<String, String> {
    if query.metadata_only() {
        let rows: u64 = (0..decode.group_count()).map(|i| decode.rows_at(i)).sum();
        query.apply_metadata_rows(rows)?;
    } else {
        let mut batch = OwnedWcolBatch::empty();
        loop {
            if decode.fill_batch(&mut batch).map_err(|e| e.to_string())? {
                query.inject_row_batch(std::mem::take(&mut batch))?;
                continue;
            }
            if decode.ingest_complete() {
                break;
            }
            let Some(read) = decode.host_read_request().map_err(|e| e.to_string())? else {
                return Err("parquet decode blocked waiting for input".to_string());
            };
            let bytes = read_at(read.offset, read.length)?;
            decode
                .complete_read(read.ticket, &bytes)
                .map_err(|e| e.to_string())?;
        }
    }
    let partial = query.finish_wire_partial_bytes()?;
    // One ungrouped aggregate per column, as the result edge prints it.
    let snap = wcol::stream::partials::partial_snapshot_from_wire(&partial)?;
    let columns: Vec<ResultColumn> = (0..snap.aggs.len() as u32)
        .map(|column| ResultColumn {
            name: format!("agg{column}"),
            column,
        })
        .collect();
    partial_json(&columns, &partial)
}
