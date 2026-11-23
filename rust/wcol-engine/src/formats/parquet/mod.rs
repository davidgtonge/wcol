//! Parquet → WCOL batch decode (compact-parquet-thrift). No query logic.

pub mod batch_decode;
pub mod decode;
pub mod ingest_memory;
pub mod schema;
pub mod stream_wcs;

pub(crate) mod alloc_track;
mod dictionary;
mod encode;
mod numeric_scan;
pub mod page_compact;
mod page_cursor;
mod page_scratch;
mod replay_metrics;

pub use batch_decode::{ParquetDecode, HOST_READ_LIMIT};
pub use decode::{DecodeProgress, ScanLimits};
pub use dictionary::chunk_has_dictionary_page;
pub use encode::{ParquetEncode, ParquetEncodeConfig, ParquetSinkColumn};
pub use ingest_memory::{
    plateau_report, DriverMemoryCounters, IngestMemoryHighWater, IngestMemorySample, PlateauReport,
    ScanMemoryBudget,
};
pub use numeric_scan::{DictIdBatch, NumericBatch, NumericScan, ScanValueMode};
pub use page_cursor::{inflate_page_body, Flow, PageCursor, PageStep};
pub use page_scratch::PageScratch;
pub use replay_metrics::{
    ingest_replay_merge, ingest_replay_reset, ingest_replay_totals, ScanReplayMetrics,
};
pub use schema::{
    encode_plan, footer_span, leaf_byte_bounds, leaf_dictionary_bytes, leaf_distinct_upper,
    leaf_int_bounds, plan_footer, Chunk, Leaf, Plan,
};

#[derive(Debug)]
pub enum Error {
    Message(&'static str),
    Detail(String),
    BadFooter,
    Io(std::io::Error),
    Thrift(compact_parquet_thrift::Error),
    Page(compact_parquet_thrift::pages::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Message(message) => f.write_str(message),
            Error::Detail(message) => f.write_str(message),
            Error::BadFooter => f.write_str("invalid parquet footer"),
            Error::Io(err) => write!(f, "{err}"),
            Error::Thrift(err) => write!(f, "thrift {err:?}"),
            Error::Page(err) => write!(f, "page {err:?}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<compact_parquet_thrift::Error> for Error {
    fn from(err: compact_parquet_thrift::Error) -> Self {
        Error::Thrift(err)
    }
}

impl From<compact_parquet_thrift::pages::Error> for Error {
    fn from(err: compact_parquet_thrift::pages::Error) -> Self {
        Error::Page(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<crate::error::WcolError> for Error {
    fn from(err: crate::error::WcolError) -> Self {
        match err {
            crate::error::WcolError::Static(s) => Error::Message(s),
            crate::error::WcolError::Dynamic(s) => Error::Detail(s),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
use std::fs::File;
#[cfg(not(target_arch = "wasm32"))]
use std::io::{Read, Seek, SeekFrom};

#[cfg(not(target_arch = "wasm32"))]
pub fn read_footer(file: &mut File, file_size: u64) -> Result<Vec<u8>, Error> {
    if file_size < 12 {
        return Err(Error::BadFooter);
    }
    let mut tail = [0u8; 8];
    file.seek(SeekFrom::End(-8))?;
    file.read_exact(&mut tail)?;
    let (start, len) = footer_span(file_size, &tail)?;
    let len =
        usize::try_from(len).map_err(|_| Error::Message("footer larger than address space"))?;
    let mut footer = vec![0u8; len];
    file.seek(SeekFrom::Start(start))?;
    file.read_exact(&mut footer)?;
    Ok(footer)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn open_plan(path: &std::path::Path) -> Result<(File, Plan), Error> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let footer = read_footer(&mut file, size)?;
    let plan = plan_footer(&footer)?;
    Ok((file, plan))
}

/// Column total from [`NumericScan`]. Integer and boolean columns only.
#[derive(Clone, Debug, PartialEq)]
pub enum Sum {
    Int(i128),
    Float(f64),
}

/// Min or max from [`NumericScan`].
#[derive(Clone, Debug, PartialEq)]
pub enum Extreme {
    Int(i128),
    Float(f64),
}

/// One-column aggregate. `bytes` stays false: string lengths are not scanned here.
#[derive(Clone, Debug)]
pub struct Agg {
    pub count: u64,
    pub nulls: u64,
    pub sum: Sum,
    pub min: Extreme,
    pub max: Extreme,
    pub bytes: bool,
}

#[cfg(not(target_arch = "wasm32"))]
pub fn aggregate_column(path: &std::path::Path, name: &str) -> Result<Agg, Error> {
    let (mut file, plan) = open_plan(path)?;
    let leaf_index = plan
        .leaves
        .iter()
        .position(|leaf| leaf.name == name)
        .ok_or(Error::Message("column not found"))?;
    let leaf = plan.leaves[leaf_index].clone();
    if !matches!(leaf.physical, 0 | 1 | 2) {
        return Err(Error::Message(
            "aggregate_column scans boolean, int32, and int64 columns",
        ));
    }
    let mut count = 0u64;
    let mut nulls = 0u64;
    let mut sum = 0i128;
    let mut min = 0i128;
    let mut max = 0i128;
    let mut saw = false;
    let mut any = false;
    for chunk in plan
        .chunks
        .iter()
        .filter(|chunk| chunk.leaf as usize == leaf_index)
    {
        any = true;
        let len = usize::try_from(chunk.compressed_size)
            .map_err(|_| Error::Message("chunk larger than address space"))?;
        let mut bytes = vec![0u8; len];
        file.seek(SeekFrom::Start(chunk.start))?;
        file.read_exact(&mut bytes)?;
        let limits = ScanLimits::for_batch_rows(65_536);
        let mut scan = NumericScan::begin_with_limits(
            leaf.clone(),
            chunk.codec,
            limits,
            chunk.compressed_size as u32,
            Some(chunk.rows as u64),
        )?;
        scan.push_compressed(&bytes)
            .map_err(|err| Error::Detail(format!("{name}: {err}")))?;
        let mut values = Vec::with_capacity(65_536);
        let mut present = Vec::with_capacity(65_536);
        while let Some(_rows) = scan
            .next_batch_into(&mut values, &mut present, 65_536)
            .map_err(|err| Error::Detail(format!("{name}: {err}")))?
        {
            for (value, flag) in values.iter().zip(present.iter()) {
                if *flag == 0 {
                    nulls += 1;
                    continue;
                }
                let number = i128::from(*value);
                if saw {
                    min = min.min(number);
                    max = max.max(number);
                } else {
                    min = number;
                    max = number;
                    saw = true;
                }
                sum = sum
                    .checked_add(number)
                    .ok_or(Error::Message("integer sum overflow"))?;
                count += 1;
            }
        }
    }
    if !any {
        return Err(Error::Message("column has no chunks"));
    }
    Ok(Agg {
        count,
        nulls,
        sum: Sum::Int(sum),
        min: Extreme::Int(min),
        max: Extreme::Int(max),
        bytes: false,
    })
}
