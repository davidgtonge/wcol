//! Node fragment wire format. The planner builds [`NodeFragment`] values; this module encodes
//! and decodes them. It does not read SQL or a physical plan.

//! Per-node execution fragment extracted from a [`WcolPhysicalPlan`].
//!
//! A fragment is everything one worker slot needs to run its part of a query: the row groups it
//! decodes, the plan's fetches and the reads they satisfy, and the operator steps in plan order.
//! It carries no SQL and no footer: column chunk codecs travel on the reads.

use std::collections::HashSet;

use crate::ir::CatalogColumn;
use crate::ir::Scalar;
use crate::program::codec::{
    decode_agg, decode_predicate, decode_scalar_list, decode_sort_key, encode_agg,
    encode_predicate, encode_scalar_list, encode_sort_key, push_str, push_u32, push_u64, push_var,
    push_var_signed, read_list, read_str, read_u32, read_u64, read_u8, read_var, read_var_signed,
    read_var_u32, LogicalWireError,
};
use crate::program::ingest::{IngestGroup, IngestGroupChunk};
use crate::program::stream_program::{encode_catalog, PlanCursor, OP_NODE_FRAGMENT, PLAN_V2};
use crate::program::types::{
    AggExpr, ClaimUnit, DecodeGroup, FetchPiece, FetchRequest, LogicalRead, NodeId, Predicate,
    SortKey,
};

const FRAGMENT_WIRE_VERSION: u8 = 7;

/// What one worker slot runs: reads, fetches, and operator steps in plan order.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeFragment {
    pub node: NodeId,
    pub decode: Vec<DecodeGroup>,
    pub logical_reads: Vec<LogicalRead>,
    pub fetches: Vec<FetchRequest>,
    pub steps: Vec<FragmentStep>,
    /// Operator id that produces this node's exchange output (partial rows or PWCL).
    pub output_op: u32,
    /// Coordinator limit and ordering that the partial header carries to the merge.
    pub tail: FragmentTail,
    /// Whole-fetch reads in flight at once on this node.
    pub fetch_depth: u32,
    /// ReadAt source (index into the job's sources) of the one object this node reads.
    pub source: u16,
    /// Column the scan fills with each row's [`row_id`](crate::program::row_id) (a late
    /// materialization pass reads rows back by it). `None`: no such column.
    pub row_id: Option<u32>,
    /// Empty: decode every group in `decode`. Otherwise every unit of work this node could
    /// run (`decode` holds their groups), and it decodes only the units the coordinator gives it.
    pub claim_units: Vec<ClaimUnit>,
    /// The ids of the units planned for this node, in order: its queue on the coordinator.
    pub own_units: Vec<u32>,
}

/// The coordinator's `Limit` and group ordering, as today's partial header records them.
///
/// `final_aggregate` is true when this fragment is the only partial: the planner emitted no
/// later merge. `rank_aggregate` is the count aggregate a final top-k orders by. A partial
/// that another node will merge leaves `rank_aggregate` empty and emits every group.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FragmentTail {
    pub skip: u64,
    pub fetch: Option<u64>,
    pub order_groups_by_count: bool,
    pub final_aggregate: bool,
    pub rank_aggregate: Option<u32>,
}

/// What a hash partition step splits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionInput {
    /// The scan's rows, routed by these source columns.
    Rows { keys: Vec<u32> },
    /// The partial states of the scan's aggregate (a combiner). Each exchange key is
    /// `(position in the partial key, source column)`: the source column is hashed with the
    /// value, so a partial row routes where its raw rows would.
    Partials { keys: Vec<(u32, u32)> },
}

#[derive(Clone, Debug, PartialEq)]
pub enum FragmentStep {
    /// Retain the graph after loading and accept explicit iteration commands.
    Resident(crate::ops::iterative::KMeansConfig),
    Filter(Vec<Predicate>),
    Project(Vec<u32>),
    PartialAggregate {
        group_by: Vec<u32>,
        aggregates: Vec<AggExpr>,
    },
    TopK {
        keys: Vec<SortKey>,
        n: u64,
    },
    /// This node writes its own Parquet part. No coordinator merges the rows.
    Parquet {
        target_row_group_rows: u32,
        /// Output ordinals. Empty is one file. Non-empty is a host demux.
        partition_columns: Vec<u32>,
    },
    /// Route each output row by `hash(keys) % partitions` and emit one WCS stream per partition.
    /// The scan still runs once. A reducer owns one partition.
    HashPartition {
        input: PartitionInput,
        partitions: u32,
        hash_version: u32,
    },
    /// Range-partition rows for distributed global sort export.
    RangePartition {
        keys: Vec<SortKey>,
        partitions: u32,
        /// `partitions - 1` interior split keys in global sort order.
        splits: Vec<Vec<Scalar>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentError {
    NoNodeWork,
    MissingRead {
        read: u32,
    },
    /// A combiner exchanges on a column its partial key does not hold.
    ExchangeKeyNotGrouped {
        column: u32,
    },
    Wire(LogicalWireError),
}

impl From<LogicalWireError> for FragmentError {
    fn from(err: LogicalWireError) -> Self {
        Self::Wire(err)
    }
}

impl std::fmt::Display for FragmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoNodeWork => write!(f, "plan has no node work"),
            Self::MissingRead { read } => write!(f, "fragment does not carry logical read {read}"),
            Self::ExchangeKeyNotGrouped { column } => {
                write!(
                    f,
                    "exchange key column {column} is not part of the partial key"
                )
            }
            Self::Wire(err) => write!(f, "node fragment wire: {err:?}"),
        }
    }
}

impl NodeFragment {
    /// [`IngestGroup`]s for [`crate::formats::parquet::ParquetDecode`], one chunk per column per
    /// row group. A dictionary page planned as its own read is folded into its column's chunk.
    pub fn ingest_groups(&self) -> Result<Vec<IngestGroup>, FragmentError> {
        let mut out = Vec::with_capacity(self.decode.len());
        for group in &self.decode {
            let mut chunks: Vec<IngestGroupChunk> = Vec::with_capacity(group.reads.len());
            for read_id in &group.reads {
                let read = self
                    .logical_reads
                    .iter()
                    .find(|read| read.id == *read_id)
                    .ok_or(FragmentError::MissingRead { read: *read_id })?;
                let length = u32::try_from(read.length).unwrap_or(u32::MAX);
                if let Some(chunk) = chunks.iter_mut().find(|chunk| chunk.column == read.column) {
                    // Dictionary page and data pages of one column chunk: one contiguous span.
                    let start = chunk.offset.min(read.offset);
                    let end =
                        (chunk.offset + u64::from(chunk.length)).max(read.offset + read.length);
                    chunk.offset = start;
                    chunk.length = u32::try_from(end - start).unwrap_or(u32::MAX);
                    chunk.dict_encoded |= read.dictionary;
                    continue;
                }
                chunks.push(IngestGroupChunk {
                    column: read.column,
                    offset: read.offset,
                    codec: read.codec,
                    length,
                    dict_encoded: read.dictionary,
                });
            }
            out.push(IngestGroup {
                row_group: group.row_group,
                rows: group.rows,
                chunks,
            });
        }
        Ok(out)
    }
}

/// A complete stream-pool program (`wcs_sp_open_graph`) that runs `fragment` on one worker ([`OP_NODE_FRAGMENT`]).
///
/// [`OP_NODE_FRAGMENT`]: crate::program::stream_program::OP_NODE_FRAGMENT
pub fn encode_node_fragment_program(
    batch_rows: u32,
    catalog: &[CatalogColumn],
    fragment: &NodeFragment,
) -> Vec<u8> {
    let body = encode_node_fragment(fragment);
    let mut out = Vec::with_capacity(body.len() + 64);
    out.push(PLAN_V2);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.push(OP_NODE_FRAGMENT);
    out.extend_from_slice(&batch_rows.to_le_bytes());
    encode_catalog(&mut out, catalog);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Mark a node program as resident; the existing graph host owns its terminal sink.
pub fn resident_node_program(
    program: &[u8],
    config: crate::ops::iterative::KMeansConfig,
) -> Result<Vec<u8>, String> {
    let mut cur = PlanCursor::new(program);
    if cur.u8().map_err(|e| e.to_string())? != PLAN_V2
        || cur.u32().map_err(|e| e.to_string())? != 1
        || cur.u8().map_err(|e| e.to_string())? != OP_NODE_FRAGMENT
    {
        return Err("resident execution requires a node fragment".into());
    }
    let batch_rows = cur.u32().map_err(|e| e.to_string())?;
    let catalog = cur.catalog().map_err(|e| e.to_string())?;
    let len = cur.u32().map_err(|e| e.to_string())? as usize;
    let mut fragment = decode_node_fragment(cur.bytes(len).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    if !cur.exhausted() {
        return Err("trailing resident program bytes".into());
    }
    if fragment
        .steps
        .iter()
        .any(|step| !matches!(step, FragmentStep::Filter(_) | FragmentStep::Project(_)))
    {
        return Err("resident input must be a row projection".into());
    }
    fragment.steps.push(FragmentStep::Resident(config));
    Ok(encode_node_fragment_program(
        batch_rows, &catalog, &fragment,
    ))
}

/// Settings for one hash reducer program.
pub struct HashReducerSpec<'a> {
    /// Number of scan streams the reducer reads.
    pub producer_count: u32,
    pub catalog: &'a [CatalogColumn],
    pub group_by: &'a [u32],
    pub aggregates: &'a [AggExpr],
    pub skip: u64,
    pub fetch: Option<u64>,
    pub rank: Option<u32>,
    pub group_budget: u64,
    /// When false, the spill budget and partition are not encoded.
    pub group_spill: bool,
    pub spill_combined_budget: u64,
    pub partition: u32,
}

/// Program for one hash reducer.
pub fn encode_hash_reducer_program(spec: &HashReducerSpec<'_>) -> Vec<u8> {
    let &HashReducerSpec {
        producer_count,
        catalog,
        group_by,
        aggregates,
        skip,
        fetch,
        rank,
        group_budget,
        group_spill,
        spill_combined_budget,
        partition,
    } = spec;
    let mut out = Vec::new();
    out.push(PLAN_V2);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.push(crate::program::stream_program::OP_HASH_REDUCER);
    push_u32(&mut out, producer_count);
    encode_catalog(&mut out, catalog);
    push_u32(&mut out, group_by.len() as u32);
    for column in group_by {
        push_u32(&mut out, *column);
    }
    let mut encoded_aggs = Vec::new();
    push_u32(&mut encoded_aggs, aggregates.len() as u32);
    for agg in aggregates {
        encode_agg(agg, &mut encoded_aggs);
    }
    push_u32(&mut out, encoded_aggs.len() as u32);
    out.extend_from_slice(&encoded_aggs);
    push_u64(&mut out, skip);
    match fetch {
        Some(fetch) => {
            out.push(1);
            push_u64(&mut out, fetch);
        }
        None => out.push(0),
    }
    push_u32(&mut out, rank.unwrap_or(u32::MAX));
    push_u64(&mut out, group_budget);
    out.push(u8::from(group_spill));
    if group_spill {
        push_u64(&mut out, spill_combined_budget);
        push_u32(&mut out, partition);
    }
    out
}

/// One program per row group for a partial aggregate. The coordinator adds the counts.
///
/// A node program that is not a partial aggregate is returned unchanged. Row groups are not
/// partitioned by the group key, so each program still emits every key it saw.
pub fn split_node_program_per_row_group(program: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut cur = PlanCursor::new(program);
    let tag = cur.u8().map_err(|err| err.to_string())?;
    if tag != PLAN_V2 {
        return Ok(vec![program.to_vec()]);
    }
    let ops = cur.u32().map_err(|err| err.to_string())?;
    let op = cur.u8().map_err(|err| err.to_string())?;
    if ops != 1 || op != OP_NODE_FRAGMENT {
        return Ok(vec![program.to_vec()]);
    }
    let batch_rows = cur.u32().map_err(|err| err.to_string())?;
    let catalog = cur.catalog().map_err(|err| err.to_string())?;
    let len = cur.u32().map_err(|err| err.to_string())? as usize;
    let fragment = decode_node_fragment(cur.bytes(len).map_err(|err| err.to_string())?)
        .map_err(|err| err.to_string())?;
    let partial = fragment
        .steps
        .iter()
        .any(|step| matches!(step, FragmentStep::PartialAggregate { .. }));
    let parts = fragment_per_row_group(&fragment);
    if !partial || parts.len() <= 1 {
        return Ok(vec![program.to_vec()]);
    }
    Ok(parts
        .iter()
        .map(|part| encode_node_fragment_program(batch_rows, &catalog, part))
        .collect())
}

fn fragment_per_row_group(fragment: &NodeFragment) -> Vec<NodeFragment> {
    if fragment.decode.len() <= 1 {
        return vec![fragment.clone()];
    }
    fragment
        .decode
        .iter()
        .map(|group| {
            let read_ids: HashSet<u32> = group.reads.iter().copied().collect();
            let logical_reads: Vec<LogicalRead> = fragment
                .logical_reads
                .iter()
                .filter(|read| read_ids.contains(&read.id))
                .cloned()
                .collect();
            let fetch_ids: HashSet<u32> = logical_reads
                .iter()
                .flat_map(|read| {
                    let mut ids: Vec<u32> =
                        read.pieces.iter().map(|piece| piece.fetch_id).collect();
                    ids.push(read.fetch_id);
                    ids
                })
                .collect();
            let fetches: Vec<FetchRequest> = fragment
                .fetches
                .iter()
                .filter(|fetch| fetch_ids.contains(&fetch.id))
                .cloned()
                .collect();
            NodeFragment {
                node: fragment.node,
                decode: vec![group.clone()],
                logical_reads,
                fetches,
                steps: fragment.steps.clone(),
                output_op: fragment.output_op,
                tail: fragment.tail,
                fetch_depth: fragment.fetch_depth,
                source: fragment.source,
                row_id: fragment.row_id,
                claim_units: Vec::new(),
                own_units: Vec::new(),
            }
        })
        .collect()
}

pub fn encode_node_fragment(fragment: &NodeFragment) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(FRAGMENT_WIRE_VERSION);
    push_u32(&mut out, fragment.node.0);
    push_u32(&mut out, fragment.output_op);
    push_u32(&mut out, fragment.fetch_depth);
    push_u32(&mut out, u32::from(fragment.source));
    push_u64(&mut out, fragment.tail.skip);
    match fragment.tail.fetch {
        Some(fetch) => {
            out.push(1);
            push_u64(&mut out, fetch);
        }
        None => out.push(0),
    }
    out.push(u8::from(fragment.tail.order_groups_by_count));
    out.push(u8::from(fragment.tail.final_aggregate));
    push_u32(&mut out, fragment.tail.rank_aggregate.unwrap_or(u32::MAX));
    push_u32(&mut out, fragment.row_id.unwrap_or(u32::MAX));
    let objects = object_table(fragment);
    push_var(&mut out, objects.len() as u64);
    for object in &objects {
        push_str(&mut out, object);
    }
    let object = |id: &str| {
        objects
            .iter()
            .position(|o| *o == id)
            .expect("object in table") as u64
    };
    let mut deltas = Deltas::default();
    push_var(&mut out, fragment.decode.len() as u64);
    for group in &fragment.decode {
        encode_decode_group(group, object(&group.object_id), &mut deltas, &mut out);
    }
    push_var(&mut out, fragment.fetches.len() as u64);
    for fetch in &fragment.fetches {
        encode_fetch(fetch, object(&fetch.object_id), &mut deltas, &mut out);
    }
    push_var(&mut out, fragment.logical_reads.len() as u64);
    for read in &fragment.logical_reads {
        encode_read(read, object(&read.object_id), &mut deltas, &mut out);
    }
    push_u32(&mut out, fragment.steps.len() as u32);
    for step in &fragment.steps {
        encode_step(step, &mut out);
    }
    push_var(&mut out, fragment.claim_units.len() as u64);
    for unit in &fragment.claim_units {
        deltas.unit.push(&mut out, u64::from(unit.id));
        push_ids(&mut out, &mut deltas.row_group, &unit.row_groups);
    }
    push_ids(&mut out, &mut deltas.unit, &fragment.own_units);
    out
}

pub fn decode_node_fragment(bytes: &[u8]) -> Result<NodeFragment, FragmentError> {
    let mut cur = bytes;
    if read_u8(&mut cur)? != FRAGMENT_WIRE_VERSION {
        return Err(LogicalWireError::BadVersion.into());
    }
    let node = NodeId(read_u32(&mut cur)?);
    let output_op = read_u32(&mut cur)?;
    let fetch_depth = read_u32(&mut cur)?;
    let source = u16::try_from(read_u32(&mut cur)?).map_err(|_| LogicalWireError::UnknownTag)?;
    let skip = read_u64(&mut cur)?;
    let fetch = match read_u8(&mut cur)? {
        0 => None,
        _ => Some(read_u64(&mut cur)?),
    };
    let order_groups_by_count = read_u8(&mut cur)? != 0;
    let final_aggregate = read_u8(&mut cur)? != 0;
    let rank = read_u32(&mut cur)?;
    let rank_aggregate = if rank == u32::MAX { None } else { Some(rank) };
    let row_id = Some(read_u32(&mut cur)?).filter(|&column| column != u32::MAX);
    let objects = read_var_list(&mut cur, read_str)?;
    let object = |cur: &mut &[u8]| -> Result<String, LogicalWireError> {
        let index = usize::try_from(read_var(cur)?).map_err(|_| LogicalWireError::UnknownTag)?;
        objects
            .get(index)
            .cloned()
            .ok_or(LogicalWireError::UnknownTag)
    };
    let mut deltas = Deltas::default();
    let decode = read_var_list(&mut cur, |cur| {
        let object_id = object(cur)?;
        decode_decode_group(cur, object_id, &mut deltas)
    })?;
    let fetches = read_var_list(&mut cur, |cur| {
        let object_id = object(cur)?;
        decode_fetch(cur, node, object_id, &mut deltas)
    })?;
    let logical_reads = read_var_list(&mut cur, |cur| {
        let object_id = object(cur)?;
        decode_read(cur, node, object_id, &mut deltas)
    })?;
    let steps = read_list(&mut cur, decode_step)?;
    let claim_units = read_var_list(&mut cur, |cur| {
        let id = deltas.unit.read_u32(cur)?;
        let row_groups = read_ids(cur, &mut deltas.row_group)?;
        Ok(ClaimUnit { id, row_groups })
    })?;
    let own_units = read_ids(&mut cur, &mut deltas.unit)?;
    if !cur.is_empty() {
        return Err(LogicalWireError::TrailingBytes.into());
    }
    Ok(NodeFragment {
        node,
        decode,
        logical_reads,
        fetches,
        steps,
        output_op,
        tail: FragmentTail {
            skip,
            fetch,
            order_groups_by_count,
            final_aggregate,
            rank_aggregate,
        },
        fetch_depth,
        source,
        row_id,
        claim_units,
        own_units,
    })
}

fn read_var_list<T>(
    cur: &mut &[u8],
    mut item: impl FnMut(&mut &[u8]) -> Result<T, LogicalWireError>,
) -> Result<Vec<T>, LogicalWireError> {
    let count = usize::try_from(read_var(cur)?).map_err(|_| LogicalWireError::UnexpectedEnd)?;
    // Every item is at least one byte, so a count beyond the input is corrupt.
    if count > cur.len() {
        return Err(LogicalWireError::UnexpectedEnd);
    }
    (0..count).map(|_| item(cur)).collect()
}

/// The fragment's object ids, first seen first. Items name an object by its index: a scan
/// reads one object, so the id would otherwise repeat on every read and fetch.
fn object_table(fragment: &NodeFragment) -> Vec<&str> {
    let mut objects: Vec<&str> = Vec::new();
    let ids = fragment
        .decode
        .iter()
        .map(|group| group.object_id.as_str())
        .chain(
            fragment
                .fetches
                .iter()
                .map(|fetch| fetch.object_id.as_str()),
        )
        .chain(
            fragment
                .logical_reads
                .iter()
                .map(|read| read.object_id.as_str()),
        );
    for id in ids {
        if !objects.contains(&id) {
            objects.push(id);
        }
    }
    objects
}

/// A field's values as signed varint steps from its previous value. Ids and offsets mostly
/// ascend through a fragment, so a scan's thousands of reads and fetches stay small on the
/// wire: every scan program crosses the network once per scan.
#[derive(Default)]
struct Delta(u64);

impl Delta {
    fn push(&mut self, out: &mut Vec<u8>, value: u64) {
        push_var_signed(out, value.wrapping_sub(self.0) as i64);
        self.0 = value;
    }

    fn read(&mut self, cur: &mut &[u8]) -> Result<u64, LogicalWireError> {
        self.0 = self.0.wrapping_add(read_var_signed(cur)? as u64);
        Ok(self.0)
    }

    fn read_u32(&mut self, cur: &mut &[u8]) -> Result<u32, LogicalWireError> {
        u32::try_from(self.read(cur)?).map_err(|_| LogicalWireError::UnknownTag)
    }
}

/// One [`Delta`] per field kind, shared by every item of a fragment.
#[derive(Default)]
struct Deltas {
    row_group: Delta,
    read: Delta,
    fetch: Delta,
    fetch_offset: Delta,
    read_offset: Delta,
    unit: Delta,
}

fn push_ids(out: &mut Vec<u8>, delta: &mut Delta, ids: &[u32]) {
    push_var(out, ids.len() as u64);
    for id in ids {
        delta.push(out, u64::from(*id));
    }
}

fn read_ids(cur: &mut &[u8], delta: &mut Delta) -> Result<Vec<u32>, LogicalWireError> {
    read_var_list(cur, |cur| delta.read_u32(cur))
}

fn encode_decode_group(group: &DecodeGroup, object: u64, deltas: &mut Deltas, out: &mut Vec<u8>) {
    push_var(out, object);
    deltas.row_group.push(out, u64::from(group.row_group));
    push_var(out, group.rows);
    push_var(out, group.columns.len() as u64);
    for column in &group.columns {
        push_var(out, u64::from(*column));
    }
    push_ids(out, &mut deltas.read, &group.reads);
}

fn decode_decode_group(
    cur: &mut &[u8],
    object_id: String,
    deltas: &mut Deltas,
) -> Result<DecodeGroup, LogicalWireError> {
    Ok(DecodeGroup {
        object_id,
        row_group: deltas.row_group.read_u32(cur)?,
        rows: read_var(cur)?,
        columns: read_var_list(cur, read_var_u32)?,
        reads: read_ids(cur, &mut deltas.read)?,
    })
}

fn encode_fetch(fetch: &FetchRequest, object: u64, deltas: &mut Deltas, out: &mut Vec<u8>) {
    push_var(out, object);
    deltas.fetch.push(out, u64::from(fetch.id));
    deltas.fetch_offset.push(out, fetch.offset);
    push_var(out, fetch.length);
    push_ids(out, &mut deltas.read, &fetch.logical_reads);
}

fn decode_fetch(
    cur: &mut &[u8],
    node: NodeId,
    object_id: String,
    deltas: &mut Deltas,
) -> Result<FetchRequest, LogicalWireError> {
    Ok(FetchRequest {
        id: deltas.fetch.read_u32(cur)?,
        node,
        object_id,
        offset: deltas.fetch_offset.read(cur)?,
        length: read_var(cur)?,
        logical_reads: read_ids(cur, &mut deltas.read)?,
    })
}

fn encode_read(read: &LogicalRead, object: u64, deltas: &mut Deltas, out: &mut Vec<u8>) {
    push_var(out, object);
    deltas.read.push(out, u64::from(read.id));
    push_var(out, u64::from(read.scan));
    deltas.row_group.push(out, u64::from(read.row_group));
    push_var(out, u64::from(read.column));
    deltas.read_offset.push(out, read.offset);
    push_var(out, read.length);
    out.push(u8::from(read.dictionary));
    push_var_signed(out, i64::from(read.codec));
    deltas.fetch.push(out, u64::from(read.fetch_id));
    push_var(out, read.pieces.len() as u64);
    for piece in &read.pieces {
        deltas.fetch.push(out, u64::from(piece.fetch_id));
        push_var(out, piece.fetch_offset);
        push_var(out, piece.read_offset);
        push_var(out, piece.length);
    }
}

fn decode_read(
    cur: &mut &[u8],
    node: NodeId,
    object_id: String,
    deltas: &mut Deltas,
) -> Result<LogicalRead, LogicalWireError> {
    Ok(LogicalRead {
        id: deltas.read.read_u32(cur)?,
        scan: read_var_u32(cur)?,
        node,
        object_id,
        row_group: deltas.row_group.read_u32(cur)?,
        column: read_var_u32(cur)?,
        offset: deltas.read_offset.read(cur)?,
        length: read_var(cur)?,
        dictionary: read_u8(cur)? != 0,
        codec: i32::try_from(read_var_signed(cur)?).map_err(|_| LogicalWireError::UnknownTag)?,
        fetch_id: deltas.fetch.read_u32(cur)?,
        pieces: read_var_list(cur, |cur| {
            Ok(FetchPiece {
                fetch_id: deltas.fetch.read_u32(cur)?,
                fetch_offset: read_var(cur)?,
                read_offset: read_var(cur)?,
                length: read_var(cur)?,
            })
        })?,
    })
}

fn encode_step(step: &FragmentStep, out: &mut Vec<u8>) {
    match step {
        FragmentStep::Resident(config) => {
            out.push(8);
            push_u32(out, config.dimensions as u32);
            push_u32(out, config.clusters as u32);
            push_u32(out, config.max_iterations);
            push_u64(out, config.tolerance.to_bits());
        }
        FragmentStep::Filter(predicates) => {
            out.push(1);
            push_u32(out, predicates.len() as u32);
            for predicate in predicates {
                encode_predicate(predicate, out);
            }
        }
        FragmentStep::Project(columns) => {
            out.push(2);
            push_u32(out, columns.len() as u32);
            for column in columns {
                push_u32(out, *column);
            }
        }
        FragmentStep::PartialAggregate {
            group_by,
            aggregates,
        } => {
            out.push(3);
            push_u32(out, group_by.len() as u32);
            for column in group_by {
                push_u32(out, *column);
            }
            push_u32(out, aggregates.len() as u32);
            for agg in aggregates {
                encode_agg(agg, out);
            }
        }
        FragmentStep::TopK { keys, n } => {
            out.push(4);
            push_u64(out, *n);
            push_u32(out, keys.len() as u32);
            for key in keys {
                encode_sort_key(key, out);
            }
        }
        FragmentStep::Parquet {
            target_row_group_rows,
            partition_columns,
        } => {
            out.push(5);
            push_u32(out, *target_row_group_rows);
            push_u32(out, partition_columns.len() as u32);
            for column in partition_columns {
                push_u32(out, *column);
            }
        }
        FragmentStep::HashPartition {
            input,
            partitions,
            hash_version,
        } => {
            out.push(match input {
                PartitionInput::Rows { .. } => 6,
                PartitionInput::Partials { .. } => 9,
            });
            push_u32(out, *partitions);
            push_u32(out, *hash_version);
            match input {
                PartitionInput::Rows { keys } => {
                    push_u32(out, keys.len() as u32);
                    for key in keys {
                        push_u32(out, *key);
                    }
                }
                PartitionInput::Partials { keys } => {
                    push_u32(out, keys.len() as u32);
                    for (position, column) in keys {
                        push_u32(out, *position);
                        push_u32(out, *column);
                    }
                }
            }
        }
        FragmentStep::RangePartition {
            keys,
            partitions,
            splits,
        } => {
            out.push(7);
            push_u32(out, *partitions);
            push_u32(out, keys.len() as u32);
            for key in keys {
                encode_sort_key(key, out);
            }
            push_u32(out, splits.len() as u32);
            for split in splits {
                encode_scalar_list(split, out);
            }
        }
    }
}

fn decode_step(cur: &mut &[u8]) -> Result<FragmentStep, LogicalWireError> {
    let tag = read_u8(cur)?;
    Ok(match tag {
        8 => FragmentStep::Resident(crate::ops::iterative::KMeansConfig {
            dimensions: read_u32(cur)? as usize,
            clusters: read_u32(cur)? as usize,
            max_iterations: read_u32(cur)?,
            tolerance: f64::from_bits(read_u64(cur)?),
        }),
        1 => FragmentStep::Filter(read_list(cur, decode_predicate)?),
        2 => FragmentStep::Project(read_list(cur, read_u32)?),
        3 => FragmentStep::PartialAggregate {
            group_by: read_list(cur, read_u32)?,
            aggregates: read_list(cur, decode_agg)?,
        },
        4 => {
            let n = read_u64(cur)?;
            FragmentStep::TopK {
                keys: read_list(cur, decode_sort_key)?,
                n,
            }
        }
        5 => FragmentStep::Parquet {
            target_row_group_rows: read_u32(cur)?,
            partition_columns: read_list(cur, read_u32)?,
        },
        6 | 9 => {
            let partitions = read_u32(cur)?;
            let hash_version = read_u32(cur)?;
            let input = if tag == 6 {
                PartitionInput::Rows {
                    keys: read_list(cur, read_u32)?,
                }
            } else {
                PartitionInput::Partials {
                    keys: read_list(cur, |cur| Ok((read_u32(cur)?, read_u32(cur)?)))?,
                }
            };
            FragmentStep::HashPartition {
                input,
                partitions,
                hash_version,
            }
        }
        7 => {
            let partitions = read_u32(cur)?;
            let keys = read_list(cur, decode_sort_key)?;
            let splits = if cur.is_empty() {
                Vec::new()
            } else {
                read_list(cur, decode_scalar_list)?
            };
            FragmentStep::RangePartition {
                keys,
                partitions,
                splits,
            }
        }
        _ => return Err(LogicalWireError::UnknownTag),
    })
}

#[cfg(test)]
mod partition_step_wire_tests {
    use super::*;

    #[test]
    fn both_partition_inputs_round_trip() {
        for input in [
            PartitionInput::Rows { keys: vec![3, 9] },
            PartitionInput::Partials {
                keys: vec![(0, 3), (2, 9)],
            },
        ] {
            let step = FragmentStep::HashPartition {
                input,
                partitions: 64,
                hash_version: 1,
            };
            let mut out = Vec::new();
            encode_step(&step, &mut out);
            let mut cur = out.as_slice();
            assert_eq!(decode_step(&mut cur).unwrap(), step);
            assert!(cur.is_empty());
            // Any truncation is refused, never read as a shorter key list.
            for len in 0..out.len() {
                assert!(decode_step(&mut &out[..len]).is_err(), "prefix {len}");
            }
        }
    }
}
