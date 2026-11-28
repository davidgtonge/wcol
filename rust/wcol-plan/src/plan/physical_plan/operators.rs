//! Physical operators and the finished plan.

use super::*;

#[derive(Clone, Debug, PartialEq)]
pub enum OpKind {
    R2Fetch {
        fetch_ids: Vec<u32>,
    },
    Decode {
        groups: Vec<DecodeGroup>,
    },
    Filter {
        predicates: Vec<Predicate>,
    },
    Project {
        columns: Vec<u32>,
    },
    PartialAggregate {
        group_by: Vec<u32>,
        aggregates: Vec<AggExpr>,
    },
    /// Merges [`OpKind::PartialAggregate`] states.
    FinalAggregate {
        group_by: Vec<u32>,
        aggregates: Vec<AggExpr>,
    },
    Exchange,
    /// Rows are hash-partitioned onto `partitions` reducers. Gather is [`OpKind::Exchange`].
    HashExchange {
        keys: Vec<u32>,
        partitions: u32,
        hash_version: u32,
    },
    /// Rows are range-partitioned for distributed global sort.
    RangeExchange {
        keys: Vec<SortKey>,
        partitions: u32,
        splits: Vec<Vec<Scalar>>,
    },
    Sort {
        keys: Vec<SortKey>,
    },
    Limit {
        skip: u64,
        fetch: Option<u64>,
    },
    /// An aggregate answered from footer metadata alone (no column reads): the table's row
    /// count, and each aggregate's footer bounds (`None` for `COUNT(*)`).
    MetadataAggregate {
        rows: u64,
        aggregates: Vec<AggExpr>,
        bounds: Vec<Option<(i64, i64)>>,
    },
    /// Per-node bounded sort before exchange (row queries only).
    TopK {
        keys: Vec<SortKey>,
        n: u64,
    },
    /// Result format. Row queries end here so the dispatcher does not choose it.
    Sink {
        kind: SinkKind,
    },
}

/// What a finished plan writes. Parquet policy is the export path; rows are interactive results.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkKind {
    Rows,
    Parquet {
        target_row_group_rows: u32,
        /// Output ordinals. Empty writes one object. The host demux keeps one
        /// object open per distinct value of these columns.
        partition_columns: Vec<u32>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Operator {
    pub id: u32,
    pub node: Option<NodeId>,
    pub kind: OpKind,
    pub inputs: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeMetrics {
    pub node: NodeId,
    pub logical_bytes: u64,
    pub fetched_bytes: u64,
    pub request_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannerMetrics {
    pub nodes: Vec<NodeMetrics>,
    pub request_count: u32,
    pub logical_bytes: u64,
    pub fetched_bytes: u64,
    pub over_read_bytes: u64,
    /// `over_read_bytes * 1_000_000 / fetched_bytes`, or 0 when nothing is fetched.
    pub over_read_ppm: u64,
    /// `1` when the plan gathers partials. A power of two when it hash-partitions.
    pub hash_reducers: u32,
    /// Distinct-group upper bound used to size `hash_reducers`.
    pub estimated_distinct_groups: u64,
    /// That bound times the bytes a group's state is charged: the aggregate's memory need.
    pub estimated_group_bytes: u64,
}

impl PlannerMetrics {
    pub fn over_read_percent(&self) -> f64 {
        self.over_read_ppm as f64 / 10_000.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct WcolPhysicalPlan {
    pub root: u32,
    pub operators: Vec<Operator>,
    pub fetches: Vec<FetchRequest>,
    pub logical_reads: Vec<LogicalRead>,
    pub metrics: PlannerMetrics,
}

impl WcolPhysicalPlan {
    pub fn canonical_text(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("root {}\n", self.root));
        for fetch in &self.fetches {
            out.push_str(&format!(
                "fetch {} node={} object={:?} offset={} length={} reads={:?}\n",
                fetch.id,
                fetch.node.0,
                fetch.object_id,
                fetch.offset,
                fetch.length,
                fetch.logical_reads
            ));
        }
        for read in &self.logical_reads {
            let pieces = read
                .pieces
                .iter()
                .map(|piece| format!("f{}@{}+{}", piece.fetch_id, piece.read_offset, piece.length))
                .collect::<Vec<_>>()
                .join(",");
            out.push_str(&format!(
                "read {} scan={} node={} object={:?} row_group={} column={} offset={} length={} dictionary={} codec={} fetch={} pieces=[{pieces}]\n",
                read.id,
                read.scan,
                read.node.0,
                read.object_id,
                read.row_group,
                read.column,
                read.offset,
                read.length,
                u8::from(read.dictionary),
                read.codec,
                read.fetch_id
            ));
        }
        for op in &self.operators {
            let node = op
                .node
                .map(|id| id.0.to_string())
                .unwrap_or_else(|| "coordinator".into());
            out.push_str(&format!(
                "op {} node={node} {} inputs={:?}\n",
                op.id,
                fmt_kind(&op.kind),
                op.inputs
            ));
        }
        for node in &self.metrics.nodes {
            out.push_str(&format!(
                "metric node={} logical={} fetched={} requests={}\n",
                node.node.0, node.logical_bytes, node.fetched_bytes, node.request_count
            ));
        }
        out.push_str(&format!(
            "metric requests={} logical={} fetched={} over_read={} over_read_ppm={}\n",
            self.metrics.request_count,
            self.metrics.logical_bytes,
            self.metrics.fetched_bytes,
            self.metrics.over_read_bytes,
            self.metrics.over_read_ppm
        ));
        out
    }
}
