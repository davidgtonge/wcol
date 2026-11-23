//! Execution-facing plan values. The planner fills these in; the engine runs them.
//!
//! These are the wire types. They are not SQL, and they are not the batch-kernel IR.

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CmpOp {
    pub fn flip(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::NotEq => Self::NotEq,
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
        }
    }
}

/// Owned literal. Floats are IEEE-754 bits so the plan stays ordered and comparable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Literal {
    Int(i64),
    Float(u64),
    Bool(bool),
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Compare {
    pub column: u32,
    pub op: CmpOp,
    pub value: Literal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Predicate {
    Compare(Compare),
    IsNull {
        column: u32,
    },
    IsNotNull {
        column: u32,
    },
    Like {
        column: u32,
        pattern: String,
        negated: bool,
    },
    In {
        column: u32,
        values: Vec<Literal>,
        negated: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggKind {
    CountStar,
    Count,
    Sum,
    Min,
    Max,
    Avg,
    CountDistinct,
    ApproxDistinct,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggExpr {
    pub kind: AggKind,
    pub column: Option<u32>,
    /// Constant added to the column before aggregating, as in `SUM(col + 3)`. Zero otherwise.
    pub offset: i8,
}

/// What a sort key refers to. An aggregate ordinal is an output of the child
/// aggregate, not a scan column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortTarget {
    Column(u32),
    Aggregate(u32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortKey {
    pub target: SortTarget,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteSpan {
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchPiece {
    pub fetch_id: u32,
    /// Byte offset within the coalesced fetch (`fetch.offset + fetch_offset`).
    pub fetch_offset: u64,
    /// Byte offset within this logical read.
    pub read_offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalRead {
    pub id: u32,
    pub scan: u32,
    pub node: NodeId,
    pub object_id: String,
    pub row_group: u32,
    pub column: u32,
    pub offset: u64,
    pub length: u64,
    pub dictionary: bool,
    /// Parquet compression codec of the column chunk this read belongs to.
    pub codec: i32,
    /// Primary fetch (first piece). Prefer [`Self::pieces`] for byte coverage.
    pub fetch_id: u32,
    pub pieces: Vec<FetchPiece>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchRequest {
    pub id: u32,
    pub node: NodeId,
    pub object_id: String,
    pub offset: u64,
    pub length: u64,
    pub logical_reads: Vec<u32>,
}

/// Scan work one scan claims at run time: row groups whose fetches serve no other unit.
/// A claimed unit is decoded by exactly that scan; the others skip it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimUnit {
    pub id: u32,
    pub row_groups: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeGroup {
    pub object_id: String,
    pub row_group: u32,
    pub rows: u64,
    pub columns: Vec<u32>,
    pub reads: Vec<u32>,
}
