//! Execution IR for the engine. Operators and batch kernels take only these types.
//!
//! SQL text and the parser's borrowed plan never reach this module. The planner converts
//! its physical predicates and the hand parser's filters into these owned values.

/// What a scan column is, for filters and group keys. Callers map Parquet physical types onto this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Number,
    Text,
}

/// One catalog column. `physical` is the Parquet physical type tag.
#[derive(Clone, Debug)]
pub struct CatalogColumn {
    pub name: String,
    pub kind: ColumnKind,
    pub physical: i32,
    pub type_length: i32,
    pub max_def: u8,
    pub max_rep: u8,
}

/// Owned comparison literal. Integers stay exact; they are not rounded through `f64`.
#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Int(i64),
    Number(f64),
    Text(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// One `WHERE` predicate. A filter is the conjunction of these.
#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    Compare {
        column: u32,
        op: CmpOp,
        value: Scalar,
    },
    In {
        column: u32,
        values: Vec<Scalar>,
        negated: bool,
    },
    Like {
        column: u32,
        pattern: String,
        negated: bool,
    },
}

impl Predicate {
    pub fn column(&self) -> u32 {
        match self {
            Predicate::Compare { column, .. }
            | Predicate::In { column, .. }
            | Predicate::Like { column, .. } => *column,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggKind {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    ApproxDistinct,
    /// `COUNT(DISTINCT col)`: exact distinct non-null values.
    CountDistinct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggExpr {
    /// `None` is `COUNT(*)`.
    pub column: Option<u32>,
    pub kind: AggKind,
    /// Constant added before aggregating, as in `SUM(col + 3)`. Zero otherwise.
    pub offset: i8,
}

/// One row sort key. Nulls sort last unless `nulls_first`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortKey {
    pub column: u32,
    pub descending: bool,
    pub nulls_first: bool,
}

/// Most `GROUP BY` columns a running aggregate may have, including a hidden `COUNT(DISTINCT)` key.
pub const MAX_GROUP_KEYS: usize = 4;
