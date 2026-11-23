//! Syntax tree of the hand-written SQL subset.

#[derive(Debug, Clone, PartialEq)]
pub struct Query<'a> {
    pub select: Vec<SelectItem<'a>>,
    pub from: Ident<'a>,
    pub where_: Option<Expr<'a>>,
    pub group_by: Vec<Expr<'a>>,
    pub having: Option<Expr<'a>>,
    pub order_by: Vec<OrderItem<'a>>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement<'a> {
    Query(Query<'a>),
    CreateModel {
        name: String,
        options: KMeansOptions,
        query: Box<Query<'a>>,
        query_sql: &'a str,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct KMeansOptions {
    pub clusters: u32,
    pub max_iterations: u32,
    pub seed: u64,
    pub tolerance: f64,
}

impl Default for KMeansOptions {
    fn default() -> Self {
        Self {
            clusters: 32,
            max_iterations: 100,
            seed: 42,
            tolerance: 1e-4,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem<'a> {
    pub expr: Expr<'a>,
    pub alias: Option<Ident<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem<'a> {
    pub expr: Expr<'a>,
    pub desc: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ident<'a>(pub &'a str);

#[derive(Debug, Clone, PartialEq)]
pub enum Expr<'a> {
    Star,
    Ident(Ident<'a>),
    Int(i64),
    Number(f64),
    Str(&'a str),

    // Aggregates (v0)
    Agg {
        func: AggFn,
        distinct: bool,
        arg: Box<Expr<'a>>, // Star or Ident (strict)
    },

    // Minimal arithmetic (optional, but cheap)
    Binary {
        op: BinOp,
        left: Box<Expr<'a>>,
        right: Box<Expr<'a>>,
    },

    // Predicates are just expressions with comparison/AND operators
    Compare {
        op: CmpOp,
        left: Box<Expr<'a>>,
        right: Box<Expr<'a>>,
    },
    Like {
        left: Box<Expr<'a>>,
        pattern: Box<Expr<'a>>, // must be Str in v0
        negated: bool,
    },
    And(Box<Expr<'a>>, Box<Expr<'a>>),

    /// Function call: name(args...), e.g. length(URL), DATE_TRUNC('minute', EventTime)
    Call {
        name: Ident<'a>,
        args: Vec<Expr<'a>>,
    },

    /// IN (list) and NOT IN (list), e.g. col IN (-1, 6)
    In {
        left: Box<Expr<'a>>,
        values: Vec<Expr<'a>>,
        negated: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    ApproxDistinct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
}

pub use crate::ir::CmpOp;

#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub msg: &'static str,
    pub at: usize, // byte offset
}

pub(super) type PResult<T> = Result<T, ParseError>;
