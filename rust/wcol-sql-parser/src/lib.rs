// tiny_sql_v0.rs
// 0 deps, wasm-friendly, strict SQL-ish parser for a small dialect.

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub msg: &'static str,
    pub at: usize, // byte offset
}

type PResult<T> = Result<T, ParseError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokKind {
    Eof,

    // punctuation
    LParen,
    RParen,
    Comma,
    Semi,
    Star,

    // operators
    Plus,
    Minus,
    Eq,
    Ne, // <>
    Lt,
    Le,
    Gt,
    Ge,

    // literals / id
    Ident,
    Int,
    Str,

    // keywords
    KwSelect,
    KwFrom,
    KwWhere,
    KwGroup,
    KwBy,
    KwHaving,
    KwOrder,
    KwAsc,
    KwDesc,
    KwLimit,
    KwOffset,
    KwAs,
    KwDistinct,
    KwAnd,
    KwNot,
    KwLike,
    KwIn,

    // aggregate names treated as keywords for speed
    KwCount,
    KwSum,
    KwAvg,
    KwMin,
    KwMax,
    KwApproxCountDistinct,
}

#[derive(Debug, Clone, Copy)]
struct Tok<'a> {
    kind: TokKind,
    start: usize,
    #[allow(dead_code)]
    end: usize,
    lex: &'a str,
}

struct Lexer<'a> {
    s: &'a str,
    b: &'a [u8],
    i: usize,
    len: usize,
}

impl<'a> Lexer<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            s,
            b: s.as_bytes(),
            i: 0,
            len: s.len(),
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        if self.i < self.len {
            Some(self.b[self.i])
        } else {
            None
        }
    }

    fn bump(&mut self) -> Option<u8> {
        if self.i < self.len {
            let c = self.b[self.i];
            self.i += 1;
            Some(c)
        } else {
            None
        }
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek_byte() {
            if c == b' ' || c == b'\n' || c == b'\r' || c == b'\t' {
                self.i += 1;
            } else {
                break;
            }
        }
    }

    fn next(&mut self) -> PResult<Tok<'a>> {
        self.skip_ws();
        let start = self.i;
        let Some(c) = self.bump() else {
            return Ok(Tok {
                kind: TokKind::Eof,
                start,
                end: start,
                lex: "",
            });
        };

        let kind = match c {
            b'(' => TokKind::LParen,
            b')' => TokKind::RParen,
            b',' => TokKind::Comma,
            b';' => TokKind::Semi,
            b'*' => TokKind::Star,
            b'+' => TokKind::Plus,
            b'-' => {
                // could be negative int; we treat '-' as token, parser can handle unary if desired
                TokKind::Minus
            }
            b'=' => TokKind::Eq,
            b'<' => {
                if self.peek_byte() == Some(b'=') {
                    self.i += 1;
                    TokKind::Le
                } else if self.peek_byte() == Some(b'>') {
                    self.i += 1;
                    TokKind::Ne
                } else {
                    TokKind::Lt
                }
            }
            b'>' => {
                if self.peek_byte() == Some(b'=') {
                    self.i += 1;
                    TokKind::Ge
                } else {
                    TokKind::Gt
                }
            }
            b'\'' => {
                // string literal: supports '' escape
                while let Some(n) = self.peek_byte() {
                    self.i += 1;
                    if n == b'\'' {
                        // if doubled quote, continue
                        if self.peek_byte() == Some(b'\'') {
                            self.i += 1;
                            continue;
                        }
                        break;
                    }
                }
                TokKind::Str
            }
            _ if is_ident_start(c) => {
                while let Some(n) = self.peek_byte() {
                    if is_ident_continue(n) {
                        self.i += 1;
                    } else {
                        break;
                    }
                }
                TokKind::Ident
            }
            _ if is_digit(c) => {
                while let Some(n) = self.peek_byte() {
                    if is_digit(n) {
                        self.i += 1;
                    } else {
                        break;
                    }
                }
                TokKind::Int
            }
            _ => {
                return Err(ParseError {
                    msg: "unexpected character",
                    at: start,
                });
            }
        };

        let end = self.i;
        let lex = &self.s[start..end];

        let kind = if kind == TokKind::Ident {
            match keyword_kind(lex) {
                Some(k) => k,
                None => TokKind::Ident,
            }
        } else {
            kind
        };

        Ok(Tok {
            kind,
            start,
            end,
            lex,
        })
    }
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_lowercase() || c.is_ascii_uppercase() || c == b'_'
}
fn is_ident_continue(c: u8) -> bool {
    is_ident_start(c) || is_digit(c)
}
fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

fn eq_kw(lex: &str, kw: &str) -> bool {
    // ASCII case-insensitive, no alloc
    lex.eq_ignore_ascii_case(kw)
}

fn keyword_kind(lex: &str) -> Option<TokKind> {
    Some(if eq_kw(lex, "select") {
        TokKind::KwSelect
    } else if eq_kw(lex, "from") {
        TokKind::KwFrom
    } else if eq_kw(lex, "where") {
        TokKind::KwWhere
    } else if eq_kw(lex, "group") {
        TokKind::KwGroup
    } else if eq_kw(lex, "by") {
        TokKind::KwBy
    } else if eq_kw(lex, "having") {
        TokKind::KwHaving
    } else if eq_kw(lex, "order") {
        TokKind::KwOrder
    } else if eq_kw(lex, "asc") {
        TokKind::KwAsc
    } else if eq_kw(lex, "desc") {
        TokKind::KwDesc
    } else if eq_kw(lex, "limit") {
        TokKind::KwLimit
    } else if eq_kw(lex, "offset") {
        TokKind::KwOffset
    } else if eq_kw(lex, "as") {
        TokKind::KwAs
    } else if eq_kw(lex, "distinct") {
        TokKind::KwDistinct
    } else if eq_kw(lex, "and") {
        TokKind::KwAnd
    } else if eq_kw(lex, "not") {
        TokKind::KwNot
    } else if eq_kw(lex, "like") {
        TokKind::KwLike
    } else if eq_kw(lex, "in") {
        TokKind::KwIn
    } else if eq_kw(lex, "count") {
        TokKind::KwCount
    } else if eq_kw(lex, "sum") {
        TokKind::KwSum
    } else if eq_kw(lex, "avg") {
        TokKind::KwAvg
    } else if eq_kw(lex, "min") {
        TokKind::KwMin
    } else if eq_kw(lex, "max") {
        TokKind::KwMax
    } else if eq_kw(lex, "approx_count_distinct") {
        TokKind::KwApproxCountDistinct
    } else {
        return None;
    })
}

struct Parser<'a> {
    lx: Lexer<'a>,
    cur: Tok<'a>,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> PResult<Self> {
        let mut lx = Lexer::new(input);
        let cur = lx.next()?;
        Ok(Self { lx, cur })
    }

    fn bump(&mut self) -> PResult<()> {
        self.cur = self.lx.next()?;
        Ok(())
    }

    fn expect(&mut self, k: TokKind) -> PResult<Tok<'a>> {
        if self.cur.kind == k {
            let t = self.cur;
            self.bump()?;
            Ok(t)
        } else {
            Err(ParseError {
                msg: "unexpected token",
                at: self.cur.start,
            })
        }
    }

    fn eat(&mut self, k: TokKind) -> PResult<bool> {
        if self.cur.kind == k {
            self.bump()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn parse_query(mut self) -> PResult<Query<'a>> {
        self.expect(TokKind::KwSelect)?;
        let select = self.parse_select_list()?;

        self.expect(TokKind::KwFrom)?;
        let from = self.parse_ident()?;

        // Optional clauses
        let where_ = if self.cur.kind == TokKind::KwWhere {
            self.bump()?;
            Some(self.parse_and_predicate()?)
        } else {
            None
        };

        let mut group_by = Vec::new();
        if self.cur.kind == TokKind::KwGroup {
            self.bump()?;
            self.expect(TokKind::KwBy)?;
            group_by = self.parse_expr_list()?;
        }

        let having = if self.cur.kind == TokKind::KwHaving {
            self.bump()?;
            Some(self.parse_and_predicate()?)
        } else {
            None
        };

        let mut order_by = Vec::new();
        if self.cur.kind == TokKind::KwOrder {
            self.bump()?;
            self.expect(TokKind::KwBy)?;
            order_by = self.parse_order_list()?;
        }

        let offset = if self.cur.kind == TokKind::KwOffset {
            self.bump()?;
            Some(self.parse_u64()?)
        } else {
            None
        };

        let limit = if self.cur.kind == TokKind::KwLimit {
            self.bump()?;
            Some(self.parse_u64()?)
        } else {
            None
        };

        // Optional trailing semicolon
        let _ = self.eat(TokKind::Semi)?;
        self.expect(TokKind::Eof)?;

        Ok(Query {
            select,
            from,
            where_,
            group_by,
            having,
            order_by,
            offset,
            limit,
        })
    }

    fn parse_select_list(&mut self) -> PResult<Vec<SelectItem<'a>>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr_bp(0)?;
            let alias = if self.cur.kind == TokKind::KwAs {
                self.bump()?;
                Some(self.parse_ident()?)
            } else {
                None
            };
            items.push(SelectItem { expr, alias });
            if !self.eat(TokKind::Comma)? {
                break;
            }
        }
        Ok(items)
    }

    fn parse_order_list(&mut self) -> PResult<Vec<OrderItem<'a>>> {
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expr_bp(0)?;
            let desc = if self.cur.kind == TokKind::KwDesc {
                self.bump()?;
                true
            } else if self.cur.kind == TokKind::KwAsc {
                self.bump()?;
                false
            } else {
                false
            };
            items.push(OrderItem { expr, desc });
            if !self.eat(TokKind::Comma)? {
                break;
            }
        }
        Ok(items)
    }

    fn parse_expr_list(&mut self) -> PResult<Vec<Expr<'a>>> {
        let mut xs = Vec::new();
        loop {
            xs.push(self.parse_expr_bp(0)?);
            if !self.eat(TokKind::Comma)? {
                break;
            }
        }
        Ok(xs)
    }

    fn parse_and_predicate(&mut self) -> PResult<Expr<'a>> {
        let mut left = self.parse_predicate()?;
        while self.cur.kind == TokKind::KwAnd {
            self.bump()?;
            let right = self.parse_predicate()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_predicate(&mut self) -> PResult<Expr<'a>> {
        // <expr> then [NOT] LIKE <pat> | [NOT] IN (list) | <cmp> <expr>
        let left = self.parse_expr_bp(0)?;

        let mut neg = false;
        if self.cur.kind == TokKind::KwNot {
            self.bump()?;
            neg = true;
        }

        // LIKE / NOT LIKE
        if self.cur.kind == TokKind::KwLike {
            self.bump()?;
            let pat = self.parse_expr_bp(0)?;
            return Ok(Expr::Like {
                left: Box::new(left),
                pattern: Box::new(pat),
                negated: neg,
            });
        }

        // IN / NOT IN (list)
        if self.cur.kind == TokKind::KwIn {
            self.bump()?;
            self.expect(TokKind::LParen)?;
            let values = self.parse_expr_list()?;
            self.expect(TokKind::RParen)?;
            return Ok(Expr::In {
                left: Box::new(left),
                values,
                negated: neg,
            });
        }
        if neg {
            return Err(ParseError {
                msg: "expected LIKE or IN after NOT",
                at: self.cur.start,
            });
        }

        // Comparisons
        let op = match self.cur.kind {
            TokKind::Eq => CmpOp::Eq,
            TokKind::Ne => CmpOp::Ne,
            TokKind::Lt => CmpOp::Lt,
            TokKind::Le => CmpOp::Le,
            TokKind::Gt => CmpOp::Gt,
            TokKind::Ge => CmpOp::Ge,
            _ => {
                return Err(ParseError {
                    msg: "expected comparison operator",
                    at: self.cur.start,
                })
            }
        };
        self.bump()?;
        let right = self.parse_expr_bp(0)?;
        Ok(Expr::Compare {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    // Pratt parser with only + and - binary ops (left-assoc)
    fn parse_expr_bp(&mut self, min_bp: u8) -> PResult<Expr<'a>> {
        let mut lhs = self.parse_primary()?;

        loop {
            let (op, l_bp, r_bp) = match self.cur.kind {
                TokKind::Plus => (BinOp::Add, 1, 2),
                TokKind::Minus => (BinOp::Sub, 1, 2),
                _ => break,
            };
            if l_bp < min_bp {
                break;
            }
            self.bump()?;
            let rhs = self.parse_expr_bp(r_bp)?;
            lhs = Expr::Binary {
                op,
                left: Box::new(lhs),
                right: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    fn parse_primary(&mut self) -> PResult<Expr<'a>> {
        match self.cur.kind {
            TokKind::Minus => {
                self.bump()?;
                let e = self.parse_expr_bp(2)?; // binding power so -1+2 is (-1)+2
                Ok(Expr::Binary {
                    op: BinOp::Sub,
                    left: Box::new(Expr::Int(0)),
                    right: Box::new(e),
                })
            }
            TokKind::Star => {
                self.bump()?;
                Ok(Expr::Star)
            }
            TokKind::Ident => {
                let id = self.parse_ident()?;
                if self.cur.kind == TokKind::LParen {
                    self.bump()?;
                    let args = self.parse_expr_list()?;
                    self.expect(TokKind::RParen)?;
                    Ok(Expr::Call { name: id, args })
                } else {
                    Ok(Expr::Ident(id))
                }
            }
            TokKind::Int => {
                let t = self.cur;
                self.bump()?;
                let v = t.lex.parse::<i64>().map_err(|_| ParseError {
                    msg: "bad int",
                    at: t.start,
                })?;
                Ok(Expr::Int(v))
            }
            TokKind::Str => {
                let t = self.cur;
                self.bump()?;
                // Keep raw slice including quotes? We'll store inner content slice.
                // For v0, we store without surrounding quotes, but without unescaping (small & fine).
                let inner = if t.lex.len() >= 2 {
                    &t.lex[1..t.lex.len() - 1]
                } else {
                    ""
                };
                Ok(Expr::Str(inner))
            }

            // Aggregates as keywords
            TokKind::KwCount
            | TokKind::KwSum
            | TokKind::KwAvg
            | TokKind::KwMin
            | TokKind::KwMax
            | TokKind::KwApproxCountDistinct => self.parse_aggregate(),

            TokKind::LParen => {
                self.bump()?;
                let e = self.parse_expr_bp(0)?;
                self.expect(TokKind::RParen)?;
                Ok(e)
            }

            _ => Err(ParseError {
                msg: "expected expression",
                at: self.cur.start,
            }),
        }
    }

    fn parse_aggregate(&mut self) -> PResult<Expr<'a>> {
        let func = match self.cur.kind {
            TokKind::KwCount => AggFn::Count,
            TokKind::KwSum => AggFn::Sum,
            TokKind::KwAvg => AggFn::Avg,
            TokKind::KwMin => AggFn::Min,
            TokKind::KwMax => AggFn::Max,
            TokKind::KwApproxCountDistinct => AggFn::ApproxDistinct,
            _ => {
                return Err(ParseError {
                    msg: "expected aggregate",
                    at: self.cur.start,
                })
            }
        };
        self.bump()?;
        self.expect(TokKind::LParen)?;

        let distinct = self.eat(TokKind::KwDistinct)?;

        let arg = if self.cur.kind == TokKind::Star {
            self.bump()?;
            Expr::Star
        } else {
            self.parse_expr_bp(0)?
        };

        self.expect(TokKind::RParen)?;

        // v0 strictness: SUM/AVG/MIN/MAX/APPROX_COUNT_DISTINCT cannot take Star
        // DISTINCT only allowed with COUNT, not with approx_count_distinct (it's inherently distinct)
        if func != AggFn::Count {
            if distinct && func != AggFn::ApproxDistinct {
                return Err(ParseError {
                    msg: "DISTINCT only allowed with COUNT in v0",
                    at: self.cur.start,
                });
            }
            if matches!(arg, Expr::Star) && func != AggFn::ApproxDistinct {
                return Err(ParseError {
                    msg: "only COUNT can use *",
                    at: self.cur.start,
                });
            }
        }

        // approx_count_distinct cannot use * (needs a column)
        if func == AggFn::ApproxDistinct && matches!(arg, Expr::Star) {
            return Err(ParseError {
                msg: "approx_count_distinct requires a column argument",
                at: self.cur.start,
            });
        }

        Ok(Expr::Agg {
            func,
            distinct,
            arg: Box::new(arg),
        })
    }

    fn parse_ident(&mut self) -> PResult<Ident<'a>> {
        if self.cur.kind == TokKind::Ident {
            let t = self.cur;
            self.bump()?;
            Ok(Ident(t.lex))
        } else {
            Err(ParseError {
                msg: "expected identifier",
                at: self.cur.start,
            })
        }
    }

    fn parse_u64(&mut self) -> PResult<u64> {
        if self.cur.kind == TokKind::Int {
            let t = self.cur;
            self.bump()?;
            t.lex.parse::<u64>().map_err(|_| ParseError {
                msg: "bad u64",
                at: t.start,
            })
        } else {
            Err(ParseError {
                msg: "expected integer",
                at: self.cur.start,
            })
        }
    }
}

pub fn parse_sql_v0(input: &str) -> PResult<Query<'_>> {
    Parser::new(input)?.parse_query()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_count() {
        let q = parse_sql_v0("SELECT COUNT(*) FROM hits;").unwrap();
        assert_eq!(q.from.0, "hits");
        assert_eq!(q.select.len(), 1);
    }

    #[test]
    fn parse_where_group_order_limit() {
        let sql = "SELECT AdvEngineID, COUNT(*) AS c \
                   FROM hits WHERE AdvEngineID <> 0 \
                   GROUP BY AdvEngineID \
                   ORDER BY c DESC LIMIT 10;";
        let q = parse_sql_v0(sql).unwrap();
        assert!(q.where_.is_some());
        assert_eq!(q.group_by.len(), 1);
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn parse_like() {
        let sql = "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';";
        let q = parse_sql_v0(sql).unwrap();
        assert!(q.where_.is_some());
    }

    #[test]
    fn parse_count_distinct() {
        let sql = "SELECT COUNT(DISTINCT UserID) FROM hits;";
        let q = parse_sql_v0(sql).unwrap();
        assert_eq!(q.select.len(), 1);
        assert_eq!(q.from.0, "hits");
    }

    #[test]
    fn parse_approx_count_distinct() {
        let sql = "SELECT approx_count_distinct(UserID) FROM hits;";
        let q = parse_sql_v0(sql).unwrap();
        assert_eq!(q.select.len(), 1);
        assert_eq!(q.from.0, "hits");
        match &q.select[0].expr {
            Expr::Agg { func, arg, .. } => {
                assert_eq!(*func, AggFn::ApproxDistinct);
                assert!(matches!(arg.as_ref(), Expr::Ident(_)));
            }
            _ => panic!("expected Agg"),
        }
    }

    #[test]
    fn parse_approx_count_distinct_star_fails() {
        let sql = "SELECT approx_count_distinct(*) FROM hits;";
        let result = parse_sql_v0(sql);
        assert!(result.is_err(), "approx_count_distinct(*) should fail");
    }

    #[test]
    fn parse_multiple_aggs_and_alias() {
        let sql = "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;";
        let q = parse_sql_v0(sql).unwrap();
        assert_eq!(q.select.len(), 2);
        assert_eq!(q.group_by.len(), 1);
        assert_eq!(q.order_by.len(), 1);
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn parse_offset_limit() {
        // Parser expects OFFSET then LIMIT (v0)
        let sql = "SELECT URL FROM hits ORDER BY EventTime OFFSET 1000 LIMIT 10;";
        let q = parse_sql_v0(sql).unwrap();
        assert_eq!(q.offset, Some(1000));
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn parse_having() {
        let sql = "SELECT CounterID, COUNT(*) AS c FROM hits GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY c DESC LIMIT 25;";
        let q = parse_sql_v0(sql).unwrap();
        assert!(q.having.is_some());
        assert_eq!(q.limit, Some(25));
    }

    #[test]
    fn parse_no_semicolon() {
        let q = parse_sql_v0("SELECT 1 FROM t").unwrap();
        assert_eq!(q.from.0, "t");
    }

    #[test]
    fn parse_star_select() {
        let q = parse_sql_v0("SELECT * FROM hits LIMIT 10").unwrap();
        assert_eq!(q.select.len(), 1);
        assert!(matches!(q.select[0].expr, Expr::Star));
    }

    #[test]
    fn parse_function_call() {
        let q = parse_sql_v0("SELECT length(URL) FROM hits").unwrap();
        assert_eq!(q.select.len(), 1);
        match &q.select[0].expr {
            Expr::Call { name, args } => {
                assert_eq!(name.0, "length");
                assert_eq!(args.len(), 1);
                assert!(matches!(args[0], Expr::Ident(_)));
            }
            _ => panic!("expected Call"),
        }
    }

    #[test]
    fn parse_agg_of_call() {
        let q = parse_sql_v0("SELECT AVG(length(URL)) AS l FROM hits").unwrap();
        assert_eq!(q.select.len(), 1);
        match &q.select[0].expr {
            Expr::Agg { arg, .. } => match arg.as_ref() {
                Expr::Call { name, args } => {
                    assert_eq!(name.0, "length");
                    assert_eq!(args.len(), 1);
                }
                _ => panic!("expected Call inside Agg"),
            },
            _ => panic!("expected Agg"),
        }
    }

    #[test]
    fn parse_in() {
        let q = parse_sql_v0("SELECT 1 FROM hits WHERE TraficSourceID IN (-1, 6)").unwrap();
        assert!(q.where_.is_some());
        match q.where_.as_ref().unwrap() {
            Expr::In {
                left,
                values,
                negated,
            } => {
                assert!(!negated);
                assert!(matches!(left.as_ref(), Expr::Ident(_)));
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected In"),
        }
    }

    #[test]
    fn parse_not_in() {
        let q = parse_sql_v0("SELECT 1 FROM hits WHERE col NOT IN (1, 2, 3)").unwrap();
        match q.where_.as_ref().unwrap() {
            Expr::In {
                negated, values, ..
            } => {
                assert!(*negated);
                assert_eq!(values.len(), 3);
            }
            _ => panic!("expected In"),
        }
    }

    /// Rewrites that avoid parser-unsupported syntax (see readme).
    #[test]
    fn parse_rewritten_extract_and_date() {
        // #19 rewrite: extract('minute', EventTime) instead of extract(minute FROM EventTime)
        let q = parse_sql_v0("SELECT UserID, extract('minute', EventTime) AS m FROM hits GROUP BY UserID, extract('minute', EventTime)").unwrap();
        assert_eq!(q.select.len(), 2);
        assert_eq!(q.group_by.len(), 2);
        // #37-style rewrite: DATE '...' → '...'
        let q2 = parse_sql_v0("SELECT URL, COUNT(*) AS c FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' GROUP BY URL").unwrap();
        assert!(q2.where_.is_some());
        assert_eq!(q2.group_by.len(), 1);
        // #41: IN is supported; with DATE→string rewrite the full query parses
        let q3 = parse_sql_v0("SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC OFFSET 100 LIMIT 10").unwrap();
        assert!(q3.where_.is_some());
        assert_eq!(q3.offset, Some(100));
        assert_eq!(q3.limit, Some(10));
    }

    /// All SELECT queries from readme.md (V1 target); v0 parser supports a subset.
    const README_QUERIES: &[&str] = &[
        "SELECT COUNT(*) FROM hits;",
        "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;",
        "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;",
        "SELECT AVG(UserID) FROM hits;",
        "SELECT COUNT(DISTINCT UserID) FROM hits;",
        "SELECT COUNT(DISTINCT SearchPhrase) FROM hits;",
        "SELECT MIN(EventDate), MAX(EventDate) FROM hits;",
        "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;",
        "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;",
        "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;",
        "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;",
        "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;",
        "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
        "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;",
        "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;",
        "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;",
        "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;",
        "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;",
        "SELECT UserID, extract('minute', EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract('minute', EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;",
        "SELECT UserID FROM hits WHERE UserID = 435090932899640449;",
        "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';",
        "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
        "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
        "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;",
        "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;",
        "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;",
        "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits;",
        "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;",
        "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;",
        "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;",
        "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;",
        "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;",
        "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;",
        "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;",
        "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;",
        "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10;",
        "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= DATE '2013-07-01' AND EventDate <= DATE '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END, URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10;",
        "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC OFFSET 100 LIMIT 10;",
        "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC OFFSET 10000 LIMIT 10;",
        "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) OFFSET 1000 LIMIT 10;",
    ];

    #[test]
    fn readme_queries_parse_count() {
        let mut ok = 0;
        let mut fail = Vec::new();
        for (i, sql) in README_QUERIES.iter().enumerate() {
            match parse_sql_v0(sql) {
                Ok(_) => ok += 1,
                Err(e) => fail.push((i + 1, sql, e)),
            }
        }
        eprintln!(
            "readme: {} parsed, {} failed (of {})",
            ok,
            fail.len(),
            README_QUERIES.len()
        );
        for (idx, sql, e) in &fail {
            eprintln!(
                "  #{} at {}: {} ... {:?}",
                idx,
                e.at,
                e.msg,
                &sql[..sql.len().min(80)]
            );
        }
        assert_eq!(
            ok,
            42,
            "readme: 42 of 43 queries parse (only #40 CASE unparsed); {} parsed, {} failed",
            ok,
            fail.len()
        );
    }
}
