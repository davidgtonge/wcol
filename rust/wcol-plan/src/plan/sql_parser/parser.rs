//! Recursive-descent parser from tokens to the syntax tree.

use super::*;

pub(super) struct Parser<'a> {
    pub(super) lx: Lexer<'a>,
    pub(super) cur: Tok<'a>,
}

impl<'a> Parser<'a> {
    pub(super) fn new(input: &'a str) -> PResult<Self> {
        let mut lx = Lexer::new(input);
        let cur = lx.next()?;
        Ok(Self { lx, cur })
    }

    pub(super) fn bump(&mut self) -> PResult<()> {
        self.cur = self.lx.next()?;
        Ok(())
    }

    pub(super) fn expect(&mut self, k: TokKind) -> PResult<Tok<'a>> {
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

    pub(super) fn eat(&mut self, k: TokKind) -> PResult<bool> {
        if self.cur.kind == k {
            self.bump()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn parse_query(mut self) -> PResult<Query<'a>> {
        let query = self.parse_select()?;
        let _ = self.eat(TokKind::Semi)?;
        self.expect(TokKind::Eof)?;
        Ok(query)
    }

    pub(super) fn parse_select(&mut self) -> PResult<Query<'a>> {
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

        // `LIMIT n OFFSET m` (standard) or `OFFSET m LIMIT n`, each at most once.
        let mut offset = None;
        let mut limit = None;
        loop {
            if self.cur.kind == TokKind::KwOffset && offset.is_none() {
                self.bump()?;
                offset = Some(self.parse_u64()?);
            } else if self.cur.kind == TokKind::KwLimit && limit.is_none() {
                self.bump()?;
                limit = Some(self.parse_u64()?);
            } else {
                break;
            }
        }

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

    pub(super) fn parse_create_model(&mut self) -> PResult<Statement<'a>> {
        self.expect_ident_word("CREATE")?;
        self.expect_ident_word("MODEL")?;
        let name = self.parse_ident()?.0.to_string();
        self.expect_ident_word("OPTIONS")?;
        self.expect(TokKind::LParen)?;
        let mut options = KMeansOptions::default();
        let mut saw_model_type = false;
        let mut seen = Vec::<String>::new();
        loop {
            let key = self.parse_ident()?.0;
            let normalized = key.to_ascii_lowercase();
            if seen.iter().any(|previous| previous == &normalized) {
                return Err(ParseError {
                    msg: "duplicate model option",
                    at: self.cur.start,
                });
            }
            seen.push(normalized.clone());
            self.expect(TokKind::Eq)?;
            match normalized.as_str() {
                "model_type" => {
                    let value = self.expect(TokKind::Str)?;
                    saw_model_type = true;
                    if !unquote(value.lex).eq_ignore_ascii_case("kmeans") {
                        return Err(ParseError {
                            msg: "only model_type 'kmeans' is supported",
                            at: value.start,
                        });
                    }
                }
                "num_clusters" => options.clusters = self.parse_u32_value()?,
                "max_iterations" => options.max_iterations = self.parse_u32_value()?,
                "seed" => options.seed = self.parse_u64()?,
                "tolerance" => options.tolerance = self.parse_f64_value()?,
                _ => {
                    return Err(ParseError {
                        msg: "unknown model option",
                        at: self.cur.start,
                    })
                }
            }
            if self.eat(TokKind::Comma)? {
                continue;
            }
            break;
        }
        self.expect(TokKind::RParen)?;
        if !saw_model_type {
            return Err(ParseError {
                msg: "model_type option is required",
                at: self.cur.start,
            });
        }
        if options.clusters == 0
            || options.max_iterations == 0
            || !options.tolerance.is_finite()
            || options.tolerance < 0.0
        {
            return Err(ParseError {
                msg: "invalid kmeans options",
                at: self.cur.start,
            });
        }
        self.expect(TokKind::KwAs)?;
        let query_start = self.cur.start;
        let query = self.parse_select()?;
        let query_end = self.cur.start;
        let query_sql = &self.lx.s[query_start..query_end];
        let _ = self.eat(TokKind::Semi)?;
        self.expect(TokKind::Eof)?;
        Ok(Statement::CreateModel {
            name,
            options,
            query: Box::new(query),
            query_sql,
        })
    }

    pub(super) fn expect_ident_word(&mut self, word: &str) -> PResult<()> {
        if self.cur.kind == TokKind::Ident && self.cur.lex.eq_ignore_ascii_case(word) {
            self.bump()
        } else {
            Err(ParseError {
                msg: "unexpected token",
                at: self.cur.start,
            })
        }
    }

    pub(super) fn parse_u32_value(&mut self) -> PResult<u32> {
        let at = self.cur.start;
        let value = self.parse_u64()?;
        u32::try_from(value).map_err(|_| ParseError {
            msg: "integer option out of range",
            at,
        })
    }

    pub(super) fn parse_f64_value(&mut self) -> PResult<f64> {
        if matches!(self.cur.kind, TokKind::Int | TokKind::Num) {
            let token = self.cur;
            self.bump()?;
            token
                .lex
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())
                .ok_or(ParseError {
                    msg: "invalid numeric option",
                    at: token.start,
                })
        } else {
            Err(ParseError {
                msg: "expected numeric option",
                at: self.cur.start,
            })
        }
    }

    pub(super) fn parse_select_list(&mut self) -> PResult<Vec<SelectItem<'a>>> {
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

    pub(super) fn parse_order_list(&mut self) -> PResult<Vec<OrderItem<'a>>> {
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

    pub(super) fn parse_expr_list(&mut self) -> PResult<Vec<Expr<'a>>> {
        let mut xs = Vec::new();
        loop {
            xs.push(self.parse_expr_bp(0)?);
            if !self.eat(TokKind::Comma)? {
                break;
            }
        }
        Ok(xs)
    }

    pub(super) fn parse_and_predicate(&mut self) -> PResult<Expr<'a>> {
        let mut left = self.parse_predicate()?;
        while self.cur.kind == TokKind::KwAnd {
            self.bump()?;
            let right = self.parse_predicate()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    pub(super) fn parse_predicate(&mut self) -> PResult<Expr<'a>> {
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
    pub(super) fn parse_expr_bp(&mut self, min_bp: u8) -> PResult<Expr<'a>> {
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

    pub(super) fn parse_primary(&mut self) -> PResult<Expr<'a>> {
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
            TokKind::Num => {
                let t = self.cur;
                self.bump()?;
                let v = t.lex.parse::<f64>().map_err(|_| ParseError {
                    msg: "bad number",
                    at: t.start,
                })?;
                Ok(Expr::Number(v))
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

    pub(super) fn parse_aggregate(&mut self) -> PResult<Expr<'a>> {
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

    pub(super) fn parse_ident(&mut self) -> PResult<Ident<'a>> {
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

    pub(super) fn parse_u64(&mut self) -> PResult<u64> {
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

pub(super) fn unquote(value: &str) -> String {
    value
        .get(1..value.len().saturating_sub(1))
        .unwrap_or("")
        .replace("''", "'")
}
