//! Tokens and the lexer.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TokKind {
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
    /// `1.5` or `1e308`. Integers stay [`TokKind::Int`] so values above 2^53 stay exact.
    Num,
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
pub(super) struct Tok<'a> {
    pub(super) kind: TokKind,
    pub(super) start: usize,
    #[allow(dead_code)]
    pub(super) end: usize,
    pub(super) lex: &'a str,
}

pub(super) struct Lexer<'a> {
    pub(super) s: &'a str,
    pub(super) b: &'a [u8],
    pub(super) i: usize,
    pub(super) len: usize,
}

impl<'a> Lexer<'a> {
    pub(super) fn new(s: &'a str) -> Self {
        Self {
            s,
            b: s.as_bytes(),
            i: 0,
            len: s.len(),
        }
    }

    pub(super) fn peek_byte(&self) -> Option<u8> {
        if self.i < self.len {
            Some(self.b[self.i])
        } else {
            None
        }
    }

    pub(super) fn bump(&mut self) -> Option<u8> {
        if self.i < self.len {
            let c = self.b[self.i];
            self.i += 1;
            Some(c)
        } else {
            None
        }
    }

    pub(super) fn skip_ws(&mut self) {
        while let Some(c) = self.peek_byte() {
            if c == b' ' || c == b'\n' || c == b'\r' || c == b'\t' {
                self.i += 1;
            } else {
                break;
            }
        }
    }

    pub(super) fn next(&mut self) -> PResult<Tok<'a>> {
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
                let mut is_float = false;
                if self.peek_byte() == Some(b'.')
                    && self.b.get(self.i + 1).is_some_and(|d| is_digit(*d))
                {
                    is_float = true;
                    self.i += 1;
                    while let Some(n) = self.peek_byte() {
                        if is_digit(n) {
                            self.i += 1;
                        } else {
                            break;
                        }
                    }
                }
                if matches!(self.peek_byte(), Some(b'e' | b'E')) {
                    let mut exp = self.i + 1;
                    if matches!(self.b.get(exp).copied(), Some(b'+' | b'-')) {
                        exp += 1;
                    }
                    if self.b.get(exp).is_some_and(|d| is_digit(*d)) {
                        is_float = true;
                        self.i = exp + 1;
                        while let Some(n) = self.peek_byte() {
                            if is_digit(n) {
                                self.i += 1;
                            } else {
                                break;
                            }
                        }
                    }
                }
                if is_float {
                    TokKind::Num
                } else {
                    TokKind::Int
                }
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

pub(super) fn is_ident_start(c: u8) -> bool {
    c.is_ascii_lowercase() || c.is_ascii_uppercase() || c == b'_'
}
pub(super) fn is_ident_continue(c: u8) -> bool {
    is_ident_start(c) || is_digit(c)
}
pub(super) fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

pub(super) fn eq_kw(lex: &str, kw: &str) -> bool {
    // ASCII case-insensitive, no alloc
    lex.eq_ignore_ascii_case(kw)
}

pub(super) fn keyword_kind(lex: &str) -> Option<TokKind> {
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
