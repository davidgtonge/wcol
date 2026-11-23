//! Hand-written parser for the small SQL dialect WCOL plans from.
//! No dependencies, so it stays small in Wasm; strict about what it accepts.

mod plan;
pub use plan::{
    plan_query, AggKind, Aggregate, Column, ColumnKind, Filter, GroupSortKey, GroupSortTarget,
    OrderKey, OutputItem, OutputSource, PlanError, QueryPlan, Scalar, MAX_GROUP_KEYS,
};

mod ast;
mod lexer;
mod parser;
#[cfg(test)]
mod tests;

pub use ast::*;
use lexer::*;
use parser::*;

pub fn parse_sql_v0(input: &str) -> PResult<Query<'_>> {
    Parser::new(input)?.parse_query()
}

pub fn parse_statement(input: &str) -> PResult<Statement<'_>> {
    let mut parser = Parser::new(input)?;
    if parser.cur.kind == TokKind::Ident && parser.cur.lex.eq_ignore_ascii_case("CREATE") {
        parser.parse_create_model()
    } else {
        parser.parse_query().map(Statement::Query)
    }
}
