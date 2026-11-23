use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::planner::ContextProvider;
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::{Analyzer, Optimizer, OptimizerConfig};
use datafusion_sql::planner::{NullOrdering, ParserOptions, SqlToRel};
use datafusion_sql::sqlparser::ast::{visit_relations, ObjectName};
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;

use super::catalog::WcolCatalog;
use super::error::PlanError;
use super::required::required_source_columns;

#[derive(Clone, Debug)]
pub struct PlannedQuery {
    pub parsed_statement: String,
    pub raw_logical: String,
    pub analyzed_logical: String,
    pub optimized_logical: String,
    pub required_columns: Vec<String>,
    pub optimized: Arc<LogicalPlan>,
}

/// Table names in `sql`, in first-seen order, taken from the statement DataFusion parses.
/// Repeated references to the same relation are one entry.
pub fn statement_relations(sql: &str) -> Result<Vec<String>, PlanError> {
    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| PlanError::Parse(e.to_string()))?;
    if statements.len() != 1 {
        return Err(PlanError::Parse(format!(
            "expected exactly one statement, got {}",
            statements.len()
        )));
    }
    let mut names = Vec::new();
    let _ = visit_relations(&statements, |relation| {
        let name = relation_name(relation);
        if !names.iter().any(|existing: &String| existing == &name) {
            names.push(name);
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(names)
}

struct NoClock {
    options: Arc<ConfigOptions>,
    aliases: Arc<AliasGenerator>,
}

impl NoClock {
    fn new() -> Self {
        let mut options = ConfigOptions::default();
        options.optimizer.filter_null_join_keys = true;
        Self {
            options: Arc::new(options),
            aliases: Arc::new(AliasGenerator::new()),
        }
    }
}

impl OptimizerConfig for NoClock {
    fn query_execution_start_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        None
    }

    fn alias_generator(&self) -> &Arc<AliasGenerator> {
        &self.aliases
    }

    fn options(&self) -> Arc<ConfigOptions> {
        Arc::clone(&self.options)
    }
}

fn relation_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect::<Vec<_>>()
        .join(".")
}

pub fn plan_sql(sql: &str, catalog: &WcolCatalog) -> Result<PlannedQuery, PlanError> {
    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| PlanError::Parse(e.to_string()))?;
    if statements.len() != 1 {
        return Err(PlanError::Parse(format!(
            "expected exactly one statement, got {}",
            statements.len()
        )));
    }
    let parsed_statement = statements[0].to_string();

    // DuckDB's rule, which the engine matches: NULLs sort last in both directions unless the
    // query says otherwise. DataFusion's default follows Postgres (NULLS FIRST for DESC).
    let options = ParserOptions::from(&catalog.options().sql_parser)
        .with_default_null_ordering(NullOrdering::NullsLast);
    let sql_to_rel = SqlToRel::new_with_options(catalog, options);
    let raw = sql_to_rel
        .sql_statement_to_plan(statements[0].clone())
        .map_err(|e| PlanError::Plan(e.to_string()))?;
    let raw_logical = raw.display_indent().to_string();

    // No `OptimizerContext`: its constructor reads the clock, and this wasm target has none.
    // ClickBench planning does not fold `now()`.
    let opt_ctx = NoClock::new();
    let analyzed = Analyzer::new()
        .execute_and_check(raw, &opt_ctx.options(), |_, _| {})
        .map_err(|e| PlanError::Analyze(e.to_string()))?;
    let analyzed_logical = analyzed.display_indent().to_string();

    let optimized = {
        let mut rules = Optimizer::new().rules;
        rules.retain(|rule| rule.name() != "common_sub_expression_eliminate");
        Optimizer::with_rules(rules)
            .optimize(analyzed, &opt_ctx, |_, _| {})
            .map_err(|e| PlanError::Optimize(e.to_string()))?
    };
    let optimized_logical = optimized.display_indent().to_string();
    let required_columns = required_source_columns(&optimized);

    Ok(PlannedQuery {
        parsed_statement,
        raw_logical,
        analyzed_logical,
        optimized_logical,
        required_columns,
        optimized: Arc::new(optimized),
    })
}
