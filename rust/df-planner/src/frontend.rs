use std::sync::Arc;

use datafusion_expr::LogicalPlan;
use datafusion_optimizer::{Analyzer, Optimizer, OptimizerConfig, OptimizerContext};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;

use crate::catalog::PlannerCatalog;
use crate::error::PlanError;
use crate::required::required_source_columns;

#[derive(Clone, Debug)]
pub struct PlannedQuery {
    pub parsed_statement: String,
    pub raw_logical: String,
    pub analyzed_logical: String,
    pub optimized_logical: String,
    pub required_columns: Vec<String>,
    pub optimized: Arc<LogicalPlan>,
}

pub fn plan_sql(sql: &str, catalog: &PlannerCatalog) -> Result<PlannedQuery, PlanError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| PlanError::Parse(e.to_string()))?;
    if statements.len() != 1 {
        return Err(PlanError::Parse(format!(
            "expected exactly one statement, got {}",
            statements.len()
        )));
    }
    let parsed_statement = statements[0].to_string();

    let sql_to_rel = SqlToRel::new(catalog);
    let raw = sql_to_rel
        .sql_statement_to_plan(statements[0].clone())
        .map_err(|e| PlanError::Plan(e.to_string()))?;
    let raw_logical = raw.display_indent().to_string();

    let opt_ctx = OptimizerContext::default();
    let analyzed = Analyzer::new()
        .execute_and_check(raw, &opt_ctx.options(), |_, _| {})
        .map_err(|e| PlanError::Analyze(e.to_string()))?;
    let analyzed_logical = analyzed.display_indent().to_string();

    let optimized = Optimizer::new()
        .optimize(analyzed, &opt_ctx, |_, _| {})
        .map_err(|e| PlanError::Optimize(e.to_string()))?;
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
