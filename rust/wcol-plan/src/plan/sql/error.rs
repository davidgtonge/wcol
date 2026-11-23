use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("parse: {0}")]
    Parse(String),
    #[error("plan: {0}")]
    Plan(String),
    #[error("analyze: {0}")]
    Analyze(String),
    #[error("optimize: {0}")]
    Optimize(String),
}

#[derive(Debug, Error)]
pub enum LowerError {
    #[error("unsupported logical operator: {0}")]
    UnsupportedOperator(String),
    #[error("unsupported expression: {0}")]
    UnsupportedExpression(String),
    #[error("catalog: {0}")]
    Catalog(String),
}

impl PlanError {
    pub fn plan(msg: impl Into<String>) -> Self {
        Self::Plan(msg.into())
    }
}

impl From<datafusion_common::DataFusionError> for PlanError {
    fn from(value: datafusion_common::DataFusionError) -> Self {
        Self::Plan(value.to_string())
    }
}

impl From<datafusion_common::DataFusionError> for LowerError {
    fn from(value: datafusion_common::DataFusionError) -> Self {
        Self::UnsupportedExpression(value.to_string())
    }
}
