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
