//! DataFusion parse → analyze → optimize → [`LogicalPlan`] (no wcol, no lowering, no execution).

mod catalog;
mod error;
mod frontend;
mod required;

pub use catalog::{parquet_physical_to_arrow, CatalogColumnMeta, PlannerCatalog};
pub use error::PlanError;
pub use frontend::{plan_sql, PlannedQuery};
pub use required::required_source_columns;
