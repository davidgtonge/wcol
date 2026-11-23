use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, TableReference, plan_err};
use datafusion_expr::logical_plan::builder::LogicalTableSource;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{
    AggregateUDF, HigherOrderUDF, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_expr::test::function_stub::{avg_udaf, count_udaf, max_udaf, min_udaf, sum_udaf};
use datafusion_sql::planner::ContextProvider;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CatalogColumnMeta {
    pub name: String,
    pub physical: i32,
    pub type_length: i32,
    pub max_def: u8,
    pub max_rep: u8,
}

#[derive(Clone)]
pub struct PlannerCatalog {
    tables: HashMap<String, Arc<dyn TableSource>>,
    options: ConfigOptions,
    udafs: HashMap<String, Arc<AggregateUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl PlannerCatalog {
    pub fn single_table(table: &str, columns: &[CatalogColumnMeta]) -> Self {
        let fields = columns
            .iter()
            .map(|col| {
                Field::new(
                    col.name.clone(),
                    parquet_physical_to_arrow(col),
                    col.max_def > 0,
                )
            })
            .collect::<Vec<_>>();
        let mut tables = HashMap::new();
        tables.insert(
            table.to_string(),
            Arc::new(LogicalTableSource::new(Arc::new(Schema::new(fields)))) as Arc<dyn TableSource>,
        );
        let mut options = ConfigOptions::default();
        options.sql_parser.enable_ident_normalization = false;
        let mut udafs = HashMap::new();
        for udaf in [count_udaf(), sum_udaf(), avg_udaf(), min_udaf(), max_udaf()] {
            let arc = udaf;
            udafs.insert(arc.name().to_string(), Arc::clone(&arc));
            for alias in arc.aliases() {
                udafs.insert(alias.to_string(), Arc::clone(&arc));
            }
            // SqlToRel often resolves lowercase unquoted function names.
            udafs.insert(arc.name().to_lowercase(), Arc::clone(&arc));
        }
        Self {
            tables,
            options,
            udafs,
            expr_planners: Vec::new(),
        }
    }
}

pub fn parquet_physical_to_arrow(col: &CatalogColumnMeta) -> DataType {
    match col.physical {
        0 => DataType::Boolean,
        1 => DataType::Int32,
        2 => DataType::Int64,
        3 => DataType::Timestamp(datafusion_common::arrow::datatypes::TimeUnit::Nanosecond, None),
        4 => DataType::Float32,
        5 => DataType::Float64,
        6 => DataType::Utf8,
        7 if col.type_length == 4 => DataType::Date32,
        7 => DataType::FixedSizeBinary(col.type_length),
        _ => DataType::Utf8,
    }
}

impl ContextProvider for PlannerCatalog {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(Arc::clone(table)),
            None => plan_err!("Table not found: {}", name.table()),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_higher_order_meta(&self, _name: &str) -> Option<Arc<HigherOrderUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.udafs.get(name).cloned()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn higher_order_function_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.udafs.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
}
