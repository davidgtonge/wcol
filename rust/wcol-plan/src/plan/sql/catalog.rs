use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, Result, TableReference};
use datafusion_expr::logical_plan::builder::LogicalTableSource;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion_functions::core::planner::CoreFunctionPlanner;
use datafusion_functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_sql::planner::ContextProvider;

/// Parquet leaf metadata aligned with wcol [`CatalogColumn`] / footer `Leaf`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WcolColumnMeta {
    pub name: String,
    pub physical: i32,
    pub type_length: i32,
    pub max_def: u8,
    pub max_rep: u8,
}

/// Schema-only catalog for DataFusion planning (no table providers, no I/O).
#[derive(Clone)]
pub struct WcolCatalog {
    tables: HashMap<String, Arc<dyn TableSource>>,
    columns_by_table: HashMap<String, Vec<WcolColumnMeta>>,
    options: ConfigOptions,
    udafs: HashMap<String, Arc<AggregateUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl WcolCatalog {
    pub fn single_table(table: &str, columns: &[WcolColumnMeta]) -> Self {
        let mut tables = HashMap::new();
        let mut columns_by_table = HashMap::new();
        insert_table(&mut tables, &mut columns_by_table, table, columns);
        let mut options = ConfigOptions::default();
        // ClickBench / wcol SQL uses mixed-case column names (AdvEngineID, …).
        options.sql_parser.enable_ident_normalization = false;
        let mut udafs = HashMap::new();
        for udaf in [count_udaf(), sum_udaf(), avg_udaf(), min_udaf(), max_udaf()] {
            udafs.insert(udaf.name().to_string(), udaf);
        }
        let approx = approx_distinct_udaf();
        udafs.insert("approx_count_distinct".into(), approx.clone());
        udafs.insert(approx.name().to_string(), approx);
        Self {
            tables,
            columns_by_table,
            options,
            udafs,
            expr_planners: vec![Arc::new(CoreFunctionPlanner::default())],
        }
    }

    pub fn with_table(mut self, table: &str, columns: &[WcolColumnMeta]) -> Self {
        insert_table(&mut self.tables, &mut self.columns_by_table, table, columns);
        self
    }

    pub fn column_index(&self, table: &str, name: &str) -> Option<u32> {
        self.columns_by_table
            .get(table)
            .and_then(|cols| cols.iter().position(|c| c.name == name).map(|i| i as u32))
    }

    pub fn table_columns(&self, table: &str) -> Option<&[WcolColumnMeta]> {
        self.columns_by_table.get(table).map(|v| v.as_slice())
    }
}

/// Map Parquet physical type + wcol footer metadata to Arrow types for SQL analysis.
pub fn parquet_physical_to_arrow(col: &WcolColumnMeta) -> DataType {
    let nullable = col.max_def > 0;
    let ty = if col.name == "EventDate" || col.name.ends_with("Date") {
        if col.physical == 1 || (col.physical == 7 && col.type_length == 4) {
            DataType::Date32
        } else {
            map_physical(col)
        }
    } else {
        map_physical(col)
    };
    if nullable && !matches!(ty, DataType::Utf8) {
        // Arrow field nullability is on Field; keep type as-is
        ty
    } else {
        ty
    }
}

fn map_physical(col: &WcolColumnMeta) -> DataType {
    match col.physical {
        0 => DataType::Boolean,
        1 => DataType::Int32,
        2 => DataType::Int64,
        3 => DataType::Timestamp(
            datafusion_common::arrow::datatypes::TimeUnit::Nanosecond,
            None,
        ),
        4 => DataType::Float32,
        5 => DataType::Float64,
        6 => DataType::Utf8,
        7 if col.type_length == 4 => DataType::Date32,
        7 => DataType::FixedSizeBinary(col.type_length),
        _ => DataType::Utf8,
    }
}

fn insert_table(
    tables: &mut HashMap<String, Arc<dyn TableSource>>,
    columns_by_table: &mut HashMap<String, Vec<WcolColumnMeta>>,
    table: &str,
    columns: &[WcolColumnMeta],
) {
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
    tables.insert(table.to_string(), create_table_source(fields));
    columns_by_table.insert(table.to_string(), columns.to_vec());
}

fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(Schema::new(fields))))
}

impl ContextProvider for WcolCatalog {
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
