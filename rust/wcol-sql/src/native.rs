#[cfg(feature = "native")]
use wcol::formats::parquet::schema::Leaf;

use wcol::plan::sql::{WcolCatalog, WcolColumnMeta};

#[cfg(feature = "native")]
pub fn catalog_from_wcol_leaves(table: &str, leaves: &[Leaf]) -> WcolCatalog {
    let columns = leaves
        .iter()
        .map(|leaf| WcolColumnMeta {
            name: leaf.name.clone(),
            physical: leaf.physical,
            type_length: leaf.type_length,
            max_def: leaf.max_def,
            max_rep: leaf.max_rep,
        })
        .collect::<Vec<_>>();
    WcolCatalog::single_table(table, &columns)
}

pub fn catalog_from_column_meta(table: &str, columns: &[WcolColumnMeta]) -> WcolCatalog {
    WcolCatalog::single_table(table, columns)
}
