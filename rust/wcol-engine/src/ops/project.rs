//! Project: keep `SELECT` columns in list order. Rows stay dense; the selection mask is copied.

use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::stream::{OwnedWcolBatch, WcolColumn};

pub struct ProjectOp {
    output_ids: Vec<u32>,
}

impl ProjectOp {
    pub fn from_columns(output_ids: Vec<u32>) -> Result<Self, String> {
        if output_ids.is_empty() {
            return Err("project has no output columns".to_string());
        }
        Ok(Self { output_ids })
    }

    pub fn output_ids(&self) -> &[u32] {
        &self.output_ids
    }

    /// Copy selected columns. Does not compact rows; the selection mask is preserved.
    pub fn project_batch(&self, batch: &OwnedWcolBatch) -> Result<OwnedWcolBatch, String> {
        let mut col_view = Vec::new();
        let view = batch.view_with(&mut col_view);
        let mut out = OwnedWcolBatch::empty();
        out.set_scope(view.dict_scope);
        for id in &self.output_ids {
            let col = view
                .columns
                .iter()
                .find(|col| column_id(col) == *id)
                .ok_or_else(|| format!("project column {id} was not decoded"))?;
            match col {
                WcolColumn::I64 { values, nulls, .. } => {
                    out.push_i64_column(*id, values.to_vec(), nulls.to_vec())
                }
                WcolColumn::F64 { values, nulls, .. } => {
                    out.push_f64_column(*id, values.to_vec(), nulls.to_vec())
                }
                WcolColumn::DictId {
                    ids,
                    nulls,
                    dictionary,
                    ..
                } => out.push_dict_id_column(
                    *id,
                    ids.to_vec(),
                    nulls.to_vec(),
                    (*dictionary).clone(),
                ),
                WcolColumn::Utf8 {
                    offsets,
                    bytes,
                    nulls,
                    ..
                } => out.push_utf8_column(*id, offsets.to_vec(), bytes.to_vec(), nulls.to_vec()),
            }
        }
        out.set_row_count(batch.row_count())
            .map_err(|e| e.to_string())?;
        out.set_selection_mask(batch.effective_selection_mask());
        Ok(out)
    }
}

fn column_id(col: &WcolColumn<'_>) -> u32 {
    match col {
        WcolColumn::I64 { column, .. }
        | WcolColumn::F64 { column, .. }
        | WcolColumn::DictId { column, .. }
        | WcolColumn::Utf8 { column, .. } => *column,
    }
}

impl Operator for ProjectOp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn poll(&mut self, io: &mut OperatorIo<'_, '_>) -> Result<StreamProgress, String> {
        if !io.output_slot_free() {
            return Ok(StreamProgress::Blocked);
        }
        if let Some(slot) = io.input_slot.as_mut() {
            if let Some(item) = slot.take() {
                let projected = self.project_batch(item.batch())?;
                *io.output = Some(WcolStreamItem::from_row_batch(projected));
                return Ok(StreamProgress::HaveOutput);
            }
        } else if let Some(item) = io.input {
            let projected = self.project_batch(item.batch())?;
            *io.output = Some(WcolStreamItem::from_row_batch(projected));
            return Ok(StreamProgress::HaveOutput);
        }
        if io.input_eof {
            return Ok(StreamProgress::Finished);
        }
        Ok(StreamProgress::NeedInput)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{CatalogColumn, CmpOp, ColumnKind, Predicate, Scalar};

    fn catalog() -> Vec<CatalogColumn> {
        vec![
            CatalogColumn {
                name: "AdvEngineID".into(),
                kind: ColumnKind::Number,
                physical: 1,
                type_length: 0,
                max_def: 0,
                max_rep: 0,
            },
            CatalogColumn {
                name: "ResolutionWidth".into(),
                kind: ColumnKind::Number,
                physical: 1,
                type_length: 0,
                max_def: 0,
                max_rep: 0,
            },
            CatalogColumn {
                name: "ResolutionHeight".into(),
                kind: ColumnKind::Number,
                physical: 1,
                type_length: 0,
                max_def: 0,
                max_rep: 0,
            },
            CatalogColumn {
                name: "RegionID".into(),
                kind: ColumnKind::Number,
                physical: 1,
                type_length: 0,
                max_def: 0,
                max_rep: 0,
            },
        ]
    }

    #[test]
    fn project_drops_filter_column_and_keeps_mask() {
        let mut filter = crate::ops::FilterOp::from_predicates(
            &[Predicate::Compare {
                column: 0,
                op: CmpOp::Ne,
                value: Scalar::Int(0),
            }],
            &catalog(),
        )
        .unwrap();
        let project = ProjectOp::from_columns(vec![1, 2, 3]).unwrap();
        assert_eq!(project.output_ids(), &[1, 2, 3]);
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, vec![0, 2, 0, 5], vec![1, 1, 1, 1]);
        batch.push_i64_column(1, vec![10, 20, 30, 40], vec![1, 1, 1, 1]);
        batch.push_i64_column(2, vec![11, 21, 31, 41], vec![1, 1, 1, 1]);
        batch.push_i64_column(3, vec![1, 2, 3, 4], vec![1, 1, 1, 1]);
        batch.set_row_count(4).unwrap();
        filter.apply_filter_in_place(&mut batch).unwrap();
        let out = project.project_batch(&batch).unwrap();
        assert_eq!(out.row_count(), 4);
        assert_eq!(out.effective_selection_mask().selected_count(), 2);
        let cols = out.export_i64_columns().unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].0, 1);
        assert_eq!(cols[0].1, vec![10, 20, 30, 40]);
        assert_eq!(cols[2].0, 3);
    }
}
