//! Filter: row WCOL → same batch + [`SelectionMask`] (no column compaction).

use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::ir::{CatalogColumn, Predicate};
use crate::ops::query::batch::BatchFilter;
use crate::ops::query::SelectionMask;
use crate::stream::OwnedWcolBatch;

pub struct FilterOp {
    filter: BatchFilter,
}

impl FilterOp {
    pub fn from_predicates(
        predicates: &[Predicate],
        catalog: &[CatalogColumn],
    ) -> Result<Self, String> {
        let filter = BatchFilter::from_predicates(predicates, catalog)?
            .ok_or_else(|| "filter is not supported by the batch kernel".to_string())?;
        Ok(Self { filter })
    }

    pub fn from_batch(filter: BatchFilter) -> Self {
        Self { filter }
    }

    pub fn is_empty(&self) -> bool {
        self.filter.is_empty()
    }

    pub fn selection_mask_for_batch(
        &mut self,
        batch: &OwnedWcolBatch,
    ) -> Result<SelectionMask, String> {
        let mut col_view = Vec::new();
        let view = batch.view_with(&mut col_view);
        self.filter.selection(&view)
    }

    pub fn apply_filter_in_place(&mut self, batch: &mut OwnedWcolBatch) -> Result<(), String> {
        let mask = self.selection_mask_for_batch(batch)?;
        batch.set_selection_mask(mask);
        Ok(())
    }
}

impl Operator for FilterOp {
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
                let mut batch = item.into_batch();
                self.apply_filter_in_place(&mut batch)?;
                *io.output = Some(WcolStreamItem::from_row_batch(batch));
                return Ok(StreamProgress::HaveOutput);
            }
        } else if io.input_item().is_some() {
            return Err(
                "filter requires OperatorIo::input_slot (zero-copy batch move)".to_string(),
            );
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
    use crate::ir::{CmpOp, ColumnKind, Predicate, Scalar};

    fn catalog() -> Vec<CatalogColumn> {
        vec![CatalogColumn {
            name: "a".into(),
            kind: ColumnKind::Number,
            physical: 0,
            type_length: 0,
            max_def: 0,
            max_rep: 0,
        }]
    }

    #[test]
    fn filter_attaches_mask_without_shrinking_columns() {
        let mut filter = FilterOp::from_predicates(
            &[Predicate::Compare {
                column: 0,
                op: CmpOp::Gt,
                value: Scalar::Int(5),
            }],
            &catalog(),
        )
        .unwrap();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(0, vec![3, 7, 9], vec![1, 1, 1]);
        batch.set_row_count(3).unwrap();
        filter.apply_filter_in_place(&mut batch).unwrap();
        assert_eq!(batch.row_count(), 3);
        assert_eq!(batch.effective_selection_mask().selected_count(), 2);
        let col = batch.export_i64_columns().unwrap();
        assert_eq!(col[0].1.len(), 3);
    }

    #[test]
    fn bigint_literals_above_2_pow_53_stay_distinct() {
        let mut filter = FilterOp::from_predicates(
            &[Predicate::Compare {
                column: 0,
                op: CmpOp::Eq,
                value: Scalar::Int(9_007_199_254_740_993),
            }],
            &catalog(),
        )
        .unwrap();
        let mut batch = OwnedWcolBatch::empty();
        batch.push_i64_column(
            0,
            vec![9_007_199_254_740_992, 9_007_199_254_740_993],
            vec![1, 1],
        );
        batch.set_row_count(2).unwrap();
        filter.apply_filter_in_place(&mut batch).unwrap();
        assert!(batch.effective_selection_mask().is_selected(1));
        assert!(!batch.effective_selection_mask().is_selected(0));
    }

    #[test]
    fn like_on_a_numeric_column_fails_when_the_filter_opens() {
        let opened = FilterOp::from_predicates(
            &[Predicate::Like {
                column: 0,
                pattern: "%x".into(),
                negated: false,
            }],
            &catalog(),
        );
        match opened {
            Err(err) => assert!(err.contains("LIKE"), "{err}"),
            Ok(_) => panic!("LIKE on a numeric column was accepted"),
        }
    }
}
