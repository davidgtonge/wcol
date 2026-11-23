//! Colocated multi-operator driver (owned batches + selection masks, no WCS1).

use crate::exec::operator::{Operator, OperatorIo, StreamProgress, WcolStreamItem};
use crate::stream::{OwnedWcolBatch, WcolColumn};

/// Push row batches through `op`; returns downstream items (usually one partial after EOF).
pub fn drive_operator(
    op: &mut dyn Operator,
    inputs: Vec<WcolStreamItem>,
    finish_with_eof: bool,
) -> Result<Vec<WcolStreamItem>, String> {
    let mut outputs = Vec::new();
    for item in inputs {
        pump_once(op, Some(item), false, &mut outputs)?;
    }
    if finish_with_eof {
        pump_once(op, None, true, &mut outputs)?;
    }
    Ok(outputs)
}

fn pump_once(
    op: &mut dyn Operator,
    mut input: Option<WcolStreamItem>,
    input_eof: bool,
    outputs: &mut Vec<WcolStreamItem>,
) -> Result<(), String> {
    loop {
        let mut out_slot = None;
        let mut io = OperatorIo {
            input: None,
            input_slot: Some(&mut input),
            input_eof,
            output: &mut out_slot,
            host_bytes: None,
            output_port: None,
            bytes_written: &mut 0,
        };
        match op.poll(&mut io)? {
            StreamProgress::NeedInput => return Ok(()),
            StreamProgress::HaveOutput => {
                if let Some(item) = out_slot.take() {
                    outputs.push(item);
                }
                return Ok(());
            }
            StreamProgress::Blocked => {
                return Err("colocated driver hit backpressure".to_string());
            }
            StreamProgress::NeedHostRead { .. } => {
                return Err("host read not supported in drive_operator".to_string());
            }
            StreamProgress::Finished => return Ok(()),
        }
    }
}

pub fn row_batches(items: &[OwnedWcolBatch]) -> Vec<WcolStreamItem> {
    items
        .iter()
        .map(|b| WcolStreamItem::from_row_batch(clone_row_batch(b)))
        .collect()
}

pub(crate) fn clone_row_batch(batch: &OwnedWcolBatch) -> OwnedWcolBatch {
    let mut col_view = Vec::new();
    let view = batch.view_with(&mut col_view);
    let selected = view.row_count;
    let mut out = OwnedWcolBatch::empty();
    out.set_scope(view.dict_scope);
    for col in view.columns {
        match col {
            WcolColumn::I64 {
                column,
                values,
                nulls,
            } => out.push_i64_column(*column, values.to_vec(), nulls.to_vec()),
            WcolColumn::F64 {
                column,
                values,
                nulls,
            } => out.push_f64_column(*column, values.to_vec(), nulls.to_vec()),
            WcolColumn::DictId {
                column,
                ids,
                nulls,
                dictionary,
            } => out.push_dict_id_column(
                *column,
                ids.to_vec(),
                nulls.to_vec(),
                (*dictionary).clone(),
            ),
            WcolColumn::Utf8 {
                column,
                offsets,
                bytes,
                nulls,
            } => out.push_utf8_column(*column, offsets.to_vec(), bytes.to_vec(), nulls.to_vec()),
        }
    }
    out.set_row_count(selected).expect("clone batch");
    out.copy_stream_meta_from(batch);
    out
}
