use crate::constants::{FLAG_DICT, TYPE_STRING};
use crate::runtime::{dict_value_bytes, is_valid, read_value_f64};
use crate::types::{Column, ColumnData, Dictionary, Plan, RowKey, Runtime};

pub(crate) struct OrderCol<'a> {
    pub(crate) col: &'a Column,
    pub(crate) data: &'a ColumnData,
    pub(crate) nulls: Option<&'a [u8]>,
    pub(crate) dict: Option<&'a Dictionary>,
    pub(crate) lex_ranks: Option<&'a [u32]>,
}

pub(crate) fn build_dict_lex_ranks(dict: &Dictionary) -> Vec<u32> {
    let value_count = if !dict.offsets.is_empty() {
        dict.offsets.len().saturating_sub(1)
    } else {
        dict.values.len()
    };
    let mut ids: Vec<usize> = (0..value_count).collect();
    ids.sort_unstable_by(|a, b| {
        let a_bytes = dict_value_bytes(dict, *a).unwrap_or(&[]);
        let b_bytes = dict_value_bytes(dict, *b).unwrap_or(&[]);
        a_bytes.cmp(b_bytes).then_with(|| a.cmp(b))
    });
    let mut ranks = vec![0u32; value_count];
    for (rank, id) in ids.into_iter().enumerate() {
        ranks[id] = rank as u32;
    }
    ranks
}

pub(crate) fn ensure_row_order_lex_ranks(
    plan: &mut Plan,
    runtime: &Runtime,
    schema: &[Column],
    order_col_ids: &[u32],
) {
    for &col_id in order_col_ids {
        let col = match schema.get(col_id as usize) {
            Some(c) => c,
            None => continue,
        };
        if col.logical_type == TYPE_STRING
            && (col.flags & FLAG_DICT) != 0
            && !plan.row_order_lex_ranks.contains_key(&col.dict_id)
        {
            if let Some(dict) = runtime.dicts.get(&col.dict_id) {
                plan.row_order_lex_ranks
                    .insert(col.dict_id, build_dict_lex_ranks(dict));
            }
        }
    }
}

pub(crate) fn read_order_key(order_col: &OrderCol<'_>, row: usize) -> RowKey {
    if let Some(nulls) = order_col.nulls {
        if !is_valid(nulls, row) {
            return RowKey::Null;
        }
    }
    if (order_col.col.flags & FLAG_DICT) != 0 || order_col.col.logical_type == TYPE_STRING {
        let id = match order_col.data {
            ColumnData::U8(values) => *values.get(row).unwrap_or(&0) as usize,
            ColumnData::U16(values) => *values.get(row).unwrap_or(&0) as usize,
            ColumnData::U32(values) => *values.get(row).unwrap_or(&0) as usize,
            _ => 0usize,
        };
        if let Some(ranks) = order_col.lex_ranks {
            let rank = ranks.get(id).copied().unwrap_or(u32::MAX);
            return RowKey::Num(rank as f64);
        }
        let bytes = order_col
            .dict
            .and_then(|dict| dict_value_bytes(dict, id))
            .unwrap_or(&[]);
        return RowKey::Bytes(bytes.to_vec());
    }
    RowKey::Num(read_value_f64(order_col.col, order_col.data, row))
}
