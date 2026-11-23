//! Local streaming hash equijoin (build → probe).

use rustc_hash::FxHashMap;

use crate::exec::operator::{StreamProgress, WcolStreamItem};
use crate::ops::exchange::{
    hash_row_keys_at, join_key_is_null, join_keys_equal, WCOL_EXCHANGE_HASH_VERSION_DEFAULT,
};
use crate::stream::batch::WcolColumn;
use crate::stream::OwnedWcolBatch;

pub const JOIN_BUILD_TOO_LARGE: &str = "JOIN_BUILD_TOO_LARGE";

pub const DEFAULT_JOIN_BUILD_MEMORY_LIMIT: usize = 48 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashJoinPlan {
    pub build_keys: Vec<u32>,
    pub probe_keys: Vec<u32>,
    pub build_columns: Vec<u32>,
    pub probe_columns: Vec<u32>,
    pub hash_version: u32,
    pub build_memory_limit_bytes: usize,
    pub max_output_batch_rows: usize,
}

impl HashJoinPlan {
    pub fn equijoin(
        keys: Vec<u32>,
        build_columns: Vec<u32>,
        probe_columns: Vec<u32>,
    ) -> Result<Self, String> {
        if keys.is_empty() {
            return Err("join requires at least one key column".to_string());
        }
        let build_columns = union_columns(&keys, build_columns);
        let probe_columns = union_columns(&keys, probe_columns);
        Ok(Self {
            build_keys: keys.clone(),
            probe_keys: keys,
            build_columns,
            probe_columns,
            hash_version: WCOL_EXCHANGE_HASH_VERSION_DEFAULT,
            build_memory_limit_bytes: DEFAULT_JOIN_BUILD_MEMORY_LIMIT,
            max_output_batch_rows: 4096,
        })
    }
}

fn union_columns(keys: &[u32], mut columns: Vec<u32>) -> Vec<u32> {
    for &key in keys {
        if !columns.contains(&key) {
            columns.push(key);
        }
    }
    columns.sort_unstable();
    columns
}

pub struct HashJoinIo<'b> {
    pub build_input_slot: Option<&'b mut Option<WcolStreamItem>>,
    pub build_eof: bool,
    pub probe_input_slot: Option<&'b mut Option<WcolStreamItem>>,
    pub probe_eof: bool,
    pub output: &'b mut Option<WcolStreamItem>,
}

pub struct HashJoinOp {
    plan: HashJoinPlan,
    build_table: FxHashMap<u64, Vec<u32>>,
    build_rows: OwnedWcolBatch,
    build_bytes: usize,
    build_complete: bool,
    build_eof_seen: bool,
    probe_eof_seen: bool,
    finished: bool,
    pending_probe: Option<OwnedWcolBatch>,
    pending_probe_row: usize,
    output_builder: OwnedWcolBatch,
}

impl HashJoinOp {
    pub fn new(plan: HashJoinPlan) -> Result<Self, String> {
        if plan.build_keys.len() != plan.probe_keys.len() {
            return Err("join build/probe key counts differ".to_string());
        }
        if plan.build_memory_limit_bytes == 0 {
            return Err("join build memory limit must be > 0".to_string());
        }
        Ok(Self {
            plan,
            build_table: FxHashMap::default(),
            build_rows: OwnedWcolBatch::empty(),
            build_bytes: 0,
            build_complete: false,
            build_eof_seen: false,
            probe_eof_seen: false,
            finished: false,
            pending_probe: None,
            pending_probe_row: 0,
            output_builder: OwnedWcolBatch::empty(),
        })
    }

    pub fn build_complete(&self) -> bool {
        self.build_complete
    }

    pub fn build_row_count(&self) -> usize {
        self.build_rows.row_count()
    }

    pub fn poll(&mut self, io: &mut HashJoinIo<'_>) -> Result<StreamProgress, String> {
        if self.finished {
            return Ok(StreamProgress::Finished);
        }
        if io.output.is_some() {
            return Ok(StreamProgress::Blocked);
        }

        if io.probe_input_slot.as_ref().is_some_and(|s| s.is_some()) && !self.build_complete {
            return Err("probe before build complete".to_string());
        }

        if !self.build_complete {
            if let Some(slot) = io.build_input_slot.as_mut() {
                if let Some(item) = slot.take() {
                    self.ingest_build_batch(item.into_batch())?;
                    return Ok(StreamProgress::NeedInput);
                }
            }
            if io.build_eof && !self.build_eof_seen {
                self.build_eof_seen = true;
                self.build_complete = true;
            }
            if !self.build_complete {
                return Ok(StreamProgress::NeedInput);
            }
        }

        if let Some(slot) = io.probe_input_slot.as_mut() {
            if let Some(item) = slot.take() {
                self.pending_probe = Some(item.into_batch());
                self.pending_probe_row = 0;
            }
        }

        if self.pending_probe.is_some() {
            let probe = self.pending_probe.take().unwrap();
            let (emitted, back) = self.emit_matches_for_probe(probe, io)?;
            self.pending_probe = back;
            if emitted {
                return Ok(StreamProgress::HaveOutput);
            }
            if self.pending_probe.is_none() {
                // probe batch fully processed
            }
        }

        if io.probe_eof && !self.probe_eof_seen {
            self.probe_eof_seen = true;
        }

        if self.probe_eof_seen && self.pending_probe.is_none() {
            if self.output_builder.row_count() > 0 {
                let out = std::mem::take(&mut self.output_builder);
                *io.output = Some(WcolStreamItem::from_row_batch(out));
                return Ok(StreamProgress::HaveOutput);
            }
            self.finished = true;
            return Ok(StreamProgress::Finished);
        }

        Ok(StreamProgress::NeedInput)
    }

    fn ingest_build_batch(&mut self, batch: OwnedWcolBatch) -> Result<(), String> {
        if batch.is_partial_batch() {
            return Err("join build expects row batches".to_string());
        }
        let projected = project_columns(&batch, &self.plan.build_columns)?;
        let mask = projected.effective_selection_mask();
        let mut view_buf = Vec::new();
        let view = projected.view_with(&mut view_buf);

        for row in 0..view.row_count {
            if !mask.is_selected(row) {
                continue;
            }
            if join_key_is_null(&view, &self.plan.build_keys, row)? {
                continue;
            }
            let stored_row = self.build_rows.row_count();
            self.build_rows
                .append_rows_from(&projected, &[row])
                .map_err(|e| e.to_string())?;
            self.build_bytes = estimated_batch_bytes(&self.build_rows);
            if self.build_bytes > self.plan.build_memory_limit_bytes {
                return Err(JOIN_BUILD_TOO_LARGE.to_string());
            }
            let mut stored_buf = Vec::new();
            let stored_view = self.build_rows.view_with(&mut stored_buf);
            let hash = hash_row_keys_at(
                &stored_view,
                &self.plan.build_keys,
                stored_row,
                self.plan.hash_version,
            )?;
            self.build_table
                .entry(hash)
                .or_default()
                .push(stored_row as u32);
        }
        Ok(())
    }

    fn emit_matches_for_probe(
        &mut self,
        probe: OwnedWcolBatch,
        io: &mut HashJoinIo<'_>,
    ) -> Result<(bool, Option<OwnedWcolBatch>), String> {
        let projected = project_columns(&probe, &self.plan.probe_columns)?;
        let mask = projected.effective_selection_mask();
        let mut probe_view_buf = Vec::new();
        let probe_view = projected.view_with(&mut probe_view_buf);
        while self.pending_probe_row < probe_view.row_count {
            let probe_row = self.pending_probe_row;
            self.pending_probe_row += 1;
            if !mask.is_selected(probe_row) {
                continue;
            }
            if join_key_is_null(&probe_view, &self.plan.probe_keys, probe_row)? {
                continue;
            }
            let hash = hash_row_keys_at(
                &probe_view,
                &self.plan.probe_keys,
                probe_row,
                self.plan.hash_version,
            )?;
            let Some(build_matches) = self.build_table.get(&hash).cloned() else {
                continue;
            };
            let matching_build_rows: Vec<usize> = {
                let mut build_view_buf = Vec::new();
                let build_view = self.build_rows.view_with(&mut build_view_buf);
                build_matches
                    .into_iter()
                    .map(|r| r as usize)
                    .filter(|&build_row| {
                        join_keys_equal(
                            &build_view,
                            &self.plan.build_keys,
                            build_row,
                            &probe_view,
                            &self.plan.probe_keys,
                            probe_row,
                        )
                        .unwrap_or(false)
                    })
                    .collect()
            };
            for build_row in matching_build_rows {
                let joined = materialize_join_row(
                    &self.build_rows,
                    build_row,
                    &projected,
                    probe_row,
                    &self.plan,
                )?;
                self.output_builder
                    .append_rows_from(&joined, &[0])
                    .map_err(|e| e.to_string())?;
                if self.output_builder.row_count() >= self.plan.max_output_batch_rows {
                    let out = std::mem::take(&mut self.output_builder);
                    *io.output = Some(WcolStreamItem::from_row_batch(out));
                    return Ok((true, Some(probe)));
                }
            }
        }
        Ok((false, None))
    }
}

fn estimated_batch_bytes(batch: &OwnedWcolBatch) -> usize {
    batch.row_count().saturating_mul(16)
}

fn project_columns(batch: &OwnedWcolBatch, columns: &[u32]) -> Result<OwnedWcolBatch, String> {
    let mut col_view = Vec::new();
    let view = batch.view_with(&mut col_view);
    let mut out = OwnedWcolBatch::empty();
    out.set_scope(view.dict_scope);
    for &id in columns {
        let col = view
            .columns
            .iter()
            .find(|c| column_id(c) == id)
            .ok_or_else(|| format!("join column {id} missing"))?;
        match col {
            WcolColumn::I64 { values, nulls, .. } => {
                out.push_i64_column(id, values.to_vec(), nulls.to_vec())
            }
            WcolColumn::F64 { values, nulls, .. } => {
                out.push_f64_column(id, values.to_vec(), nulls.to_vec())
            }
            WcolColumn::DictId {
                ids,
                nulls,
                dictionary,
                ..
            } => out.push_dict_id_column(id, ids.to_vec(), nulls.to_vec(), (*dictionary).clone()),
            WcolColumn::Utf8 {
                offsets,
                bytes,
                nulls,
                ..
            } => out.push_utf8_column(id, offsets.to_vec(), bytes.to_vec(), nulls.to_vec()),
        }
    }
    out.set_row_count(batch.row_count())
        .map_err(|e| e.to_string())?;
    out.set_selection_mask(batch.effective_selection_mask());
    Ok(out)
}

fn materialize_join_row(
    build: &OwnedWcolBatch,
    build_row: usize,
    probe: &OwnedWcolBatch,
    probe_row: usize,
    plan: &HashJoinPlan,
) -> Result<OwnedWcolBatch, String> {
    let build_part = project_columns(build, &plan.build_columns)?;
    let probe_part = project_columns(probe, &plan.probe_columns)?;
    let mut out = OwnedWcolBatch::empty();
    out.append_rows_from(&build_part, &[build_row])?;
    let existing: std::collections::HashSet<u32> = plan.build_columns.iter().copied().collect();
    let mut probe_view_buf = Vec::new();
    let probe_view = probe_part.view_with(&mut probe_view_buf);
    for pcol in probe_view.columns {
        let id = column_id(pcol);
        if existing.contains(&id) {
            continue;
        }
        match pcol {
            WcolColumn::I64 { values, nulls, .. } => {
                out.push_i64_column(id, vec![values[probe_row]], vec![nulls[probe_row]]);
            }
            WcolColumn::F64 { values, nulls, .. } => {
                out.push_f64_column(id, vec![values[probe_row]], vec![nulls[probe_row]]);
            }
            WcolColumn::DictId {
                ids,
                nulls,
                dictionary,
                ..
            } => out.push_dict_id_column(
                id,
                vec![ids[probe_row]],
                vec![nulls[probe_row]],
                (*dictionary).clone(),
            ),
            WcolColumn::Utf8 {
                offsets,
                bytes,
                nulls,
                ..
            } => {
                let start = offsets[probe_row] as usize;
                let end = offsets[probe_row + 1] as usize;
                out.push_utf8_column(
                    id,
                    vec![0, (end - start) as u32],
                    bytes[start..end].to_vec(),
                    vec![nulls[probe_row]],
                );
            }
        }
    }
    out.set_row_count(1).map_err(|e| e.to_string())?;
    Ok(out)
}

fn column_id(col: &WcolColumn<'_>) -> u32 {
    match col {
        WcolColumn::I64 { column, .. }
        | WcolColumn::F64 { column, .. }
        | WcolColumn::DictId { column, .. }
        | WcolColumn::Utf8 { column, .. } => *column,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drive_join(
        op: &mut HashJoinOp,
        build: Vec<OwnedWcolBatch>,
        probe: Vec<OwnedWcolBatch>,
    ) -> Result<Vec<OwnedWcolBatch>, String> {
        let mut out = Vec::new();
        let mut build_iter = build.into_iter();
        let mut probe_iter = probe.into_iter();
        let mut build_pending = build_iter.next().map(WcolStreamItem::from_row_batch);
        let mut probe_pending = None;
        let mut build_eof = false;
        let mut probe_eof = false;
        loop {
            let mut output = None;
            let mut io = HashJoinIo {
                build_input_slot: Some(&mut build_pending),
                build_eof,
                probe_input_slot: Some(&mut probe_pending),
                probe_eof,
                output: &mut output,
            };
            match op.poll(&mut io)? {
                StreamProgress::HaveOutput => {
                    out.push(output.take().unwrap().into_batch());
                }
                StreamProgress::NeedInput => {
                    if build_pending.is_none() && !build_eof {
                        build_pending = build_iter.next().map(WcolStreamItem::from_row_batch);
                        if build_pending.is_none() {
                            build_eof = true;
                        }
                    }
                    if op.build_complete() && probe_pending.is_none() && !probe_eof {
                        probe_pending = probe_iter.next().map(WcolStreamItem::from_row_batch);
                        if probe_pending.is_none() {
                            probe_eof = true;
                        }
                    }
                }
                StreamProgress::Finished => break,
                StreamProgress::Blocked => return Err("unexpected blocked".to_string()),
                _ => {}
            }
        }
        Ok(out)
    }

    #[test]
    fn one_to_many_preserves_multiplicity() {
        let plan = HashJoinPlan::equijoin(vec![0], vec![1], vec![2]).unwrap();
        let mut op = HashJoinOp::new(plan).unwrap();
        let mut b = OwnedWcolBatch::empty();
        b.push_i64_column(0, vec![1, 1, 2], vec![1; 3]);
        b.push_i64_column(1, vec![10, 20, 30], vec![1; 3]);
        b.set_row_count(3).unwrap();

        let mut p = OwnedWcolBatch::empty();
        p.push_i64_column(0, vec![1, 2], vec![1; 2]);
        p.push_i64_column(2, vec![7, 8], vec![1; 2]);
        p.set_row_count(2).unwrap();

        let out = drive_join(&mut op, vec![b], vec![p]).unwrap();
        let rows: usize = out.iter().map(|batch| batch.row_count()).sum();
        assert_eq!(rows, 3);
    }

    #[test]
    fn null_keys_do_not_match() {
        let plan = HashJoinPlan::equijoin(vec![0], vec![1], vec![2]).unwrap();
        let mut op = HashJoinOp::new(plan).unwrap();
        let mut b = OwnedWcolBatch::empty();
        b.push_i64_column(0, vec![1], vec![1]);
        b.push_i64_column(1, vec![5], vec![1]);
        b.set_row_count(1).unwrap();
        let mut p = OwnedWcolBatch::empty();
        p.push_i64_column(0, vec![1], vec![0]);
        p.push_i64_column(2, vec![9], vec![1]);
        p.set_row_count(1).unwrap();
        let out = drive_join(&mut op, vec![b], vec![p]).unwrap();
        assert_eq!(out.iter().map(|batch| batch.row_count()).sum::<usize>(), 0);
    }

    #[test]
    fn build_memory_limit_enforced() {
        let mut plan = HashJoinPlan::equijoin(vec![0], vec![1], vec![2]).unwrap();
        plan.build_memory_limit_bytes = 64;
        let mut op = HashJoinOp::new(plan).unwrap();
        let mut b = OwnedWcolBatch::empty();
        b.push_i64_column(0, vec![0; 10_000], vec![1; 10_000]);
        b.push_i64_column(1, vec![0; 10_000], vec![1; 10_000]);
        b.set_row_count(10_000).unwrap();
        match drive_join(&mut op, vec![b], Vec::<OwnedWcolBatch>::new()) {
            Err(e) => assert_eq!(e, JOIN_BUILD_TOO_LARGE),
            Ok(_) => panic!("expected JOIN_BUILD_TOO_LARGE"),
        }
    }

    #[test]
    fn probe_before_build_complete_errors() {
        let plan = HashJoinPlan::equijoin(vec![0], vec![1], vec![2]).unwrap();
        let mut op = HashJoinOp::new(plan).unwrap();
        let mut p = OwnedWcolBatch::empty();
        p.push_i64_column(0, vec![1], vec![1]);
        p.set_row_count(1).unwrap();
        let mut probe_pending = Some(WcolStreamItem::from_row_batch(p));
        let mut build_pending: Option<WcolStreamItem> = None;
        let mut output: Option<WcolStreamItem> = None;
        let mut io = HashJoinIo {
            build_input_slot: Some(&mut build_pending),
            build_eof: false,
            probe_input_slot: Some(&mut probe_pending),
            probe_eof: false,
            output: &mut output,
        };
        let err = op.poll(&mut io).unwrap_err();
        assert_eq!(err, "probe before build complete");
    }
}
