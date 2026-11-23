//! Row consumption: dictionary binding, group slots, partial merges and totals.

use super::*;

impl BatchQuery {
    pub(super) fn bind_int_dict(&mut self, entries: &Arc<Vec<i64>>) {
        let table = &mut self.groups;
        if !table.dict.as_ref().is_some_and(|d| Arc::ptr_eq(d, entries)) {
            table.dict = Some(Arc::clone(entries));
            table.dict_f = None;
            table.dict_bytes = None;
            table.dict_slots.clear();
            table.dict_slots.resize(entries.len(), NO_SLOT);
        }
    }

    pub(super) fn bind_float_dict(&mut self, entries: &Arc<Vec<f64>>) {
        let table = &mut self.groups;
        if !table
            .dict_f
            .as_ref()
            .is_some_and(|d| Arc::ptr_eq(d, entries))
        {
            table.dict_f = Some(Arc::clone(entries));
            table.dict = None;
            table.dict_bytes = None;
            table.dict_slots.clear();
            table.dict_slots.resize(entries.len(), NO_SLOT);
        }
    }

    pub(super) fn bind_bytes_dict(&mut self, offsets: &Arc<Vec<u32>>, bytes: &Arc<Vec<u8>>) {
        let table = &mut self.groups;
        let key = (Arc::clone(offsets), Arc::clone(bytes));
        if !table
            .dict_bytes
            .as_ref()
            .is_some_and(|d| Arc::ptr_eq(&d.0, &key.0) && Arc::ptr_eq(&d.1, &key.1))
        {
            table.dict_bytes = Some(key);
            table.dict = None;
            table.dict_f = None;
            let n = offsets.len().saturating_sub(1);
            table.dict_slots.clear();
            table.dict_slots.resize(n, NO_SLOT);
        }
    }

    pub(super) fn cached_dict_slot(&self, id: usize) -> u32 {
        self.groups.dict_slots.get(id).copied().unwrap_or(NO_SLOT)
    }

    pub(super) fn store_dict_slot(&mut self, id: usize, slot: u32) {
        if id < self.groups.dict_slots.len() {
            self.groups.dict_slots[id] = slot;
        }
    }

    pub(super) fn live_slot_for_row(
        &mut self,
        key_cols: &[ColRef<'_>],
        row: usize,
    ) -> Result<SpillProgress<u32>, String> {
        match key_cols {
            [ColRef::I64 { values, nulls }]
                if self.group_by.len() == 1
                    && self.group_key_text == 0
                    && self.group_key_float == 0 =>
            {
                let key = if nulls[row] == 0 {
                    GroupTable::null_key(1)
                } else {
                    GroupTable::value_key(values[row])
                };
                self.live_slot(key)
            }
            [ColRef::Dict {
                ids,
                nulls,
                entries,
            }] if self.group_key_text == 0 => {
                self.bind_int_dict(entries);
                if nulls[row] == 0 {
                    return self.live_slot(GroupTable::null_key(1));
                }
                let id = ids[row] as usize;
                let cached = self.cached_dict_slot(id);
                if cached != NO_SLOT {
                    return Ok(SpillProgress::Ready(cached));
                }
                let slot = spill_pull!(self.live_slot(GroupTable::value_key(entries[id])));
                self.store_dict_slot(id, slot);
                Ok(SpillProgress::Ready(slot))
            }
            [ColRef::DictBytes {
                ids,
                nulls,
                offsets,
                bytes,
            }] if self.group_key_text == 1 => {
                self.bind_bytes_dict(offsets, bytes);
                if nulls[row] == 0 {
                    return self.live_slot(GroupTable::null_key(1));
                }
                let id = ids[row] as usize;
                let cached = self.cached_dict_slot(id);
                if cached != NO_SLOT {
                    return Ok(SpillProgress::Ready(cached));
                }
                if self.spill_flush_due() {
                    spill_pull!(self.flush_live_run_cooperative());
                }
                let intern = self
                    .interner
                    .intern(string_filter::dict_entry_slice(offsets, bytes, id));
                let slot = spill_pull!(self.live_slot(GroupTable::value_key(i64::from(intern))));
                self.store_dict_slot(id, slot);
                Ok(SpillProgress::Ready(slot))
            }
            [ColRef::DictF64 {
                ids,
                nulls,
                entries,
            }] if self.group_key_float == 1 && self.group_key_text == 0 => {
                self.bind_float_dict(entries);
                if nulls[row] == 0 {
                    return self.live_slot(GroupTable::null_key(1));
                }
                let id = ids[row] as usize;
                let cached = self.cached_dict_slot(id);
                if cached != NO_SLOT {
                    return Ok(SpillProgress::Ready(cached));
                }
                let slot = spill_pull!(
                    self.live_slot(GroupTable::value_key(float_key(entries[id]) as i64))
                );
                self.store_dict_slot(id, slot);
                Ok(SpillProgress::Ready(slot))
            }
            _ => {
                let mut key = GroupKey {
                    nulls: 0,
                    values: [0; MAX_KEYS],
                };
                for (k, col) in key_cols.iter().enumerate() {
                    if col.nulls()[row] == 0 {
                        key.nulls |= 1 << k;
                    } else if self.group_key_text & (1u32 << k) != 0 {
                        if self.spill_flush_due() {
                            spill_pull!(self.flush_live_run_cooperative());
                        }
                        let intern = self.interner.intern(col_string_bytes(*col, row)?);
                        key.values[k] = i64::from(intern);
                    } else if self.group_key_float & (1u32 << k) != 0 {
                        key.values[k] = float_key(f64_at(*col, row)) as i64;
                    } else {
                        key.values[k] = col.value(row);
                    }
                }
                self.live_slot(key)
            }
        }
    }

    pub(super) fn bump_main_row_agg(
        &mut self,
        slot: u32,
        agg_index: usize,
        row: usize,
        col: ColRef<'_>,
    ) {
        let agg = &self.aggs[agg_index];
        let at = slot as usize;
        if self.agg_float[agg_index] {
            let acc = &mut self.groups.accs_f[agg_index][at];
            if agg.kind == KIND_APPROX_DISTINCT {
                acc.count += 1;
                self.groups.sketches[agg_index][at]
                    .get_or_insert_with(Hll::new)
                    .insert_i64(float_key(f64_at(col, row)) as i64);
            } else if agg.kind == KIND_COUNT || agg.kind == KIND_COUNT_DISTINCT {
                acc.count += 1;
            } else {
                acc.add(f64_at(col, row));
            }
        } else {
            let acc = &mut self.groups.accs[agg_index][at];
            if agg.kind == KIND_APPROX_DISTINCT {
                acc.count += 1;
                self.groups.sketches[agg_index][at]
                    .get_or_insert_with(Hll::new)
                    .insert_i64(col.value(row));
            } else if agg.kind == KIND_COUNT || agg.kind == KIND_COUNT_DISTINCT {
                acc.count += 1;
            } else {
                acc.add(col.value(row));
            }
        }
    }

    /// Merge another node's partial state as if its rows had been consumed here: a scan-side
    /// combiner sends these instead of raw rows. Like [`BatchQuery::consume`], call again with
    /// the same state after [`SpillProgress::AwaitHost`]; it resumes where it stopped.
    pub fn consume_partial(
        &mut self,
        state: &PartialStateWcol,
    ) -> Result<SpillProgress<()>, String> {
        if state.schema.aggs != self.partial_aggs()
            || usize::from(state.schema.group_key_len) != self.group_by.len()
        {
            return Err("partial state does not match this aggregate".to_string());
        }
        let rows = PartialRows::new(state)?;
        let n_aggs = self.aggs.len();
        if self.group_by.is_empty() || state.row_count == 0 {
            for row in 0..state.row_count {
                self.totals_hit = true;
                for index in 0..n_aggs {
                    self.merge_partial_stat(None, index, &rows.stat(row, index)?)?;
                }
            }
            self.passed_rows = self.passed_rows.saturating_add(state.passed_rows);
            return Ok(SpillProgress::Ready(()));
        }
        let start = self.spill_consume_at.min(state.row_count);
        for row in start..state.row_count {
            let slot = match self.partial_row_slot(&rows, row)? {
                SpillProgress::Ready(slot) => slot,
                SpillProgress::AwaitHost => {
                    self.spill_consume_at = row;
                    return Ok(SpillProgress::AwaitHost);
                }
            };
            for index in 0..n_aggs {
                self.merge_partial_stat(Some(slot), index, &rows.stat(row, index)?)?;
            }
        }
        // Counted once: a resume after the last row's flush starts at `row_count`.
        if start < state.row_count {
            self.passed_rows = self.passed_rows.saturating_add(state.passed_rows);
        }
        self.finish_spill_batch(state.row_count)
    }

    /// The live slot for a partial row's key. As on the row path, a due flush runs before a
    /// new key's text is interned: the flush clears the interner.
    pub(super) fn partial_row_slot(
        &mut self,
        rows: &PartialRows<'_>,
        row: usize,
    ) -> Result<SpillProgress<u32>, String> {
        let (text, float, key_len) = (
            self.group_key_text,
            self.group_key_float,
            self.group_by.len(),
        );
        let interner = &self.interner;
        let seen = partial_key(rows, row, key_len, text, float, |bytes| {
            interner.lookup(bytes).map(i64::from)
        })?;
        let existing = seen.and_then(|(nulls, values)| match &self.pairs {
            Some(pairs) => pairs.get(&pair_key_of(nulls, &values)),
            None => self.groups.get_slot(&GroupKey { nulls, values }),
        });
        if let Some(slot) = existing {
            return Ok(SpillProgress::Ready(slot));
        }
        if self.spill.is_some() {
            self.note_spill_live();
            if self.spill_flush_due() {
                spill_pull!(self.flush_live_run_cooperative());
            }
        }
        let interner = &mut self.interner;
        let (nulls, values) = partial_key(rows, row, key_len, text, float, |bytes| {
            Some(i64::from(interner.intern(bytes)))
        })?
        .ok_or("interning always yields a key")?;
        Ok(SpillProgress::Ready(match self.pairs.as_mut() {
            Some(pairs) => pairs.slot(pair_key_of(nulls, &values)),
            None => self.groups.slot(GroupKey { nulls, values }),
        }))
    }

    /// Fold one partial stat into a group's accumulators, or the totals when `slot` is `None`.
    pub(super) fn merge_partial_stat(
        &mut self,
        slot: Option<u32>,
        index: usize,
        stat: &PartialStat,
    ) -> Result<(), String> {
        let (kind, offset) = (self.aggs[index].kind, self.offsets[index]);
        if let (Some(slot), Some(pairs)) = (slot, self.pairs.as_mut()) {
            pairs.merge(slot, index, &Acc::from_partial(stat, kind, offset)?);
            return Ok(());
        }
        let (acc, facc, sketch) = match slot {
            Some(slot) => {
                let at = slot as usize;
                (
                    &mut self.groups.accs[index][at],
                    &mut self.groups.accs_f[index][at],
                    &mut self.groups.sketches[index][at],
                )
            }
            None => (
                &mut self.totals[index],
                &mut self.totals_f[index],
                &mut self.totals_sketches[index],
            ),
        };
        if self.agg_float[index] {
            facc.merge(&FAcc::from_partial(stat, kind, offset));
        } else {
            acc.merge(&Acc::from_partial(stat, kind, offset)?);
        }
        if let (KIND_APPROX_DISTINCT, Some(other)) = (kind, &stat.sketch) {
            sketch.get_or_insert_with(Hll::new).merge(other);
        }
        Ok(())
    }

    /// Aggregate the rows of `batch` selected by `selection`.
    pub fn consume(
        &mut self,
        batch: &WcolBatch<'_>,
        selection: &SelectionMask,
    ) -> Result<SpillProgress<()>, String> {
        let _phase = phase(&PHASE_HASH_NS);
        let rows = batch.row_count;
        if selection.rows() != rows {
            return Err("selection mask does not match batch rows".to_string());
        }
        let cols = cols_of(batch, self.width)?;
        let words = selection_words(selection);
        let selected = mask::count(&words);
        self.passed_rows = self.passed_rows.saturating_add(selected);
        if selected == 0 {
            return Ok(SpillProgress::Ready(()));
        }
        if self.group_by.is_empty() {
            self.consume_totals(&cols, &words, rows, selected)?;
            Ok(SpillProgress::Ready(()))
        } else {
            self.consume_groups(&cols, &words)
        }
    }

    pub(super) fn consume_totals(
        &mut self,
        cols: &Cols<'_>,
        words: &[u32],
        rows: usize,
        selected: u64,
    ) -> Result<(), String> {
        self.totals_hit = true;
        let full = selected == rows as u64;
        let mut done: Vec<(u32, Acc)> = Vec::new();
        for (index, agg) in self.aggs.iter().enumerate() {
            if agg.kind == KIND_COUNT_STAR {
                self.totals[index].count += selected;
                continue;
            }
            if self.agg_float[index] {
                let facc = column_facc(column(cols, agg.column)?, words, rows, full);
                self.totals_f[index].merge(&facc);
                if agg.kind == KIND_APPROX_DISTINCT {
                    let col = column(cols, agg.column)?;
                    let sketch = self.totals_sketches[index].get_or_insert_with(Hll::new);
                    fill_sketch_f64(col, words, rows, full, sketch);
                }
                continue;
            }
            let acc = match done.iter().find(|(column, _)| *column == agg.column) {
                Some((_, acc)) => *acc,
                None => {
                    let col = column(cols, agg.column)?;
                    let acc = column_acc(col, words, rows, full, &mut self.hist_scratch);
                    done.push((agg.column, acc));
                    acc
                }
            };
            self.totals[index].merge(&acc);
            if agg.kind == KIND_APPROX_DISTINCT {
                let col = column(cols, agg.column)?;
                let sketch = self.totals_sketches[index].get_or_insert_with(Hll::new);
                fill_sketch(col, words, rows, full, &mut self.hist_scratch, sketch);
            }
        }
        Ok(())
    }
}
