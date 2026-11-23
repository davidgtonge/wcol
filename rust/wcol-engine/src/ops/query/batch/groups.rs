//! The general grouped row kernel.

use super::*;

impl BatchQuery {
    pub(super) fn consume_groups(
        &mut self,
        cols: &Cols<'_>,
        words: &[u32],
    ) -> Result<SpillProgress<()>, String> {
        if self.pairs.is_some() {
            if self.spill.is_some() {
                return self.consume_two_int_spill(cols, words);
            }
            self.consume_two_int(cols, words)?;
            return Ok(SpillProgress::Ready(()));
        }
        let mut rows_sel = std::mem::take(&mut self.rows_scratch);
        let mut slots = std::mem::take(&mut self.slots_scratch);
        rows_sel.clear();
        slots.clear();
        mask::for_each_set(words, |row| rows_sel.push(row as u32));

        let key_cols: Vec<ColRef<'_>> = self
            .group_by
            .iter()
            .map(|id| column(cols, *id))
            .collect::<Result<_, _>>()?;
        if self.spill.is_some() {
            let result = match key_cols.as_slice() {
                [ColRef::I64 { values, nulls }]
                    if self.group_by.len() == 1
                        && self.group_key_text == 0
                        && self.group_key_float == 0 =>
                {
                    self.consume_single_i64_group_spill(cols, &rows_sel, values, nulls)
                }
                _ => self.consume_groups_spilling(cols, &rows_sel, &key_cols),
            };
            self.rows_scratch = rows_sel;
            self.slots_scratch = slots;
            return result;
        }
        match key_cols.as_slice() {
            [ColRef::I64 { values, nulls }]
                if self.group_by.len() == 1
                    && self.group_key_text == 0
                    && self.group_key_float == 0 =>
            {
                let mut null_slot = NO_SLOT;
                for &row in &rows_sel {
                    let row = row as usize;
                    let (key, _parts) = if nulls[row] == 0 {
                        (GroupTable::null_key(1), vec![PartialKeyPart::Null])
                    } else {
                        let v = values[row];
                        (
                            GroupTable::value_key(v),
                            vec![PartialKeyPart::Int(i128::from(v))],
                        )
                    };
                    let packed = if nulls[row] == 0 {
                        if null_slot == NO_SLOT {
                            null_slot = spill_pull!(self.live_slot(key));
                        }
                        null_slot
                    } else {
                        spill_pull!(self.live_slot(key))
                    };
                    slots.push(packed);
                }
            }
            [ColRef::Dict {
                ids,
                nulls,
                entries,
            }] if self.group_key_text == 0 => {
                {
                    let table = &mut self.groups;
                    if !table.dict.as_ref().is_some_and(|d| Arc::ptr_eq(d, entries)) {
                        table.dict = Some(Arc::clone(entries));
                        table.dict_f = None;
                        table.dict_bytes = None;
                        table.dict_slots.clear();
                        table.dict_slots.resize(entries.len(), NO_SLOT);
                    }
                }
                let mut null_slot = NO_SLOT;
                for &row in &rows_sel {
                    let row = row as usize;
                    let slot = if nulls[row] == 0 {
                        if null_slot == NO_SLOT {
                            null_slot = spill_pull!(self.live_slot(GroupTable::null_key(1)));
                        }
                        null_slot
                    } else {
                        let id = ids[row] as usize;
                        let mut slot = self.groups.dict_slots[id];
                        if slot == NO_SLOT {
                            slot = spill_pull!(self.live_slot(GroupTable::value_key(entries[id])));
                            self.groups.dict_slots[id] = slot;
                        }
                        slot
                    };
                    slots.push(slot);
                }
            }
            [ColRef::DictBytes {
                ids,
                nulls,
                offsets,
                bytes,
            }] if self.group_key_text == 1 => {
                let offsets_c = Arc::clone(offsets);
                let bytes_c = Arc::clone(bytes);
                {
                    let table = &mut self.groups;
                    let key = (offsets_c.clone(), bytes_c.clone());
                    if !table
                        .dict_bytes
                        .as_ref()
                        .is_some_and(|d| Arc::ptr_eq(&d.0, &key.0) && Arc::ptr_eq(&d.1, &key.1))
                    {
                        table.dict_bytes = Some(key);
                        table.dict = None;
                        table.dict_f = None;
                        let n = offsets_c.len().saturating_sub(1);
                        table.dict_slots.clear();
                        table.dict_slots.resize(n, NO_SLOT);
                    }
                }
                let mut null_slot = NO_SLOT;
                for &row in &rows_sel {
                    let row = row as usize;
                    let slot = if nulls[row] == 0 {
                        if null_slot == NO_SLOT {
                            null_slot = spill_pull!(self.live_slot(GroupTable::null_key(1)));
                        }
                        null_slot
                    } else {
                        let id = ids[row] as usize;
                        let mut slot = self.groups.dict_slots[id];
                        if slot == NO_SLOT {
                            if self.spill_flush_due() {
                                spill_pull!(self.flush_live_run());
                            }
                            let intern = self
                                .interner
                                .intern(string_filter::dict_entry_slice(&offsets_c, &bytes_c, id));
                            slot = spill_pull!(
                                self.live_slot(GroupTable::value_key(i64::from(intern)))
                            );
                            self.groups.dict_slots[id] = slot;
                        }
                        slot
                    };
                    slots.push(slot);
                }
            }
            [ColRef::DictF64 {
                ids,
                nulls,
                entries,
            }] if self.group_key_float == 1 && self.group_key_text == 0 => {
                {
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
                let mut null_slot = NO_SLOT;
                for &row in &rows_sel {
                    let row = row as usize;
                    let slot = if nulls[row] == 0 {
                        if null_slot == NO_SLOT {
                            null_slot = spill_pull!(self.live_slot(GroupTable::null_key(1)));
                        }
                        null_slot
                    } else {
                        let id = ids[row] as usize;
                        let mut slot = self.groups.dict_slots[id];
                        if slot == NO_SLOT {
                            slot = spill_pull!(self
                                .live_slot(GroupTable::value_key(float_key(entries[id]) as i64)));
                            self.groups.dict_slots[id] = slot;
                        }
                        slot
                    };
                    slots.push(slot);
                }
            }
            _ => {
                for &row in &rows_sel {
                    let row = row as usize;
                    let mut key = GroupKey {
                        nulls: 0,
                        values: [0; MAX_KEYS],
                    };
                    for (k, col) in key_cols.iter().enumerate() {
                        if col.nulls()[row] == 0 {
                            key.nulls |= 1 << k;
                        } else if self.group_key_text & (1u32 << k) != 0 {
                            if self.spill_flush_due() {
                                spill_pull!(self.flush_live_run());
                            }
                            let bytes = col_string_bytes(*col, row)?;
                            let intern = self.interner.intern(bytes);
                            key.values[k] = i64::from(intern);
                        } else if self.group_key_float & (1u32 << k) != 0 {
                            key.values[k] = float_key(f64_at(*col, row)) as i64;
                        } else {
                            key.values[k] = col.value(row);
                        }
                    }
                    slots.push(spill_pull!(self.live_slot(key)));
                }
            }
        }

        let count_star_indices: Vec<usize> = self
            .aggs
            .iter()
            .enumerate()
            .filter(|(_, agg)| agg.kind == KIND_COUNT_STAR)
            .map(|(index, _)| index)
            .collect();
        for index in count_star_indices {
            for &slot in &slots {
                self.bump_count_star(slot, index);
            }
        }
        for (index, agg) in self.aggs.iter().enumerate() {
            if agg.kind == KIND_COUNT_STAR {
                continue;
            }
            let col = column(cols, agg.column)?;
            let nulls = col.nulls();
            if self.agg_float[index] {
                let accs_f = &mut self.groups.accs_f[index];
                if agg.kind == KIND_APPROX_DISTINCT {
                    let sketches = &mut self.groups.sketches[index];
                    for (&row, &slot) in rows_sel.iter().zip(&slots) {
                        let row = row as usize;
                        if nulls[row] == 0 {
                            continue;
                        }
                        let at = slot as usize;
                        accs_f[at].count += 1;
                        sketches[at]
                            .get_or_insert_with(Hll::new)
                            .insert_i64(float_key(f64_at(col, row)) as i64);
                    }
                    continue;
                }
                let count_only = agg.kind == KIND_COUNT || agg.kind == KIND_COUNT_DISTINCT;
                for (&row, &slot) in rows_sel.iter().zip(&slots) {
                    let row = row as usize;
                    if nulls[row] == 0 {
                        continue;
                    }
                    let acc = &mut accs_f[slot as usize];
                    if count_only {
                        acc.count += 1;
                    } else {
                        acc.add(f64_at(col, row));
                    }
                }
                continue;
            }
            let accs = &mut self.groups.accs[index];
            if agg.kind == KIND_APPROX_DISTINCT {
                let sketches = &mut self.groups.sketches[index];
                for (&row, &slot) in rows_sel.iter().zip(&slots) {
                    let row = row as usize;
                    if nulls[row] == 0 {
                        continue;
                    }
                    let at = slot as usize;
                    accs[at].count += 1;
                    sketches[at]
                        .get_or_insert_with(Hll::new)
                        .insert_i64(col.value(row));
                }
                continue;
            }
            let count_only = agg.kind == KIND_COUNT || agg.kind == KIND_COUNT_DISTINCT;
            for (&row, &slot) in rows_sel.iter().zip(&slots) {
                let row = row as usize;
                if nulls[row] == 0 {
                    continue;
                }
                let acc = &mut accs[slot as usize];
                if count_only {
                    acc.count += 1;
                } else {
                    acc.add(col.value(row));
                }
            }
        }
        self.rows_scratch = rows_sel;
        self.slots_scratch = slots;
        spill_pull!(self.spill_flush_if_needed());
        Ok(SpillProgress::Ready(()))
    }
}
