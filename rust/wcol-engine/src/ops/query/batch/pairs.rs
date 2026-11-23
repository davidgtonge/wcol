//! Two-integer-key groups kept in the compact pair table.

use super::*;

impl BatchQuery {
    /// The pair key columns in GROUP BY order, the counted value last. A part a shorter key
    /// list does not have stays zero and never NULL, so every row shares it.
    pub(super) fn pair_key_cols<'c>(
        &self,
        cols: &Cols<'c>,
    ) -> Result<[Option<ColRef<'c>>; 3], String> {
        let mut key_cols: [Option<ColRef<'c>>; 3] = [None; 3];
        for (part, &id) in self.group_by.iter().enumerate() {
            key_cols[part] = Some(column(cols, id)?);
        }
        Ok(key_cols)
    }

    pub(super) fn pair_agg_inputs<'c>(
        &self,
        cols: &Cols<'c>,
    ) -> Result<([u8; 8], [Option<ColRef<'c>>; 8]), String> {
        let n_aggs = self.aggs.len();
        if n_aggs > 8 {
            return Err("pair grouping supports at most 8 aggregates".to_string());
        }
        let mut kind = [0u8; 8];
        let mut value_cols: [Option<ColRef<'c>>; 8] = [None; 8];
        for (index, agg) in self.aggs.iter().enumerate() {
            kind[index] = agg.kind;
            if agg.kind != KIND_COUNT_STAR {
                value_cols[index] = Some(column(cols, agg.column)?);
            }
        }
        Ok((kind, value_cols))
    }

    pub(super) fn consume_two_int(&mut self, cols: &Cols<'_>, words: &[u32]) -> Result<(), String> {
        let key_cols = self.pair_key_cols(cols)?;
        let (kind, value_cols) = self.pair_agg_inputs(cols)?;
        let (text_parts, hash_value, n_aggs) =
            (self.group_key_text, self.text_distinct, self.aggs.len());
        let pairs = self.pairs.as_mut().expect("pair groups");
        let interner = &mut self.interner;
        let mut error = None;
        mask::for_each_set(words, |row| {
            if error.is_some() {
                return;
            }
            let key = match pair_key(&key_cols, text_parts, hash_value, row, |bytes| {
                Some(i64::from(interner.intern(bytes)))
            }) {
                Ok(key) => key.expect("interning always yields a key"),
                Err(e) => {
                    error = Some(e);
                    return;
                }
            };
            let slot = pairs.slot(key);
            bump_pair(pairs, slot, &kind, &value_cols, n_aggs, row);
        });
        match error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub(super) fn consume_two_int_spill(
        &mut self,
        cols: &Cols<'_>,
        words: &[u32],
    ) -> Result<SpillProgress<()>, String> {
        let key_cols = self.pair_key_cols(cols)?;
        let (kind, value_cols) = self.pair_agg_inputs(cols)?;
        let (text_parts, hash_value, n_aggs) =
            (self.group_key_text, self.text_distinct, self.aggs.len());
        let mut rows_sel = std::mem::take(&mut self.rows_scratch);
        rows_sel.clear();
        mask::for_each_set(words, |row| rows_sel.push(row as u32));
        let mut sel_at = self.spill_consume_at.min(rows_sel.len());
        while sel_at < rows_sel.len() {
            let row = rows_sel[sel_at] as usize;
            let interner = &self.interner;
            let existing = pair_key(&key_cols, text_parts, hash_value, row, |bytes| {
                interner.lookup(bytes).map(i64::from)
            })?
            .and_then(|key| self.pairs.as_ref().and_then(|pairs| pairs.get(&key)));
            let slot = match existing {
                Some(slot) => slot,
                None => {
                    // A flush clears the interner, so it runs before this row's text is interned.
                    self.note_spill_live();
                    if self.spill_flush_due() {
                        match self.flush_live_run_cooperative() {
                            Ok(SpillProgress::Ready(())) => {}
                            Ok(SpillProgress::AwaitHost) => {
                                self.spill_consume_at = sel_at;
                                self.rows_scratch = rows_sel;
                                return Ok(SpillProgress::AwaitHost);
                            }
                            Err(e) => {
                                self.rows_scratch = rows_sel;
                                return Err(e);
                            }
                        }
                    }
                    let interner = &mut self.interner;
                    let key = pair_key(&key_cols, text_parts, hash_value, row, |bytes| {
                        Some(i64::from(interner.intern(bytes)))
                    })?
                    .expect("interning always yields a key");
                    self.pairs.as_mut().expect("pair groups").slot(key)
                }
            };
            let pairs = self.pairs.as_mut().expect("pair groups");
            bump_pair(pairs, slot, &kind, &value_cols, n_aggs, row);
            sel_at += 1;
        }
        self.rows_scratch = rows_sel;
        let n = self.rows_scratch.len();
        self.finish_spill_batch(n)
    }

    pub(super) fn finish_pairs(&self) -> PartialStateWcol {
        let pairs = self.pairs.as_ref().expect("two-integer groups");
        let n_aggs = self.aggs.len();
        let slots = self.pair_slots();
        let rows = slots.len();
        let key_len = self.group_by.len();
        self.note_groups(pairs.len() as u64, rows as u64);
        let mut out = PartialColumnsBuilder::new(key_len, &self.partial_aggs(), rows);
        for &slot in &slots {
            let key = pairs.key(slot);
            for part in 0..key_len {
                out.push_key(
                    part,
                    (key.nulls & (1 << part) == 0).then_some(key.part(part)),
                );
            }
        }
        let empty_f = FAcc::empty();
        let mut stats = Vec::with_capacity(rows);
        for index in 0..n_aggs {
            stats.clear();
            stats.extend(
                slots
                    .iter()
                    .map(|&slot| self.partial_stat(index, &pairs.acc(slot, index), &empty_f, None)),
            );
            out.extend_stats(index, &stats);
        }
        out.finish(
            PartialStateSchemaV1 {
                version: PARTIAL_SCHEMA_VERSION,
                grouped: self.shell.grouped,
                order_groups_by_count: self.shell.order_groups_by_count,
                limit: self.shell.limit,
                offset: self.shell.offset,
                group_key_len: key_len as u8,
                group_key_float_mask: 0,
                group_key_string_mask: 0,
                aggs: self.partial_aggs(),
            },
            self.passed_rows,
            Vec::new(),
        )
    }

    pub(super) fn pair_slots(&self) -> Vec<u32> {
        let pairs = self.pairs.as_ref().expect("two-integer groups");
        let n = pairs.len();
        if let Some(index) = self.final_rank {
            if let Some(slots) = self.pair_rank_slots(index as usize) {
                return slots;
            }
        }
        let mut slots: Vec<u32> = (0..n as u32).collect();
        slots.sort_unstable_by(|&a, &b| self.cmp_pair_key(pairs.key(a), pairs.key(b)));
        slots
    }

    /// Pair keys in output order: each text part by its bytes, every other part by value.
    pub(super) fn cmp_pair_key(&self, a: &PairKey, b: &PairKey) -> std::cmp::Ordering {
        (0..self.group_by.len())
            .map(|part| {
                if self.group_key_text & (1 << part) == 0 {
                    return cmp_key_part(a, b, part);
                }
                match (a.nulls & (1 << part) != 0, b.nulls & (1 << part) != 0) {
                    (true, true) => std::cmp::Ordering::Equal,
                    (true, false) => std::cmp::Ordering::Less,
                    (false, true) => std::cmp::Ordering::Greater,
                    (false, false) => self
                        .interner
                        .cmp_ids(a.part(part) as u32, b.part(part) as u32),
                }
            })
            .find(|ord| ord.is_ne())
            .unwrap_or(std::cmp::Ordering::Equal)
    }

    pub(super) fn pair_rank_slots(&self, agg: usize) -> Option<Vec<u32>> {
        let pairs = self.pairs.as_ref().expect("two-integer groups");
        let limit = self.shell.limit;
        if limit == u32::MAX || agg >= self.aggs.len() {
            return None;
        }
        let keep = (self.shell.offset as u64).saturating_add(u64::from(limit));
        let groups = pairs.len() as u64;
        if keep == 0 || keep >= groups {
            return None;
        }
        Some(self.pair_keep_largest(agg, keep as usize))
    }

    pub(super) fn pair_above(&self, a: u32, b: u32, agg: usize) -> bool {
        let pairs = self.pairs.as_ref().expect("two-integer groups");
        let ca = pairs.count(a, agg);
        let cb = pairs.count(b, agg);
        ca > cb || (ca == cb && cmp_key2(pairs.key(a), pairs.key(b)) == std::cmp::Ordering::Less)
    }

    pub(super) fn pair_keep_largest(&self, agg: usize, keep: usize) -> Vec<u32> {
        let pairs = self.pairs.as_ref().expect("two-integer groups");
        keep_top_slots(pairs.len() as u32, keep, |a, b| self.pair_above(a, b, agg))
    }
}
