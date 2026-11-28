//! Finish a query: ranking, distinct counts, partial states and snapshots.

use super::*;

impl BatchQuery {
    /// Group slots in the row kernel's key order (per part, `NULL` before any value).
    pub(crate) fn sorted_slots(&self) -> Vec<u32> {
        let keys = self.group_by.len();
        let mut slots: Vec<u32> = (0..self.groups.len() as u32).collect();
        slots.sort_unstable_by(|&a, &b| {
            cmp_keys(
                &self.groups.keys[a as usize],
                &self.groups.keys[b as usize],
                keys,
                self.group_key_text,
                self.group_key_float,
                &self.interner,
            )
        });
        slots
    }

    /// Slots to write. A final count top-k keeps `offset + limit` groups; every other finish
    /// emits the full key order.
    pub(crate) fn emit_slots(&self) -> Vec<u32> {
        if let Some(index) = self.final_rank {
            if let Some(slots) = self.rank_slots(index as usize) {
                return slots;
            }
        }
        // A distinct set is merged by hash, then counted. Sorting the strings is not part of that.
        if !self.shell.grouped && self.aggs.iter().all(|agg| agg.kind == KIND_COUNT_DISTINCT) {
            return (0..self.groups.len() as u32).collect();
        }
        self.sorted_slots()
    }

    /// Final ungrouped `COUNT(DISTINCT)`: one cardinality, not a group per value.
    /// Text identity is the interner. Anything that still has to merge returns `None`.
    pub(super) fn distinct_cardinality(&self) -> Option<u64> {
        if !self.final_aggregate || self.shell.grouped || self.aggs.is_empty() {
            return None;
        }
        if self.aggs.iter().any(|agg| agg.kind != KIND_COUNT_DISTINCT) {
            return None;
        }
        if let Some(pairs) = &self.pairs {
            // One pair per (value); a NULL value is not counted.
            let values = self
                .pair_slots()
                .into_iter()
                .filter(|&slot| pairs.key(slot).nulls & 1 == 0)
                .count();
            return Some(values as u64);
        }
        if self.group_key_text != 0 {
            return Some(self.interner.len() as u64);
        }
        let nulls = self.groups.keys.iter().filter(|key| key.nulls != 0).count();
        Some((self.groups.len() - nulls) as u64)
    }

    pub(super) fn finish_distinct_count(&self, count: u64) -> PartialStateWcol {
        self.note_groups(self.groups.len() as u64, 1);
        let stat = stat_to_partial(&count_stat(count));
        let mut out = PartialColumnsBuilder::new(0, &self.partial_aggs(), 1);
        for index in 0..self.aggs.len() {
            out.extend_stats(index, std::slice::from_ref(&stat));
        }
        out.finish(
            PartialStateSchemaV1 {
                version: PARTIAL_SCHEMA_VERSION,
                grouped: false,
                order_groups_by_count: self.shell.order_groups_by_count,
                limit: self.shell.limit,
                offset: self.shell.offset,
                group_key_len: 0,
                group_key_float_mask: 0,
                group_key_string_mask: 0,
                aggs: self.partial_aggs(),
            },
            self.passed_rows,
            Vec::new(),
        )
    }

    /// `None` when the limit does not shrink the table, so the caller keeps key order.
    pub(super) fn rank_slots(&self, agg: usize) -> Option<Vec<u32>> {
        let limit = self.shell.limit;
        if limit == u32::MAX || agg >= self.aggs.len() {
            return None;
        }
        let keep = (self.shell.offset as u64).saturating_add(u64::from(limit));
        let groups = self.groups.len() as u64;
        if keep == 0 || keep >= groups {
            return None;
        }
        Some(self.keep_largest(agg, keep as usize))
    }

    pub(super) fn group_count(&self, slot: u32, agg: usize) -> u64 {
        if self.agg_float[agg] {
            self.groups.accs_f[agg][slot as usize].count
        } else {
            self.groups.accs[agg][slot as usize].count
        }
    }

    /// `a` ranks above `b`: higher count, then the smaller group key.
    pub(super) fn ranks_above(&self, a: u32, b: u32, agg: usize) -> bool {
        let ca = self.group_count(a, agg);
        let cb = self.group_count(b, agg);
        ca > cb
            || (ca == cb
                && cmp_keys(
                    &self.groups.keys[a as usize],
                    &self.groups.keys[b as usize],
                    self.group_by.len(),
                    self.group_key_text,
                    self.group_key_float,
                    &self.interner,
                ) == std::cmp::Ordering::Less)
    }

    /// Min-heap of the `keep` highest ranks. The root is the worst of those kept.
    pub(super) fn keep_largest(&self, agg: usize, keep: usize) -> Vec<u32> {
        keep_top_slots(self.groups.len() as u32, keep, |a, b| {
            self.ranks_above(a, b, agg)
        })
    }

    /// Typed partial state of a query that holds everything in memory. A query that has
    /// spilled finishes with [`Self::try_finish_partial_state`], which may wait on the host.
    pub fn finish_partial_state(&mut self) -> PartialStateWcol {
        assert!(
            !self.spill_finalization_needed(),
            "a spilled query finishes with try_finish_partial_state"
        );
        self.finish_partial_state_eager()
    }

    /// Typed partial state, written straight into its columns (no per-group allocation).
    /// With spilled runs it first flushes the live table, which can wait on the host.
    pub fn try_finish_partial_state(&mut self) -> Result<SpillProgress<PartialStateWcol>, String> {
        if self.spill_finalization_needed()
            && matches!(self.flush_live_run()?, SpillProgress::AwaitHost)
        {
            return Ok(SpillProgress::AwaitHost);
        }
        if self
            .spill
            .as_ref()
            .is_some_and(|spill| spill.spill_finalization_needed())
        {
            return finish_stream::finish_partial_state_streaming(self).map(SpillProgress::Ready);
        }
        Ok(SpillProgress::Ready(self.finish_partial_state_eager()))
    }

    pub(crate) fn finish_partial_state_eager(&mut self) -> PartialStateWcol {
        let _phase = phase(&PHASE_FINISH_NS);
        if let Some(count) = self.distinct_cardinality() {
            return self.finish_distinct_count(count);
        }
        if self.pairs.is_some() && self.group_key_text == 0 {
            return self.finish_pairs();
        }
        if self.pairs.is_some() {
            let state = crate::stream::partials::partial_snapshot_to_wcol(&self.finish_snapshot())
                .expect("keyed partial snapshot");
            self.note_groups(state.row_count as u64, state.row_count as u64);
            return state;
        }
        if self.group_key_text != 0 || self.group_key_float != 0 {
            let state = crate::stream::partials::partial_snapshot_to_wcol(&self.finish_snapshot())
                .expect("keyed partial snapshot");
            self.note_groups(self.groups.len() as u64, state.row_count as u64);
            return state;
        }
        let n_aggs = self.aggs.len();
        let key_len = self.group_by.len();
        let slots = if key_len == 0 {
            Vec::new()
        } else {
            self.emit_slots()
        };
        let rows = if self.group_by.is_empty() {
            usize::from(self.totals_hit)
        } else {
            slots.len()
        };
        self.note_groups(self.groups.len() as u64, rows as u64);
        let mut out = PartialColumnsBuilder::new(key_len, &self.partial_aggs(), rows);
        if self.group_by.is_empty() {
            if self.totals_hit {
                for (index, acc) in self.totals.iter().enumerate() {
                    out.extend_stats(
                        index,
                        &[self.partial_stat(
                            index,
                            acc,
                            &self.totals_f[index],
                            self.totals_sketches[index].as_ref(),
                        )],
                    );
                }
            }
        } else {
            for &slot in &slots {
                let key = &self.groups.keys[slot as usize];
                for k in 0..key_len {
                    out.push_key(k, (key.nulls & (1 << k) == 0).then_some(key.values[k]));
                }
            }
            let mut stats = Vec::with_capacity(slots.len());
            for index in 0..n_aggs {
                stats.clear();
                stats.extend(slots.iter().map(|&slot| {
                    let at = slot as usize;
                    self.partial_stat(
                        index,
                        &self.groups.accs[index][at],
                        &self.groups.accs_f[index][at],
                        self.groups.sketches[index][at].as_ref(),
                    )
                }));
                out.extend_stats(index, &stats);
            }
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

    pub(crate) fn push_partial_group_to_builder(
        &self,
        out: &mut PartialColumnsBuilder,
        group: &PartialGroup,
    ) {
        for (k, part) in group.key.iter().enumerate() {
            match part {
                PartialKeyPart::Null => out.push_key(k, None),
                PartialKeyPart::Int(v) => out.push_key(k, Some(*v as i64)),
                PartialKeyPart::Float(bits) => out.push_key(k, Some(*bits as i64)),
                PartialKeyPart::Bytes(_) => out.push_key(k, None),
            }
        }
        for (index, stat) in group.stats.iter().enumerate() {
            out.extend_stats(index, std::slice::from_ref(stat));
        }
    }

    /// Aggregate `index`'s partial stat from its accumulator (and sketch, for approx).
    pub(super) fn partial_stat(
        &self,
        index: usize,
        acc: &Acc,
        facc: &FAcc,
        sketch: Option<&Hll>,
    ) -> PartialStat {
        let agg = self.aggs[index];
        let built = if self.agg_float[index] {
            facc.to_stat(agg.kind, self.offsets[index])
        } else {
            acc.to_stat(agg.kind, self.offsets[index])
        };
        let mut stat = stat_to_partial(&built);
        if agg.kind == KIND_APPROX_DISTINCT {
            stat.sketch = Some(sketch.cloned().unwrap_or_default());
        }
        stat
    }

    pub(crate) fn partial_group_from_main_slot(&self, slot: u32) -> PartialGroup {
        let n_aggs = self.aggs.len();
        let key = &self.groups.keys[slot as usize];
        let key_parts = (0..self.group_by.len())
            .map(|k| self.key_part(k, key))
            .collect();
        let stats = (0..n_aggs)
            .map(|index| {
                let at = slot as usize;
                self.partial_stat(
                    index,
                    &self.groups.accs[index][at],
                    &self.groups.accs_f[index][at],
                    self.groups.sketches[index][at].as_ref(),
                )
            })
            .collect();
        PartialGroup {
            key: key_parts,
            stats,
        }
    }

    pub(crate) fn empty_grouped_partial(&self) -> PartialStateWcol {
        let key_len = self.group_by.len();
        let out = PartialColumnsBuilder::new(key_len, &self.partial_aggs(), 0);
        out.finish(
            PartialStateSchemaV1 {
                version: PARTIAL_SCHEMA_VERSION,
                grouped: self.shell.grouped,
                order_groups_by_count: self.shell.order_groups_by_count,
                limit: self.shell.limit,
                offset: self.shell.offset,
                group_key_len: key_len as u8,
                group_key_float_mask: self.group_key_float,
                group_key_string_mask: self.group_key_text,
                aggs: self.partial_aggs(),
            },
            self.passed_rows,
            Vec::new(),
        )
    }

    pub(crate) fn partial_from_groups(
        &self,
        groups: Vec<PartialGroup>,
    ) -> Result<PartialStateWcol, String> {
        crate::stream::partials::partial_snapshot_to_wcol(&PartialSnapshot {
            grouped: self.shell.grouped,
            order_groups_by_count: self.shell.order_groups_by_count,
            limit: self.shell.limit,
            offset: self.shell.offset,
            passed_rows: self.passed_rows,
            aggs: self.partial_aggs(),
            groups,
            key_len: self.group_by.len() as u8,
            key_float_mask: self.group_key_float,
            key_string_mask: self.group_key_text,
        })
    }

    pub(crate) fn partial_aggs(&self) -> Vec<PartialAggSpec> {
        self.aggs
            .iter()
            .map(|agg| PartialAggSpec {
                kind: agg.kind,
                column: agg.column,
            })
            .collect()
    }

    pub(super) fn key_part(&self, k: usize, key: &GroupKey) -> PartialKeyPart {
        if key.nulls & (1 << k) != 0 {
            PartialKeyPart::Null
        } else if self.group_key_text & (1u32 << k) != 0 {
            PartialKeyPart::Bytes(self.interner.bytes(key.values[k] as u32).to_vec())
        } else if self.group_key_float & (1u32 << k) != 0 {
            PartialKeyPart::Float(canonical_f64(key_to_f64(key.values[k] as u64)).to_bits())
        } else {
            PartialKeyPart::Int(i128::from(key.values[k]))
        }
    }

    /// Partial state in the row kernel's format: groups ordered by key as its `BTreeMap`
    /// orders them (per part, `NULL` before any value).
    pub fn finish_snapshot(&self) -> PartialSnapshot {
        if let Some(pairs) = &self.pairs {
            let n_aggs = self.aggs.len();
            let empty = FAcc::empty();
            let groups = self
                .pair_slots()
                .into_iter()
                .map(|slot| {
                    let key = pairs.key(slot);
                    PartialGroup {
                        key: (0..self.group_by.len())
                            .map(|part| self.pair_key_part(part, key))
                            .collect(),
                        stats: (0..n_aggs)
                            .map(|index| {
                                self.partial_stat(index, &pairs.acc(slot, index), &empty, None)
                            })
                            .collect(),
                    }
                })
                .collect();
            return PartialSnapshot {
                grouped: self.shell.grouped,
                order_groups_by_count: self.shell.order_groups_by_count,
                limit: self.shell.limit,
                offset: self.shell.offset,
                passed_rows: self.passed_rows,
                aggs: self.partial_aggs(),
                groups,
                key_len: self.group_by.len() as u8,
                key_float_mask: self.group_key_float,
                key_string_mask: self.group_key_text,
            };
        }
        let stats = |accs: &[Acc], faccs: &[FAcc], sketches: &[Option<Hll>]| -> Vec<PartialStat> {
            accs.iter()
                .zip(faccs)
                .zip(sketches)
                .enumerate()
                .map(|(index, ((acc, facc), sketch))| {
                    self.partial_stat(index, acc, facc, sketch.as_ref())
                })
                .collect()
        };
        let mut groups = Vec::new();
        if self.group_by.is_empty() {
            if self.totals_hit {
                groups.push(PartialGroup {
                    key: Vec::new(),
                    stats: stats(&self.totals, &self.totals_f, &self.totals_sketches),
                });
            }
        } else {
            let keys = self.group_by.len();
            let n_aggs = self.aggs.len();
            let order = self.emit_slots();
            groups.reserve(order.len());
            for slot in order {
                let slot = slot as usize;
                let key = &self.groups.keys[slot];
                groups.push(PartialGroup {
                    key: (0..keys).map(|k| self.key_part(k, key)).collect(),
                    stats: (0..n_aggs)
                        .map(|index| {
                            self.partial_stat(
                                index,
                                &self.groups.accs[index][slot],
                                &self.groups.accs_f[index][slot],
                                self.groups.sketches[index][slot].as_ref(),
                            )
                        })
                        .collect(),
                });
            }
        }
        PartialSnapshot {
            grouped: self.shell.grouped,
            order_groups_by_count: self.shell.order_groups_by_count,
            limit: self.shell.limit,
            offset: self.shell.offset,
            passed_rows: self.passed_rows,
            aggs: self.partial_aggs(),
            groups,
            key_len: self.group_by.len() as u8,
            key_float_mask: self.group_key_float,
            key_string_mask: self.group_key_text,
        }
    }
}
