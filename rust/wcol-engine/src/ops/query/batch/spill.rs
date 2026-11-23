//! Spill bookkeeping: run encoding, flush timing and retained capacity.

use super::*;

impl BatchQuery {
    pub fn close_spill(&mut self) -> Result<SpillProgress<()>, String> {
        let Some(spill) = self.spill.as_mut() else {
            return Ok(SpillProgress::Ready(()));
        };
        if matches!(spill.end()?, SpillProgress::AwaitHost) {
            return Ok(SpillProgress::AwaitHost);
        }
        self.spill = None;
        Ok(SpillProgress::Ready(()))
    }

    pub(crate) fn has_group_spill(&self) -> bool {
        self.spill.is_some()
    }

    /// The spill's resident memory budget, if group spill is on.
    pub(crate) fn spill_budget_bytes(&self) -> Option<u64> {
        self.spill
            .as_ref()
            .map(group_spill::GroupSpill::combined_budget_bytes)
    }

    pub(crate) fn ingest_spill_host_read(&mut self) -> Result<(), String> {
        if let Some(spill) = self.spill.as_mut() {
            spill.ingest_host_read_completion()
        } else {
            Ok(())
        }
    }

    pub(crate) fn on_spill_run_put_handoff(&mut self) -> Result<(), String> {
        if let Some(spill) = self.spill.as_mut() {
            spill.on_spill_run_put_handoff()
        } else {
            Ok(())
        }
    }

    pub(crate) fn on_spill_run_commit_host_complete(&mut self) -> Result<(), String> {
        if let Some(spill) = self.spill.as_mut() {
            spill.on_spill_run_commit_host_complete()?;
            publish_spill_stats(spill.stats());
        }
        Ok(())
    }

    pub(crate) fn spill_host_op_pending(&self) -> bool {
        self.spill
            .as_ref()
            .is_some_and(|spill| spill.host_pending())
    }

    pub(crate) fn spill_needs_finish_run(&self) -> bool {
        self.spill
            .as_ref()
            .is_some_and(|spill| spill.needs_finish_run())
    }

    pub(crate) fn spill_finalization_needed(&self) -> bool {
        self.spill
            .as_ref()
            .is_some_and(|spill| spill.spill_finalization_needed())
    }

    /// The batch's rows are already applied. A flush that waits on the host must
    /// resume after those rows, not replay them.
    pub(super) fn finish_spill_batch(
        &mut self,
        row_count: usize,
    ) -> Result<SpillProgress<()>, String> {
        self.spill_consume_at = row_count;
        match self.spill_flush_if_needed()? {
            SpillProgress::Ready(()) => {
                self.spill_consume_at = 0;
                Ok(SpillProgress::Ready(()))
            }
            SpillProgress::AwaitHost => Ok(SpillProgress::AwaitHost),
        }
    }

    pub(crate) fn spill_flush_if_needed(&mut self) -> Result<SpillProgress<()>, String> {
        if self.spill.is_none() {
            return Ok(SpillProgress::Ready(()));
        }
        self.note_spill_live();
        if self.spill_flush_due() {
            spill_pull!(self.flush_live_run());
        }
        Ok(SpillProgress::Ready(()))
    }

    /// A flush has been handed to the host, or a sorted run still has pages left.
    /// New groups must wait; `reset_live_after_run` would drop them.
    ///
    /// Includes `needs_finish_run` so an R2 put that finished uploading but has
    /// not yet committed still counts as incomplete (PutInFlight → CommitPending).
    pub(crate) fn spill_flush_incomplete(&self) -> bool {
        self.spill_flush_at > 0
            || self
                .spill
                .as_ref()
                .is_some_and(|spill| spill.host_pending() || spill.needs_finish_run())
    }

    pub(super) fn note_spill_live(&mut self) {
        let groups = self.live_group_len() as u64;
        let resident = self.resident_state_bytes();
        let interner = self.interner.occupied_bytes() as u64;
        if let Some(spill) = self.spill.as_mut() {
            spill.note_live(groups, resident, interner);
            publish_spill_stats(spill.stats());
        }
    }

    /// Ingest must enter `flush_live_run` before inserting: a run is mid-flush, a flushed
    /// run is still uploading or uncommitted, or the live table reached its budget. Waiting
    /// on an unfinished run is what bounds memory to one table plus one run body.
    pub(super) fn spill_flush_due(&self) -> bool {
        self.spill_flush_at > 0
            || self.spill.as_ref().is_some_and(|spill| {
                spill.needs_finish_run() || spill.resident_exceeds(self.resident_state_bytes())
            })
    }

    pub(crate) fn live_group_len(&self) -> usize {
        if let Some(pairs) = &self.pairs {
            pairs.len()
        } else {
            self.groups.len()
        }
    }

    pub(super) fn prepare_spill_flush_order(&mut self) {
        let n = self.live_group_len() as u32;
        let mut order = std::mem::take(&mut self.spill_sort_scratch);
        order.clear();
        order.extend(0..n);
        if let Some(pairs) = self.pairs.as_ref() {
            // Text parts order by their bytes: the run must be in canonical key order.
            order.sort_unstable_by(|&a, &b| self.cmp_pair_key(pairs.key(a), pairs.key(b)));
        } else {
            let key_len = self.group_by.len();
            let text = self.group_key_text;
            let float = self.group_key_float;
            order.sort_unstable_by(|&a, &b| {
                cmp_keys(
                    &self.groups.keys[a as usize],
                    &self.groups.keys[b as usize],
                    key_len,
                    text,
                    float,
                    &self.interner,
                )
            });
        }
        self.spill_sort_scratch = order;
    }

    pub(super) fn encode_main_slot(&self, slot: u32) -> Result<(Vec<u8>, Vec<u8>), String> {
        let key = &self.groups.keys[slot as usize];
        let parts: Vec<PartialKeyPart> = (0..self.group_by.len())
            .map(|k| self.key_part(k, key))
            .collect();
        let blob = encode_canonical_key(&parts).map_err(|e| e.to_string())?;
        let group = self.partial_group_from_main_slot(slot);
        Ok((blob, encode_partial_group_payload(&group)?))
    }

    /// One key part of a pair group. A text part is an interner id; its bytes are the part.
    pub(super) fn pair_key_part(&self, part: usize, key: &PairKey) -> PartialKeyPart {
        if key.nulls & (1 << part) != 0 {
            PartialKeyPart::Null
        } else if self.group_key_text & (1 << part) != 0 {
            PartialKeyPart::Bytes(self.interner.bytes(key.part(part) as u32).to_vec())
        } else {
            PartialKeyPart::Int(i128::from(key.part(part)))
        }
    }

    pub(super) fn encode_pair_slot(&self, slot: u32) -> Result<(Vec<u8>, Vec<u8>), String> {
        let pairs = self.pairs.as_ref().expect("two-integer groups");
        let key = pairs.key(slot);
        let parts: Vec<PartialKeyPart> = (0..self.group_by.len())
            .map(|part| self.pair_key_part(part, key))
            .collect();
        let blob = encode_canonical_key(&parts).map_err(|e| e.to_string())?;
        let n_aggs = self.aggs.len();
        let empty = FAcc::empty();
        let stats = (0..n_aggs)
            .map(|index| self.partial_stat(index, &pairs.acc(slot, index), &empty, None))
            .collect();
        let group = PartialGroup { key: parts, stats };
        Ok((blob, encode_partial_group_payload(&group)?))
    }

    /// Hand everything aggregated so far to the reducers, one partition at a time, and start
    /// again empty: a scan-side combiner's flush. Each group goes where its raw rows would
    /// (`route`). A partition's partial is built straight from the table and handed to
    /// `emit` before the next is built, so a flush holds the table and one partition's
    /// partial, never the whole partial: built whole, Q33's URL groups ran scans out of
    /// memory. Merging every partial handed on equals one partial of all the rows: the first
    /// partition handed on carries `passed_rows`. The emptied table keeps at most
    /// `retain_bytes`, since a table kept above a combiner's budget would flush every batch.
    pub(crate) fn flush_routed(
        &mut self,
        route: &crate::ops::exchange::CombinerRoute,
        retain_bytes: u64,
        mut emit: impl FnMut(u32, PartialStateWcol) -> Result<(), String>,
    ) -> Result<(), String> {
        let key_len = self.group_by.len();
        if key_len == 0 || route.keys.iter().any(|(position, _)| *position >= key_len) {
            return Err("a combiner routes by keys of its own partial key".to_string());
        }
        let rows = self
            .pairs
            .as_ref()
            .map_or(self.groups.len(), PairGroups::len);
        let mut slots: Vec<Vec<u32>> = vec![Vec::new(); route.partitions.max(1) as usize];
        let mut buf = Vec::new();
        for slot in 0..rows as u32 {
            let (nulls, values) = self.slot_key(slot);
            let parts = route
                .keys
                .iter()
                .map(|&(position, column)| (column, self.key_part_ref(position, nulls, &values)));
            slots[route.partition(parts, &mut buf)? as usize].push(slot);
        }
        let mut passed = self.passed_rows;
        for (partition, part) in slots.iter().enumerate() {
            if part.is_empty() {
                continue;
            }
            emit(partition as u32, self.table_partial_state(part, passed))?;
            passed = 0;
        }
        self.reset_live_after_run();
        self.passed_rows = 0;
        self.totals.fill(Acc::EMPTY);
        self.totals_f.fill(FAcc::empty());
        self.totals_sketches.fill(None);
        self.totals_hit = false;
        if self.resident_state_bytes() > retain_bytes {
            self.release_excess_capacity();
        }
        Ok(())
    }

    /// Null bits and values of a live group's key, from whichever table holds it.
    pub(super) fn slot_key(&self, slot: u32) -> (u8, [i64; MAX_KEYS]) {
        match &self.pairs {
            Some(pairs) => {
                let key = pairs.key(slot);
                let mut values = [0i64; MAX_KEYS];
                values[..3].copy_from_slice(&[key.a, key.b, key.c]);
                (key.nulls, values)
            }
            None => {
                let key = &self.groups.keys[slot as usize];
                (key.nulls, key.values)
            }
        }
    }

    /// Key part `k` as a partial reports it: interned text as its bytes, float keys as
    /// canonical bits.
    pub(super) fn key_part_ref(
        &self,
        k: usize,
        nulls: u8,
        values: &[i64; MAX_KEYS],
    ) -> KeyPartRef<'_> {
        if nulls & (1 << k) != 0 {
            KeyPartRef::Null
        } else if self.group_key_text & (1u32 << k) != 0 {
            KeyPartRef::Bytes(self.interner.bytes(values[k] as u32))
        } else if self.group_key_float & (1u32 << k) != 0 {
            KeyPartRef::Float(canonical_f64(key_to_f64(values[k] as u64)).to_bits())
        } else {
            KeyPartRef::Int(values[k])
        }
    }

    /// The partial of `slots`, in that order, written straight from the table: no sorted
    /// snapshot (which held every key and stat as its own allocation, several times the
    /// table). It carries `passed_rows`.
    pub(super) fn table_partial_state(&self, slots: &[u32], passed_rows: u64) -> PartialStateWcol {
        let key_len = self.group_by.len();
        let rows = slots.len();
        self.note_groups(rows as u64, rows as u64);
        let mut out = PartialColumnsBuilder::new(key_len, &self.partial_aggs(), rows);
        let mut strings: Vec<PartialStringKeyColumn> = (0..key_len)
            .filter(|k| self.group_key_text & (1u32 << k) != 0)
            .map(|k| PartialStringKeyColumn {
                key_index: k as u8,
                offsets: vec![0],
                bytes: Vec::new(),
                nulls: Vec::with_capacity(rows),
            })
            .collect();
        for &slot in slots {
            let (nulls, values) = self.slot_key(slot);
            let mut text = strings.iter_mut();
            for (k, &value) in values.iter().enumerate().take(key_len) {
                let present = nulls & (1 << k) == 0;
                if self.group_key_text & (1u32 << k) != 0 {
                    out.push_key(k, present.then_some(0));
                    let column = text.next().expect("a string column per text key");
                    if present {
                        column
                            .bytes
                            .extend_from_slice(self.interner.bytes(value as u32));
                    }
                    column.nulls.push(u8::from(present));
                    column.offsets.push(column.bytes.len() as u32);
                } else if self.group_key_float & (1u32 << k) != 0 {
                    let bits = canonical_f64(key_to_f64(value as u64)).to_bits() as i64;
                    out.push_key(k, present.then_some(bits));
                } else {
                    out.push_key(k, present.then_some(value));
                }
            }
        }
        let empty = FAcc::empty();
        let mut stats = Vec::with_capacity(rows);
        for index in 0..self.aggs.len() {
            stats.clear();
            stats.extend(slots.iter().map(|&slot| match &self.pairs {
                Some(pairs) => self.partial_stat(index, &pairs.acc(slot, index), &empty, None),
                None => {
                    let at = slot as usize;
                    self.partial_stat(
                        index,
                        &self.groups.accs[index][at],
                        &self.groups.accs_f[index][at],
                        self.groups.sketches[index][at].as_ref(),
                    )
                }
            }));
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
                group_key_float_mask: self.group_key_float,
                group_key_string_mask: self.group_key_text,
                aggs: self.partial_aggs(),
            },
            passed_rows,
            strings,
        )
    }

    pub(super) fn reset_live_after_run(&mut self) {
        // Every live slot has already been encoded into the spill store. The
        // commit may still be waiting on the host; that wait holds the run
        // body, not these groups.
        self.groups.clear_retaining_capacity();
        if let Some(pairs) = self.pairs.as_mut() {
            pairs.clear_retaining_capacity();
        }
        self.interner.clear_retaining_capacity();
        self.spill_flush_at = 0;
        self.spill_sort_scratch.clear();
        if self.resident_state_bytes() > self.retained_capacity_limit() {
            self.release_excess_capacity();
        }
    }

    /// Low-water mark for an empty table: 8 MiB, and always under the flush lock.
    pub(super) fn retained_capacity_limit(&self) -> u64 {
        let under_lock = self
            .spill
            .as_ref()
            .map(|spill| spill.config().main_lock_threshold().saturating_sub(1))
            .unwrap_or(RETAINED_CAPACITY_TARGET);
        RETAINED_CAPACITY_TARGET.min(under_lock)
    }

    /// Free the emptied table before the final merge. Input has ended, so nothing refills
    /// it, and its retained capacity is the hole the merge's read buffers should reuse.
    pub(crate) fn release_table_for_merge(&mut self) {
        debug_assert_eq!(self.live_group_len(), 0, "table released with live groups");
        self.release_excess_capacity();
    }

    pub(super) fn release_excess_capacity(&mut self) {
        self.groups.release_excess_capacity();
        if let Some(pairs) = self.pairs.as_mut() {
            pairs.release_excess_capacity();
        }
        self.interner.release_excess_capacity();
        self.spill_sort_scratch = Vec::new();
    }

    pub(super) fn drain_spill_finish_runs(&mut self) -> Result<SpillProgress<()>, String> {
        let mut pass = 0u32;
        loop {
            let Some(spill) = self.spill.as_ref() else {
                return Ok(SpillProgress::Ready(()));
            };
            if !spill.needs_finish_run() {
                break;
            }
            if pass >= 32 {
                return Err("spill finish run drain cap".into());
            }
            spill_pull!(self.spill.as_mut().expect("spill").finish_run());
            if self.spill.as_ref().expect("spill").host_pending() {
                return Ok(SpillProgress::AwaitHost);
            }
            pass += 1;
        }
        Ok(SpillProgress::Ready(()))
    }
}
