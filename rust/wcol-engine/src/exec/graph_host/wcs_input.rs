//! WCS input frames: dictionaries, data batches, partial batches.

use super::*;

impl GraphHostCore {
    pub(super) fn handle_wcs_dictionary(&mut self, port: u32, payload: &[u8]) -> WcolResult<()> {
        let chunk = decode_dictionary_chunk(payload)?;
        let GraphHostIngress::WcsRowBatches {
            ports,
            dicts,
            next_fresh_scope,
            ..
        } = &mut self.ingress
        else {
            return Err(WcolError::from("dictionary frame on a non-row ingress"));
        };
        let producer = ports
            .get_mut(port as usize)
            .ok_or_else(|| WcolError::msg(format!("dictionary on invalid port {port}")))?;
        let original = chunk.scope;
        let mut scope = producer
            .scope_remap
            .get(&original)
            .copied()
            .unwrap_or(original);
        let start_index = chunk.start_index;
        let column = chunk.column;
        let key = (scope, column);
        if start_index == 0 {
            if let Some(previous) = dicts.get(&key) {
                if previous.entry_count() > 0 {
                    if producer.installed_scopes.contains(&original) {
                        return Err(WcolError::from(
                            "dictionary changed for a scope that was already installed",
                        ));
                    }
                    scope = *next_fresh_scope;
                    *next_fresh_scope = next_fresh_scope.saturating_add(1);
                    producer.scope_remap.insert(original, scope);
                }
            }
        } else if dicts.get(&key).is_none() {
            return Err(WcolError::from("wcs: dictionary chunk gap at stream start"));
        }
        if start_index == 0 && !producer.installed_scopes.contains(&original) {
            wcs_port::release_superseded_dicts(port, producer, scope, dicts);
        }
        let mut chunk = chunk;
        chunk.scope = scope;
        let install_key = (scope, column);
        match dicts.get_mut(&install_key) {
            None => {
                if start_index != 0 {
                    return Err(WcolError::from("wcs: dictionary chunk gap at stream start"));
                }
                dicts.insert(install_key, chunk.into_values()?);
            }
            Some(existing) => merge_dictionary_chunk(existing, &chunk)?,
        }
        if start_index == 0 {
            producer.installed_scopes.insert(original);
        }
        producer.installed_dict_keys.insert(install_key);
        if let Some(dict) = dicts.get(&install_key) {
            crate::exec::dict_traffic::note_installed(
                port,
                install_key.0,
                install_key.1,
                crate::exec::dict_traffic::payload_bytes(dict),
                crate::exec::dict_traffic::allocation_bytes(dict),
            );
        }
        Ok(())
    }

    pub(super) fn note_wcs_growth(&self) {
        let (wcs_pending, active_inputs) = match &self.ingress {
            GraphHostIngress::WcsRowBatches { ports, pending, .. }
            | GraphHostIngress::WcsPartialReduce { ports, pending, .. } => {
                let queued = wcs_port::pending_queue_bytes(pending) as u64;
                let retained = ports.iter().fold(0u64, |sum, port| {
                    sum.saturating_add(port.retained_bytes() as u64)
                });
                let active = ports.iter().filter(|port| !port.transport_eof).count() as u64;
                (queued.saturating_add(retained), active)
            }
            GraphHostIngress::HostParquet => (0, 0),
        };
        if let Some(aggregate) = self.row_aggregate.as_ref() {
            aggregate.publish_growth_gauges();
        }
        crate::exec::mem_growth::set_phase(crate::exec::mem_growth::PHASE_WCS_INGRESS);
        crate::exec::mem_growth::set_ingress(wcs_pending, active_inputs, 0);
        crate::exec::mem_growth::observe(false);
    }

    pub(super) fn handle_wcs_data_batch(&mut self, port: u32, payload: &[u8]) -> WcolResult<()> {
        let batch = {
            let (dicts, scope_remap) = match &self.ingress {
                GraphHostIngress::WcsRowBatches { ports, dicts, .. } => {
                    let producer = ports.get(port as usize).ok_or_else(|| {
                        WcolError::msg(format!("data batch on invalid port {port}"))
                    })?;
                    (dicts, &producer.scope_remap)
                }
                _ => return Err(WcolError::from("data batch on a non-row ingress")),
            };
            if payload.len() < 5 {
                return Err(WcolError::from("truncated data batch"));
            }
            let original = u32::from_le_bytes(payload[1..5].try_into().unwrap());
            let mapped = scope_remap.get(&original).copied().unwrap_or(original);
            if dicts.is_empty() {
                owned_batch_from_projection_body(payload)?
            } else if mapped == original {
                owned_batch_from_projection_body_with_dicts(payload, dicts)?
            } else {
                let mut patched = payload.to_vec();
                patched[1..5].copy_from_slice(&mapped.to_le_bytes());
                owned_batch_from_projection_body_with_dicts(&patched, dicts)?
            }
        };
        self.note_wcs_growth();
        self.consume_reducer_input(ReducerInput::Rows(batch))
    }

    /// A scan-side combiner's partial: merged into the reducer's table like the rows it sums.
    pub(super) fn handle_wcs_partial_batch(&mut self, payload: &[u8]) -> WcolResult<()> {
        if self.row_aggregate.is_none() {
            return Err(WcolError::from(
                "partial frame on a reducer without an aggregate",
            ));
        }
        let state = crate::stream::partial_wcs::partial_state_from_wire_payload(payload)
            .map_err(|e| WcolError::msg(format!("partial frame decode failed: {e}")))?;
        self.note_wcs_growth();
        self.consume_reducer_input(ReducerInput::Partial(state))
    }

    pub(super) fn consume_reducer_input(&mut self, input: ReducerInput) -> WcolResult<()> {
        if let Some(aggregate) = self.row_aggregate.as_mut() {
            if aggregate.spill_flush_incomplete() {
                match aggregate.flush_live_run() {
                    Ok(SpillProgress::Ready(())) => {}
                    Ok(SpillProgress::AwaitHost) => {
                        self.set_spill_resume(input, false);
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(WcolError::msg(format!(
                            "hash reducer aggregate failed: {e}"
                        )));
                    }
                }
            }
            let consumed = match &input {
                ReducerInput::Rows(batch) => aggregate.consume_row_batch(
                    batch,
                    &crate::stream::SelectionMask::all(batch.row_count()),
                ),
                ReducerInput::Partial(state) => aggregate.consume_partial(state),
            };
            let hold_frames = match consumed {
                Ok(SpillProgress::Ready(())) => {
                    crate::exec::mem_growth::set_phase(
                        crate::exec::mem_growth::PHASE_HASH_AGGREGATE,
                    );
                    aggregate.spill_flush_incomplete()
                }
                Ok(SpillProgress::AwaitHost) => {
                    let store_needs_reconcile = self
                        .row_aggregate
                        .as_ref()
                        .is_some_and(|a| a.spill_host_op_pending());
                    let yield_host = !self.spill_host_pending() && !store_needs_reconcile;
                    self.set_spill_resume(input, yield_host);
                    return Ok(());
                }
                Err(e) => {
                    return Err(WcolError::msg(format!(
                        "hash reducer aggregate failed: {e}"
                    )));
                }
            };
            self.clear_spill_resume();
            if hold_frames {
                self.set_hold_frames();
            }
            Ok(())
        } else {
            let ReducerInput::Rows(batch) = input else {
                return Err(WcolError::from("partial input without a reducer aggregate"));
            };
            self.graph
                .inject_row_batch(batch)
                .map_err(|e| WcolError::msg(format!("inject batch failed: {e}")))
        }
    }

    pub(super) fn retry_spill_resume_input(&mut self) -> WcolResult<()> {
        if self.spill_yield_pending()
            || !self.spill_resume_pending()
            || self.spill_bridge_host_pending()
        {
            return Ok(());
        }
        let input = self.take_spill_resume().expect("resume input");
        self.consume_reducer_input(input)
    }

    pub(super) fn poll_wcs_rows(
        &mut self,
        output: &mut [u8],
    ) -> WcolResult<crate::stream::core::Progress> {
        if self.graph.has_byte_sink() {
            return self.poll_wcs_parquet(output);
        }
        if self.row_egress.is_some() {
            return self.poll_wcs_row_output(output);
        }
        if self.emit.wire_complete() {
            return Ok(progress_after_io(0, 0, true));
        }
        let flushed = self.emit.flush(output)?;
        if flushed > 0 {
            return Ok(progress_after_io(0, flushed, self.emit.wire_complete()));
        }
        self.drain_row_frames()?;
        if self.wcs_input_ended() {
            let produced = self.emit_partial_frame(output)?;
            if produced == 0 {
                if self.spill_host_pending() {
                    return Ok(progress_after_io(0, 0, false));
                }
                let flushed = self.emit.flush(output)?;
                return Ok(progress_after_io(0, flushed, self.emit.wire_complete()));
            }
            return Ok(progress_after_io(0, produced, self.emit.wire_complete()));
        }
        Ok(progress_after_io(0, 0, false))
    }
}
