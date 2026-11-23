//! Reducer ingress state: WCS ports, held frames and spill phase flags.

use super::*;

impl GraphHostCore {
    pub(super) fn finalize_mode(&self) -> bool {
        !matches!(self.finalize, FinalizePhase::Inactive)
    }

    pub(super) fn is_finalized(&self) -> bool {
        matches!(self.finalize, FinalizePhase::Emitted)
    }

    pub(super) fn take_spill_yield(&mut self) -> bool {
        if let SpillPhase::Resume { yield_host, .. } = &mut self.spill_drive.phase {
            if *yield_host {
                *yield_host = false;
                return true;
            }
        }
        false
    }

    pub(super) fn spill_resume_pending(&self) -> bool {
        matches!(self.spill_drive.phase, SpillPhase::Resume { .. })
    }

    pub(super) fn spill_yield_pending(&self) -> bool {
        matches!(
            self.spill_drive.phase,
            SpillPhase::Resume {
                yield_host: true,
                ..
            }
        )
    }

    pub(super) fn spill_chunks_finished(&self) -> bool {
        self.spill_drive.chunks_finished
    }

    pub(super) fn spill_blocks_rows(&self) -> bool {
        matches!(
            self.spill_drive.phase,
            SpillPhase::Resume { .. } | SpillPhase::HoldFrames
        )
    }

    pub(super) fn set_spill_resume(&mut self, input: ReducerInput, yield_host: bool) {
        self.spill_drive.phase = SpillPhase::Resume { input, yield_host };
    }

    pub(super) fn clear_spill_resume(&mut self) {
        if matches!(self.spill_drive.phase, SpillPhase::Resume { .. }) {
            self.spill_drive.phase = SpillPhase::Idle;
        }
    }

    pub(super) fn take_spill_resume(&mut self) -> Option<ReducerInput> {
        match std::mem::replace(&mut self.spill_drive.phase, SpillPhase::Idle) {
            SpillPhase::Resume { input, .. } => Some(input),
            other => {
                self.spill_drive.phase = other;
                None
            }
        }
    }

    pub(super) fn set_hold_frames(&mut self) {
        self.spill_drive.phase = SpillPhase::HoldFrames;
    }

    pub(super) fn clear_hold_frames(&mut self) {
        if matches!(self.spill_drive.phase, SpillPhase::HoldFrames) {
            self.spill_drive.phase = SpillPhase::Idle;
        }
    }

    pub(super) fn set_chunks_finished(&mut self) {
        self.spill_drive.chunks_finished = true;
    }

    pub(super) fn wcs_ports_pending(
        &mut self,
    ) -> Option<(&mut Vec<WcsPort>, &mut VecDeque<(u32, CompleteFrame)>)> {
        match &mut self.ingress {
            GraphHostIngress::WcsRowBatches { ports, pending, .. }
            | GraphHostIngress::WcsPartialReduce { ports, pending, .. } => Some((ports, pending)),
            GraphHostIngress::HostParquet => None,
        }
    }

    pub(super) fn hold_suffix(&mut self, port: u32, suffix: &[u8]) -> WcolResult<()> {
        if suffix.is_empty() {
            return Ok(());
        }
        let (ports, _) = self
            .wcs_ports_pending()
            .ok_or_else(|| WcolError::from("hold_suffix on parquet ingress"))?;
        let slot = ports
            .get_mut(port as usize)
            .ok_or_else(|| WcolError::msg(format!("invalid WCS input port {port}")))?;
        slot.hold_suffix(suffix);
        Ok(())
    }

    /// Move retained suffixes into the decoder, one frame per port per call.
    pub(super) fn drain_held_ingress(&mut self) -> WcolResult<()> {
        let count = match &self.ingress {
            GraphHostIngress::WcsRowBatches { ports, .. }
            | GraphHostIngress::WcsPartialReduce { ports, .. } => ports.len(),
            GraphHostIngress::HostParquet => return Ok(()),
        };
        for port in 0..count {
            let held = {
                let (ports, _) = self
                    .wcs_ports_pending()
                    .ok_or_else(|| WcolError::from("drain on parquet ingress"))?;
                ports[port].take_held()
            };
            if held.is_empty() {
                continue;
            }
            let n = self.feed_port(port as u32, &held)?;
            if n < held.len() {
                self.hold_suffix(port as u32, &held[n..])?;
            }
        }
        Ok(())
    }

    pub(super) fn feed_port(&mut self, port: u32, bytes: &[u8]) -> WcolResult<usize> {
        let (ports, pending) = self
            .wcs_ports_pending()
            .ok_or_else(|| WcolError::from("feed_port on parquet ingress"))?;
        let consumed = wcs_port::feed_port(ports, pending, port, bytes)?;
        self.try_finish_wcs();
        Ok(consumed)
    }

    pub(super) fn end_port(&mut self, port: u32) -> WcolResult<()> {
        match &mut self.ingress {
            GraphHostIngress::WcsRowBatches { ports, .. }
            | GraphHostIngress::WcsPartialReduce { ports, .. } => wcs_port::end_port(ports, port)?,
            GraphHostIngress::HostParquet => {
                return Err(WcolError::from("end_port on parquet ingress"));
            }
        }
        self.try_finish_wcs();
        self.release_finished_stream_dicts();
        Ok(())
    }

    pub(super) fn mark_absent_port(&mut self, port: u32) -> WcolResult<()> {
        match &mut self.ingress {
            GraphHostIngress::WcsRowBatches { ports, .. }
            | GraphHostIngress::WcsPartialReduce { ports, .. } => {
                wcs_port::mark_absent(ports, port)?
            }
            GraphHostIngress::HostParquet => {
                return Err(WcolError::from("mark_absent on parquet ingress"));
            }
        }
        self.try_finish_wcs();
        self.release_finished_stream_dicts();
        Ok(())
    }

    pub(super) fn release_finished_stream_dicts(&mut self) {
        let GraphHostIngress::WcsRowBatches {
            ports,
            pending,
            dicts,
            ..
        } = &mut self.ingress
        else {
            return;
        };
        wcs_port::release_completed_stream_dicts(ports, pending, dicts);
    }

    pub(super) fn try_finish_wcs(&mut self) {
        match &mut self.ingress {
            GraphHostIngress::WcsRowBatches {
                ports, input_ended, ..
            } => {
                if wcs_port::all_complete(ports) && !*input_ended {
                    *input_ended = true;
                    self.graph.mark_ingest_done();
                }
            }
            GraphHostIngress::WcsPartialReduce {
                ports, input_ended, ..
            } => {
                if wcs_port::all_complete(ports) {
                    *input_ended = true;
                }
            }
            GraphHostIngress::HostParquet => {}
        }
    }

    pub(super) fn take_pending_frames(&mut self) -> Vec<(u32, CompleteFrame)> {
        match &mut self.ingress {
            GraphHostIngress::WcsRowBatches { pending, .. }
            | GraphHostIngress::WcsPartialReduce { pending, .. } => pending.drain(..).collect(),
            GraphHostIngress::HostParquet => Vec::new(),
        }
    }

    pub(super) fn prepend_pending_frames(&mut self, frames: Vec<(u32, CompleteFrame)>) {
        match &mut self.ingress {
            GraphHostIngress::WcsRowBatches { pending, .. }
            | GraphHostIngress::WcsPartialReduce { pending, .. } => {
                for frame in frames.into_iter().rev() {
                    pending.push_front(frame);
                }
            }
            GraphHostIngress::HostParquet => {}
        }
    }

    pub(super) fn wcs_awaiting_input(&self) -> bool {
        if self
            .spill_bridge
            .as_ref()
            .is_some_and(|b| b.borrow().has_pending())
        {
            return false;
        }
        match &self.ingress {
            GraphHostIngress::WcsRowBatches {
                pending,
                ports,
                input_ended,
                ..
            }
            | GraphHostIngress::WcsPartialReduce {
                pending,
                ports,
                input_ended,
            } => {
                pending.is_empty()
                    && !*input_ended
                    && ports.iter().all(|port| !port.has_held())
                    && ports.iter().any(|port| !port.is_complete())
            }
            GraphHostIngress::HostParquet => false,
        }
    }

    pub(super) fn spill_host_pending(&self) -> bool {
        if self
            .spill_bridge
            .as_ref()
            .is_some_and(|b| b.borrow().has_pending())
        {
            return true;
        }
        self.row_aggregate
            .as_ref()
            .is_some_and(|a| a.spill_host_op_pending())
    }

    pub(super) fn reconcile_spill_host_reads(&mut self) -> WcolResult<()> {
        if let Some(aggregate) = self.row_aggregate.as_mut() {
            aggregate.ingest_spill_host_read().map_err(WcolError::msg)?;
        }
        Ok(())
    }

    pub(super) fn spill_bridge_host_pending(&self) -> bool {
        self.spill_bridge
            .as_ref()
            .is_some_and(|b| b.borrow().has_pending())
    }

    pub(super) fn wcs_input_ended(&self) -> bool {
        match &self.ingress {
            GraphHostIngress::WcsRowBatches { input_ended, .. }
            | GraphHostIngress::WcsPartialReduce { input_ended, .. } => *input_ended,
            GraphHostIngress::HostParquet => false,
        }
    }

    pub(super) fn wcs_has_pending_row_work(&self) -> bool {
        if self.spill_resume_pending() || self.spill_yield_pending() {
            return true;
        }
        match &self.ingress {
            GraphHostIngress::WcsRowBatches { pending, .. }
            | GraphHostIngress::WcsPartialReduce { pending, .. } => !pending.is_empty(),
            GraphHostIngress::HostParquet => false,
        }
    }
}
