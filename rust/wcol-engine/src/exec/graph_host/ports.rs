//! Ingress and egress port I/O for the host.

use super::*;

impl GraphHost {
    pub fn input(&mut self) -> &mut [u8] {
        self.input.writable()
    }

    pub fn commit_input(&mut self, n: usize) -> WcolResult<()> {
        match &self.core.ingress {
            GraphHostIngress::HostParquet => {
                if self.error.is_some() || self.input.is_closed() || n > self.input().len() {
                    return Err(WcolError::from("invalid input commit"));
                }
                self.input.commit(n);
                Ok(())
            }
            GraphHostIngress::WcsRowBatches { .. } | GraphHostIngress::WcsPartialReduce { .. } => {
                self.commit_input_port(0, n)
            }
        }
    }

    pub fn end_input(&mut self) -> WcolResult<()> {
        match &self.core.ingress {
            GraphHostIngress::HostParquet => {
                if self.input.is_closed() {
                    return Err(WcolError::from("input already ended"));
                }
                self.input.mark_closed();
                Ok(())
            }
            GraphHostIngress::WcsRowBatches { .. } | GraphHostIngress::WcsPartialReduce { .. } => {
                self.end_input_port(0)
            }
        }
    }

    /// Push a host chunk into one WCS ingress port. Bytes that do not fit stay on the port
    /// and are retried from [`GraphHost::poll`].
    pub fn feed_input_port(&mut self, port: u32, bytes: &[u8]) -> WcolResult<()> {
        let n = self.try_feed_input_port(port, bytes)?;
        if n < bytes.len() {
            self.core.hold_suffix(port, &bytes[n..])?;
        }
        Ok(())
    }

    /// Bytes accepted from this chunk. `Ok(0)` means the port queue or shared budget is full
    /// and the caller must keep `bytes` (the stream-pool mailbox).
    pub fn try_feed_input_port(&mut self, port: u32, bytes: &[u8]) -> WcolResult<usize> {
        if self.error.is_some() {
            return Err(WcolError::from("graph host failed"));
        }
        match self.core.feed_port(port, bytes) {
            Ok(n) => Ok(n),
            Err(err) => {
                let message = err.as_str().to_string();
                self.error = Some(WcolError::msg(message.clone()));
                Err(WcolError::msg(message))
            }
        }
    }

    pub fn hash_partition_count(&self) -> u32 {
        self.core
            .hash_fanout
            .as_ref()
            .map(|fanout| fanout.egress.len() as u32)
            .unwrap_or(0)
    }

    pub fn hash_opened(&self, part: usize) -> bool {
        self.core
            .hash_fanout
            .as_ref()
            .and_then(|fanout| fanout.opened.get(part).copied())
            .unwrap_or(false)
    }

    pub fn hash_partition_has_pending_output(&self, part: usize) -> bool {
        self.core
            .hash_fanout
            .as_ref()
            .and_then(|fanout| fanout.egress.get(part))
            .is_some_and(|egress| egress.has_pending_output() || egress.mid_frame())
    }

    pub fn hash_write_partition(&mut self, part: usize, dst: &mut [u8]) -> WcolResult<usize> {
        let fanout = self
            .core
            .hash_fanout
            .as_mut()
            .ok_or_else(|| WcolError::from("missing hash fanout"))?;
        if part >= fanout.egress.len() {
            return Err(WcolError::from("hash partition out of range"));
        }
        fanout.egress[part].poll_write(dst)
    }

    /// Copy `n` ring bytes into the decoder for `port` only. Incomplete frames stay on that port.
    pub fn commit_input_port(&mut self, port: u32, n: usize) -> WcolResult<()> {
        if self.error.is_some() || n > self.input().len() {
            return Err(WcolError::from("invalid input commit"));
        }
        if matches!(self.core.ingress, GraphHostIngress::HostParquet) {
            return Err(WcolError::from("commit_input_port on parquet ingress"));
        }
        self.input.commit(n);
        let bytes = self.input.readable()[..n].to_vec();
        self.input.consume(n);
        self.feed_input_port(port, &bytes)
    }

    pub fn end_input_port(&mut self, port: u32) -> WcolResult<()> {
        if self.error.is_some() {
            return Err(WcolError::from("graph host failed"));
        }
        if matches!(self.core.ingress, GraphHostIngress::HostParquet) {
            return Err(WcolError::from("end_input_port on parquet ingress"));
        }
        match self.core.end_port(port) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.error = Some(WcolError::msg(err.as_str().to_string()));
                Err(err)
            }
        }
    }

    /// Dictionaries still owned by ingress, after completed streams have released theirs.
    #[cfg(test)]
    pub fn retained_dictionary_count(&self) -> usize {
        match &self.core.ingress {
            GraphHostIngress::WcsRowBatches { dicts, .. } => dicts.len(),
            _ => 0,
        }
    }

    pub fn mark_input_absent(&mut self, port: u32) -> WcolResult<()> {
        if self.error.is_some() {
            return Err(WcolError::from("graph host failed"));
        }
        if matches!(self.core.ingress, GraphHostIngress::HostParquet) {
            return Err(WcolError::from("mark_input_absent on parquet ingress"));
        }
        match self.core.mark_absent_port(port) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.error = Some(WcolError::msg(err.as_str().to_string()));
                Err(err)
            }
        }
    }

    pub fn output(&self) -> &[u8] {
        self.output.readable()
    }

    pub fn consume_output(&mut self, n: usize) -> WcolResult<()> {
        if n > self.output().len() {
            return Err(WcolError::from("invalid output acknowledgment"));
        }
        self.output.consume(n);
        Ok(())
    }
}
