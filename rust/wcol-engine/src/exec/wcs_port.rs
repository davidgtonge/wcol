//! Per-edge WCS decoder: lazy ingress, FRAME_END vs transport EOF, isolated dict remap.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::{WcolError, WcolResult};
use crate::exec::operators::{CompleteFrame, WcsIngress};
use crate::stream::core::WCS_MAX_PAYLOAD;

/// Decoder bytes across every input port. Mailboxes are not included.
/// One 64 KiB frame on each of 100 ports is 6.25 MiB.
pub const WCS_INGRESS_BUDGET_BYTES: usize = 8 * 1024 * 1024;
use crate::stream::wcs::FRAME_END;

pub struct WcsPort {
    ingress: Option<WcsIngress>,
    /// Bytes a caller could not keep, retried once the queue has room.
    held: Vec<u8>,
    pub saw_end: bool,
    pub transport_eof: bool,
    pub scope_remap: HashMap<u32, u32>,
    pub installed_scopes: HashSet<u32>,
    /// Dictionary keys this stream installed. Dropped once the stream is complete
    /// and none of its frames are still waiting to be decoded.
    pub installed_dict_keys: HashSet<(u32, u32)>,
}

impl WcsPort {
    pub fn new() -> Self {
        Self {
            ingress: None,
            held: Vec::new(),
            saw_end: false,
            transport_eof: false,
            scope_remap: HashMap::new(),
            installed_scopes: HashSet::new(),
            installed_dict_keys: HashSet::new(),
        }
    }

    pub fn is_complete(&self) -> bool {
        self.saw_end && self.transport_eof
    }

    pub fn retained_bytes(&self) -> usize {
        self.held.len()
            + self
                .ingress
                .as_ref()
                .map(WcsIngress::buffered_bytes)
                .unwrap_or(0)
    }
}

/// Drop dictionaries that belong only to streams whose frames have all been decoded.
pub fn release_completed_stream_dicts(
    ports: &mut [WcsPort],
    pending: &VecDeque<(u32, CompleteFrame)>,
    dicts: &mut HashMap<(u32, u32), crate::stream::DictionaryValues>,
) {
    let busy: HashSet<u32> = pending.iter().map(|(port, _)| *port).collect();
    for (index, port) in ports.iter_mut().enumerate() {
        if !port.is_complete()
            || busy.contains(&(index as u32))
            || port.installed_dict_keys.is_empty()
        {
            continue;
        }
        let keys: Vec<(u32, u32)> = port.installed_dict_keys.drain().collect();
        crate::exec::dict_traffic::note_stream_released(index as u32, &keys);
        for key in keys {
            dicts.remove(&key);
        }
        port.scope_remap.clear();
        port.installed_scopes.clear();
    }
}

/// Drop the dictionaries `port` installed for scopes other than `current`.
///
/// A scope is one Parquet row group, and a scan decodes its row groups in order, once each,
/// so no later frame on this stream references an earlier scope. Holding them until the
/// stream ended kept every row group's dictionaries: on full hits, ~100 MB of URL
/// dictionaries per reducer. Decoded batches hold their own `Arc` to the entries.
pub fn release_superseded_dicts(
    index: u32,
    port: &mut WcsPort,
    current: u32,
    dicts: &mut HashMap<(u32, u32), crate::stream::DictionaryValues>,
) {
    let stale: Vec<(u32, u32)> = port
        .installed_dict_keys
        .iter()
        .filter(|(scope, _)| *scope != current)
        .copied()
        .collect();
    if stale.is_empty() {
        return;
    }
    crate::exec::dict_traffic::note_stream_released(index, &stale);
    for key in &stale {
        port.installed_dict_keys.remove(key);
        dicts.remove(key);
    }
}

pub fn empty_ports(count: u32) -> Vec<WcsPort> {
    (0..count).map(|_| WcsPort::new()).collect()
}

pub fn feed_port(
    ports: &mut [WcsPort],
    pending: &mut VecDeque<(u32, CompleteFrame)>,
    port: u32,
    bytes: &[u8],
) -> WcolResult<usize> {
    let idx = port as usize;
    if idx >= ports.len() {
        return Err(WcolError::msg(format!("invalid WCS input port {port}")));
    }
    if ports[idx].transport_eof || bytes.is_empty() {
        return Ok(0);
    }
    if ports[idx].saw_end {
        return Err(WcolError::from("data after FRAME_END"));
    }
    let mid = ports[idx]
        .ingress
        .as_ref()
        .is_some_and(WcsIngress::incomplete);
    let waiting = pending.iter().any(|(queued, _)| *queued == port);
    if waiting && !mid {
        return Ok(0);
    }
    let used = pending_queue_bytes(pending)
        + ports
            .iter()
            .map(|port| {
                port.ingress
                    .as_ref()
                    .map(WcsIngress::buffered_bytes)
                    .unwrap_or(0)
            })
            .sum::<usize>();
    let budget_left = WCS_INGRESS_BUDGET_BYTES.saturating_sub(used);
    if !mid && budget_left < WCS_MAX_PAYLOAD {
        return Ok(0);
    }
    if ports[idx].ingress.is_none() {
        ports[idx].ingress = Some(WcsIngress::new());
    }
    let (consumed, frames) =
        ports[idx]
            .ingress
            .as_mut()
            .unwrap()
            .feed_limited(bytes, 1, budget_left)?;
    for frame in frames {
        if frame.frame_type == FRAME_END {
            if ports[idx].saw_end {
                return Err(WcolError::from("duplicate FRAME_END"));
            }
            ports[idx].saw_end = true;
            ports[idx].ingress = None;
        }
        pending.push_back((port, frame));
    }
    Ok(consumed)
}

pub fn end_port(ports: &mut [WcsPort], port: u32) -> WcolResult<()> {
    let idx = port as usize;
    if idx >= ports.len() {
        return Err(WcolError::msg(format!("invalid WCS input port {port}")));
    }
    let state = &mut ports[idx];
    if state.transport_eof {
        return Ok(());
    }
    if state.ingress.as_ref().is_some_and(WcsIngress::incomplete) {
        return Err(WcolError::from("incomplete WCS frame at EOF"));
    }
    if !state.saw_end {
        return Err(WcolError::from("transport EOF before FRAME_END"));
    }
    state.transport_eof = true;
    state.ingress = None;
    Ok(())
}

/// Coordinator finished this producer without opening a stream.
#[allow(dead_code)]
pub fn mark_absent(ports: &mut [WcsPort], port: u32) -> WcolResult<()> {
    let idx = port as usize;
    if idx >= ports.len() {
        return Err(WcolError::msg(format!("invalid WCS input port {port}")));
    }
    let state = &mut ports[idx];
    if state.transport_eof {
        return Ok(());
    }
    if state.ingress.is_some() || state.saw_end {
        return Err(WcolError::from("absent producer already sent a stream"));
    }
    state.saw_end = true;
    state.transport_eof = true;
    Ok(())
}

pub fn all_complete(ports: &[WcsPort]) -> bool {
    !ports.is_empty() && ports.iter().all(WcsPort::is_complete)
}

impl WcsPort {
    pub fn has_held(&self) -> bool {
        !self.held.is_empty()
    }

    pub fn take_held(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.held)
    }

    pub fn hold_suffix(&mut self, suffix: &[u8]) {
        self.held.extend_from_slice(suffix);
    }
}

pub fn pending_queue_bytes(pending: &VecDeque<(u32, CompleteFrame)>) -> usize {
    pending.iter().map(|(_, frame)| frame.payload.len()).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::wcs::FRAME_END;

    #[test]
    fn absent_producer_is_complete_without_a_frame() {
        let mut ports = empty_ports(1);
        assert!(end_port(&mut ports, 0).is_err());
        mark_absent(&mut ports, 0).unwrap();
        assert!(ports[0].is_complete());
        assert!(all_complete(&ports));
    }

    #[test]
    fn end_and_absent_fire_once() {
        let mut ports = empty_ports(2);
        mark_absent(&mut ports, 0).unwrap();
        assert!(mark_absent(&mut ports, 0).is_ok());
        let end_frame = {
            let mut frame = vec![0u8; 64];
            let n = crate::stream::wcs::encode_frame(FRAME_END, 0, &[], &mut frame).unwrap();
            frame.truncate(n);
            frame
        };
        let mut pending = VecDeque::new();
        feed_port(&mut ports, &mut pending, 1, &end_frame).unwrap();
        end_port(&mut ports, 1).unwrap();
        assert!(end_port(&mut ports, 1).is_ok());
    }

    #[test]
    fn pending_queue_bytes_tracks_frame_payloads() {
        let mut pending = VecDeque::new();
        pending.push_back((
            0,
            CompleteFrame {
                frame_type: 0,
                payload: vec![0u8; 1024],
            },
        ));
        assert_eq!(pending_queue_bytes(&pending), 1024);
    }

    #[test]
    fn one_chunk_of_two_frames_stops_when_the_port_queue_is_full() {
        let mut ports = empty_ports(1);
        let mut pending = VecDeque::new();
        let mut first = vec![0u8; 64];
        let n1 = crate::stream::wcs::encode_frame(
            crate::stream::wcs::FRAME_DATA_BATCH,
            0,
            b"one",
            &mut first,
        )
        .unwrap();
        let mut second = vec![0u8; 64];
        let n2 = crate::stream::wcs::encode_frame(FRAME_END, 1, b"two", &mut second).unwrap();
        let mut chunk = first[..n1].to_vec();
        chunk.extend_from_slice(&second[..n2]);
        let consumed = feed_port(&mut ports, &mut pending, 0, &chunk).unwrap();
        assert_eq!(consumed, n1);
        assert_eq!(pending.len(), 1);
        assert_eq!(
            feed_port(&mut ports, &mut pending, 0, &chunk[consumed..]).unwrap(),
            0
        );
        pending.pop_front();
        let rest = feed_port(&mut ports, &mut pending, 0, &chunk[consumed..]).unwrap();
        assert_eq!(rest, n2);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1.payload, b"two");
    }

    #[test]
    fn a_finished_stream_keeps_dictionaries_until_its_frames_are_decoded() {
        use crate::stream::{DictEntries, DictionaryValues};
        use std::sync::Arc;

        let mut ports = empty_ports(2);
        for port in &mut ports {
            port.saw_end = true;
            port.transport_eof = true;
        }
        ports[0].installed_dict_keys.insert((1, 2));
        ports[1].installed_dict_keys.insert((3, 4));
        let entry = || DictionaryValues {
            scope: 0,
            column: 0,
            entries: DictEntries::Int(Arc::new(vec![1])),
        };
        let mut dicts = HashMap::new();
        dicts.insert((1, 2), entry());
        dicts.insert((3, 4), entry());
        let mut pending = VecDeque::new();
        pending.push_back((
            1,
            CompleteFrame {
                frame_type: 0,
                payload: vec![1],
            },
        ));
        release_completed_stream_dicts(&mut ports, &pending, &mut dicts);
        assert!(!dicts.contains_key(&(1, 2)));
        assert!(dicts.contains_key(&(3, 4)));
        assert!(ports[1].installed_dict_keys.contains(&(3, 4)));
        pending.clear();
        release_completed_stream_dicts(&mut ports, &pending, &mut dicts);
        assert!(dicts.is_empty());
        assert!(ports[1].installed_dict_keys.is_empty());
    }

    #[test]
    fn a_new_scope_releases_only_this_ports_older_dictionaries() {
        use crate::stream::{DictEntries, DictionaryValues};
        use std::sync::Arc;

        let entry = || DictionaryValues {
            scope: 0,
            column: 0,
            entries: DictEntries::Int(Arc::new(vec![1])),
        };
        let mut ports = empty_ports(2);
        let mut dicts = HashMap::new();
        for key in [(1, 7), (1, 8), (2, 7)] {
            ports[0].installed_dict_keys.insert(key);
            dicts.insert(key, entry());
        }
        ports[1].installed_dict_keys.insert((5, 7));
        dicts.insert((5, 7), entry());

        release_superseded_dicts(0, &mut ports[0], 2, &mut dicts);

        assert_eq!(
            dicts.keys().copied().collect::<HashSet<_>>(),
            HashSet::from([(2, 7), (5, 7)])
        );
        assert_eq!(ports[0].installed_dict_keys, HashSet::from([(2, 7)]));
        assert!(ports[1].installed_dict_keys.contains(&(5, 7)));
    }
}
