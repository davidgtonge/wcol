//! A test host's answer to a scan's "next unit" ReadAt: the scan's own planned units in
//! order, then none, as its coordinator would answer with no other scan to steal from.

use std::collections::VecDeque;

use wcol::ops::parquet_source::{CLAIM_SOURCE, NO_UNIT};

pub struct Claims(VecDeque<u32>);

impl Claims {
    pub fn new(own_units: &[u32]) -> Self {
        Self(own_units.iter().copied().collect())
    }

    /// The four bytes to commit for a ReadAt on `source`, when it asks for a unit.
    pub fn answer(&mut self, source: u16) -> Option<[u8; 4]> {
        (source == CLAIM_SOURCE).then(|| self.0.pop_front().unwrap_or(NO_UNIT).to_le_bytes())
    }
}
